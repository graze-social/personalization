//! Offline builder for durable co-liker taste profiles (`ucl:{hash}`).
//!
//! # What this solves
//!
//! The live co-liker derivation seeds from `ul:`, which retains 6 days. A user who liked
//! 200 posts last month and nothing this week therefore derives **zero** co-likers and
//! gets 100% fallback. Measured on production: of 138,745 feed requesters in 3 days,
//! 13,196 have zero likes in the 6-day window but ≥20 in history, against 21,597 who are
//! currently personalizable — a 61% larger addressable population.
//!
//! Long-range history lives in ClickHouse `user_action_logs` (`action_type =
//! 'app.bsky.feed.like'`: ~141M likes across ~986k users, years deep). This job walks
//! that history, derives each user's top-K co-likers, and stores them as a durable
//! profile. Only the *seed* and *co-liker discovery* need history — the candidates being
//! ranked still come from the live `pl:`/`ap:` graph — so a stored co-liker set is all
//! that is required.
//!
//! For one sampled lurker this took scoreable candidates from **0 to 4,736 of algo 396's
//! 10,473-post pool**, versus ~1,700–1,860 for a typical active user.
//!
//! # Why offline
//!
//! The co-liker self-join full-scans ~141M rows — impossible per request. It also does
//! not fit in one query: a naive single-shot OOMed at 18 GiB with 2,000 users. Two bounds
//! fix that and *improve* the signal, since both discard low-specificity mass:
//!
//! - seed capped to the most recent `max_seed_posts` liked posts per user;
//! - seed posts with more than `max_seed_post_likers` likers dropped — a 5,000-liker post
//!   contributes `1/5000` to `Σ 1/L_j` while dragging 5,000 rows into the join.
//!
//! Measured after those bounds: 2,000 users in 3m50s and 8,000 users in 4m50s (4× the
//! users for 1.26× the time — the scan is a fixed cost, so prefer large chunks), with
//! profiles still averaging ~124 of a 128 cap.
//!
//! # Phase A scope
//!
//! This writes `ucl:` keys and nothing else. No read path, no serving change. See
//! `DESIGN-durable-coliker-profiles.md`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, info, warn};

use graze_common::clickhouse::ClickHouseConfig;
use graze_common::{encode_profile_from_dids, hash_did, Keys, RedisClient};

/// Tuning for the profile builder. Defaults match the measured design points.
#[derive(Debug, Clone)]
pub struct ProfileBuilderConfig {
    /// How far back to read like history for the seed.
    pub history_days: u32,
    /// Only build profiles for users who requested a feed within this window.
    pub requester_window_days: u32,
    /// Minimum lifetime likes to bother building a profile.
    pub min_history_likes: u32,
    /// Upper bound excluding hyper-likers. `bot:filtered` holds only 13 users because
    /// `BOT_LIKE_THRESHOLD=5000`/6d ≈ 833/day, so it cannot be relied on; sampling found
    /// real accounts at 216,073 likes/year. 959 users sit above this bound.
    pub max_history_likes: u32,
    /// Most recent liked posts used as seed, per user.
    pub max_seed_posts: u32,
    /// Drop seed posts with more likers than this (viral-post filter).
    pub max_seed_post_likers: u32,
    /// Profile size cap. 128 keeps a ZSET listpack-encoded and the packed form at 1,536
    /// bytes; measured coverage at 128 already exceeds what active users get.
    pub max_colikers: u32,
    /// Skip writing profiles smaller than this — a 1-member profile yields near-zero
    /// coverage and gives `overlap_count` nothing to discriminate on.
    pub min_profile_size: usize,
    /// Number of `cityHash64(user_did) % n` buckets to split the job into.
    pub chunk_count: u32,
    /// Only build this single bucket, if set. Otherwise all buckets run in sequence.
    pub only_bucket: Option<u32>,
    /// TTL on `ucl:` keys — long enough to survive several failed nightly runs.
    pub profile_ttl_days: u32,
    /// Redis writes per pipeline.
    pub write_batch: usize,
    /// Per-chunk ClickHouse timeout.
    pub query_timeout_secs: u64,
    /// Compute and report, but write nothing.
    pub dry_run: bool,
}

impl Default for ProfileBuilderConfig {
    fn default() -> Self {
        Self {
            history_days: 365,
            requester_window_days: 30,
            min_history_likes: 20,
            max_history_likes: 5_000,
            max_seed_posts: 128,
            max_seed_post_likers: 500,
            max_colikers: 128,
            min_profile_size: 10,
            chunk_count: 8,
            only_bucket: None,
            profile_ttl_days: 7,
            write_batch: 500,
            query_timeout_secs: 1_800,
            dry_run: false,
        }
    }
}

impl ProfileBuilderConfig {
    /// Read overrides from the environment, falling back to [`Default`].
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            history_days: env_u32("PROFILE_HISTORY_DAYS", d.history_days),
            requester_window_days: env_u32(
                "PROFILE_REQUESTER_WINDOW_DAYS",
                d.requester_window_days,
            ),
            min_history_likes: env_u32("PROFILE_MIN_HISTORY_LIKES", d.min_history_likes),
            max_history_likes: env_u32("PROFILE_MAX_HISTORY_LIKES", d.max_history_likes),
            max_seed_posts: env_u32("PROFILE_MAX_SEED_POSTS", d.max_seed_posts),
            max_seed_post_likers: env_u32("PROFILE_MAX_SEED_POST_LIKERS", d.max_seed_post_likers),
            max_colikers: env_u32("PROFILE_MAX_COLIKERS", d.max_colikers),
            min_profile_size: env_u32("PROFILE_MIN_SIZE", d.min_profile_size as u32) as usize,
            chunk_count: env_u32("PROFILE_CHUNK_COUNT", d.chunk_count).max(1),
            only_bucket: std::env::var("PROFILE_ONLY_BUCKET")
                .ok()
                .and_then(|s| s.parse().ok()),
            profile_ttl_days: env_u32("PROFILE_TTL_DAYS", d.profile_ttl_days),
            write_batch: env_u32("PROFILE_WRITE_BATCH", d.write_batch as u32) as usize,
            query_timeout_secs: env_u32("PROFILE_QUERY_TIMEOUT_SECS", d.query_timeout_secs as u32)
                as u64,
            dry_run: std::env::var("PROFILE_DRY_RUN")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(d.dry_run),
        }
    }
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Totals across every chunk.
#[derive(Debug, Default, Clone)]
pub struct BuildStats {
    pub chunks_run: u32,
    pub chunks_failed: u32,
    pub users_returned: usize,
    pub profiles_written: usize,
    pub skipped_too_small: usize,
    pub entries_written: usize,
    pub bytes_written: usize,
}

impl BuildStats {
    fn merge(&mut self, other: &ChunkStats) {
        self.users_returned += other.users_returned;
        self.profiles_written += other.profiles_written;
        self.skipped_too_small += other.skipped_too_small;
        self.entries_written += other.entries_written;
        self.bytes_written += other.bytes_written;
    }

    /// Mean profile size across everything written.
    pub fn mean_profile_size(&self) -> f64 {
        if self.profiles_written == 0 {
            0.0
        } else {
            self.entries_written as f64 / self.profiles_written as f64
        }
    }
}

/// Per-chunk counters.
#[derive(Debug, Default, Clone)]
pub struct ChunkStats {
    pub users_returned: usize,
    pub profiles_written: usize,
    pub skipped_too_small: usize,
    pub entries_written: usize,
    pub bytes_written: usize,
}

/// One row of the profile query: user DID, co-liker DIDs, and their scores.
type ProfileRow = (String, Vec<String>, Vec<f64>);

#[derive(Deserialize)]
struct ChResponse {
    data: Vec<ProfileRow>,
}

pub struct ProfileBuilder {
    http: Client,
    clickhouse: Arc<ClickHouseConfig>,
    redis: Arc<RedisClient>,
    config: ProfileBuilderConfig,
}

impl ProfileBuilder {
    pub fn new(
        clickhouse: Arc<ClickHouseConfig>,
        redis: Arc<RedisClient>,
        config: ProfileBuilderConfig,
    ) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(config.query_timeout_secs))
            .build()
            .expect("HTTP client");
        Self {
            http,
            clickhouse,
            redis,
            config,
        }
    }

    /// Build every configured bucket in sequence.
    ///
    /// Buckets are independent and stable across runs (`cityHash64(user_did) % n`), so a
    /// failed bucket can be retried on its own via `PROFILE_ONLY_BUCKET` without redoing
    /// the rest. A bucket failure is logged and skipped rather than aborting the run.
    pub async fn run(&self) -> anyhow::Result<BuildStats> {
        let started = Instant::now();
        let mut stats = BuildStats::default();

        let buckets: Vec<u32> = match self.config.only_bucket {
            Some(b) => vec![b],
            None => (0..self.config.chunk_count).collect(),
        };

        info!(
            buckets = buckets.len(),
            chunk_count = self.config.chunk_count,
            history_days = self.config.history_days,
            max_colikers = self.config.max_colikers,
            min_profile_size = self.config.min_profile_size,
            dry_run = self.config.dry_run,
            "coliker_profile_build_starting"
        );

        for bucket in buckets {
            let t0 = Instant::now();
            match self.run_chunk(bucket).await {
                Ok(chunk) => {
                    stats.chunks_run += 1;
                    stats.merge(&chunk);
                    info!(
                        bucket,
                        users_returned = chunk.users_returned,
                        profiles_written = chunk.profiles_written,
                        skipped_too_small = chunk.skipped_too_small,
                        entries_written = chunk.entries_written,
                        bytes_written = chunk.bytes_written,
                        elapsed_secs = t0.elapsed().as_secs(),
                        "coliker_profile_chunk_complete"
                    );
                }
                Err(e) => {
                    stats.chunks_failed += 1;
                    warn!(
                        bucket,
                        elapsed_secs = t0.elapsed().as_secs(),
                        error = %e,
                        "coliker_profile_chunk_failed"
                    );
                }
            }
        }

        info!(
            chunks_run = stats.chunks_run,
            chunks_failed = stats.chunks_failed,
            users_returned = stats.users_returned,
            profiles_written = stats.profiles_written,
            skipped_too_small = stats.skipped_too_small,
            mean_profile_size = stats.mean_profile_size(),
            bytes_written = stats.bytes_written,
            elapsed_secs = started.elapsed().as_secs(),
            "coliker_profile_build_complete"
        );

        Ok(stats)
    }

    async fn run_chunk(&self, bucket: u32) -> anyhow::Result<ChunkStats> {
        let rows = self.fetch_chunk(bucket).await?;
        let mut stats = ChunkStats {
            users_returned: rows.len(),
            ..Default::default()
        };

        let ttl_secs = self.config.profile_ttl_days as u64 * 24 * 60 * 60;
        let mut pending: Vec<(String, Vec<u8>)> = Vec::with_capacity(self.config.write_batch);

        for (user_did, coliker_dids, scores) in rows {
            // Zip and sort descending here rather than trusting groupArray() to preserve
            // the subquery's ORDER BY under parallel execution.
            let mut entries: Vec<(String, f64)> = coliker_dids
                .into_iter()
                .zip(scores)
                .filter(|(did, score)| !did.is_empty() && score.is_finite() && *score > 0.0)
                .collect();
            entries.sort_unstable_by(|a, b| b.1.total_cmp(&a.1));
            entries.truncate(self.config.max_colikers as usize);

            if entries.len() < self.config.min_profile_size {
                stats.skipped_too_small += 1;
                continue;
            }

            let packed = encode_profile_from_dids(&entries);
            if packed.is_empty() {
                stats.skipped_too_small += 1;
                continue;
            }

            stats.profiles_written += 1;
            stats.entries_written += entries.len();
            stats.bytes_written += packed.len();

            if !self.config.dry_run {
                pending.push((Keys::user_colikers(&hash_did(&user_did)), packed));
                if pending.len() >= self.config.write_batch {
                    self.redis.set_ex_bytes_multi(&pending, ttl_secs).await?;
                    pending.clear();
                }
            }
        }

        if !pending.is_empty() {
            self.redis.set_ex_bytes_multi(&pending, ttl_secs).await?;
        }

        Ok(stats)
    }

    async fn fetch_chunk(&self, bucket: u32) -> anyhow::Result<Vec<ProfileRow>> {
        // Bound both windows client-side. `user_action_logs.action_time` for
        // `app.bsky.feed.like` spans 2017→2038 (bad TIDs and clock skew), so every window
        // needs an upper bound as well as a lower one.
        let now = Utc::now();
        let history_from = (now - chrono::Duration::days(self.config.history_days as i64))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let requester_from = (now
            - chrono::Duration::days(self.config.requester_window_days as i64))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
        let now_str = now.format("%Y-%m-%d %H:%M:%S").to_string();

        debug!(
            bucket,
            history_from = %history_from,
            requester_from = %requester_from,
            "coliker_profile_query_starting"
        );

        let response = self
            .http
            .post(self.clickhouse.base_url())
            .basic_auth(&self.clickhouse.user, Some(&self.clickhouse.password))
            .header("Content-Type", "text/plain")
            .query(&[
                ("enable_http_compression", "1"),
                (
                    "max_execution_time",
                    &self.config.query_timeout_secs.to_string(),
                ),
                ("join_algorithm", "parallel_hash"),
                ("param_database", self.clickhouse.database.as_str()),
                ("param_history_from", &history_from),
                ("param_requester_from", &requester_from),
                ("param_now", &now_str),
                ("param_chunk_count", &self.config.chunk_count.to_string()),
                ("param_bucket", &bucket.to_string()),
                (
                    "param_min_history",
                    &self.config.min_history_likes.to_string(),
                ),
                (
                    "param_max_history",
                    &self.config.max_history_likes.to_string(),
                ),
                ("param_max_seed", &self.config.max_seed_posts.to_string()),
                (
                    "param_max_post_likers",
                    &self.config.max_seed_post_likers.to_string(),
                ),
                ("param_max_colikers", &self.config.max_colikers.to_string()),
            ])
            .body(PROFILE_QUERY)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "ClickHouse profile query failed ({}): {}",
                status,
                &body[..body.len().min(600)]
            );
        }

        let parsed: ChResponse = response.json().await?;
        Ok(parsed.data)
    }
}

/// The profile query.
///
/// Shape and bounds are the ones validated against production — see the module docs for
/// the OOM this structure avoids. `LIMIT n BY u` does top-K per user server-side so
/// nothing oversized is ever materialised client-side, and folding `lj` into `seed`
/// (rather than joining it again in the outer query) removes one of the two chained joins
/// that caused the original blow-up.
const PROFILE_QUERY: &str = r#"
WITH targets AS (
    SELECT user_did
    FROM (
        SELECT user_did, count() AS c
        FROM {database:Identifier}.user_action_logs
        WHERE action_type = 'app.bsky.feed.like'
          AND action_time >= {history_from:DateTime}
          AND action_time <= {now:DateTime}
          AND cityHash64(user_did) % {chunk_count:UInt32} = {bucket:UInt32}
        GROUP BY user_did
        HAVING c >= {min_history:UInt32} AND c <= {max_history:UInt32}
    )
    WHERE user_did IN (
        SELECT DISTINCT user_did
        FROM {database:Identifier}.user_action_logs
        WHERE action_type = 'app.bsky.feed.defs#interactionSeen'
          AND action_time >= {requester_from:DateTime}
          AND action_time <= {now:DateTime}
    )
),
seed0 AS (
    SELECT user_did AS u, action_identifier AS post, min(action_time) AS t
    FROM {database:Identifier}.user_action_logs
    WHERE action_type = 'app.bsky.feed.like'
      AND action_time >= {history_from:DateTime}
      AND action_time <= {now:DateTime}
      AND user_did IN (SELECT user_did FROM targets)
    GROUP BY u, post
    ORDER BY u, t DESC
    LIMIT {max_seed:UInt32} BY u
),
lj AS (
    SELECT action_identifier AS post, count() AS l
    FROM {database:Identifier}.user_action_logs
    WHERE action_type = 'app.bsky.feed.like'
      AND action_identifier IN (SELECT DISTINCT post FROM seed0)
    GROUP BY post
    HAVING l <= {max_post_likers:UInt32}
),
seed AS (
    SELECT s.u AS u, s.post AS post, s.t AS t, j.l AS l
    FROM seed0 s
    INNER JOIN lj j ON j.post = s.post
)
SELECT u, groupArray(cl) AS cls, groupArray(sc) AS scs
FROM (
    SELECT s.u AS u, o.user_did AS cl, sum(1.0 / s.l) AS sc
    FROM {database:Identifier}.user_action_logs o
    INNER JOIN seed s ON o.action_identifier = s.post
    WHERE o.action_type = 'app.bsky.feed.like'
      AND o.user_did != s.u
      AND o.action_time < s.t
    GROUP BY u, cl
    ORDER BY u, sc DESC
    LIMIT {max_colikers:UInt32} BY u
)
GROUP BY u
FORMAT JSONCompact
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_measured_design_points() {
        let c = ProfileBuilderConfig::default();
        // 128 is the ZSET listpack boundary and the validated coverage point.
        assert_eq!(c.max_colikers, 128);
        assert_eq!(c.max_seed_posts, 128);
        // The viral-post filter is what keeps the join inside the memory limit.
        assert_eq!(c.max_seed_post_likers, 500);
        // Excludes the 959 hyper-likers that the inert bot filter misses.
        assert_eq!(c.max_history_likes, 5_000);
    }

    #[test]
    fn ttl_outlives_several_failed_nightly_runs() {
        let c = ProfileBuilderConfig::default();
        assert!(c.profile_ttl_days >= 3, "ttl too tight for a nightly job");
    }

    #[test]
    fn query_bounds_every_time_window_on_both_sides() {
        // action_time spans 2017..2038 in production; an unbounded upper edge would pull
        // in garbage timestamps.
        assert_eq!(PROFILE_QUERY.matches("{now:DateTime}").count(), 3);
        assert!(PROFILE_QUERY.contains("{history_from:DateTime}"));
        assert!(PROFILE_QUERY.contains("{requester_from:DateTime}"));
    }

    #[test]
    fn query_applies_both_cost_bounds() {
        assert!(PROFILE_QUERY.contains("LIMIT {max_seed:UInt32} BY u"));
        assert!(PROFILE_QUERY.contains("HAVING l <= {max_post_likers:UInt32}"));
        assert!(PROFILE_QUERY.contains("LIMIT {max_colikers:UInt32} BY u"));
    }

    #[test]
    fn query_excludes_self_and_respects_liked_before_me() {
        assert!(PROFILE_QUERY.contains("o.user_did != s.u"));
        assert!(PROFILE_QUERY.contains("o.action_time < s.t"));
    }

    #[test]
    fn stats_mean_is_zero_without_writes() {
        let s = BuildStats::default();
        assert_eq!(s.mean_profile_size(), 0.0);
    }

    #[test]
    fn stats_mean_tracks_entries_per_profile() {
        let mut s = BuildStats::default();
        s.merge(&ChunkStats {
            users_returned: 10,
            profiles_written: 4,
            skipped_too_small: 6,
            entries_written: 496,
            bytes_written: 5_952,
        });
        assert_eq!(s.mean_profile_size(), 124.0);
        assert_eq!(s.users_returned, 10);
    }
}
