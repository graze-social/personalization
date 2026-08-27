//! Follow-graph seeds (`uf:{hash}`) for users the co-liker engine structurally cannot reach.
//!
//! **Phase A: this writes `uf:` keys and nothing else.** No read path consumes them, so it cannot
//! change what any user is served. Same shape as Phase A of the durable co-liker profiles, and for
//! the same reason — the write side can be validated on production data at zero serving risk.
//!
//! Why follows. Measured 2026-08-27 (`DESIGN-coverage-next-lever-2026-08.md`): `no_user_data` is
//! **98.1% of addressable non-personalization**, 100.0% in 22 of 24 hours. Of the users it turns
//! away, **73.6% have zero likes in 365 days** — and the engine seeds exclusively from likes, so
//! nothing built from like history reaches them (durable profiles cap out at 3.7% for exactly this
//! reason). But **86% of them follow 10+ accounts, median 50**. Follows are the one available seed
//! that does not require a like history.
//!
//! Cost is already measured and acceptable: 59 followed authors fan out to ~2,530 unique sources at
//! 354 Redis ops and 90 ms, against 6 authors / 410 sources / 40 ops for the like path. That runs
//! roughly once per user per hour behind `AUTHOR_AFFINITY_TTL_SECONDS`, not per request, and the
//! downstream scoring cost is bounded by the 500-post `total_scored` cap.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tracing::{debug, info, warn};

use graze_common::clickhouse::ClickHouseConfig;
use graze_common::{hash_did, Keys, RedisClient};

/// Users turned away with `no_user_data` — the exact cohort follows are meant to reach.
///
/// This query is only possible because `fallback_reason` reaches the provenance blob; before that
/// the reason existed solely in a log line and the cohort could not be selected at all.
const TARGET_QUERY: &str = r#"
SELECT DISTINCT did
FROM {database:Identifier}.feed_interactions
WHERE occurred >= {from:DateTime}
  AND interaction_feed_context != ''
  AND JSONHas(tryBase64Decode(interaction_feed_context), 'algo_id')
  AND JSONExtractString(tryBase64Decode(interaction_feed_context), 'fallback_reason') = 'no_user_data'
LIMIT {limit:UInt32}
FORMAT JSON
"#;

#[derive(Debug, Clone)]
pub struct FollowSeedConfig {
    /// Report what would be written, write nothing.
    pub dry_run: bool,
    pub appview_base: String,
    /// Follows kept per user. Matches `AUTHOR_AFFINITY_MAX_AUTHORS`, since the seed step caps
    /// there anyway — storing more would be dead weight.
    pub max_follows: usize,
    /// Below this a seed is not worth storing. 10 is the threshold the 86% figure was measured at.
    pub min_follows: usize,
    /// TTL on `uf:` keys. Deliberately long: follows are far more stable than likes, and the
    /// durable-profile post-mortem is the specification for getting this wrong — a 7-day TTL there
    /// expired silently and would have produced a null read as "the idea does not work".
    pub ttl_days: u32,
    /// Retry cadence for users whose fetch produced nothing usable.
    pub miss_ttl_hours: u32,
    pub max_users: usize,
    /// Politeness delay between AppView calls.
    pub request_delay_ms: u64,
    pub lookback_days: u32,
    pub query_timeout_secs: u64,
}

impl Default for FollowSeedConfig {
    fn default() -> Self {
        Self {
            dry_run: false,
            appview_base: "https://public.api.bsky.app".to_string(),
            max_follows: 100,
            min_follows: 10,
            ttl_days: 30,
            miss_ttl_hours: 24,
            max_users: 5_000,
            request_delay_ms: 150,
            lookback_days: 7,
            query_timeout_secs: 300,
        }
    }
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl FollowSeedConfig {
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            dry_run: std::env::var("FOLLOW_SEED_DRY_RUN")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(d.dry_run),
            appview_base: std::env::var("FOLLOW_SEED_APPVIEW_BASE").unwrap_or(d.appview_base),
            max_follows: env_u32("FOLLOW_SEED_MAX_FOLLOWS", d.max_follows as u32) as usize,
            min_follows: env_u32("FOLLOW_SEED_MIN_FOLLOWS", d.min_follows as u32) as usize,
            ttl_days: env_u32("FOLLOW_SEED_TTL_DAYS", d.ttl_days),
            miss_ttl_hours: env_u32("FOLLOW_SEED_MISS_TTL_HOURS", d.miss_ttl_hours),
            max_users: env_u32("FOLLOW_SEED_MAX_USERS", d.max_users as u32) as usize,
            request_delay_ms: env_u32("FOLLOW_SEED_REQUEST_DELAY_MS", d.request_delay_ms as u32)
                as u64,
            lookback_days: env_u32("FOLLOW_SEED_LOOKBACK_DAYS", d.lookback_days),
            query_timeout_secs: env_u32(
                "FOLLOW_SEED_QUERY_TIMEOUT_SECS",
                d.query_timeout_secs as u32,
            ) as u64,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FollowSeedStats {
    pub targets: usize,
    pub already_seeded: usize,
    pub already_missed: usize,
    pub fetched: usize,
    pub written: usize,
    pub too_few_follows: usize,
    pub fetch_errors: usize,
    pub follows_total: usize,
}

impl FollowSeedStats {
    pub fn mean_follows(&self) -> f64 {
        if self.written == 0 {
            0.0
        } else {
            self.follows_total as f64 / self.written as f64
        }
    }
}

#[derive(Deserialize)]
struct TargetRow {
    did: String,
}

#[derive(Deserialize)]
struct ChResponse {
    data: Vec<TargetRow>,
}

#[derive(Deserialize)]
struct FollowEntry {
    did: String,
}

#[derive(Deserialize)]
struct FollowsResponse {
    #[serde(default)]
    follows: Vec<FollowEntry>,
}

pub struct FollowSeedBuilder {
    clickhouse: Arc<ClickHouseConfig>,
    redis: Arc<RedisClient>,
    http: reqwest::Client,
    config: FollowSeedConfig,
}

impl FollowSeedBuilder {
    pub fn new(
        clickhouse: Arc<ClickHouseConfig>,
        redis: Arc<RedisClient>,
        config: FollowSeedConfig,
    ) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .user_agent("graze-personalization/follow-seeds")
            .build()?;
        Ok(Self {
            clickhouse,
            redis,
            http,
            config,
        })
    }

    /// DIDs turned away with `no_user_data` in the lookback window.
    pub async fn target_dids(&self) -> anyhow::Result<Vec<String>> {
        let from = (chrono::Utc::now()
            - chrono::Duration::days(self.config.lookback_days.max(1) as i64))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();

        let response = self
            .http
            .post(self.clickhouse.base_url())
            .basic_auth(&self.clickhouse.user, Some(&self.clickhouse.password))
            .header("Content-Type", "text/plain")
            .timeout(Duration::from_secs(self.config.query_timeout_secs))
            .query(&[
                (
                    "max_execution_time",
                    self.config.query_timeout_secs.to_string().as_str(),
                ),
                ("param_database", self.clickhouse.database.as_str()),
                ("param_from", from.as_str()),
                ("param_limit", &self.config.max_users.to_string()),
            ])
            .body(TARGET_QUERY)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "target query failed ({}): {}",
                status,
                &body[..body.len().min(600)]
            );
        }
        let parsed: ChResponse = response.json().await?;
        Ok(parsed.data.into_iter().map(|r| r.did).collect())
    }

    /// Followed DIDs for one actor, capped at `max_follows`.
    ///
    /// One page is enough by construction: the cap equals `AUTHOR_AFFINITY_MAX_AUTHORS`, which is
    /// also the AppView's page limit, so pagination would fetch authors the seed step discards.
    async fn fetch_follows(&self, did: &str) -> anyhow::Result<Vec<String>> {
        let url = format!(
            "{}/xrpc/app.bsky.graph.getFollows",
            self.config.appview_base.trim_end_matches('/')
        );
        let response = self
            .http
            .get(&url)
            .query(&[
                ("actor", did),
                ("limit", &self.config.max_follows.min(100).to_string()),
            ])
            .send()
            .await?;
        if !response.status().is_success() {
            anyhow::bail!("getFollows {} -> {}", did, response.status());
        }
        let parsed: FollowsResponse = response.json().await?;
        Ok(parsed
            .follows
            .into_iter()
            .map(|f| f.did)
            .take(self.config.max_follows)
            .collect())
    }

    pub async fn run(&self) -> anyhow::Result<FollowSeedStats> {
        let targets = self.target_dids().await?;
        let mut stats = FollowSeedStats {
            targets: targets.len(),
            ..Default::default()
        };
        info!(
            targets = stats.targets,
            dry_run = self.config.dry_run,
            "follow_seed_run_starting"
        );

        for did in targets {
            let hash = hash_did(&did);
            // Skip work we have already done. Checked per user rather than in bulk because the
            // AppView call dominates by orders of magnitude, so two Redis EXISTS are free here.
            if self.redis.exists(&Keys::user_follows(&hash)).await? {
                stats.already_seeded += 1;
                continue;
            }
            if self.redis.exists(&Keys::user_follows_miss(&hash)).await? {
                stats.already_missed += 1;
                continue;
            }

            let follows = match self.fetch_follows(&did).await {
                Ok(f) => {
                    stats.fetched += 1;
                    f
                }
                Err(e) => {
                    stats.fetch_errors += 1;
                    warn!(error = %e, "follow_seed_fetch_failed");
                    // Mark as a miss so a permanently broken account is not retried every run.
                    if !self.config.dry_run {
                        let _ = self
                            .redis
                            .set_ex(
                                &Keys::user_follows_miss(&hash),
                                "error",
                                self.config.miss_ttl_hours as u64 * 3600,
                            )
                            .await;
                    }
                    tokio::time::sleep(Duration::from_millis(self.config.request_delay_ms)).await;
                    continue;
                }
            };

            if follows.len() < self.config.min_follows {
                stats.too_few_follows += 1;
                if !self.config.dry_run {
                    self.redis
                        .set_ex(
                            &Keys::user_follows_miss(&hash),
                            "too_few",
                            self.config.miss_ttl_hours as u64 * 3600,
                        )
                        .await?;
                }
            } else {
                let author_hashes: Vec<String> = follows.iter().map(|d| hash_did(d)).collect();
                stats.written += 1;
                stats.follows_total += author_hashes.len();
                if !self.config.dry_run {
                    let items: Vec<(f64, &str)> =
                        author_hashes.iter().map(|h| (1.0, h.as_str())).collect();
                    let key = Keys::user_follows(&hash);
                    self.redis.zadd(&key, &items).await?;
                    self.redis
                        .expire(&key, self.config.ttl_days as i64 * 86_400)
                        .await?;
                }
                debug!(follows = author_hashes.len(), "follow_seed_written");
            }

            tokio::time::sleep(Duration::from_millis(self.config.request_delay_ms)).await;
        }

        info!(
            targets = stats.targets,
            written = stats.written,
            mean_follows = stats.mean_follows(),
            already_seeded = stats.already_seeded,
            already_missed = stats.already_missed,
            too_few_follows = stats.too_few_follows,
            fetch_errors = stats.fetch_errors,
            dry_run = self.config.dry_run,
            "follow_seed_run_complete"
        );
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mean_follows_is_zero_rather_than_nan_when_nothing_was_written() {
        let s = FollowSeedStats::default();
        assert_eq!(s.mean_follows(), 0.0);
    }

    #[test]
    fn mean_follows_averages_over_written_users_only() {
        let s = FollowSeedStats {
            written: 4,
            follows_total: 200,
            too_few_follows: 99,
            ..Default::default()
        };
        assert_eq!(s.mean_follows(), 50.0);
    }

    /// The default TTL must outlive an experiment horizon. A 7-day TTL is what silently expired the
    /// durable co-liker profiles between 8/18 and 8/27, leaving a flag that would have served
    /// nothing and read as a refutation.
    #[test]
    fn default_ttl_outlives_an_experiment() {
        let d = FollowSeedConfig::default();
        assert!(
            d.ttl_days >= 30,
            "TTL {} days is too short to survive an experiment",
            d.ttl_days
        );
        assert!(d.miss_ttl_hours <= 48, "misses must be retried promptly");
    }

    /// Storing more follows than the seed step reads would be dead weight.
    #[test]
    fn max_follows_matches_the_author_affinity_cap() {
        assert_eq!(FollowSeedConfig::default().max_follows, 100);
    }

    #[test]
    fn min_follows_matches_the_measured_threshold() {
        // 86% of the unreachable cohort has >=10 follows; that is the threshold measured.
        assert_eq!(FollowSeedConfig::default().min_follows, 10);
    }
}
