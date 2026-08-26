//! Python-style scorer for LinkLonk algorithm.
//!
//! This module implements the inverted algorithm scoring, using Redis
//! purely as a data store. This reduces Redis CPU load significantly
//! since Lua scripts block Redis's single thread.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use rand::seq::SliceRandom;
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::debug;

use crate::algorithm::liker_cache::LikerCache;
use crate::algorithm::params::LinkLonkParams;
use crate::audit::{AuditCollector, PostBreakdownData};
use rand::Rng;

use crate::algorithm::walk;
use crate::config::Config;
use crate::error::Result;
use graze_common::{Keys, RedisClient, DEFAULT_RETENTION_DAYS};

/// Maximum results to return from scorer (early termination optimization).
const DEFAULT_MAX_SCORER_RESULTS: usize = 500;

/// Overlap-count histogram buckets: `1, 2, 3-4, 5-8, 9-16, 17-32, 33-64, 65+`.
pub const OVERLAP_BUCKETS: usize = 8;

/// Bucket an `overlap_count` for the [`ScoringResult::overlap_hist`] histogram.
#[inline]
fn overlap_bucket(n: usize) -> usize {
    match n {
        0..=1 => 0,
        2 => 1,
        3..=4 => 2,
        5..=8 => 3,
        9..=16 => 4,
        17..=32 => 5,
        33..=64 => 6,
        _ => 7,
    }
}

/// Result of scoring operation, including the scored posts themselves.
#[derive(Debug, Clone, Default)]
pub struct ScoringResult {
    /// The scored posts: (score, post_id)
    pub scored_posts: Vec<(f64, String)>,
    pub scored_count: usize,
    pub posts_checked: usize,
    pub posts_skipped_no_likers: usize,
    pub posts_skipped_few_likers: usize,
    pub scoring_time_ms: f64,
    /// Cache statistics for this scoring run
    pub cache_hits: usize,
    pub cache_misses: usize,
    /// Candidates dropped by `min_overlapping_colikers`. Previously invisible, and needed
    /// to tell "no reachable candidates" apart from "reachable but too thinly overlapped".
    pub posts_skipped_low_overlap: usize,
    /// Sum of `overlap_count` over scored posts. Use [`Self::overlap_mean`] rather than
    /// dividing by `scored_count`, which is truncated to `DEFAULT_MAX_SCORER_RESULTS`.
    pub overlap_sum: usize,
    /// Largest `overlap_count` seen on a scored post.
    pub overlap_max: usize,
    /// True when this result is an interleaved draft, so the order carries the ranker
    /// assignment and **must not** be re-sorted downstream.
    ///
    /// Enforced rather than merely configured: forgetting `DIVERSITY_PRESERVE_ORDER` would let
    /// diversity re-sort by adjusted score, scrambling attribution and silently invalidating the
    /// experiment rather than producing an obvious failure.
    pub requires_preserved_order: bool,
    /// Which ranker produced each scored post, keyed by post id.
    ///
    /// A map rather than a parallel vector deliberately: diversity filters and reorders the
    /// scored list, and a map survives both without needing to be threaded through every
    /// transformation. Empty outside interleaving experiments.
    pub ranker_by_post: std::collections::HashMap<String, String>,
    /// Distribution of `overlap_count` over scored posts. `overlap_count` feeds a
    /// *nonlinear* `paths_boost = overlap_count^num_paths_power`, so a durable profile
    /// (~128 co-likers) versus a live one (~127 but drawn from 6 days) can shift ranking
    /// shape rather than merely rescale it. This histogram is what makes that visible.
    pub overlap_hist: [u32; OVERLAP_BUCKETS],
    /// Fraction of the co-liker seed retained by per-feed author faceting, when that ranker ran.
    ///
    /// Carried on the result so a readout can tell a *ranking* effect apart from a *coverage*
    /// collapse: a keep rate near zero means the treatment was effectively "no personalization",
    /// which would otherwise look like a ranking finding.
    pub seed_keep_rate: Option<f64>,
}

impl ScoringResult {
    /// Number of posts the overlap stats were gathered over.
    ///
    /// This is the count *before* `post_scores` is truncated to
    /// `DEFAULT_MAX_SCORER_RESULTS`, so it can exceed `scored_count`. Deriving it from the
    /// histogram keeps [`Self::overlap_mean`] exact without a redundant counter.
    pub fn overlap_observed(&self) -> usize {
        self.overlap_hist.iter().map(|c| *c as usize).sum()
    }

    /// Mean `overlap_count` across posts that cleared the overlap gate and scored.
    pub fn overlap_mean(&self) -> f64 {
        let observed = self.overlap_observed();
        if observed == 0 {
            0.0
        } else {
            self.overlap_sum as f64 / observed as f64
        }
    }

    /// Histogram rendered for structured logs, e.g. `1:4,2:9,3-4:2`.
    pub fn overlap_hist_str(&self) -> String {
        const LABELS: [&str; OVERLAP_BUCKETS] =
            ["1", "2", "3-4", "5-8", "9-16", "17-32", "33-64", "65+"];
        self.overlap_hist
            .iter()
            .enumerate()
            .filter(|(_, c)| **c > 0)
            .map(|(i, c)| format!("{}:{}", LABELS[i], c))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Scorer for the inverted LinkLonk algorithm.
///
/// This replaces Lua script execution with Rust-side scoring,
/// using Redis purely for data storage.
pub struct Scorer {
    redis: Arc<RedisClient>,
    liker_cache: Arc<LikerCache>,
    min_post_likes: usize,
    max_likers_per_post: usize,
    max_posts_to_score: usize,
    max_coliker_weight: f64,
    min_overlapping_colikers: usize,
    read_only: bool,
    liker_cache_enabled: bool,
    inverted_coliker_like_days: u32,
    inverted_coliker_like_limit: usize,
    /// Sampled-walk knobs. Flattened like the rest so the hot path never dereferences the config.
    walk_count: usize,
    walk_max_users: usize,
    walk_user_like_limit: usize,
    walk_early_stop_np: usize,
    walk_early_stop_nv: u32,
    walk_popularity_power: f64,
    walk_min_visits: u32,
    walk_min_seed_for_sampling: usize,
}

impl Scorer {
    /// Create a new scorer.
    pub fn new(redis: Arc<RedisClient>, liker_cache: Arc<LikerCache>, config: Arc<Config>) -> Self {
        Self {
            redis,
            liker_cache,
            min_post_likes: config.inverted_min_post_likes,
            max_likers_per_post: config.inverted_max_likers_per_post,
            max_posts_to_score: config.inverted_max_posts_to_score,
            max_coliker_weight: config.max_coliker_weight,
            min_overlapping_colikers: config.min_overlapping_colikers,
            read_only: config.read_only_mode,
            liker_cache_enabled: config.liker_cache_enabled,
            inverted_coliker_like_days: config.inverted_coliker_like_days,
            inverted_coliker_like_limit: config.inverted_coliker_like_limit,
            walk_count: config.walk_count,
            walk_max_users: config.walk_max_users,
            walk_user_like_limit: config.walk_user_like_limit,
            walk_early_stop_np: config.walk_early_stop_np,
            walk_early_stop_nv: config.walk_early_stop_nv,
            walk_popularity_power: config.walk_popularity_power,
            walk_min_visits: config.walk_min_visits,
            walk_min_seed_for_sampling: config.walk_min_seed_for_sampling,
        }
    }

    /// Score by walking the like graph **co-liker-first** instead of post-first.
    ///
    /// # Why
    ///
    /// [`Self::score`] iterates candidates and fetches `pl:{post}` for each one. That costs one
    /// key per candidate — with `INVERTED_MAX_POSTS_TO_SCORE = 0` (unlimited) that measured
    /// **~118,000 Redis ops for a single request** on algo 2323 (19,667 candidates). It also
    /// takes only the `max_likers_per_post` *most recent* likers of each post, so a co-liker who
    /// liked it earlier is invisible: **72–100% of the candidates that actually overlap have more
    /// than 30 likers**, and modelling `min(1, 30/L)` reproduced production almost exactly.
    ///
    /// This method inverts the traversal: for each of the user's ~128 co-likers, read their
    /// recent likes from `ul:{coliker}` and intersect with the pool. Cost scales with the number
    /// of **co-likers** rather than candidates, and one `ZREVRANGEBYSCORE` returns 500 members as
    /// cheaply as 30 — so the per-post truncation bias disappears entirely.
    ///
    /// Measured on 10 live seeded users against algo 2323: **1.88× the scoreable coverage** for
    /// **810–3,570 ops instead of ~118,000 (33–145× fewer)**.
    ///
    /// # What replaces the old bias
    ///
    /// Truncation moves from "the N most recent likers of each post" to "the N most recent likes
    /// of each co-liker" (`inverted_coliker_like_limit`). That is far weaker: the median seeded
    /// user has only ~16 likes in the window so it rarely binds, it is one cap per *person*
    /// rather than per *post* so it cannot systematically hide old likes on popular content, and
    /// because ops do not scale with the limit it can be set generously for free.
    ///
    /// # Counter semantics (for comparing arms)
    ///
    /// `scored_count` is directly comparable to [`Self::score`]. `posts_checked` is the pool size
    /// after exclusions, as there. `posts_skipped_no_likers` becomes "pool posts no co-liker
    /// reached" — the analogous dead-end, though not the identical measurement. Cache counters are
    /// unused: this path is not backed by the post-keyed `LikerCache` (a co-liker-keyed cache is a
    /// follow-up; the op count is already small enough that it was not worth the risk in v1).
    /// The configured seed floor below which sampling is not worth its variance.
    pub fn walk_min_seed_for_sampling(&self) -> usize {
        self.walk_min_seed_for_sampling
    }

    /// Whether the user has at least `floor` likes in the scoring window.
    ///
    /// Deliberately a `ZCARD`-style existence check rather than a full fetch: the caller only needs to
    /// choose a strategy, and paying for the seed twice would hand back the latency sampling is meant
    /// to save.
    pub async fn seed_size_at_least(
        &self,
        user_hash: &str,
        params: &LinkLonkParams,
        floor: usize,
    ) -> Result<bool> {
        if floor == 0 {
            return Ok(true);
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let min_time = now - params.time_window_hours * 3600.0;
        let keys = Keys::user_likes_retention(user_hash, DEFAULT_RETENTION_DAYS);
        let seed = self
            .redis
            .zrevrangebyscore_merged(&keys, now, min_time, floor)
            .await?;
        Ok(seed.len() >= floor)
    }

    /// Score by sampled random walk over the like graph.
    ///
    /// See `algorithm/walk.rs` for why sampling escapes the cost/bias dilemma that both enumerative
    /// paths hit. Two batched phases rather than a round trip per step:
    ///
    /// 1. Fetch liker lists for the user's seed posts, and sample `(seed, co-liker)` pairs from them
    ///    with the walk budget allocated by seed degree.
    /// 2. Fetch the sampled co-likers' like lists in one pipeline, and tally visits that land in the
    ///    feed pool.
    ///
    /// Ignores `source_weights` entirely — the walk *is* the derivation, so there is nothing to reuse
    /// from the co-liker path. Deterministic per (user, day), like every other sampling site, so the
    /// interleaving harness's null stays null.
    pub async fn score_sampled_walk(
        &self,
        user_hash: &str,
        algo_id: i32,
        params: &LinkLonkParams,
    ) -> Result<ScoringResult> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let min_time = now - params.time_window_hours * 3600.0;

        let algo_posts_key = Keys::algo_posts(algo_id);
        let algo_counts_key = Keys::algo_posts_counts(algo_id);
        let user_likes_keys = Keys::user_likes_retention(user_hash, DEFAULT_RETENTION_DAYS);
        let user_seen_key = Keys::user_seen(user_hash);
        let seen_limit = if params.max_seen_posts > 0 {
            (params.max_seen_posts as isize) - 1
        } else {
            -1
        };

        let (algo_posts_result, user_likes_result, seen_posts_result) = tokio::join!(
            self.redis.smembers(&algo_posts_key),
            self.redis.zrevrangebyscore_merged(
                &user_likes_keys,
                now,
                min_time,
                params.max_user_likes
            ),
            self.redis.zrevrange(&user_seen_key, 0, seen_limit)
        );

        let algo_posts: Vec<String> = algo_posts_result?;
        let seed: Vec<(String, f64)> = user_likes_result?;
        if algo_posts.is_empty() || seed.is_empty() {
            return Ok(ScoringResult {
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        let mut excluded: FxHashSet<String> = seed.iter().map(|(id, _)| id.clone()).collect();
        excluded.extend(seen_posts_result.unwrap_or_default());
        let pool: FxHashSet<&str> = algo_posts
            .iter()
            .map(|s| s.as_str())
            .filter(|p| !excluded.contains(*p))
            .collect();
        let pool_size = pool.len();
        if pool_size == 0 {
            return Ok(ScoringResult {
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        // Phase 1: liker lists for the seed. Bounded per post by the walk's own sampling rather than
        // by a hard truncation, so this fetch is deliberately generous — the bias the post-first path
        // carries comes from cutting these lists to the 30 most recent.
        let seed_key_groups: Vec<Vec<String>> = seed
            .iter()
            .map(|(id, _)| Keys::post_likers_retention_bounded(id, DEFAULT_RETENTION_DAYS))
            .collect();
        let seed_likers = self
            .redis
            .zrevrangebyscore_merged_multi(
                &seed_key_groups,
                now,
                min_time,
                self.inverted_coliker_like_limit,
            )
            .await?;

        // Only likers who liked BEFORE the user count, matching every other path: a person who liked
        // after you is not evidence that they led you anywhere.
        let candidates_per_seed: Vec<Vec<String>> = seed_likers
            .into_iter()
            .zip(seed.iter())
            .map(|(likers, (_, user_like_time))| {
                likers
                    .into_iter()
                    .filter(|(h, t)| t < user_like_time && h != user_hash)
                    .map(|(h, _)| h)
                    .collect()
            })
            .collect();

        let degrees: Vec<usize> = candidates_per_seed.iter().map(|v| v.len()).collect();
        let budget = walk::allocate_walks(&degrees, self.walk_count);

        // Sample (seed, co-liker) pairs. Recording which seed produced each co-liker is what lets the
        // multi-hit booster see breadth later.
        let mut rng = crate::algorithm::coliker::daily_rng(user_hash);
        let mut sampled: FxHashMap<String, Vec<usize>> = FxHashMap::default();
        for (seed_idx, walks) in budget.iter().enumerate() {
            let pool_of_likers = &candidates_per_seed[seed_idx];
            if pool_of_likers.is_empty() || *walks == 0 {
                continue;
            }
            for _ in 0..*walks {
                let pick = &pool_of_likers[rng.gen_range(0..pool_of_likers.len())];
                let entry = sampled.entry(pick.clone()).or_default();
                if !entry.contains(&seed_idx) {
                    entry.push(seed_idx);
                }
                if sampled.len() >= self.walk_max_users {
                    break;
                }
            }
            if sampled.len() >= self.walk_max_users {
                break;
            }
        }

        if sampled.is_empty() {
            return Ok(ScoringResult {
                posts_checked: pool_size,
                posts_skipped_no_likers: pool_size,
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        // Phase 2: one pipelined fetch of the sampled co-likers' likes.
        let walkers: Vec<&String> = sampled.keys().collect();
        let walker_key_groups: Vec<Vec<String>> = walkers
            .iter()
            .map(|h| Keys::user_likes_retention(h, self.inverted_coliker_like_days))
            .collect();
        let walker_likes = self
            .redis
            .zrevrangebyscore_merged_multi(
                &walker_key_groups,
                now,
                min_time,
                self.walk_user_like_limit,
            )
            .await?;

        let mut tallies: FxHashMap<String, walk::VisitTally> = FxHashMap::default();
        let mut visits = 0usize;
        let mut early_stopped = false;
        for (i, likes) in walker_likes.into_iter().enumerate() {
            let seeds_reached = &sampled[walkers[i]];
            for (post_id, _) in likes {
                if !pool.contains(post_id.as_str()) {
                    continue;
                }
                visits += 1;
                let tally = tallies.entry(post_id).or_default();
                // One slot per originating seed, so `per_seed` length is the breadth the booster
                // rewards and its values are the depth it discounts.
                while tally.per_seed.len() < seeds_reached.len() {
                    tally.per_seed.push(0);
                }
                for slot in tally.per_seed.iter_mut().take(seeds_reached.len()) {
                    *slot += 1;
                }
            }
            if walk::early_stop_reached(&tallies, self.walk_early_stop_np, self.walk_early_stop_nv)
            {
                early_stopped = true;
                break;
            }
        }

        if tallies.is_empty() {
            return Ok(ScoringResult {
                posts_checked: pool_size,
                posts_skipped_no_likers: pool_size,
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        // Liker counts only for visited candidates — the same saving the inverted path found.
        let visited: Vec<&str> = tallies.keys().map(|s| s.as_str()).collect();
        let count_strs = self.redis.hmget(&algo_counts_key, &visited).await?;
        let liker_counts: FxHashMap<String, usize> = visited
            .iter()
            .zip(count_strs)
            .map(|(id, opt)| {
                (
                    (*id).to_string(),
                    opt.and_then(|s| s.parse().ok()).unwrap_or(0),
                )
            })
            .collect();

        let below_min: usize = liker_counts
            .values()
            .filter(|c| **c < self.min_post_likes)
            .count();
        let mut ranked = walk::rank_from_tallies(
            &tallies,
            &liker_counts,
            self.walk_popularity_power,
            self.walk_min_visits,
        );
        ranked.retain(|(_, id)| liker_counts.get(id).copied().unwrap_or(0) >= self.min_post_likes);
        ranked.truncate(DEFAULT_MAX_SCORER_RESULTS);

        // Mean breadth is the load-bearing number for this whole ranker. The multi-hit booster only
        // does anything if candidates are reached from *several* seeds, and the enumerative paths
        // measure `overlap_mean` at approximately 1.0 — i.e. they essentially never see breadth. If
        // this comes back near 1.0 too, the walk is not finding structure the other paths miss and the
        // booster is inert, which would make a null result expected rather than surprising.
        let breadth_sum: usize = tallies.values().map(|t| t.breadth()).sum();
        let breadth_mean = breadth_sum as f64 / tallies.len().max(1) as f64;
        let multi_seed_candidates = tallies.values().filter(|t| t.breadth() > 1).count();

        let scored_count = ranked.len();
        let scoring_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;
        debug!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            algo_id,
            seed = seed.len(),
            breadth_mean = format!("{:.3}", breadth_mean),
            multi_seed_candidates,
            budget_spent = budget.iter().sum::<usize>(),
            walkers = walkers.len(),
            pool_size,
            visits,
            candidates = tallies.len(),
            below_min_post_likes = below_min,
            early_stopped,
            scored_count,
            scoring_time_ms = format!("{:.2}", scoring_time_ms),
            "scorer_sampled_walk_completed"
        );

        Ok(ScoringResult {
            scored_count,
            scored_posts: ranked,
            posts_checked: pool_size,
            posts_skipped_few_likers: below_min,
            scoring_time_ms,
            ..Default::default()
        })
    }

    pub async fn score_inverted(
        &self,
        user_hash: &str,
        algo_id: i32,
        source_weights: &HashMap<String, f64>,
        params: &LinkLonkParams,
        audit: Option<&mut AuditCollector>,
    ) -> Result<ScoringResult> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let min_time = now - params.time_window_hours * 3600.0;
        let recency_half_life_seconds = params.recency_half_life_hours * 3600.0;

        if source_weights.is_empty() {
            return Ok(ScoringResult {
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        let source_weights_fx: FxHashMap<&String, f64> =
            source_weights.iter().map(|(k, v)| (k, *v)).collect();

        let algo_posts_key = Keys::algo_posts(algo_id);
        let algo_counts_key = Keys::algo_posts_counts(algo_id);
        let user_likes_keys = Keys::user_likes_retention(user_hash, DEFAULT_RETENTION_DAYS);
        let user_seen_key = Keys::user_seen(user_hash);

        let seen_limit = if params.max_seen_posts > 0 {
            (params.max_seen_posts as isize) - 1
        } else {
            -1
        };
        let (algo_posts_result, user_likes_result, seen_posts_result) = tokio::join!(
            self.redis.smembers(&algo_posts_key),
            self.redis.zrevrangebyscore_merged(
                &user_likes_keys,
                now,
                min_time,
                params.max_user_likes
            ),
            self.redis.zrevrange(&user_seen_key, 0, seen_limit)
        );

        let algo_posts: Vec<String> = algo_posts_result?;
        if algo_posts.is_empty() {
            return Ok(ScoringResult {
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        let mut excluded: FxHashSet<String> =
            user_likes_result?.into_iter().map(|(id, _)| id).collect();
        excluded.extend(seen_posts_result.unwrap_or_default());

        // The pool becomes a membership set rather than a list to iterate. This is the one
        // structural requirement of the inverted path: testing membership with SISMEMBER per
        // returned post would cost more than the scan it replaces, so the pool must be resident.
        let pool: FxHashSet<&str> = algo_posts
            .iter()
            .map(|s| s.as_str())
            .filter(|p| !excluded.contains(*p))
            .collect();
        let pool_size = pool.len();
        if pool_size == 0 {
            return Ok(ScoringResult {
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        // One key group per co-liker. Bounded to `inverted_coliker_like_days` because pool posts
        // are capped at SYNC_PREFERRED_MAX_AGE_HOURS (72h), so a like on a pool post cannot be
        // older than that plus a margin — older shards can only return posts that fail the pool
        // test anyway.
        let colikers: Vec<&String> = source_weights.keys().collect();
        let coliker_key_groups: Vec<Vec<String>> = colikers
            .iter()
            .map(|h| Keys::user_likes_retention(h, self.inverted_coliker_like_days))
            .collect();

        let fetched = self
            .redis
            .zrevrangebyscore_merged_multi(
                &coliker_key_groups,
                now,
                min_time,
                self.inverted_coliker_like_limit,
            )
            .await?;

        // Invert: (co-liker → posts) becomes (post → co-likers), keeping only pool members.
        let mut hits: FxHashMap<&str, Vec<(String, f64)>> = FxHashMap::default();
        let mut coliker_likes_seen = 0usize;
        for (i, likes) in fetched.into_iter().enumerate() {
            let coliker = colikers[i];
            for (post_id, like_time) in likes {
                coliker_likes_seen += 1;
                if let Some(pool_post) = pool.get(post_id.as_str()) {
                    hits.entry(*pool_post)
                        .or_default()
                        .push((coliker.clone(), like_time));
                }
            }
        }

        let posts_reached = hits.len();
        if posts_reached == 0 {
            return Ok(ScoringResult {
                posts_checked: pool_size,
                posts_skipped_no_likers: pool_size,
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        // Liker counts only for posts a co-liker actually reached — a few hundred instead of the
        // whole pool. This is the second, quieter saving of the inversion.
        let hit_ids: Vec<&str> = hits.keys().copied().collect();
        let count_strs = self.redis.hmget(&algo_counts_key, &hit_ids).await?;
        let liker_counts: Vec<usize> = count_strs
            .into_iter()
            .map(|opt| opt.and_then(|s| s.parse().ok()).unwrap_or(0))
            .collect();

        let mut posts_to_score: Vec<(&str, usize)> = Vec::with_capacity(hit_ids.len());
        let mut all_likers: Vec<Vec<(String, f64)>> = Vec::with_capacity(hit_ids.len());
        let mut posts_skipped_few_likers = 0usize;
        for (post_id, count) in hit_ids.iter().zip(liker_counts.iter()) {
            // A post reached by a co-liker has at least one liker by construction, so the
            // "no likers" bucket cannot apply here; only min_post_likes can reject it.
            if *count < self.min_post_likes {
                posts_skipped_few_likers += 1;
                continue;
            }
            if let Some(likers) = hits.remove(*post_id) {
                posts_to_score.push((*post_id, *count));
                all_likers.push(likers);
            }
        }

        if posts_to_score.is_empty() {
            return Ok(ScoringResult {
                posts_checked: pool_size,
                posts_skipped_no_likers: pool_size.saturating_sub(posts_reached),
                posts_skipped_few_likers,
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        let (mut post_scores, posts_skipped_low_overlap, overlap_sum, overlap_max, overlap_hist) =
            self.score_prepared(
                &posts_to_score,
                &all_likers,
                &source_weights_fx,
                params,
                now,
                recency_half_life_seconds,
                audit,
            );

        post_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        post_scores.truncate(DEFAULT_MAX_SCORER_RESULTS);
        let scored_count = post_scores.len();
        let scoring_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;

        debug!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            algo_id,
            colikers = colikers.len(),
            coliker_likes_seen,
            pool_size,
            posts_reached,
            posts_skipped_few_likers,
            posts_skipped_low_overlap,
            scored_count,
            // ops = one merged fetch per co-liker key group, plus SMEMBERS/likes/seen/HMGET
            redis_key_groups = colikers.len(),
            scoring_time_ms = format!("{:.2}", scoring_time_ms),
            "scorer_inverted_completed"
        );

        Ok(ScoringResult {
            scored_posts: post_scores,
            scored_count,
            posts_checked: pool_size,
            posts_skipped_no_likers: pool_size.saturating_sub(posts_reached),
            posts_skipped_few_likers,
            scoring_time_ms,
            cache_hits: 0,
            cache_misses: colikers.len(),
            posts_skipped_low_overlap,
            overlap_sum,
            overlap_max,
            overlap_hist,
            seed_keep_rate: None,
            ranker_by_post: Default::default(),
            requires_preserved_order: false,
        })
    }

    /// Score a prepared candidate set. **Both the post-first and co-liker-first paths call
    /// this**, so the ranking math cannot diverge between them.
    ///
    /// Inputs are the two parallel arrays the two lookup strategies both produce:
    /// `posts_to_score[i] = (post_id, total_liker_count)` and `all_likers[i] =
    /// [(liker_hash, like_time), ...]` restricted to the retention/time window.
    ///
    /// Returns `(post_scores, posts_skipped_low_overlap, overlap_sum, overlap_max,
    /// overlap_hist)`; sorting and truncation stay with the caller.
    #[allow(clippy::too_many_arguments)]
    fn score_prepared(
        &self,
        posts_to_score: &[(&str, usize)],
        all_likers: &[Vec<(String, f64)>],
        source_weights: &FxHashMap<&String, f64>,
        params: &LinkLonkParams,
        now: f64,
        recency_half_life_seconds: f64,
        mut audit: Option<&mut AuditCollector>,
    ) -> (
        Vec<(f64, String)>,
        usize,
        usize,
        usize,
        [u32; OVERLAP_BUCKETS],
    ) {
        // Step 5b: Build corater rank map for decay (zero-cost when corater_decay == 0.0)
        // For each co-liker, ranks their likes across candidate posts by recency.
        // rank 0 = most recent like, rank 1 = second most recent, etc.
        // Structure: post_index -> co_liker_hash -> rank
        let corater_decay = params.corater_decay;
        let corater_ranks: FxHashMap<usize, FxHashMap<&str, usize>> = if corater_decay > 0.0 {
            // Transpose all_likers into per-co-liker view: co_liker_hash -> [(post_idx, like_time)]
            let mut per_coliker: FxHashMap<&str, Vec<(usize, f64)>> = FxHashMap::default();
            for (post_idx, likers) in all_likers.iter().enumerate() {
                for (liker_hash, like_time) in likers {
                    if source_weights.contains_key(liker_hash) {
                        per_coliker
                            .entry(liker_hash.as_str())
                            .or_default()
                            .push((post_idx, *like_time));
                    }
                }
            }
            // Sort each co-liker's likes by time descending, assign ranks, build lookup
            let mut ranks: FxHashMap<usize, FxHashMap<&str, usize>> = FxHashMap::default();
            for (co_liker, mut entries) in per_coliker {
                entries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                for (rank, (post_idx, _)) in entries.iter().enumerate() {
                    ranks.entry(*post_idx).or_default().insert(co_liker, rank);
                }
            }
            ranks
        } else {
            FxHashMap::default()
        };
        let decay_base = 1.0 - corater_decay;

        // Step 6: Score all posts
        let estimated_capacity =
            (posts_to_score.len() / 5).clamp(100, DEFAULT_MAX_SCORER_RESULTS * 2);
        let mut post_scores: Vec<(f64, String)> = Vec::with_capacity(estimated_capacity);

        // Overlap observability, accumulated identically in both the audit and fast paths.
        let mut posts_skipped_low_overlap = 0usize;
        let mut overlap_sum = 0usize;
        let mut overlap_max = 0usize;
        let mut overlap_hist = [0u32; OVERLAP_BUCKETS];

        if let Some(ref mut a) = audit {
            // Audit-enabled path
            for (post_idx, ((post_id, liker_count), likers)) in
                posts_to_score.iter().zip(all_likers.iter()).enumerate()
            {
                let mut score = 0.0;
                let mut overlap_count = 0usize;
                let post_ranks = corater_ranks.get(&post_idx);

                for (liker_hash, like_time) in likers {
                    if let Some(weight) = source_weights.get(liker_hash) {
                        overlap_count += 1;
                        // Cap the co-liker weight to prevent any single user from dominating
                        let capped_weight = weight.min(self.max_coliker_weight);
                        let age_seconds = now - like_time;
                        let recency_weight =
                            (-0.693 * age_seconds / recency_half_life_seconds).exp();
                        // Apply corater decay: (1 - decay)^rank where rank 0 = most recent
                        let decay_mult = if corater_decay > 0.0 {
                            post_ranks
                                .and_then(|pr| pr.get(liker_hash.as_str()))
                                .map(|&rank| decay_base.powi(rank as i32))
                                .unwrap_or(1.0)
                        } else {
                            1.0
                        };
                        let contribution = capped_weight * recency_weight * decay_mult;
                        score += contribution;

                        a.add_contribution(
                            post_id,
                            liker_hash,
                            *weight,
                            recency_weight,
                            contribution,
                            age_seconds,
                        );
                    }
                }

                // Skip posts without enough overlapping co-likers (prevents single-user contamination)
                if overlap_count < self.min_overlapping_colikers {
                    posts_skipped_low_overlap += 1;
                    continue;
                }

                if score > 0.0 {
                    overlap_sum += overlap_count;
                    overlap_max = overlap_max.max(overlap_count);
                    overlap_hist[overlap_bucket(overlap_count)] += 1;
                    // (1) Num paths exponent: boost posts with more distinct co-liker paths
                    let num_paths = overlap_count.max(1) as f64;
                    let paths_boost = num_paths.powf(params.num_paths_power);
                    let score_after_paths = score * paths_boost;
                    // (2) Popularity exponent: demote viral posts
                    let popularity_penalty = if params.popularity_power > 0.0 && *liker_count > 1 {
                        (1.0 / *liker_count as f64).powf(params.popularity_power * 0.5)
                    } else {
                        1.0
                    };
                    let final_score = score_after_paths * popularity_penalty;
                    a.set_post_breakdown(
                        post_id,
                        PostBreakdownData {
                            raw_score: score,
                            final_score,
                            popularity_penalty,
                            liker_count: *liker_count,
                            num_paths: overlap_count,
                            paths_boost,
                        },
                    );
                    post_scores.push((final_score, (*post_id).to_string()));
                }
            }
        } else {
            // Fast path - no audit
            for (post_idx, ((post_id, liker_count), likers)) in
                posts_to_score.iter().zip(all_likers.iter()).enumerate()
            {
                let mut score = 0.0;
                let mut overlap_count = 0usize;
                let post_ranks = corater_ranks.get(&post_idx);

                for (liker_hash, like_time) in likers {
                    if let Some(weight) = source_weights.get(liker_hash) {
                        overlap_count += 1;
                        // Cap the co-liker weight to prevent any single user from dominating
                        let capped_weight = weight.min(self.max_coliker_weight);
                        let age_seconds = now - like_time;
                        let recency_weight =
                            (-0.693 * age_seconds / recency_half_life_seconds).exp();
                        // Apply corater decay: (1 - decay)^rank where rank 0 = most recent
                        let decay_mult = if corater_decay > 0.0 {
                            post_ranks
                                .and_then(|pr| pr.get(liker_hash.as_str()))
                                .map(|&rank| decay_base.powi(rank as i32))
                                .unwrap_or(1.0)
                        } else {
                            1.0
                        };
                        score += capped_weight * recency_weight * decay_mult;
                    }
                }

                // Skip posts without enough overlapping co-likers (prevents single-user contamination)
                if overlap_count < self.min_overlapping_colikers {
                    posts_skipped_low_overlap += 1;
                    continue;
                }

                if score > 0.0 {
                    overlap_sum += overlap_count;
                    overlap_max = overlap_max.max(overlap_count);
                    overlap_hist[overlap_bucket(overlap_count)] += 1;
                    // (1) Num paths exponent: boost posts with more distinct co-liker paths
                    let num_paths = overlap_count.max(1) as f64;
                    let paths_boost = num_paths.powf(params.num_paths_power);
                    let score_after_paths = score * paths_boost;
                    // (2) Popularity exponent: demote viral posts
                    let popularity_penalty = if params.popularity_power > 0.0 && *liker_count > 1 {
                        (1.0 / *liker_count as f64).powf(params.popularity_power * 0.5)
                    } else {
                        1.0
                    };
                    post_scores.push((
                        score_after_paths * popularity_penalty,
                        (*post_id).to_string(),
                    ));
                }
            }
        }

        (
            post_scores,
            posts_skipped_low_overlap,
            overlap_sum,
            overlap_max,
            overlap_hist,
        )
    }

    /// Score posts for a user using the inverted algorithm.
    ///
    /// If `audit` is provided, detailed scoring information will be collected.
    /// Returns scored posts directly to avoid re-fetching from Redis.
    pub async fn score(
        &self,
        user_hash: &str,
        algo_id: i32,
        source_weights: &HashMap<String, f64>,
        params: &LinkLonkParams,
        audit: Option<&mut AuditCollector>,
    ) -> Result<ScoringResult> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let time_window_seconds = params.time_window_hours * 3600.0;
        let min_time = now - time_window_seconds;
        let recency_half_life_seconds = params.recency_half_life_hours * 3600.0;

        // Convert to FxHashMap for faster lookups in the hot scoring loop
        let source_weights: FxHashMap<&String, f64> =
            source_weights.iter().map(|(k, v)| (k, *v)).collect();

        // Keys for data
        let result_key = Keys::cached_result(algo_id, user_hash);
        let algo_posts_key = Keys::algo_posts(algo_id);
        let algo_counts_key = Keys::algo_posts_counts(algo_id);
        let user_likes_keys = Keys::user_likes_retention(user_hash, DEFAULT_RETENTION_DAYS);
        let user_seen_key = Keys::user_seen(user_hash);

        // Step 1: Load algo posts, user's likes, and seen posts IN PARALLEL
        // If seed_sample_pool > 0, fetch a larger pool for random sampling
        let fetch_limit = if params.seed_sample_pool > 0 {
            params.seed_sample_pool.max(params.max_user_likes)
        } else {
            params.max_user_likes
        };
        let seen_limit = if params.max_seen_posts > 0 {
            (params.max_seen_posts as isize) - 1
        } else {
            -1
        };
        let (algo_posts_result, user_likes_result, seen_posts_result) = tokio::join!(
            self.redis.smembers(&algo_posts_key),
            self.redis
                .zrevrangebyscore_merged(&user_likes_keys, now, min_time, fetch_limit),
            self.redis.zrevrange(&user_seen_key, 0, seen_limit)
        );

        let algo_posts: Vec<String> = algo_posts_result?;
        let mut user_likes = user_likes_result?;
        let seen_posts: Vec<String> = seen_posts_result.unwrap_or_default();

        // Random seed sampling: shuffle and truncate to max_user_likes.
        //
        // Deterministic per (user, day) for the same reason as the matching block in `coliker.rs`:
        // a fresh draw per request made one ranker disagree with itself, which the interleaving
        // self-check measured as a 6% spurious disagreement floor.
        if params.seed_sample_pool > 0 && user_likes.len() > params.max_user_likes {
            user_likes.shuffle(&mut crate::algorithm::coliker::daily_rng(user_hash));
            user_likes.truncate(params.max_user_likes);
        }

        if algo_posts.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                algo_id,
                "early_exit: no candidates in algo posts set"
            );
            return Ok(ScoringResult {
                scored_posts: Vec::new(),
                scored_count: 0,
                posts_checked: 0,
                posts_skipped_no_likers: 0,
                posts_skipped_few_likers: 0,
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        let mut excluded_posts: FxHashSet<String> =
            user_likes.into_iter().map(|(id, _)| id).collect();
        excluded_posts.extend(seen_posts);

        // Step 2: Filter posts (remove already liked or seen)
        let mut candidates: Vec<String> = algo_posts
            .into_iter()
            .filter(|p| !excluded_posts.contains(p))
            .collect();

        if self.max_posts_to_score > 0 && candidates.len() > self.max_posts_to_score {
            candidates.truncate(self.max_posts_to_score);
        }

        if candidates.is_empty() {
            return Ok(ScoringResult {
                scored_posts: Vec::new(),
                scored_count: 0,
                posts_checked: 0,
                posts_skipped_no_likers: 0,
                posts_skipped_few_likers: 0,
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        // Step 3: Get pre-computed liker counts from hash (faster than ZCARD)
        let post_id_refs: Vec<&str> = candidates.iter().map(|s| s.as_str()).collect();
        let count_strs = self.redis.hmget(&algo_counts_key, &post_id_refs).await?;

        // Parse counts, defaulting to 0 for missing entries
        let liker_counts: Vec<usize> = count_strs
            .into_iter()
            .map(|opt| opt.and_then(|s| s.parse().ok()).unwrap_or(0))
            .collect();

        // Step 4: Filter by min likes and prepare fetch list
        let mut posts_skipped_no_likers = 0;
        let mut posts_skipped_few_likers = 0;
        let mut posts_to_score: Vec<(&str, usize)> = Vec::with_capacity(candidates.len());

        for (post_id, count) in candidates.iter().zip(liker_counts.iter()) {
            if *count == 0 {
                posts_skipped_no_likers += 1;
            } else if *count < self.min_post_likes {
                posts_skipped_few_likers += 1;
            } else {
                posts_to_score.push((post_id.as_str(), *count));
            }
        }

        if posts_to_score.is_empty() {
            return Ok(ScoringResult {
                scored_posts: Vec::new(),
                scored_count: 0,
                posts_checked: candidates.len(),
                posts_skipped_no_likers,
                posts_skipped_few_likers,
                scoring_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                ..Default::default()
            });
        }

        // Step 5: Fetch likers - check local cache first, then batch fetch misses
        let mut cache_hits = 0;
        let mut cache_misses = 0;
        let mut all_likers: Vec<Vec<(String, f64)>> = Vec::with_capacity(posts_to_score.len());
        let mut cache_miss_indices: Vec<usize> = Vec::new();
        let mut cache_miss_post_ids: Vec<&str> = Vec::new();

        // Check cache for each post
        if self.liker_cache_enabled {
            for (i, (post_id, _)) in posts_to_score.iter().enumerate() {
                if let Some(entry) = self.liker_cache.get(post_id) {
                    // Filter by time window - use owned version to avoid extra clone
                    let filtered =
                        LikerCache::filter_likers_by_time_owned(entry.likers, min_time, now);
                    all_likers.push(filtered);
                    cache_hits += 1;
                } else {
                    // Mark for fetching
                    all_likers.push(Vec::new()); // Placeholder
                    cache_miss_indices.push(i);
                    cache_miss_post_ids.push(post_id);
                    cache_misses += 1;
                }
            }
        } else {
            // Cache disabled - fetch all
            for (post_id, _) in &posts_to_score {
                all_likers.push(Vec::new());
                cache_miss_indices.push(all_likers.len() - 1);
                cache_miss_post_ids.push(post_id);
            }
            cache_misses = posts_to_score.len();
        }

        // Batch fetch cache misses from Redis using date-based keys
        if !cache_miss_post_ids.is_empty() {
            // Build date-based key groups for each post
            let post_key_groups: Vec<Vec<String>> = cache_miss_post_ids
                .iter()
                .map(|post_id| Keys::post_likers_retention_bounded(post_id, DEFAULT_RETENTION_DAYS))
                .collect();

            // Fetch from all date-based keys and merge results per post
            let fetched_likers = self
                .redis
                .zrevrangebyscore_merged_multi(
                    &post_key_groups,
                    now,
                    min_time,
                    self.max_likers_per_post,
                )
                .await?;

            // Fill in results for scoring
            for (idx, likers) in cache_miss_indices.iter().zip(fetched_likers) {
                all_likers[*idx] = likers;
            }
        }

        let (mut post_scores, posts_skipped_low_overlap, overlap_sum, overlap_max, overlap_hist) =
            self.score_prepared(
                &posts_to_score,
                &all_likers,
                &source_weights,
                params,
                now,
                recency_half_life_seconds,
                audit,
            );
        // Sort and truncate
        post_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        post_scores.truncate(DEFAULT_MAX_SCORER_RESULTS);

        let scored_count = post_scores.len();

        // Populate local liker cache with fetched data (after scoring is done)
        // This clone is necessary since the cache needs owned data
        if self.liker_cache_enabled && !cache_miss_indices.is_empty() {
            for idx in &cache_miss_indices {
                let likers = &all_likers[*idx];
                if !likers.is_empty() {
                    let (post_id, liker_count) = posts_to_score[*idx];
                    self.liker_cache
                        .set(post_id.to_string(), likers.clone(), liker_count);
                }
            }
        }

        // Store in Redis cache for future requests (single pipelined call)
        if !post_scores.is_empty() && !self.read_only {
            let items: Vec<(f64, &str)> = post_scores
                .iter()
                .map(|(score, id)| (*score, id.as_str()))
                .collect();
            self.redis
                .store_sorted_set(&result_key, &items, params.result_ttl_seconds as i64)
                .await?;
        }

        let scoring_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;

        debug!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            algo_id,
            scored_count,
            posts_checked = candidates.len(),
            // Where candidates die before scoring. Both were already computed for
            // ScoringResult but never logged, which made the "why is the feed mostly
            // fallback?" question un-answerable without offline reconstruction:
            //   no_likers  = candidate has zero recorded likers in apc:{algo_id}
            //   few_likers = has likers but fewer than min_post_likes
            posts_skipped_no_likers,
            posts_skipped_few_likers,
            //   low_overlap = had likers, but fewer than min_overlapping_colikers of them
            //                 were in this user's co-liker set
            posts_skipped_low_overlap,
            min_post_likes = self.min_post_likes,
            min_overlapping_colikers = self.min_overlapping_colikers,
            overlap_max,
            cache_hits,
            cache_misses,
            scoring_time_ms = format!("{:.2}", scoring_time_ms),
            "scorer_completed"
        );

        Ok(ScoringResult {
            scored_posts: post_scores,
            scored_count,
            posts_checked: candidates.len(),
            posts_skipped_no_likers,
            posts_skipped_few_likers,
            scoring_time_ms,
            cache_hits,
            cache_misses,
            posts_skipped_low_overlap,
            overlap_sum,
            overlap_max,
            overlap_hist,
            seed_keep_rate: None,
            ranker_by_post: Default::default(),
            requires_preserved_order: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_buckets_cover_every_count_monotonically() {
        // Boundaries matter: paths_boost is nonlinear in overlap_count, so the histogram
        // must not smear adjacent magnitudes together.
        assert_eq!(overlap_bucket(0), 0);
        assert_eq!(overlap_bucket(1), 0);
        assert_eq!(overlap_bucket(2), 1);
        assert_eq!(overlap_bucket(3), 2);
        assert_eq!(overlap_bucket(4), 2);
        assert_eq!(overlap_bucket(5), 3);
        assert_eq!(overlap_bucket(8), 3);
        assert_eq!(overlap_bucket(9), 4);
        assert_eq!(overlap_bucket(16), 4);
        assert_eq!(overlap_bucket(17), 5);
        assert_eq!(overlap_bucket(32), 5);
        assert_eq!(overlap_bucket(33), 6);
        assert_eq!(overlap_bucket(64), 6);
        assert_eq!(overlap_bucket(65), 7);
        assert_eq!(overlap_bucket(170), 7); // observed max on a real lurker profile

        let mut last = 0;
        for n in 0..500 {
            let b = overlap_bucket(n);
            assert!(b >= last && b < OVERLAP_BUCKETS);
            last = b;
        }
    }

    #[test]
    fn overlap_mean_divides_by_observed_not_truncated_scored_count() {
        // scored_count is truncated to DEFAULT_MAX_SCORER_RESULTS while the overlap stats
        // are gathered pre-truncation; dividing by scored_count would overstate the mean.
        let r = ScoringResult {
            scored_count: 500,
            overlap_sum: 3_000,
            overlap_hist: [0, 0, 0, 1_000, 0, 0, 0, 0],
            ..Default::default()
        };
        assert_eq!(r.overlap_observed(), 1_000);
        assert_eq!(r.overlap_mean(), 3.0);
    }

    #[test]
    fn overlap_mean_is_zero_when_nothing_scored() {
        assert_eq!(ScoringResult::default().overlap_mean(), 0.0);
        assert_eq!(ScoringResult::default().overlap_observed(), 0);
    }

    #[test]
    fn overlap_hist_str_lists_only_populated_buckets() {
        let r = ScoringResult {
            overlap_hist: [4, 9, 2, 0, 0, 0, 0, 1],
            ..Default::default()
        };
        assert_eq!(r.overlap_hist_str(), "1:4,2:9,3-4:2,65+:1");
        assert_eq!(ScoringResult::default().overlap_hist_str(), "");
    }

    #[test]
    fn default_result_serves_nothing() {
        // The empty early-exit paths rely on Default meaning "no personalization".
        let r = ScoringResult::default();
        assert!(r.scored_posts.is_empty());
        assert_eq!(r.scored_count, 0);
        assert_eq!(r.posts_skipped_low_overlap, 0);
    }
}
