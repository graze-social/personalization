//! Co-Liker Pre-computation Worker.
//!
//! This module pre-computes "users who liked similar posts" relationships
//! to dramatically reduce Step 2 computation time in the LinkLonk algorithm.
//!
//! The computation:
//! 1. Gets a user's recent likes
//! 2. For each liked post, finds other users who liked it (before the user)
//! 3. Aggregates weights based on recency (using optimized scoring_core)
//! 4. Stores top N co-likers with their accumulated weights
//!
//! When LinkLonk normalization is enabled, the aggregation includes:
//! - 1/|user_likes| (Step 1 normalization)
//! - 1/|sources_who_liked_j| (Step 2 normalization)
//! - 1/|items_s_liked| (Step 3 normalization)
//!
//! This converts O(n) per-request computation into O(1) lookup.

use std::collections::HashMap;

use rand::seq::SliceRandom;
use rand::thread_rng;
use rustc_hash::FxHashSet;
use std::sync::Arc;
use std::time::Instant;

use rustc_hash::FxHashMap;
use tracing::debug;

use crate::algorithm::scoring_core::{
    aggregate_coliker_weights_normalized_parallel, aggregate_coliker_weights_parallel,
};
use crate::config::Config;
use crate::error::Result;
use graze_common::{Keys, RedisClient, DEFAULT_RETENTION_DAYS};

/// Computes and caches co-liker aggregations for users.
pub struct ColikerWorker {
    redis: Arc<RedisClient>,
    config: Arc<Config>,
}

impl ColikerWorker {
    /// Create a new co-liker worker.
    pub fn new(redis: Arc<RedisClient>, config: Arc<Config>) -> Self {
        Self { redis, config }
    }

    /// Get or compute co-liker weights for a user.
    ///
    /// This method checks if pre-computed co-likers exist and are fresh.
    /// If not, it triggers computation.
    ///
    /// Cache invalidation happens when:
    /// 1. TTL expires (coliker_ttl_seconds, default 6 hours)
    /// 2. User has new likes since cache was computed
    /// 3. force_refresh is true
    #[allow(clippy::too_many_arguments)]
    pub async fn get_or_compute_colikes(
        &self,
        user_hash: &str,
        max_user_likes: usize,
        max_sources_per_post: usize,
        max_total_sources: usize,
        time_window_seconds: u64,
        recency_half_life_seconds: u64,
        force_refresh: bool,
        seed_sample_pool: usize,
    ) -> Result<HashMap<String, f64>> {
        if !self.config.coliker_enabled {
            debug!(user_hash = %&user_hash[..8.min(user_hash.len())], "early_exit: coliker disabled");
            return Ok(HashMap::new());
        }

        let colikes_key = Keys::colikes(user_hash);
        let colikes_ts_key = Keys::colikes_timestamp(user_hash);

        if !force_refresh {
            // Check if cache exists and is fresh enough
            let ttl = self.redis.ttl(&colikes_key).await?;

            if ttl > 0 {
                // Cache exists - check if user has new likes since computation
                let should_invalidate = self
                    .check_user_has_new_likes(user_hash, &colikes_ts_key)
                    .await?;

                if should_invalidate {
                    debug!(
                        user_hash = %&user_hash[..8.min(user_hash.len())],
                        "cache_invalidated: user has new likes since computation"
                    );
                } else if ttl > self.config.coliker_refresh_threshold_seconds as i64 {
                    // Cache is fresh and user has no new likes, return it
                    return self.get_cached_colikes(&colikes_key).await;
                } else {
                    // Cache is getting stale but user has no new likes - still usable
                    let cached = self.get_cached_colikes(&colikes_key).await?;
                    if !cached.is_empty() {
                        return Ok(cached);
                    }
                }
            }
        }

        // Cache miss, invalidated, or force refresh - compute now
        self.compute_colikes(
            user_hash,
            max_user_likes,
            max_sources_per_post,
            max_total_sources,
            time_window_seconds,
            recency_half_life_seconds,
            seed_sample_pool,
        )
        .await
    }

    /// Check if user has new likes since the co-likers cache was computed.
    async fn check_user_has_new_likes(
        &self,
        user_hash: &str,
        colikes_ts_key: &str,
    ) -> Result<bool> {
        // Get the timestamp when co-likers were last computed
        let cached_ts = self.redis.get_string(colikes_ts_key).await?;
        let Some(ts_str) = cached_ts else {
            // No timestamp stored - treat as needing refresh for safety
            return Ok(true);
        };

        let cached_timestamp: f64 = ts_str.parse().unwrap_or(0.0);
        if cached_timestamp == 0.0 {
            return Ok(true);
        }

        // Get user's most recent like timestamp from date-based keys
        // Only need to check today's key since that's where new likes go
        let today = graze_common::today_date();
        let user_likes_key = Keys::user_likes_date(user_hash, &today);
        let recent_likes = self
            .redis
            .zrevrangebyscore_with_scores(&user_likes_key, f64::INFINITY, cached_timestamp, 1)
            .await?;

        // If there are likes newer than cached_timestamp, cache is stale
        Ok(!recent_likes.is_empty())
    }

    /// Compute co-liker aggregation for a user.
    ///
    /// This replicates Step 2 of the Lua script but stores the result
    /// for later O(1) retrieval. Uses optimized parallel aggregation from scoring_core.
    #[allow(clippy::too_many_arguments)]
    pub async fn compute_colikes(
        &self,
        user_hash: &str,
        max_user_likes: usize,
        max_sources_per_post: usize,
        max_total_sources: usize,
        time_window_seconds: u64,
        recency_half_life_seconds: u64,
        seed_sample_pool: usize,
    ) -> Result<HashMap<String, f64>> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let min_time = now - time_window_seconds as f64;

        // Step 1: Get user's recent likes from date-based keys
        // If seed_sample_pool > 0, fetch a larger pool and randomly sample max_user_likes
        let fetch_limit = if seed_sample_pool > 0 {
            seed_sample_pool.max(max_user_likes)
        } else {
            max_user_likes
        };
        let user_likes_keys = Keys::user_likes_retention(user_hash, DEFAULT_RETENTION_DAYS);
        let mut user_likes = self
            .redis
            .zrevrangebyscore_merged(&user_likes_keys, now, min_time, fetch_limit)
            .await?;

        if user_likes.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                time_window_seconds,
                max_user_likes,
                "early_exit: user has no likes in time window"
            );
            return Ok(HashMap::new());
        }

        // Random seed sampling: shuffle and truncate to max_user_likes
        if seed_sample_pool > 0 && user_likes.len() > max_user_likes {
            user_likes.shuffle(&mut thread_rng());
            user_likes.truncate(max_user_likes);
        }

        // Parse likes into list of (post_id, like_time)
        let liked_posts: Vec<(String, f64)> = user_likes;

        // Step 2: Fetch co-likers for ALL posts using pipelined requests
        // Build date-based key groups for each post
        let post_key_groups: Vec<Vec<String>> = liked_posts
            .iter()
            .map(|(post_id, _)| Keys::post_likers_retention(post_id, DEFAULT_RETENTION_DAYS))
            .collect();

        // Fetch from all day-tranche keys and merge results per post
        let all_results = self
            .redis
            .zrevrangebyscore_merged_multi(
                &post_key_groups,
                now, // Use now as max since we filter by user_like_time below
                min_time,
                max_sources_per_post,
            )
            .await?;

        // Filter results to only include likers who liked BEFORE the user
        let all_results: Vec<Vec<(String, f64)>> = all_results
            .into_iter()
            .zip(liked_posts.iter())
            .map(|(likers, (_, user_like_time))| {
                likers
                    .into_iter()
                    .filter(|(_, like_time)| *like_time < *user_like_time)
                    .collect()
            })
            .collect();

        // Filter out empty results
        let all_likers_data: Vec<Vec<(String, f64)>> = all_results
            .into_iter()
            .filter(|likers| !likers.is_empty())
            .collect();

        if all_likers_data.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                posts_checked = liked_posts.len(),
                "early_exit: no co-likers found for any liked posts"
            );
            return Ok(HashMap::new());
        }

        // Step 3: Aggregate co-liker weights
        let sorted_sources = if self.config.linklonk_normalization_enabled {
            // Use normalized LinkLonk formula with all branching factors

            // Collect unique source hashes to fetch their like counts
            let unique_sources: FxHashSet<String> = all_likers_data
                .iter()
                .flatten()
                .filter(|(hash, _)| hash != user_hash)
                .map(|(hash, _)| hash.clone())
                .collect();

            // Fetch source like counts from the ulc hash
            let source_hashes: Vec<&str> = unique_sources.iter().map(|s| s.as_str()).collect();
            let source_like_counts: FxHashMap<String, i64> = if !source_hashes.is_empty() {
                let counts = self
                    .redis
                    .hmget(Keys::USER_LIKE_COUNTS, &source_hashes)
                    .await?;

                source_hashes
                    .into_iter()
                    .zip(counts.into_iter())
                    .map(|(hash, count)| {
                        let c = count.and_then(|s| s.parse::<i64>().ok()).unwrap_or(1);
                        (hash.to_string(), c)
                    })
                    .collect()
            } else {
                FxHashMap::default()
            };

            debug!(
                sources_with_counts = source_like_counts.len(),
                user_likes_count = liked_posts.len(),
                "linklonk_normalization_enabled"
            );

            aggregate_coliker_weights_normalized_parallel(
                all_likers_data,
                user_hash,
                liked_posts.len(),
                &source_like_counts,
                now,
                recency_half_life_seconds as f64,
                max_total_sources,
                self.config.max_coliker_weight,
            )
        } else {
            // Use original simplified aggregation (recency only)
            aggregate_coliker_weights_parallel(
                all_likers_data,
                user_hash,
                now,
                recency_half_life_seconds as f64,
                max_total_sources,
            )
        };

        if sorted_sources.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                posts_checked = liked_posts.len(),
                "early_exit: no sources after aggregation (all self-likes?)"
            );
            return Ok(HashMap::new());
        }

        // Store in Redis sorted set along with the timestamp for cache invalidation
        let colikes_key = Keys::colikes(user_hash);
        let colikes_ts_key = Keys::colikes_timestamp(user_hash);

        // Get the most recent like timestamp to use for cache invalidation checks
        let most_recent_like_ts = liked_posts
            .iter()
            .map(|(_, ts)| *ts)
            .fold(f64::NEG_INFINITY, f64::max);

        if self.config.read_only_mode {
            // Log skipped write in read-only mode
            tracing::info!(
                target: "graze::read_only",
                operation = "ZADD",
                key = %colikes_key,
                items_count = sorted_sources.len(),
                "write_skipped"
            );
        } else {
            // Store sorted set + timestamp in a single pipelined call (4 RTT → 1)
            let items: Vec<(f64, &str)> = sorted_sources
                .iter()
                .map(|(hash, weight)| (*weight, hash.as_str()))
                .collect();
            self.redis
                .store_sorted_set_with_timestamp(
                    &colikes_key,
                    &items,
                    self.config.coliker_ttl_seconds,
                    &colikes_ts_key,
                    &most_recent_like_ts.to_string(),
                )
                .await?;
        }

        let compute_time = start_time.elapsed();
        debug!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            sources = sorted_sources.len(),
            posts_checked = liked_posts.len(),
            compute_time_ms = compute_time.as_millis(),
            "coliker_computed"
        );

        Ok(sorted_sources.into_iter().collect())
    }

    /// Get cached co-likers from Redis.
    async fn get_cached_colikes(&self, colikes_key: &str) -> Result<HashMap<String, f64>> {
        let results = self
            .redis
            .zrevrangebyscore_with_scores(colikes_key, f64::INFINITY, f64::NEG_INFINITY, 10000)
            .await?;

        if results.is_empty() {
            return Ok(HashMap::new());
        }

        Ok(results.into_iter().collect())
    }

    /// Invalidate cached co-likers for a user.
    pub async fn invalidate_colikes(&self, user_hash: &str) -> Result<bool> {
        let colikes_key = Keys::colikes(user_hash);
        let colikes_ts_key = Keys::colikes_timestamp(user_hash);
        if self.config.read_only_mode {
            tracing::info!(
                target: "graze::read_only",
                operation = "DEL",
                key = %colikes_key,
                "write_skipped"
            );
            return Ok(true);
        }
        // Delete both keys in a single pipeline (2 RTT → 1)
        self.redis
            .del_multi(&[colikes_key.as_str(), colikes_ts_key.as_str()])
            .await?;
        Ok(true)
    }

    /// Check if co-liker cache exists and its TTL.
    pub async fn check_cache_status(&self, user_hash: &str) -> Result<(bool, i64)> {
        let colikes_key = Keys::colikes(user_hash);
        let ttl = self.redis.ttl(&colikes_key).await?;

        if ttl < 0 {
            return Ok((false, 0));
        }

        Ok((true, ttl))
    }
}
