//! Author-Level Co-Liker Pre-computation.
//!
//! This module pre-computes "users who liked similar authors" relationships
//! to provide denser connections than post-level co-likers.
//!
//! The computation:
//! 1. Gets authors a user has liked (from ula:{hash})
//! 2. For each liked author, finds other users who liked them (from authl:{hash})
//! 3. Aggregates weights: more shared authors = higher weight
//! 4. Stores top N author-level co-likers with their accumulated weights
//!
//! This creates a thicker network than post-level co-likers because:
//! - Author-level: Match on any posts from an author (dense)
//! - Post-level: Must match on exact posts (sparse)
//!
//! Used to supplement post-level personalization when it doesn't fill the feed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tracing::debug;

use crate::config::Config;
use crate::error::Result;
use graze_common::{Keys, RedisClient, DEFAULT_RETENTION_DAYS};

/// Computes and caches author-level co-liker aggregations for users.
pub struct AuthorColikerWorker {
    redis: Arc<RedisClient>,
    config: Arc<Config>,
}

impl AuthorColikerWorker {
    /// Create a new author co-liker worker.
    pub fn new(redis: Arc<RedisClient>, config: Arc<Config>) -> Self {
        Self { redis, config }
    }

    /// Get or compute author-level co-liker weights for a user.
    ///
    /// Checks if pre-computed author co-likers exist and are fresh.
    /// If not, it triggers computation.
    pub async fn get_or_compute_author_colikes(
        &self,
        user_hash: &str,
        force_refresh: bool,
    ) -> Result<HashMap<String, f64>> {
        if !self.config.author_affinity_enabled {
            debug!(user_hash = %&user_hash[..8.min(user_hash.len())], "early_exit: author affinity disabled");
            return Ok(HashMap::new());
        }

        let author_colikes_key = Keys::author_colikes(user_hash);

        if !force_refresh {
            // Check if cache exists and is fresh enough
            let ttl = self.redis.ttl(&author_colikes_key).await?;

            if ttl > self.config.author_affinity_refresh_threshold_seconds as i64 {
                // Cache is fresh, return it
                return self.get_cached_author_colikes(&author_colikes_key).await;
            } else if ttl > 0 {
                // Cache exists but stale, return it anyway
                let cached = self.get_cached_author_colikes(&author_colikes_key).await?;
                if !cached.is_empty() {
                    return Ok(cached);
                }
            }
        }

        // Cache miss or force refresh - compute now
        self.compute_author_colikes(user_hash).await
    }

    /// Compute author-level co-liker aggregation for a user.
    pub async fn compute_author_colikes(&self, user_hash: &str) -> Result<HashMap<String, f64>> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let time_window_seconds = self.config.author_affinity_time_window_hours as f64 * 3600.0;
        let min_time = now - time_window_seconds;

        let max_authors = self.config.author_affinity_max_authors;
        let max_colikers = self.config.author_affinity_max_colikers;
        let max_likers_per_author = self.config.author_affinity_max_likers_per_author;
        let min_author_likes = self.config.author_affinity_min_author_likes;

        // Step 1: Get authors this user has liked (from ula:{user_hash})
        // Sorted by like count (weight)
        let user_liked_authors_key = Keys::user_liked_authors(user_hash);
        let liked_authors = self
            .redis
            .zrevrange_with_scores(&user_liked_authors_key, 0, (max_authors - 1) as isize)
            .await?;

        if liked_authors.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                max_authors,
                "early_exit: user has no liked authors"
            );
            return Ok(HashMap::new());
        }

        // Filter authors with insufficient likes
        let liked_authors: Vec<(String, f64)> = liked_authors
            .into_iter()
            .filter(|(_, like_count)| *like_count >= min_author_likes as f64)
            .collect();

        if liked_authors.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                min_author_likes,
                "early_exit: no authors meet min_author_likes threshold"
            );
            return Ok(HashMap::new());
        }

        // Step 2: For each liked author, get other users who liked them (pipelined)
        // Build date-based key groups for each author
        let author_key_groups: Vec<Vec<String>> = liked_authors
            .iter()
            .map(|(author_hash, _)| {
                Keys::author_likers_retention(author_hash, DEFAULT_RETENTION_DAYS)
            })
            .collect();

        // Fetch from all date-based keys and merge results per author
        let all_likers = self
            .redis
            .zrevrangebyscore_merged_multi(&author_key_groups, now, min_time, max_likers_per_author)
            .await?;

        // Aggregate weights from all results
        let mut source_weights: HashMap<String, f64> = HashMap::new();

        for ((_, user_like_count), likers) in liked_authors.iter().zip(all_likers.iter()) {
            for (source_hash, _like_time) in likers {
                // Skip self
                if source_hash == user_hash {
                    continue;
                }

                // Weight by user's affinity for this author
                // More likes to this author = stronger signal
                let weight = user_like_count.sqrt(); // Sqrt to dampen extreme values
                *source_weights.entry(source_hash.clone()).or_insert(0.0) += weight;
            }
        }

        if source_weights.is_empty() {
            debug!(
                user_hash = %&user_hash[..8.min(user_hash.len())],
                authors_checked = liked_authors.len(),
                "early_exit: no co-likers after self-exclusion"
            );
            return Ok(HashMap::new());
        }

        // Sort by weight and keep top N
        let mut sorted_sources: Vec<(String, f64)> = source_weights.into_iter().collect();
        sorted_sources.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        sorted_sources.truncate(max_colikers);

        // Store in Redis sorted set
        let author_colikes_key = Keys::author_colikes(user_hash);

        self.redis.del(&author_colikes_key).await?;

        let items: Vec<(f64, &str)> = sorted_sources
            .iter()
            .map(|(hash, weight)| (*weight, hash.as_str()))
            .collect();
        self.redis.zadd(&author_colikes_key, &items).await?;
        self.redis
            .expire(
                &author_colikes_key,
                self.config.author_affinity_ttl_seconds as i64,
            )
            .await?;

        let compute_time = start_time.elapsed();
        debug!(
            user_hash = %&user_hash[..8.min(user_hash.len())],
            sources = sorted_sources.len(),
            authors_checked = liked_authors.len(),
            compute_time_ms = compute_time.as_millis(),
            "author_colikes_computed"
        );

        Ok(sorted_sources.into_iter().collect())
    }

    /// Get cached author co-likers from Redis.
    async fn get_cached_author_colikes(&self, key: &str) -> Result<HashMap<String, f64>> {
        let results = self
            .redis
            .zrevrangebyscore_with_scores(key, f64::INFINITY, f64::NEG_INFINITY, 10000)
            .await?;

        Ok(results.into_iter().collect())
    }

    /// Invalidate cached author co-likers for a user.
    pub async fn invalidate_author_colikes(&self, user_hash: &str) -> Result<bool> {
        let author_colikes_key = Keys::author_colikes(user_hash);
        self.redis.del(&author_colikes_key).await?;
        Ok(true)
    }
}
