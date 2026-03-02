//! Feed Skeleton Cache.
//!
//! This module provides per-user feed skeleton caching for O(log N) pagination
//! at any depth. Instead of re-computing the entire personalization for each
//! page, we cache the full ranked list and serve pages from it.
//!
//! Key: `fsc:{algo_id}:{user_hash}` (Redis LIST)
//!
//! The cache implements stale-while-revalidate pattern:
//! - Fresh (TTL > threshold): Serve from cache
//! - Stale (0 < TTL < threshold): Serve from cache, trigger background refresh
//! - Missing: Compute fresh and cache

use std::sync::Arc;
use std::time::Instant;

use tracing::debug;

use crate::config::Config;
use crate::error::Result;
use graze_common::{Keys, RedisClient};

/// Feed skeleton cache manager.
pub struct FeedCache {
    redis: Arc<RedisClient>,
    config: Arc<Config>,
}

impl FeedCache {
    /// Create a new feed cache.
    pub fn new(redis: Arc<RedisClient>, config: Arc<Config>) -> Self {
        Self { redis, config }
    }

    /// Get cached feed skeleton for a user.
    ///
    /// Returns (posts, is_fresh) where is_fresh indicates if a background
    /// refresh should be triggered.
    pub async fn get(
        &self,
        algo_id: i32,
        user_hash: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Option<(Vec<String>, bool)>> {
        if !self.config.feed_cache_enabled {
            debug!(algo_id, user_hash = %&user_hash[..8.min(user_hash.len())], "early_exit: feed cache disabled");
            return Ok(None);
        }

        let key = Keys::feed_cache(algo_id, user_hash);
        let ttl = self.redis.ttl(&key).await?;

        if ttl <= 0 {
            debug!(
                algo_id,
                user_hash = %&user_hash[..8.min(user_hash.len())],
                "feed_cache_miss"
            );
            return Ok(None);
        }

        // Get range from list
        let start = offset as isize;
        let stop = (offset + limit - 1) as isize;
        let posts = self.redis.lrange(&key, start, stop).await?;

        if posts.is_empty() {
            debug!(
                algo_id,
                user_hash = %&user_hash[..8.min(user_hash.len())],
                ttl,
                "early_exit: feed cache empty despite TTL"
            );
            return Ok(None);
        }

        // Determine freshness
        let is_fresh = ttl > self.config.feed_cache_stale_threshold_seconds as i64;

        debug!(
            algo_id,
            user_hash = %&user_hash[..8.min(user_hash.len())],
            offset,
            limit,
            returned = posts.len(),
            ttl,
            is_fresh,
            "feed_cache_hit"
        );

        Ok(Some((posts, is_fresh)))
    }

    /// Store feed skeleton in cache.
    ///
    /// Stores the full ranked list of post URIs for later pagination.
    pub async fn set(&self, algo_id: i32, user_hash: &str, posts: &[String]) -> Result<()> {
        if !self.config.feed_cache_enabled || posts.is_empty() {
            return Ok(());
        }

        let key = Keys::feed_cache(algo_id, user_hash);

        if self.config.read_only_mode {
            // Log skipped write in read-only mode
            tracing::info!(
                target: "graze::read_only",
                operation = "RPUSH",
                key = %key,
                items_count = posts.len(),
                "write_skipped"
            );
            return Ok(());
        }

        let start_time = Instant::now();

        // Store list atomically (DEL + RPUSH + EXPIRE in single pipeline)
        self.redis
            .store_list(&key, posts, self.config.feed_cache_ttl_seconds as i64)
            .await?;

        let elapsed = start_time.elapsed();
        debug!(
            algo_id,
            user_hash = %&user_hash[..8.min(user_hash.len())],
            posts_cached = posts.len(),
            write_time_ms = elapsed.as_millis(),
            ttl_seconds = self.config.feed_cache_ttl_seconds,
            "feed_cache_set"
        );

        Ok(())
    }

    /// Invalidate feed cache for a user.
    pub async fn invalidate(&self, algo_id: i32, user_hash: &str) -> Result<bool> {
        let key = Keys::feed_cache(algo_id, user_hash);
        if self.config.read_only_mode {
            tracing::info!(
                target: "graze::read_only",
                operation = "DEL",
                key = %key,
                "write_skipped"
            );
            return Ok(true);
        }
        self.redis.del(&key).await?;
        debug!(
            algo_id,
            user_hash = %&user_hash[..8.min(user_hash.len())],
            "feed_cache_invalidated"
        );
        Ok(true)
    }

    /// Get cache statistics for a user's feed.
    pub async fn get_stats(&self, algo_id: i32, user_hash: &str) -> Result<FeedCacheStats> {
        let key = Keys::feed_cache(algo_id, user_hash);
        let ttl = self.redis.ttl(&key).await?;
        let length = if ttl > 0 {
            self.redis.llen(&key).await?
        } else {
            0
        };

        Ok(FeedCacheStats {
            exists: ttl > 0,
            ttl_seconds: ttl.max(0) as u64,
            length,
        })
    }
}

/// Statistics about a user's feed cache.
#[derive(Debug, Clone)]
pub struct FeedCacheStats {
    /// Whether the cache exists.
    pub exists: bool,
    /// Remaining TTL in seconds.
    pub ttl_seconds: u64,
    /// Number of posts in the cache.
    pub length: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feed_cache_stats_default() {
        let stats = FeedCacheStats {
            exists: false,
            ttl_seconds: 0,
            length: 0,
        };
        assert!(!stats.exists);
        assert_eq!(stats.ttl_seconds, 0);
        assert_eq!(stats.length, 0);
    }

    #[test]
    fn test_feed_cache_stats_with_data() {
        let stats = FeedCacheStats {
            exists: true,
            ttl_seconds: 300,
            length: 50,
        };
        assert!(stats.exists);
        assert_eq!(stats.ttl_seconds, 300);
        assert_eq!(stats.length, 50);
    }

    #[test]
    fn test_feed_cache_key_format() {
        let key = Keys::feed_cache(123, "userhash");
        assert_eq!(key, "fsc:123:userhash");
    }
}
