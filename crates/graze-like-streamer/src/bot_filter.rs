//! Bot filtering for like ingestion.
//!
//! Detects and filters likes from identities that exceed a configurable
//! like count threshold, indicating bot-like behavior.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tracing::{debug, info, warn};

use crate::config::Config;
use graze_common::Result;
use graze_common::{Keys, RedisClient, DEFAULT_RETENTION_DAYS};

/// Filters likes from suspected bot accounts.
///
/// Uses a two-tier caching strategy:
/// 1. In-memory set of known bot user_hashes (hot cache)
/// 2. In-memory dict of like count estimates for users seen this session
/// 3. Redis backing store for persistence across restarts
pub struct BotFilter {
    redis: Arc<RedisClient>,
    config: Arc<Config>,
    /// In-memory cache of known bot user hashes.
    known_bots: Mutex<HashSet<String>>,
    /// In-memory like count estimates for users seen this session.
    like_counts: Mutex<HashMap<String, u64>>,
    /// Last time we refreshed from Redis.
    last_refresh: Mutex<Instant>,
}

impl BotFilter {
    /// Create a new bot filter.
    pub fn new(redis: Arc<RedisClient>, config: Arc<Config>) -> Self {
        Self {
            redis,
            config,
            known_bots: Mutex::new(HashSet::new()),
            like_counts: Mutex::new(HashMap::new()),
            last_refresh: Mutex::new(Instant::now() - Duration::from_secs(3600)),
        }
    }

    /// Load known bots from Redis on startup.
    pub async fn initialize(&self) -> Result<()> {
        self.refresh_known_bots().await
    }

    /// Refresh known bots set from Redis.
    async fn refresh_known_bots(&self) -> Result<()> {
        match self.redis.zrevrange(Keys::BOT_FILTERED, 0, -1).await {
            Ok(bot_hashes) => {
                let count = bot_hashes.len();
                {
                    let mut known_bots = self.known_bots.lock();
                    *known_bots = bot_hashes.into_iter().collect();
                }
                {
                    let mut last_refresh = self.last_refresh.lock();
                    *last_refresh = Instant::now();
                }
                debug!(known_bots = count, "Bot filter refreshed");
                Ok(())
            }
            Err(e) => {
                warn!(error = %e, "Bot filter refresh failed");
                Err(e)
            }
        }
    }

    /// Check if a user's likes should be filtered.
    ///
    /// Returns true if likes should be filtered (dropped).
    pub async fn should_filter(&self, user_did: &str, user_hash: &str) -> Result<bool> {
        if !self.config.bot_filter_enabled {
            return Ok(false);
        }

        // Check 1: Already known bot (O(1) in-memory)
        {
            let known_bots = self.known_bots.lock();
            if known_bots.contains(user_hash) {
                return Ok(true);
            }
        }

        // Check 2: Check local count estimate
        let local_count = {
            let like_counts = self.like_counts.lock();
            like_counts.get(user_hash).copied().unwrap_or(0)
        };

        if local_count >= self.config.bot_like_threshold {
            // Newly detected bot
            self.mark_as_bot(user_did, user_hash, local_count).await?;
            return Ok(true);
        }

        // Check 3: Periodic Redis sync for unknown users
        let should_refresh = {
            let last_refresh = self.last_refresh.lock();
            last_refresh.elapsed() > Duration::from_secs(self.config.bot_cache_refresh_seconds)
        };

        if should_refresh {
            self.refresh_known_bots().await?;
            // Re-check after refresh
            let known_bots = self.known_bots.lock();
            if known_bots.contains(user_hash) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Increment the local like count estimate for a user.
    ///
    /// Called after a like is successfully stored.
    pub fn increment_count(&self, user_hash: &str, count: u64) {
        let mut like_counts = self.like_counts.lock();
        *like_counts.entry(user_hash.to_string()).or_default() += count;
    }

    /// Check Redis for actual count and update local estimate.
    ///
    /// Used for spot-checking users not seen before.
    pub async fn check_and_update_count(&self, user_hash: &str) -> Result<usize> {
        // Sum cardinalities from all date-based keys within retention window
        let keys: Vec<String> = Keys::user_likes_retention(user_hash, DEFAULT_RETENTION_DAYS);
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let counts = self.redis.zcard_multi(&key_refs).await?;
        let actual_count: usize = counts.into_iter().sum();

        {
            let mut like_counts = self.like_counts.lock();
            like_counts.insert(user_hash.to_string(), actual_count as u64);
        }

        Ok(actual_count)
    }

    /// Mark a user as a bot and log to Redis.
    async fn mark_as_bot(&self, user_did: &str, user_hash: &str, like_count: u64) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Add to in-memory cache immediately
        {
            let mut known_bots = self.known_bots.lock();
            known_bots.insert(user_hash.to_string());
        }

        // Log to Redis - add to filtered set with detection timestamp
        self.redis
            .zadd(Keys::BOT_FILTERED, &[(now as f64, user_hash)])
            .await?;

        // Store metadata in single HSET call
        let meta_key = Keys::bot_meta(user_hash);
        let now_str = now.to_string();
        let like_count_str = like_count.to_string();
        self.redis
            .hset_multiple(
                &meta_key,
                &[
                    ("did", user_did),
                    ("detected_at", &now_str),
                    ("like_count", &like_count_str),
                    ("reason", "threshold_exceeded"),
                ],
            )
            .await?;

        // Set TTL on metadata
        let ttl_seconds = self.config.bot_log_ttl_days as i64 * 24 * 60 * 60;
        self.redis.expire(&meta_key, ttl_seconds).await?;

        info!(
            user_hash = &user_hash[..8],
            like_count = like_count,
            threshold = self.config.bot_like_threshold,
            "Bot detected"
        );

        Ok(())
    }

    /// Check if user is in the known bots set (Redis lookup).
    pub async fn is_known_bot(&self, user_hash: &str) -> Result<bool> {
        // First check in-memory cache
        {
            let known_bots = self.known_bots.lock();
            if known_bots.contains(user_hash) {
                return Ok(true);
            }
        }

        // Check Redis
        let _results = self
            .redis
            .zrevrangebyscore_with_scores(Keys::BOT_FILTERED, f64::INFINITY, f64::NEG_INFINITY, 1)
            .await?;

        // Check if our user_hash is in the set by doing a ZSCORE equivalent
        // We need to check if the member exists
        let script = r#"
            local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
            return score ~= false
        "#;
        let exists: bool = self
            .redis
            .eval(script, &[Keys::BOT_FILTERED], &[user_hash])
            .await
            .unwrap_or(false);

        Ok(exists)
    }

    /// Get statistics about filtered bots.
    pub async fn get_filtered_stats(&self) -> Result<BotFilterStats> {
        let total = self.redis.zcard(Keys::BOT_FILTERED).await?;
        let cache_size = {
            let known_bots = self.known_bots.lock();
            known_bots.len()
        };
        let local_counts_tracked = {
            let like_counts = self.like_counts.lock();
            like_counts.len()
        };

        Ok(BotFilterStats {
            total_filtered_dids: total,
            cache_size,
            local_counts_tracked,
        })
    }

    /// Get recently filtered bot DIDs with metadata.
    pub async fn get_recent_filtered(&self, limit: usize) -> Result<Vec<BotRecord>> {
        // Get most recent by score (timestamp) descending
        let recent = self
            .redis
            .zrevrange_with_scores(Keys::BOT_FILTERED, 0, limit as isize - 1)
            .await?;

        let mut results = Vec::with_capacity(recent.len());

        for (user_hash, detected_at) in recent {
            let meta_key = Keys::bot_meta(&user_hash);
            let meta = self.redis.hgetall(&meta_key).await?;
            let meta_map: HashMap<String, String> = meta.into_iter().collect();

            results.push(BotRecord {
                user_hash,
                detected_at: detected_at as i64,
                did: meta_map.get("did").cloned(),
                like_count: meta_map
                    .get("like_count")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                reason: meta_map.get("reason").cloned(),
            });
        }

        Ok(results)
    }

    /// Get the number of known bots in the in-memory cache.
    pub fn cache_size(&self) -> usize {
        let known_bots = self.known_bots.lock();
        known_bots.len()
    }

    /// Clear the local like counts cache.
    ///
    /// Useful for testing or periodic cleanup.
    pub fn clear_like_counts(&self) {
        let mut like_counts = self.like_counts.lock();
        like_counts.clear();
    }
}

/// Statistics about bot filtering.
#[derive(Debug, Clone)]
pub struct BotFilterStats {
    /// Total number of filtered DIDs in Redis.
    pub total_filtered_dids: usize,
    /// Number of bots in the in-memory cache.
    pub cache_size: usize,
    /// Number of users being tracked locally for like counts.
    pub local_counts_tracked: usize,
}

/// A record of a detected bot.
#[derive(Debug, Clone)]
pub struct BotRecord {
    /// Hashed user DID.
    pub user_hash: String,
    /// Unix timestamp when detected.
    pub detected_at: i64,
    /// Original DID if available.
    pub did: Option<String>,
    /// Like count when detected.
    pub like_count: u64,
    /// Detection reason.
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bot_filter_stats() {
        let stats = BotFilterStats {
            total_filtered_dids: 100,
            cache_size: 50,
            local_counts_tracked: 200,
        };

        assert_eq!(stats.total_filtered_dids, 100);
        assert_eq!(stats.cache_size, 50);
        assert_eq!(stats.local_counts_tracked, 200);
    }

    #[test]
    fn test_bot_record() {
        let record = BotRecord {
            user_hash: "abc123".to_string(),
            detected_at: 1706140800,
            did: Some("did:plc:testuser".to_string()),
            like_count: 5000,
            reason: Some("threshold_exceeded".to_string()),
        };

        assert_eq!(record.user_hash, "abc123");
        assert_eq!(record.detected_at, 1706140800);
        assert_eq!(record.did, Some("did:plc:testuser".to_string()));
        assert_eq!(record.like_count, 5000);
    }
}
