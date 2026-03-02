//! Background cleanup worker for maintaining rolling time windows.
//!
//! NOTE: With day-tranched keys (ul:{hash}:d{0-7}, pl:{id}:d{0-7}, authl:{hash}:d{0-7}),
//! this worker is NO LONGER NEEDED. TTL on day-tranche keys handles expiration automatically.
//!
//! This worker remains for backwards compatibility but performs no actual cleanup.
//! It will log that cleanup is skipped due to day-tranched key architecture.
//!
//! Legacy behavior (now disabled):
//! 1. `ul:{hash}` (user likes) - Only contains likes from the last N days
//! 2. `pl:{post_id}` (post likers) - Only contains likers from the last N days
//! 3. `authl:{hash}` (author likers) - Only contains likers from the last N days

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info};

use crate::config::Config;
use graze_common::RedisClient;
use graze_common::Result;

#[allow(dead_code)]
/// Statistics from a cleanup run.
#[derive(Debug, Default)]
pub struct CleanupStats {
    pub keys_scanned: usize,
    pub keys_pruned: usize,
    pub entries_removed: usize,
    pub duration_ms: u64,
}

/// Background cleanup worker for pruning old entries from sorted sets.
pub struct CleanupWorker {
    #[allow(dead_code)]
    redis: Arc<RedisClient>,
    config: Arc<Config>,
}

impl CleanupWorker {
    /// Create a new cleanup worker.
    pub fn new(redis: Arc<RedisClient>, config: Arc<Config>) -> Self {
        Self { redis, config }
    }

    /// Run the cleanup worker in a loop.
    ///
    /// NOTE: With day-tranched keys, this worker does nothing.
    /// TTL handles expiration automatically.
    pub async fn run(&self, interval_hours: u64) -> Result<()> {
        let interval = Duration::from_secs(interval_hours * 3600);

        info!(
            interval_hours,
            like_ttl_days = self.config.like_ttl_days,
            "Cleanup worker started (day-tranched mode - TTL handles expiration)"
        );

        loop {
            info!("Cleanup skipped - day-tranched keys use TTL for automatic expiration");
            tokio::time::sleep(interval).await;
        }
    }

    /// Run a single cleanup pass.
    ///
    /// NOTE: With day-tranched keys, this does nothing. TTL handles expiration.
    pub async fn run_cleanup(&self) -> Result<CleanupStats> {
        let start = Instant::now();
        let stats = CleanupStats::default();

        info!(
            like_ttl_days = self.config.like_ttl_days,
            "Cleanup skipped - day-tranched keys use TTL for automatic expiration"
        );

        // No cleanup needed - TTL handles everything
        // Return empty stats
        Ok(CleanupStats {
            duration_ms: start.elapsed().as_millis() as u64,
            ..stats
        })
    }

    /// Cleanup a single key pattern.
    #[allow(dead_code)]
    async fn cleanup_key_pattern(&self, pattern: &str, cutoff: f64) -> Result<CleanupStats> {
        let mut stats = CleanupStats::default();
        let mut cursor = 0u64;
        let batch_size = 1000;
        let yield_interval = Duration::from_millis(10);

        debug!(pattern, "Starting cleanup for pattern");

        loop {
            // Scan for keys matching pattern
            let (new_cursor, keys) = self.redis.scan(cursor, pattern, batch_size).await?;

            stats.keys_scanned += keys.len();

            // Process keys in batches to avoid overwhelming Redis
            for key in &keys {
                let removed = self
                    .redis
                    .zremrangebyscore(key, f64::NEG_INFINITY, cutoff)
                    .await?;

                if removed > 0 {
                    stats.keys_pruned += 1;
                    stats.entries_removed += removed;
                }
            }

            // Yield to other tasks periodically
            if stats.keys_scanned % 10000 == 0 && stats.keys_scanned > 0 {
                debug!(
                    pattern,
                    keys_scanned = stats.keys_scanned,
                    entries_removed = stats.entries_removed,
                    "Cleanup progress"
                );
                tokio::time::sleep(yield_interval).await;
            }

            cursor = new_cursor;
            if cursor == 0 {
                break;
            }

            // Small yield between batches to avoid blocking
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        debug!(
            pattern,
            keys_scanned = stats.keys_scanned,
            keys_pruned = stats.keys_pruned,
            entries_removed = stats.entries_removed,
            "Cleanup complete for pattern"
        );

        Ok(stats)
    }

    /// Run cleanup once and exit (for manual/cron invocation).
    pub async fn run_once(&self) -> Result<CleanupStats> {
        self.run_cleanup().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cleanup_stats_default() {
        let stats = CleanupStats::default();
        assert_eq!(stats.keys_scanned, 0);
        assert_eq!(stats.keys_pruned, 0);
        assert_eq!(stats.entries_removed, 0);
        assert_eq!(stats.duration_ms, 0);
    }
}
