//! Redis client with connection pooling.

use std::sync::Arc;

use deadpool_redis::redis::{self, AsyncCommands};
use deadpool_redis::{Config as PoolConfig, Connection, Pool, Runtime};
use tracing::{info, warn};

use crate::error::{GrazeError, Result};
use crate::redis::{RedisConfig, ScriptManager};

/// Async Redis client with connection pooling and Lua script support.
pub struct RedisClient {
    pool: Pool,
    scripts: Arc<ScriptManager>,
}

impl RedisClient {
    /// Create a new Redis client with connection pooling.
    ///
    /// Retries connection with exponential backoff if Redis is unavailable.
    pub async fn new(config: &RedisConfig) -> Result<Self> {
        let pool_config = PoolConfig::from_url(&config.url);
        let pool = pool_config
            .builder()
            .map_err(|e| GrazeError::Internal(format!("Redis pool config error: {}", e)))?
            .max_size(config.pool_size)
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| GrazeError::Internal(format!("Redis pool build error: {}", e)))?;

        // Retry connection with exponential backoff
        let mut delay_ms = config.connect_initial_delay_ms;
        let max_delay_ms = 5000;
        let mut last_error = None;

        for attempt in 1..=config.connect_max_retries {
            match Self::try_connect(&pool).await {
                Ok(mut conn) => {
                    info!(
                        url = %config.url,
                        pool_size = config.pool_size,
                        "Redis connected"
                    );

                    // Load Lua scripts
                    let scripts = Arc::new(ScriptManager::new());
                    scripts.load_all(&mut conn).await?;

                    return Ok(Self { pool, scripts });
                }
                Err(e) => {
                    let remaining = config.connect_max_retries - attempt;
                    if remaining > 0 {
                        warn!(
                            attempt = attempt,
                            remaining = remaining,
                            delay_ms = delay_ms,
                            error = %e,
                            "Redis connection failed, retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        delay_ms = (delay_ms * 2).min(max_delay_ms);
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            GrazeError::Internal("Redis connection failed after max retries".to_string())
        }))
    }

    /// Attempt to connect to Redis and verify with PING.
    async fn try_connect(pool: &Pool) -> Result<Connection> {
        let mut conn = pool.get().await?;
        redis::cmd("PING").query_async::<()>(&mut conn).await?;
        Ok(conn)
    }

    /// Get a connection from the pool.
    pub async fn get(&self) -> Result<Connection> {
        Ok(self.pool.get().await?)
    }

    /// Check Redis connectivity.
    pub async fn ping(&self) -> bool {
        match self.pool.get().await {
            Ok(mut conn) => redis::cmd("PING")
                .query_async::<()>(&mut conn)
                .await
                .is_ok(),
            Err(_) => false,
        }
    }

    /// Get the script manager for executing Lua scripts.
    pub fn scripts(&self) -> &Arc<ScriptManager> {
        &self.scripts
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Common Redis Operations
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Get TTL of a key in seconds.
    pub async fn ttl(&self, key: &str) -> Result<i64> {
        let mut conn = self.get().await?;
        let ttl: i64 = conn.ttl(key).await?;
        Ok(ttl)
    }

    /// Check if a key exists.
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let mut conn = self.get().await?;
        let exists: bool = conn.exists(key).await?;
        Ok(exists)
    }

    /// Delete a key.
    pub async fn del(&self, key: &str) -> Result<()> {
        let mut conn = self.get().await?;
        let _: () = conn.del(key).await?;
        Ok(())
    }

    /// Delete multiple keys in a single pipeline.
    pub async fn del_multi(&self, keys: &[&str]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.del(*key);
        }
        let _: Vec<()> = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    /// Set a key with expiration.
    pub async fn set_ex(&self, key: &str, value: &str, seconds: u64) -> Result<()> {
        let mut conn = self.get().await?;
        let _: () = conn.set_ex(key, value, seconds).await?;
        Ok(())
    }

    /// Get a string value.
    pub async fn get_string(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.pool.get().await?;
        let value: Option<String> = conn.get(key).await?;
        Ok(value)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Sorted Set Operations
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Get members from a sorted set by score range (descending).
    pub async fn zrevrangebyscore_with_scores(
        &self,
        key: &str,
        max: f64,
        min: f64,
        limit: usize,
    ) -> Result<Vec<(String, f64)>> {
        let mut conn = self.get().await?;
        let results: Vec<(String, f64)> = redis::cmd("ZREVRANGEBYSCORE")
            .arg(key)
            .arg(max)
            .arg(min)
            .arg("WITHSCORES")
            .arg("LIMIT")
            .arg(0)
            .arg(limit)
            .query_async(&mut conn)
            .await?;
        Ok(results)
    }

    /// Get members from a sorted set by score range (ascending).
    pub async fn zrangebyscore_with_scores(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<(String, f64)>> {
        let mut conn = self.get().await?;
        let results: Vec<(String, f64)> = redis::cmd("ZRANGEBYSCORE")
            .arg(key)
            .arg(min)
            .arg(max)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await?;
        Ok(results)
    }

    /// Get members from a sorted set by rank range (descending).
    pub async fn zrevrange_with_scores(
        &self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Vec<(String, f64)>> {
        let mut conn = self.get().await?;
        let results: Vec<(String, f64)> = conn.zrevrange_withscores(key, start, stop).await?;
        Ok(results)
    }

    /// Get the cardinality of a sorted set.
    pub async fn zcard(&self, key: &str) -> Result<usize> {
        let mut conn = self.get().await?;
        let count: usize = conn.zcard(key).await?;
        Ok(count)
    }

    /// Add members to a sorted set.
    pub async fn zadd(&self, key: &str, items: &[(f64, &str)]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.get().await?;
        let _: () = conn.zadd_multiple(key, items).await?;
        Ok(())
    }

    /// Increment a member's score in a sorted set.
    pub async fn zincrby(&self, key: &str, increment: f64, member: &str) -> Result<f64> {
        let mut conn = self.get().await?;
        let new_score: f64 = conn.zincr(key, member, increment).await?;
        Ok(new_score)
    }

    /// Batch increment multiple members' scores in sorted sets.
    /// Takes a list of (key, member, increment) tuples.
    pub async fn zincrby_multi(&self, items: &[(&str, &str, i64)]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for (key, member, increment) in items {
            pipe.zincr(*key, *member, *increment);
        }
        let _: Vec<f64> = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    /// Set expiration on a key.
    pub async fn expire(&self, key: &str, seconds: i64) -> Result<()> {
        let mut conn = self.get().await?;
        let _: () = conn.expire(key, seconds).await?;
        Ok(())
    }

    /// Remove members from a sorted set by rank range.
    ///
    /// Negative indices count from the end (-1 is the last element).
    /// This is used for size capping: `zremrangebyrank(key, 0, -(max+1))` keeps only the top `max` elements.
    pub async fn zremrangebyrank(&self, key: &str, start: isize, stop: isize) -> Result<usize> {
        let mut conn = self.get().await?;
        let removed: usize = conn.zremrangebyrank(key, start, stop).await?;
        Ok(removed)
    }

    /// Remove members from a sorted set by score range.
    ///
    /// This is used for time-based pruning: `zremrangebyscore(key, -inf, cutoff_time)`
    /// removes all entries older than the cutoff.
    pub async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<usize> {
        let mut conn = self.get().await?;
        let removed: usize = conn.zrembyscore(key, min, max).await?;
        Ok(removed)
    }

    /// Batch remove old entries from multiple sorted sets by score.
    /// Returns total number of entries removed.
    pub async fn zremrangebyscore_multi(&self, keys: &[&str], min: f64, max: f64) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.zrembyscore(*key, min, max);
        }
        let results: Vec<usize> = pipe.query_async(&mut conn).await?;
        Ok(results.into_iter().sum())
    }

    /// Batch ZADD to multiple sorted sets in a single pipeline.
    /// Takes a list of (key, items) where items are (score, member) pairs.
    pub async fn zadd_multi(&self, items: &[(&str, &[(f64, &str)])]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for (key, members) in items {
            if !members.is_empty() {
                pipe.zadd_multiple(*key, members);
            }
        }
        let _: Vec<()> = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    /// Batch EXPIRE on multiple keys in a single pipeline.
    pub async fn expire_multi(&self, keys: &[&str], seconds: i64) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.expire(*key, seconds);
        }
        let _: Vec<bool> = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    /// Store a sorted set atomically: DEL + ZADD + EXPIRE in a single pipeline.
    /// This reduces 3 round trips to 1.
    pub async fn store_sorted_set(
        &self,
        key: &str,
        items: &[(f64, &str)],
        ttl_seconds: i64,
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        pipe.del(key);
        pipe.zadd_multiple(key, items);
        pipe.expire(key, ttl_seconds);
        let _: ((), (), bool) = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    /// Store a sorted set with a companion timestamp key atomically.
    /// DEL + ZADD + EXPIRE + SET_EX in a single pipeline (4 RTT → 1).
    pub async fn store_sorted_set_with_timestamp(
        &self,
        key: &str,
        items: &[(f64, &str)],
        ttl_seconds: u64,
        timestamp_key: &str,
        timestamp_value: &str,
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        pipe.del(key);
        pipe.zadd_multiple(key, items);
        pipe.expire(key, ttl_seconds as i64);
        pipe.cmd("SET")
            .arg(timestamp_key)
            .arg(timestamp_value)
            .arg("EX")
            .arg(ttl_seconds);
        let _: ((), (), bool, ()) = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    /// Batch ZREMRANGEBYRANK on multiple keys in a single pipeline.
    /// Used for size capping sorted sets.
    pub async fn zremrangebyrank_multi(&self, items: &[(&str, isize, isize)]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for (key, start, stop) in items {
            pipe.zremrangebyrank(*key, *start, *stop);
        }
        let _: Vec<usize> = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    /// Get members from a sorted set by rank range (descending, without scores).
    pub async fn zrevrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<String>> {
        let mut conn = self.get().await?;
        let members: Vec<String> = conn.zrevrange(key, start, stop).await?;
        Ok(members)
    }

    /// Batch ZCARD on multiple keys in a single pipeline.
    /// Returns cardinalities in the same order as input keys.
    pub async fn zcard_multi(&self, keys: &[&str]) -> Result<Vec<usize>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.zcard(*key);
        }
        let results: Vec<usize> = pipe.query_async(&mut conn).await?;
        Ok(results)
    }

    /// Batch ZCARD summed across day-tranched keys for multiple entities.
    ///
    /// For each entity, queries all its day-tranche keys and returns the sum.
    /// This is optimized for post_likers where we need total liker counts.
    ///
    /// # Arguments
    /// * `key_groups` - List of key groups, where each group contains all day-tranche keys for one entity
    ///
    /// # Returns
    /// Sum of cardinalities for each entity
    pub async fn zcard_summed_multi(&self, key_groups: &[Vec<String>]) -> Result<Vec<usize>> {
        if key_groups.is_empty() {
            return Ok(Vec::new());
        }

        // Flatten all keys for a single pipeline call
        let mut all_keys: Vec<&str> = Vec::new();
        let mut group_sizes: Vec<usize> = Vec::new();

        for group in key_groups {
            group_sizes.push(group.len());
            for key in group {
                all_keys.push(key.as_str());
            }
        }

        // Fetch all cardinalities in one pipeline
        let all_counts = self.zcard_multi(&all_keys).await?;

        // Sum counts for each group
        let mut count_iter = all_counts.into_iter();
        let mut sums: Vec<usize> = Vec::with_capacity(key_groups.len());

        for group_size in group_sizes {
            let group_sum: usize = count_iter.by_ref().take(group_size).sum();
            sums.push(group_sum);
        }

        Ok(sums)
    }

    /// Batch ZREVRANGEBYSCORE with scores on multiple keys in a single pipeline.
    /// Returns results in the same order as input keys.
    pub async fn zrevrangebyscore_with_scores_multi(
        &self,
        requests: &[(&str, f64, f64, usize)], // (key, max, min, limit)
    ) -> Result<Vec<Vec<(String, f64)>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for (key, max, min, limit) in requests {
            pipe.cmd("ZREVRANGEBYSCORE")
                .arg(*key)
                .arg(*max)
                .arg(*min)
                .arg("WITHSCORES")
                .arg("LIMIT")
                .arg(0)
                .arg(*limit);
        }
        let results: Vec<Vec<(String, f64)>> = pipe.query_async(&mut conn).await?;
        Ok(results)
    }

    /// Batch ZREVRANGE with scores on multiple keys in a single pipeline.
    /// Returns results in the same order as input keys.
    pub async fn zrevrange_with_scores_multi(
        &self,
        requests: &[(&str, isize, isize)], // (key, start, stop)
    ) -> Result<Vec<Vec<(String, f64)>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for (key, start, stop) in requests {
            pipe.zrevrange_withscores(*key, *start, *stop);
        }
        let results: Vec<Vec<(String, f64)>> = pipe.query_async(&mut conn).await?;
        Ok(results)
    }

    /// Batch ZCOUNT on multiple keys in a single pipeline.
    /// Returns counts in the same order as input keys.
    pub async fn zcount_multi(
        &self,
        requests: &[(&str, f64, f64)], // (key, min, max)
    ) -> Result<Vec<usize>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for (key, min, max) in requests {
            pipe.cmd("ZCOUNT").arg(*key).arg(*min).arg(*max);
        }
        let results: Vec<usize> = pipe.query_async(&mut conn).await?;
        Ok(results)
    }

    /// Query multiple day-tranched keys and merge results by member (keeping highest score).
    ///
    /// This is the primary read pattern for day-tranched data. It:
    /// 1. Queries all provided keys with the same score range in a single pipeline
    /// 2. Merges results by member, keeping the highest (most recent) score
    /// 3. Sorts by score descending and returns up to `limit` entries
    ///
    /// # Arguments
    /// * `keys` - List of day-tranche keys to query (e.g., ["ul:abc:d0", "ul:abc:d1", ...])
    /// * `max` - Maximum score (typically `now` for timestamps)
    /// * `min` - Minimum score (typically `now - time_window`)
    /// * `limit` - Maximum entries to return (applied after merging)
    pub async fn zrevrangebyscore_merged(
        &self,
        keys: &[String],
        max: f64,
        min: f64,
        limit: usize,
    ) -> Result<Vec<(String, f64)>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // Build requests for all keys
        let requests: Vec<(&str, f64, f64, usize)> = keys
            .iter()
            .map(|k| (k.as_str(), max, min, limit * 2)) // Fetch more than needed since we merge
            .collect();

        // Fetch from all keys in parallel
        let all_results = self.zrevrangebyscore_with_scores_multi(&requests).await?;

        // Merge results by member, keeping highest score (most recent)
        let mut merged: std::collections::HashMap<String, f64> =
            std::collections::HashMap::with_capacity(limit * 2);
        for results in all_results {
            for (member, score) in results {
                merged
                    .entry(member)
                    .and_modify(|s| {
                        if score > *s {
                            *s = score;
                        }
                    })
                    .or_insert(score);
            }
        }

        // Sort by score descending and limit
        let mut sorted: Vec<(String, f64)> = merged.into_iter().collect();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        sorted.truncate(limit);

        Ok(sorted)
    }

    /// Query multiple day-tranched keys for multiple entities and merge results.
    ///
    /// This is used for batch operations like fetching co-likers for multiple posts.
    /// For each entity's keys, merges results and returns a Vec of merged results.
    ///
    /// # Arguments
    /// * `key_groups` - List of key groups, where each group is the day-tranche keys for one entity
    /// * `max` - Maximum score
    /// * `min` - Minimum score
    /// * `limit_per_entity` - Maximum entries per entity after merging
    pub async fn zrevrangebyscore_merged_multi(
        &self,
        key_groups: &[Vec<String>],
        max: f64,
        min: f64,
        limit_per_entity: usize,
    ) -> Result<Vec<Vec<(String, f64)>>> {
        if key_groups.is_empty() {
            return Ok(Vec::new());
        }

        // Flatten all keys for a single pipeline call
        let mut all_requests: Vec<(&str, f64, f64, usize)> = Vec::new();
        let mut group_sizes: Vec<usize> = Vec::new();

        for group in key_groups {
            group_sizes.push(group.len());
            for key in group {
                all_requests.push((key.as_str(), max, min, limit_per_entity * 2));
            }
        }

        // Fetch all in one pipeline
        let all_results = self
            .zrevrangebyscore_with_scores_multi(&all_requests)
            .await?;

        // Split results back into groups and merge each group
        let mut result_iter = all_results.into_iter();
        let mut merged_groups: Vec<Vec<(String, f64)>> = Vec::with_capacity(key_groups.len());

        for group_size in group_sizes {
            let mut merged: std::collections::HashMap<String, f64> =
                std::collections::HashMap::with_capacity(limit_per_entity * 2);

            for _ in 0..group_size {
                if let Some(results) = result_iter.next() {
                    for (member, score) in results {
                        merged
                            .entry(member)
                            .and_modify(|s| {
                                if score > *s {
                                    *s = score;
                                }
                            })
                            .or_insert(score);
                    }
                }
            }

            // Sort and limit this group
            let mut sorted: Vec<(String, f64)> = merged.into_iter().collect();
            sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            sorted.truncate(limit_per_entity);
            merged_groups.push(sorted);
        }

        Ok(merged_groups)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Set Operations
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Check if a member exists in a set.
    pub async fn sismember(&self, key: &str, member: &str) -> Result<bool> {
        let mut conn = self.get().await?;
        let is_member: bool = conn.sismember(key, member).await?;
        Ok(is_member)
    }

    /// Get all members of a set.
    pub async fn smembers(&self, key: &str) -> Result<Vec<String>> {
        let mut conn = self.get().await?;
        let members: Vec<String> = conn.smembers(key).await?;
        Ok(members)
    }

    /// Get random members from a set.
    pub async fn srandmember(&self, key: &str, count: usize) -> Result<Vec<String>> {
        let mut conn = self.get().await?;
        let members: Vec<String> = redis::cmd("SRANDMEMBER")
            .arg(key)
            .arg(count)
            .query_async(&mut conn)
            .await?;
        Ok(members)
    }

    /// Add members to a set.
    pub async fn sadd(&self, key: &str, members: &[String]) -> Result<usize> {
        if members.is_empty() {
            return Ok(0);
        }
        let mut conn = self.get().await?;
        let added: usize = conn.sadd(key, members).await?;
        Ok(added)
    }

    /// Remove members from a set.
    pub async fn srem(&self, key: &str, members: &[String]) -> Result<usize> {
        if members.is_empty() {
            return Ok(0);
        }
        let mut conn = self.get().await?;
        let removed: usize = conn.srem(key, members).await?;
        Ok(removed)
    }

    /// Get the cardinality (size) of a set.
    pub async fn scard(&self, key: &str) -> Result<usize> {
        let mut conn = self.get().await?;
        let count: usize = conn.scard(key).await?;
        Ok(count)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Hash Operations
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Get a field from a hash.
    pub async fn hget(&self, key: &str, field: &str) -> Result<Option<String>> {
        let mut conn = self.get().await?;
        let value: Option<String> = conn.hget(key, field).await?;
        Ok(value)
    }

    /// Get all fields and values from a hash.
    pub async fn hgetall(&self, key: &str) -> Result<Vec<(String, String)>> {
        let mut conn = self.get().await?;
        let pairs: Vec<(String, String)> = conn.hgetall(key).await?;
        Ok(pairs)
    }

    /// Set a field in a hash.
    pub async fn hset(&self, key: &str, field: &str, value: &str) -> Result<()> {
        let mut conn = self.get().await?;
        let _: () = conn.hset(key, field, value).await?;
        Ok(())
    }

    /// Set multiple fields in a hash at once.
    pub async fn hset_multiple(&self, key: &str, items: &[(&str, &str)]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.get().await?;
        let _: () = conn.hset_multiple(key, items).await?;
        Ok(())
    }

    /// Get multiple fields from a hash.
    pub async fn hmget(&self, key: &str, fields: &[&str]) -> Result<Vec<Option<String>>> {
        let mut conn = self.get().await?;
        let values: Vec<Option<String>> = conn.hget(key, fields).await?;
        Ok(values)
    }

    /// Get the number of fields in a hash.
    pub async fn hlen(&self, key: &str) -> Result<usize> {
        let mut conn = self.get().await?;
        let len: usize = conn.hlen(key).await?;
        Ok(len)
    }

    /// Delete a field from a hash.
    pub async fn hdel(&self, key: &str, field: &str) -> Result<usize> {
        let mut conn = self.get().await?;
        let deleted: usize = conn.hdel(key, field).await?;
        Ok(deleted)
    }

    /// Increment a key's value by 1.
    pub async fn incr(&self, key: &str) -> Result<i64> {
        let mut conn = self.get().await?;
        let value: i64 = conn.incr(key, 1).await?;
        Ok(value)
    }

    /// Increment a hash field by a given amount.
    pub async fn hincrby(&self, key: &str, field: &str, increment: i64) -> Result<i64> {
        let mut conn = self.get().await?;
        let value: i64 = conn.hincr(key, field, increment).await?;
        Ok(value)
    }

    /// Batch increment multiple hash fields by 1.
    /// Returns the number of fields incremented.
    pub async fn hincrby_multi(&self, key: &str, fields: &[&str]) -> Result<usize> {
        if fields.is_empty() {
            return Ok(0);
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        for field in fields {
            pipe.hincr(key, *field, 1i64);
        }
        let _: Vec<i64> = pipe.query_async(&mut conn).await?;
        Ok(fields.len())
    }

    /// Execute an inline Lua script.
    ///
    /// This is for simple scripts that don't need to be pre-loaded.
    pub async fn eval<T: redis::FromRedisValue>(
        &self,
        script: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<T> {
        let mut conn = self.get().await?;
        let result: T = redis::cmd("EVAL")
            .arg(script)
            .arg(keys.len())
            .arg(keys)
            .arg(args)
            .query_async(&mut conn)
            .await?;
        Ok(result)
    }

    /// Get a string value and set expiration on access.
    pub async fn getex(&self, key: &str, seconds: u64) -> Result<Option<String>> {
        let mut conn = self.get().await?;
        let value: Option<String> = redis::cmd("GETEX")
            .arg(key)
            .arg("EX")
            .arg(seconds)
            .query_async(&mut conn)
            .await?;
        Ok(value)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // List Operations
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Get a range of elements from a list.
    pub async fn lrange(&self, key: &str, start: isize, stop: isize) -> Result<Vec<String>> {
        let mut conn = self.get().await?;
        let values: Vec<String> = conn.lrange(key, start, stop).await?;
        Ok(values)
    }

    /// Prepend a value to a list.
    pub async fn lpush(&self, key: &str, value: &str) -> Result<usize> {
        let mut conn = self.get().await?;
        let len: usize = conn.lpush(key, value).await?;
        Ok(len)
    }

    /// Append values to a list.
    pub async fn rpush(&self, key: &str, values: &[String]) -> Result<usize> {
        if values.is_empty() {
            return Ok(0);
        }
        let mut conn = self.get().await?;
        let len: usize = conn.rpush(key, values).await?;
        Ok(len)
    }

    /// Get the length of a list.
    pub async fn llen(&self, key: &str) -> Result<usize> {
        let mut conn = self.get().await?;
        let len: usize = conn.llen(key).await?;
        Ok(len)
    }

    /// Store a list atomically: DEL + RPUSH + EXPIRE in a single pipeline.
    /// This reduces 3 round trips to 1.
    pub async fn store_list(&self, key: &str, items: &[String], ttl_seconds: i64) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.get().await?;
        let mut pipe = redis::pipe();
        pipe.del(key);
        pipe.rpush(key, items);
        pipe.expire(key, ttl_seconds);
        let _: ((), usize, bool) = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Key Scanning Operations
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Scan keys matching a pattern.
    ///
    /// Returns (new_cursor, keys). When new_cursor is 0, the scan is complete.
    /// Use for iterating through large keyspaces without blocking Redis.
    pub async fn scan(
        &self,
        cursor: u64,
        pattern: &str,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let mut conn = self.get().await?;
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut conn)
            .await?;
        Ok((new_cursor, keys))
    }
}
