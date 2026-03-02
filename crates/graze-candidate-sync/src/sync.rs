//! Algorithm candidate synchronization.
//!
//! This worker:
//! 1. Monitors the sync request queue in Redis
//! 2. Fetches algorithm post candidates from a configurable source (ClickHouse, HTTP, or admin-only)
//! 3. Atomically swaps post sets in Redis
//! 4. Implements rate limiting to prevent excessive syncs
//! 5. Syncs fallback tranches (trending, popular, velocity, discovery)
//! 6. Syncs algo likers (1-hop neighborhood) for source pruning

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, error, info, warn};

use deadpool_redis::redis;

use crate::config::Config;
use graze_common::services::UriInterner;
use graze_common::{CandidateSource, Keys, RedisClient, Result, DEFAULT_RETENTION_DAYS};

/// Syncs algorithm candidates from a configurable source to Redis.
pub struct CandidateSync {
    redis: Arc<RedisClient>,
    interner: Arc<UriInterner>,
    config: Arc<Config>,
    source: Arc<dyn CandidateSource>,
}

impl CandidateSync {
    /// Create a new candidate sync worker.
    pub fn new(
        redis: Arc<RedisClient>,
        interner: Arc<UriInterner>,
        config: Arc<Config>,
        source: Arc<dyn CandidateSource>,
    ) -> Self {
        Self {
            redis,
            interner,
            config,
            source,
        }
    }

    /// Run the candidate sync worker.
    ///
    /// Runs two concurrent loops:
    /// 1. Queue-based sync: Listens for explicit sync requests on SYNC_QUEUE
    /// 2. Rolling access sync: Periodically scans feed:access HSET for active feeds
    pub async fn run(&self) -> Result<()> {
        info!(
            candidate_source = %self.config.candidate_source,
            feed_access_sync_enabled = self.config.feed_access_sync_enabled,
            "Starting candidate sync worker"
        );

        if self.config.feed_access_sync_enabled {
            // Run both loops concurrently
            tokio::select! {
                result = self.run_queue_loop() => result,
                result = self.run_rolling_sync_loop() => result,
            }
        } else {
            // Only run queue-based sync
            self.run_queue_loop().await
        }
    }

    /// Run the queue-based sync loop.
    async fn run_queue_loop(&self) -> Result<()> {
        loop {
            match self.process_queue().await {
                Ok(()) => {}
                Err(e) => {
                    error!(error = %e, "Queue sync error");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Run the rolling access sync loop.
    async fn run_rolling_sync_loop(&self) -> Result<()> {
        let interval = Duration::from_secs(self.config.feed_access_scan_interval_seconds);

        loop {
            tokio::time::sleep(interval).await;

            match self.process_feed_access().await {
                Ok(()) => {}
                Err(e) => {
                    error!(error = %e, "Rolling sync error");
                }
            }
        }
    }

    /// Process feeds from the rolling access HSET.
    ///
    /// Scans feed:access for entries within the rolling window and syncs those algorithms.
    /// Removes stale entries outside the window.
    async fn process_feed_access(&self) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let cutoff = now - self.config.feed_access_window_seconds;

        // Get all entries from the HSET
        let entries = self.redis.hgetall(Keys::FEED_ACCESS).await?;

        if entries.is_empty() {
            return Ok(());
        }

        let mut stale_fields: Vec<String> = Vec::new();
        let mut algos_to_sync: Vec<i32> = Vec::new();

        for (field, value) in entries {
            let algo_id: i32 = match field.parse() {
                Ok(id) => id,
                Err(_) => {
                    warn!(field = %field, "invalid_algo_id_in_feed_access");
                    stale_fields.push(field);
                    continue;
                }
            };

            let timestamp: u64 = value.parse().unwrap_or(0);

            if timestamp < cutoff {
                // Entry is outside rolling window - mark for cleanup
                stale_fields.push(field);
            } else {
                // Entry is within window - schedule sync
                algos_to_sync.push(algo_id);
            }
        }

        // Clean up stale entries
        for field in &stale_fields {
            if let Err(e) = self.redis.hdel(Keys::FEED_ACCESS, field).await {
                warn!(field = %field, error = %e, "failed_to_remove_stale_feed_access");
            }
        }
        if !stale_fields.is_empty() {
            debug!(
                removed_count = stale_fields.len(),
                "cleaned_stale_feed_access"
            );
        }

        // Deduplicate algo_ids
        algos_to_sync.sort_unstable();
        algos_to_sync.dedup();

        // Sync each algorithm (respecting rate limits)
        for algo_id in algos_to_sync {
            // Check rate limit
            let lock_key = Keys::sync_lock(algo_id);
            let acquired: bool = {
                let mut conn = self.redis.get().await?;
                redis::cmd("SET")
                    .arg(&lock_key)
                    .arg("1")
                    .arg("NX")
                    .arg("EX")
                    .arg(self.config.sync_rate_limit_seconds)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(false)
            };

            if acquired {
                info!(algo_id, source = "rolling_sync", "sync_starting");
                if let Err(e) = self.sync_algorithm(algo_id).await {
                    error!(algo_id, error = %e, "rolling_sync_failed");
                }
            } else {
                debug!(algo_id, "rolling_sync_rate_limited");
            }
        }

        Ok(())
    }

    /// Process sync requests from the queue.
    async fn process_queue(&self) -> Result<()> {
        // Block waiting for sync requests (with timeout)
        // Use BRPOP via raw command
        let mut conn = self.redis.get().await?;
        let result: Option<(String, String)> = redis::cmd("BRPOP")
            .arg(Keys::SYNC_QUEUE)
            .arg(5) // 5 second timeout
            .query_async(&mut conn)
            .await?;

        let (_, algo_id_str) = match result {
            Some(r) => r,
            None => return Ok(()), // Timeout, no requests
        };

        let algo_id: i32 = algo_id_str.parse().unwrap_or(0);
        if algo_id == 0 {
            warn!(algo_id_str = %algo_id_str, "Invalid algo_id in sync queue");
            return Ok(());
        }

        // Remove from pending set so it can be queued again later
        let mut conn = self.redis.get().await?;
        let _: () = redis::cmd("SREM")
            .arg("pending:syncs")
            .arg(&algo_id_str)
            .query_async(&mut conn)
            .await?;

        // Check if data exists for this algorithm
        let posts_key = Keys::algo_posts(algo_id);
        let has_data = self.redis.exists(&posts_key).await?;

        // Check rate limit (but bypass if no data exists - critical sync needed)
        let lock_key = Keys::sync_lock(algo_id);
        let acquired: bool = {
            let mut conn = self.redis.get().await?;
            redis::cmd("SET")
                .arg(&lock_key)
                .arg("1")
                .arg("NX")
                .arg("EX")
                .arg(self.config.sync_rate_limit_seconds)
                .query_async(&mut conn)
                .await
                .unwrap_or(false)
        };

        if !acquired && has_data {
            debug!(algo_id = algo_id, "Sync rate limited");
            return Ok(());
        } else if !acquired {
            info!(
                algo_id = algo_id,
                reason = "no_data",
                "Sync bypassing rate limit"
            );
        }

        // Perform sync
        self.sync_algorithm(algo_id).await
    }

    /// Sync posts for a specific algorithm.
    async fn sync_algorithm(&self, algo_id: i32) -> Result<()> {
        let start_time = Instant::now();
        info!(algo_id = algo_id, "Sync starting");

        // Fetch candidates from configured source (ClickHouse, HTTP, or admin-only)
        let posts = match self.source.fetch_candidates(algo_id).await {
            Ok(p) => p,
            Err(e) => {
                error!(algo_id = algo_id, error = %e, "Sync failed");
                return Err(e);
            }
        };

        if posts.is_empty() {
            warn!(algo_id = algo_id, "No posts found");
            return Ok(());
        }

        // Intern all URIs
        let uri_to_id = self.interner.get_or_create_ids_batch(&posts).await?;

        // Store posts in Redis (atomic swap)
        let post_ids: Vec<i64> = uri_to_id.values().copied().collect();
        self.store_posts(algo_id, &post_ids).await?;

        let duration = start_time.elapsed();
        info!(
            algo_id = algo_id,
            post_count = posts.len(),
            duration_ms = duration.as_millis(),
            "Sync completed"
        );

        // Sync fallback tranches after algorithm posts are stored
        // These all run independently and failures don't affect main sync
        self.sync_all_fallback_tranches(algo_id).await;

        // Sync algo likers (1-hop neighborhood) for source pruning optimization
        if self.config.algo_likers_enabled {
            if let Err(e) = self.sync_algo_likers(algo_id).await {
                warn!(algo_id = algo_id, error = %e, "Algo likers sync failed");
            }
        }

        Ok(())
    }

    /// Store algorithm posts in Redis with atomic swap.
    ///
    /// Also fetches and stores liker counts for each post to avoid
    /// ZCARD calls during scoring (major performance optimization).
    async fn store_posts(&self, algo_id: i32, post_ids: &[i64]) -> Result<()> {
        let posts_key = Keys::algo_posts(algo_id);
        let counts_key = Keys::algo_posts_counts(algo_id);
        let meta_key = Keys::algo_meta(algo_id);
        let temp_key = format!("{}:temp", posts_key);
        let temp_counts_key = format!("{}:temp", counts_key);

        if post_ids.is_empty() {
            return Ok(());
        }

        // Convert IDs to strings for Redis
        let str_ids: Vec<String> = post_ids.iter().map(|id| id.to_string()).collect();

        // Fetch liker counts using pipelined ZCARD across date-based keys
        let post_liker_key_groups: Vec<Vec<String>> = str_ids
            .iter()
            .map(|id| Keys::post_likers_retention(id, DEFAULT_RETENTION_DAYS))
            .collect();
        let counts = self
            .redis
            .zcard_summed_multi(&post_liker_key_groups)
            .await?;

        let liker_counts: HashMap<String, usize> =
            str_ids.iter().cloned().zip(counts.into_iter()).collect();

        let ttl_seconds = self.config.algo_posts_ttl_hours as i64 * 60 * 60;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Execute all operations in a single pipeline (10 RTT → 1)
        let mut conn = self.redis.get().await?;
        let mut pipe = redis::pipe();

        // Delete temp keys
        pipe.del(&temp_key);
        pipe.del(&temp_counts_key);

        // Store posts in temp key (SADD)
        pipe.cmd("SADD").arg(&temp_key).arg(&str_ids);

        // Store liker counts in temp hash (HSET)
        if !liker_counts.is_empty() {
            let hset_cmd = pipe.cmd("HSET").arg(&temp_counts_key);
            for (post_id, count) in &liker_counts {
                hset_cmd.arg(post_id).arg(count.to_string());
            }
        }

        // Atomic rename (swap) posts
        pipe.cmd("RENAME").arg(&temp_key).arg(&posts_key);
        pipe.expire(&posts_key, ttl_seconds);

        // Atomic rename (swap) counts
        pipe.cmd("RENAME").arg(&temp_counts_key).arg(&counts_key);
        pipe.expire(&counts_key, ttl_seconds);

        // Update metadata
        pipe.cmd("HSET")
            .arg(&meta_key)
            .arg("last_sync")
            .arg(now.to_string())
            .arg("post_count")
            .arg(post_ids.len().to_string());
        pipe.expire(&meta_key, ttl_seconds);

        // Execute pipeline
        let _: Vec<redis::Value> = pipe.query_async(&mut conn).await?;

        debug!(
            algo_id = algo_id,
            post_count = post_ids.len(),
            with_liker_counts = liker_counts.len(),
            "Algo posts stored"
        );

        Ok(())
    }

    /// Sync all fallback tranches for an algorithm.
    ///
    /// Runs popular, velocity, and discovery syncs. Each one catches
    /// its own exceptions to prevent cascading failures.
    async fn sync_all_fallback_tranches(&self, algo_id: i32) {
        // Sync legacy trending
        if let Err(e) = self.sync_trending_posts(algo_id).await {
            warn!(algo_id = algo_id, error = %e, "Trending sync failed");
        }

        // Sync popular
        if let Err(e) = self.sync_popular_posts(algo_id).await {
            warn!(algo_id = algo_id, error = %e, "Popular sync failed");
        }

        // Sync velocity
        if let Err(e) = self.sync_velocity_posts(algo_id).await {
            warn!(algo_id = algo_id, error = %e, "Velocity sync failed");
        }

        // Sync author success and discovery
        if let Err(e) = self.sync_author_success_and_discovery(algo_id).await {
            warn!(algo_id = algo_id, error = %e, "Discovery sync failed");
        }
    }

    /// Sync trending posts for an algorithm based on engagement.
    ///
    /// Engagement score = like_count * recency_factor
    /// where recency_factor = exp(-0.693 * age_hours / decay_hours)
    async fn sync_trending_posts(&self, algo_id: i32) -> Result<()> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Get all posts from algorithm set
        let algo_posts_key = Keys::algo_posts(algo_id);
        let post_ids = self.redis.smembers(&algo_posts_key).await?;

        if post_ids.is_empty() {
            debug!(algo_id = algo_id, "Trending sync - no posts");
            return Ok(());
        }

        let half_life_seconds = (self.config.trending_recency_decay_hours * 3600.0) as i64;
        let mut scored_posts: Vec<(String, f64)> = Vec::new();
        let batch_size = 500;

        // Process in batches with pipelined Redis calls
        let today = graze_common::today_date();
        for batch in post_ids.chunks(batch_size) {
            // Build date-based key groups for this batch
            let pl_key_groups: Vec<Vec<String>> = batch
                .iter()
                .map(|id| Keys::post_likers_retention(id, DEFAULT_RETENTION_DAYS))
                .collect();

            // Pipelined ZCARD summed across date-based keys
            let like_counts = self.redis.zcard_summed_multi(&pl_key_groups).await?;

            // Pipelined ZREVRANGE to get earliest liker (from today's key which has newest data)
            let today_keys: Vec<String> = batch
                .iter()
                .map(|id| Keys::post_likers_date(id, &today))
                .collect();
            let range_requests: Vec<(&str, isize, isize)> = today_keys
                .iter()
                .map(|k| (k.as_str(), -1isize, -1isize))
                .collect();
            let earliest_likers = self
                .redis
                .zrevrange_with_scores_multi(&range_requests)
                .await?;

            // Process results
            for ((post_id, like_count), earliest) in batch
                .iter()
                .zip(like_counts.iter())
                .zip(earliest_likers.iter())
            {
                if *like_count == 0 {
                    continue;
                }

                let post_time = earliest
                    .first()
                    .map(|(_, score)| *score as i64)
                    .unwrap_or(now);

                let age_seconds = (now - post_time).max(0);
                let recency_factor = (-0.693 * age_seconds as f64 / half_life_seconds as f64).exp();

                let score = *like_count as f64 * recency_factor;
                if score > 0.0 {
                    scored_posts.push((post_id.clone(), score));
                }
            }

            // Yield to prevent blocking
            tokio::task::yield_now().await;
        }

        // Sort and take top N
        scored_posts.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored_posts.truncate(self.config.trending_posts_limit);

        if scored_posts.is_empty() {
            debug!(algo_id = algo_id, "Trending sync - no engagement");
            return Ok(());
        }

        // Store atomically
        self.store_sorted_set(
            &Keys::trending_posts(algo_id),
            &Keys::trending_meta(algo_id),
            &scored_posts,
            self.config.trending_posts_ttl_hours as i64 * 3600,
        )
        .await?;

        info!(
            algo_id = algo_id,
            post_count = scored_posts.len(),
            duration_ms = start_time.elapsed().as_millis(),
            "Trending sync completed"
        );

        Ok(())
    }

    /// Sync popular posts - total engagement with slow recency decay.
    ///
    /// Popular score = like_count * recency_factor (7-day half-life)
    async fn sync_popular_posts(&self, algo_id: i32) -> Result<()> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let algo_posts_key = Keys::algo_posts(algo_id);
        let post_ids = self.redis.smembers(&algo_posts_key).await?;

        if post_ids.is_empty() {
            return Ok(());
        }

        let half_life_seconds = (self.config.popular_decay_hours * 3600.0) as i64;
        let min_likes = self.config.popular_min_likes;
        let mut scored_posts: Vec<(String, f64)> = Vec::new();
        let batch_size = 500;
        let today = graze_common::today_date();

        // Process in batches with pipelined Redis calls
        for batch in post_ids.chunks(batch_size) {
            // Build date-based key groups for this batch
            let pl_key_groups: Vec<Vec<String>> = batch
                .iter()
                .map(|id| Keys::post_likers_retention(id, DEFAULT_RETENTION_DAYS))
                .collect();

            // Pipelined ZCARD summed across date-based keys
            let like_counts = self.redis.zcard_summed_multi(&pl_key_groups).await?;

            // Pipelined ZREVRANGE to get earliest liker (from today's key)
            let today_keys: Vec<String> = batch
                .iter()
                .map(|id| Keys::post_likers_date(id, &today))
                .collect();
            let range_requests: Vec<(&str, isize, isize)> = today_keys
                .iter()
                .map(|k| (k.as_str(), -1isize, -1isize))
                .collect();
            let earliest_likers = self
                .redis
                .zrevrange_with_scores_multi(&range_requests)
                .await?;

            // Process results
            for ((post_id, like_count), earliest) in batch
                .iter()
                .zip(like_counts.iter())
                .zip(earliest_likers.iter())
            {
                if *like_count < min_likes {
                    continue;
                }

                let post_time = earliest
                    .first()
                    .map(|(_, score)| *score as i64)
                    .unwrap_or(now);

                let age_seconds = (now - post_time).max(0);
                let recency_factor = (-0.693 * age_seconds as f64 / half_life_seconds as f64).exp();

                let score = *like_count as f64 * recency_factor;
                if score > 0.0 {
                    scored_posts.push((post_id.clone(), score));
                }
            }

            tokio::task::yield_now().await;
        }

        scored_posts.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored_posts.truncate(self.config.popular_posts_limit);

        if scored_posts.is_empty() {
            return Ok(());
        }

        let ttl = self.config.algo_posts_ttl_hours as i64 * 3600;
        self.store_sorted_set(
            &Keys::popular_posts(algo_id),
            &Keys::popular_meta(algo_id),
            &scored_posts,
            ttl,
        )
        .await?;

        info!(
            algo_id = algo_id,
            post_count = scored_posts.len(),
            duration_ms = start_time.elapsed().as_millis(),
            "Popular sync completed"
        );

        Ok(())
    }

    /// Sync velocity trending - posts gaining traction quickly.
    ///
    /// Velocity = likes_in_window / hours_since_first_like
    async fn sync_velocity_posts(&self, algo_id: i32) -> Result<()> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let window_seconds = (self.config.velocity_window_hours * 3600.0) as i64;
        let window_start = now - window_seconds;
        let min_velocity_likes = self.config.velocity_min_likes;

        let algo_posts_key = Keys::algo_posts(algo_id);
        let post_ids = self.redis.smembers(&algo_posts_key).await?;

        if post_ids.is_empty() {
            return Ok(());
        }

        let mut scored_posts: Vec<(String, f64)> = Vec::new();
        let batch_size = 500;
        let today = graze_common::today_date();

        // Process in batches with pipelined Redis calls
        for batch in post_ids.chunks(batch_size) {
            // Build date-based key groups for ZCOUNT (sum across all dates)
            let pl_key_groups: Vec<Vec<String>> = batch
                .iter()
                .map(|id| Keys::post_likers_retention(id, DEFAULT_RETENTION_DAYS))
                .collect();

            // Flatten keys for ZCOUNT
            let mut all_zcount_requests: Vec<(&str, f64, f64)> = Vec::new();
            for group in &pl_key_groups {
                for key in group {
                    all_zcount_requests.push((key.as_str(), window_start as f64, now as f64));
                }
            }
            let all_counts = self.redis.zcount_multi(&all_zcount_requests).await?;

            // Sum counts for each post
            let mut count_iter = all_counts.into_iter();
            let likes_in_window: Vec<usize> = pl_key_groups
                .iter()
                .map(|group| count_iter.by_ref().take(group.len()).sum())
                .collect();

            // Pipelined ZREVRANGE to get earliest liker (from today's key)
            let today_keys: Vec<String> = batch
                .iter()
                .map(|id| Keys::post_likers_date(id, &today))
                .collect();
            let range_requests: Vec<(&str, isize, isize)> = today_keys
                .iter()
                .map(|k| (k.as_str(), -1isize, -1isize))
                .collect();
            let earliest_likers = self
                .redis
                .zrevrange_with_scores_multi(&range_requests)
                .await?;

            // Process results
            for ((post_id, likes), earliest) in batch
                .iter()
                .zip(likes_in_window.iter())
                .zip(earliest_likers.iter())
            {
                if *likes < min_velocity_likes {
                    continue;
                }

                let first_like_time = earliest
                    .first()
                    .map(|(_, score)| *score as i64)
                    .unwrap_or(now);

                // Hours since first like (minimum 1 hour to avoid division issues)
                let hours_active = ((now - first_like_time) as f64 / 3600.0).max(1.0);
                let velocity = *likes as f64 / hours_active;

                if velocity > 0.0 {
                    scored_posts.push((post_id.clone(), velocity));
                }
            }

            tokio::task::yield_now().await;
        }

        scored_posts.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored_posts.truncate(self.config.velocity_posts_limit);

        if scored_posts.is_empty() {
            return Ok(());
        }

        let ttl = self.config.algo_posts_ttl_hours as i64 * 3600;
        self.store_sorted_set(
            &Keys::velocity_posts(algo_id),
            &Keys::velocity_meta(algo_id),
            &scored_posts,
            ttl,
        )
        .await?;

        info!(
            algo_id = algo_id,
            post_count = scored_posts.len(),
            duration_ms = start_time.elapsed().as_millis(),
            "Velocity sync completed"
        );

        Ok(())
    }

    /// Sync author success scores and discovery posts.
    ///
    /// 1. Calculate author success = avg engagement across their posts in feed
    /// 2. Find "cold" posts (low engagement) from successful authors = discovery
    async fn sync_author_success_and_discovery(&self, algo_id: i32) -> Result<()> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let half_life_seconds = (self.config.author_success_decay_hours * 3600.0) as i64;
        let min_posts = self.config.author_success_min_posts;
        let max_discovery_likes = self.config.discovery_max_post_likes;

        let algo_posts_key = Keys::algo_posts(algo_id);
        let post_ids = self.redis.smembers(&algo_posts_key).await?;

        if post_ids.is_empty() {
            return Ok(());
        }

        // Get URIs for all posts to extract author DIDs
        let int_ids: Vec<i64> = post_ids.iter().filter_map(|id| id.parse().ok()).collect();
        let uri_mapping = self.interner.get_uris_batch(&int_ids).await?;

        // Collect post data: {post_id: (like_count, post_time, author_did)}
        let mut post_data: HashMap<String, (usize, i64, Option<String>)> = HashMap::new();
        let batch_size = 500;
        let today = graze_common::today_date();

        // Process in batches with pipelined Redis calls
        for batch in post_ids.chunks(batch_size) {
            // Build date-based key groups for this batch
            let pl_key_groups: Vec<Vec<String>> = batch
                .iter()
                .map(|id| Keys::post_likers_retention(id, DEFAULT_RETENTION_DAYS))
                .collect();

            // Pipelined ZCARD summed across date-based keys
            let like_counts = self.redis.zcard_summed_multi(&pl_key_groups).await?;

            // Pipelined ZREVRANGE to get earliest liker (from today's key)
            let today_keys: Vec<String> = batch
                .iter()
                .map(|id| Keys::post_likers_date(id, &today))
                .collect();
            let range_requests: Vec<(&str, isize, isize)> = today_keys
                .iter()
                .map(|k| (k.as_str(), -1isize, -1isize))
                .collect();
            let earliest_likers = self
                .redis
                .zrevrange_with_scores_multi(&range_requests)
                .await?;

            // Process results
            for ((post_id, like_count), earliest) in batch
                .iter()
                .zip(like_counts.iter())
                .zip(earliest_likers.iter())
            {
                let post_time = earliest
                    .first()
                    .map(|(_, score)| *score as i64)
                    .unwrap_or(now);

                // Extract author DID from URI
                let author_did = if let Ok(id) = post_id.parse::<i64>() {
                    if let Some(uri) = uri_mapping.get(&id) {
                        extract_author_did(uri)
                    } else {
                        None
                    }
                } else {
                    None
                };

                post_data.insert(post_id.clone(), (*like_count, post_time, author_did));
            }

            tokio::task::yield_now().await;
        }

        // Aggregate by author
        let mut author_posts: HashMap<String, Vec<(String, usize, i64)>> = HashMap::new();
        for (post_id, (like_count, post_time, author_did)) in &post_data {
            if let Some(author) = author_did {
                author_posts.entry(author.clone()).or_default().push((
                    post_id.clone(),
                    *like_count,
                    *post_time,
                ));
            }
        }

        // Calculate author success scores
        let mut author_success: HashMap<String, f64> = HashMap::new();
        for (author_did, posts) in &author_posts {
            if posts.len() < min_posts {
                continue;
            }

            let mut total_weighted_likes = 0.0;
            let mut total_weight = 0.0;

            for (_, like_count, post_time) in posts {
                let age_seconds = (now - post_time).max(0);
                let recency_weight = (-0.693 * age_seconds as f64 / half_life_seconds as f64).exp();
                total_weighted_likes += *like_count as f64 * recency_weight;
                total_weight += recency_weight;
            }

            if total_weight > 0.0 {
                let success_score = total_weighted_likes / total_weight;
                // Boost for prolific authors (more posts = more confidence)
                let confidence_boost = (1.0 + posts.len() as f64 / 20.0).min(2.0);
                author_success.insert(author_did.clone(), success_score * confidence_boost);
            }
        }

        // Find discovery posts (cold posts from successful authors)
        let mut discovery_posts: Vec<(String, f64)> = Vec::new();
        for (author_did, posts) in &author_posts {
            if let Some(&author_score) = author_success.get(author_did) {
                for (post_id, like_count, post_time) in posts {
                    // "Cold" post = below threshold likes, potential discovery
                    if *like_count <= max_discovery_likes {
                        let age_seconds = (now - post_time).max(0);
                        let freshness = (-0.693 * age_seconds as f64 / (24.0 * 3600.0)).exp();
                        let discovery_score = author_score * (0.5 + 0.5 * freshness);
                        discovery_posts.push((post_id.clone(), discovery_score));
                    }
                }
            }
        }

        // Sort and limit discovery posts
        discovery_posts.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        discovery_posts.truncate(self.config.discovery_posts_limit);

        let ttl = self.config.algo_posts_ttl_hours as i64 * 3600;

        // Store author success scores using a single pipeline (5 RTT -> 1)
        if !author_success.is_empty() {
            let author_key = Keys::author_success(algo_id);
            let author_meta_key = Keys::author_success_meta(algo_id);

            let mut conn = self.redis.get().await?;
            let mut pipe = redis::pipe();

            // DEL author_key
            pipe.del(&author_key);

            // HSET author_key with all author scores
            let mut hset_cmd = redis::cmd("HSET");
            hset_cmd.arg(&author_key);
            for (author, score) in &author_success {
                hset_cmd.arg(author).arg(score.to_string());
            }
            pipe.add_command(hset_cmd);

            // EXPIRE author_key
            pipe.expire(&author_key, ttl);

            // HSET author_meta_key with last_sync and author_count
            pipe.cmd("HSET")
                .arg(&author_meta_key)
                .arg("last_sync")
                .arg(now.to_string())
                .arg("author_count")
                .arg(author_success.len().to_string());

            // EXPIRE author_meta_key
            pipe.expire(&author_meta_key, ttl);

            let _: ((), (), bool, (), bool) = pipe.query_async(&mut conn).await?;
        }

        // Store discovery posts
        if !discovery_posts.is_empty() {
            self.store_sorted_set(
                &Keys::discovery_posts(algo_id),
                &Keys::discovery_meta(algo_id),
                &discovery_posts,
                ttl,
            )
            .await?;
        }

        info!(
            algo_id = algo_id,
            author_count = author_success.len(),
            discovery_count = discovery_posts.len(),
            duration_ms = start_time.elapsed().as_millis(),
            "Discovery sync completed"
        );

        Ok(())
    }

    /// Build the set of users who have liked posts in this algorithm.
    ///
    /// This is the 1-hop neighborhood of the candidate set in the user dimension.
    async fn sync_algo_likers(&self, algo_id: i32) -> Result<()> {
        let start_time = Instant::now();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let algo_posts_key = Keys::algo_posts(algo_id);
        let post_ids = self.redis.smembers(&algo_posts_key).await?;

        if post_ids.is_empty() {
            debug!(algo_id = algo_id, "Algo likers sync - no posts");
            return Ok(());
        }

        let batch_size = self.config.algo_likers_batch_size;
        let mut all_likers: std::collections::HashSet<String> = std::collections::HashSet::new();
        let batch_delay = Duration::from_millis(self.config.sync_batch_delay_ms);
        let yield_every = self.config.sync_yield_every_n_batches;
        let mut batch_count = 0;

        for batch in post_ids.chunks(batch_size) {
            batch_count += 1;

            // Build date-based key groups for this batch
            let key_groups: Vec<Vec<String>> = batch
                .iter()
                .map(|post_id| Keys::post_likers_retention(post_id, DEFAULT_RETENTION_DAYS))
                .collect();

            // Flatten all keys and fetch in parallel
            for group in &key_groups {
                for key in group {
                    let likers = self.redis.zrevrange(key, 0, -1).await?;
                    all_likers.extend(likers);
                }
            }

            if batch_count % yield_every == 0 {
                tokio::time::sleep(batch_delay).await;
            }
        }

        if all_likers.is_empty() {
            debug!(algo_id = algo_id, "Algo likers sync - no likers");
            return Ok(());
        }

        // Store in algo likers set (atomic swap)
        let algo_likers_key = Keys::algo_likers(algo_id);
        let algo_likers_meta_key = Keys::algo_likers_meta(algo_id);
        let temp_key = format!("{}:temp", algo_likers_key);
        let ttl = self.config.algo_posts_ttl_hours as i64 * 3600;

        // Pipeline all Redis operations for atomic swap (6 RTT → 1)
        let mut conn = self.redis.get().await?;
        let likers_vec: Vec<&str> = all_likers.iter().map(|s| s.as_str()).collect();

        let mut pipe = redis::pipe();
        pipe.del(&temp_key);
        pipe.cmd("SADD").arg(&temp_key).arg(&likers_vec);
        pipe.cmd("RENAME").arg(&temp_key).arg(&algo_likers_key);
        pipe.expire(&algo_likers_key, ttl);
        pipe.cmd("HSET")
            .arg(&algo_likers_meta_key)
            .arg("last_sync")
            .arg(now.to_string())
            .arg("liker_count")
            .arg(all_likers.len().to_string())
            .arg("post_count")
            .arg(post_ids.len().to_string());
        pipe.expire(&algo_likers_meta_key, ttl);

        let _: ((), usize, (), bool, (), bool) = pipe.query_async(&mut conn).await?;

        info!(
            algo_id = algo_id,
            liker_count = all_likers.len(),
            post_count = post_ids.len(),
            duration_ms = start_time.elapsed().as_millis(),
            "Algo likers sync completed"
        );

        Ok(())
    }

    /// Helper to store a sorted set with atomic swap.
    /// Uses pipelining to reduce 6 RTTs to 1.
    async fn store_sorted_set(
        &self,
        key: &str,
        meta_key: &str,
        items: &[(String, f64)],
        ttl_seconds: i64,
    ) -> Result<()> {
        let temp_key = format!("{}:temp", key);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut conn = self.redis.get().await?;
        let mut pipe = redis::pipe();

        // DEL temp key
        pipe.del(&temp_key);

        // ZADD all items to temp key - convert (String, f64) to (f64, &str) for zadd_multiple
        let zadd_items: Vec<(f64, &str)> = items.iter().map(|(m, s)| (*s, m.as_str())).collect();
        pipe.zadd_multiple(&temp_key, &zadd_items);

        // Atomic swap: RENAME temp -> main
        pipe.rename(&temp_key, key);

        // EXPIRE main key
        pipe.expire(key, ttl_seconds);

        // HSET metadata (last_sync + post_count)
        pipe.cmd("HSET")
            .arg(meta_key)
            .arg("last_sync")
            .arg(now.to_string())
            .arg("post_count")
            .arg(items.len().to_string());

        // EXPIRE metadata
        pipe.expire(meta_key, ttl_seconds);

        // Execute pipeline (6 commands -> 1 RTT)
        let _: ((), (), (), bool, (), bool) = pipe.query_async(&mut conn).await?;

        Ok(())
    }

    /// Queue a sync request for an algorithm.
    ///
    /// Returns (queued, position) tuple.
    pub async fn queue_sync(&self, algo_id: i32, force: bool) -> Result<(bool, usize)> {
        if force {
            // Remove rate limit lock if forcing
            let lock_key = Keys::sync_lock(algo_id);
            self.redis.del(&lock_key).await?;
        }

        // Add to queue (only if not already in queue)
        let mut conn = self.redis.get().await?;
        let added: i64 = redis::cmd("SADD")
            .arg("pending:syncs")
            .arg(algo_id.to_string())
            .query_async(&mut conn)
            .await?;

        if added > 0 {
            let _: () = redis::cmd("LPUSH")
                .arg(Keys::SYNC_QUEUE)
                .arg(algo_id.to_string())
                .query_async(&mut conn)
                .await?;
        }

        let position: usize = redis::cmd("LLEN")
            .arg(Keys::SYNC_QUEUE)
            .query_async(&mut conn)
            .await?;

        Ok((added > 0, position))
    }

    /// Check if an algorithm needs syncing.
    pub async fn needs_sync(&self, algo_id: i32) -> Result<bool> {
        let posts_key = Keys::algo_posts(algo_id);
        let ttl = self.redis.ttl(&posts_key).await?;

        if ttl < 0 {
            // Key doesn't exist
            return Ok(true);
        }

        // Refresh if less than threshold remaining
        let threshold = self.config.sync_refresh_threshold_minutes as i64 * 60;
        if ttl < threshold {
            return Ok(true);
        }

        Ok(false)
    }
}

/// Extract author DID from AT-URI.
///
/// URI format: at://did:plc:xxx/app.bsky.feed.post/rkey
fn extract_author_did(uri: &str) -> Option<String> {
    if !uri.starts_with("at://") {
        return None;
    }
    let path = &uri[5..];
    let end = path.find('/')?;
    let did = &path[..end];
    if did.starts_with("did:") {
        Some(did.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_author_did_valid() {
        let uri = "at://did:plc:author123/app.bsky.feed.post/rkey456";
        assert_eq!(
            extract_author_did(uri),
            Some("did:plc:author123".to_string())
        );
    }

    #[test]
    fn test_extract_author_did_web() {
        let uri = "at://did:web:example.com/app.bsky.feed.post/xyz";
        assert_eq!(
            extract_author_did(uri),
            Some("did:web:example.com".to_string())
        );
    }

    #[test]
    fn test_extract_author_did_invalid_prefix() {
        let uri = "https://did:plc:author123/app.bsky.feed.post/rkey456";
        assert_eq!(extract_author_did(uri), None);
    }

    #[test]
    fn test_extract_author_did_not_did() {
        let uri = "at://notadid/app.bsky.feed.post/rkey456";
        assert_eq!(extract_author_did(uri), None);
    }

    #[test]
    fn test_extract_author_did_empty() {
        assert_eq!(extract_author_did(""), None);
    }
}
