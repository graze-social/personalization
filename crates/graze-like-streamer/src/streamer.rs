//! Jetstream like event consumer.
//!
//! Connects to Bluesky Jetstream and processes like events to build
//! the user-likes and post-likers graphs in Redis.
//!
//! Optimizations:
//! - Batched Redis writes using pipelines
//! - LRU-cached DID hashing
//! - Configurable batch size/interval for reduced Redis round-trips
//! - TTL refresh throttling (once per interval)
//! - Size caps via ZREMRANGEBYRANK

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use rand::Rng;
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use graze_common::services::UriInterner;
use graze_common::Result;
use graze_common::{date_from_timestamp, hash_did, ttl_for_date, Keys, RedisClient};

/// A like event from Jetstream.
#[derive(Debug, Clone)]
struct LikeEvent {
    /// User DID who liked.
    user_did: String,
    /// Post AT-URI that was liked.
    post_uri: String,
    /// Timestamp in microseconds.
    timestamp: i64,
}

/// Jetstream commit message structure.
#[derive(Debug, Deserialize)]
struct JetstreamMessage {
    kind: String,
    did: Option<String>,
    time_us: Option<i64>,
    commit: Option<JetstreamCommit>,
}

#[derive(Debug, Deserialize)]
struct JetstreamCommit {
    collection: Option<String>,
    operation: Option<String>,
    record: Option<JetstreamRecord>,
}

#[derive(Debug, Deserialize)]
struct JetstreamRecord {
    subject: Option<JetstreamSubject>,
}

#[derive(Debug, Deserialize)]
struct JetstreamSubject {
    uri: Option<String>,
}

/// Extract author DID from AT-URI.
///
/// AT-URI format: at://did:plc:xxx/app.bsky.feed.post/rkey
fn extract_author_did(post_uri: &str) -> Option<&str> {
    if !post_uri.starts_with("at://") {
        return None;
    }
    let path = &post_uri[5..];
    let end = path.find('/')?;
    let did = &path[..end];
    if did.starts_with("did:") {
        Some(did)
    } else {
        None
    }
}

/// Calculate backoff delay with jitter to prevent thundering herd.
///
/// Adds up to 25% random jitter to the base delay.
fn backoff_with_jitter(base: Duration, max: Duration) -> Duration {
    let capped = base.min(max);
    let jitter_range = capped.as_millis() as u64 / 4; // 25% jitter
    if jitter_range == 0 {
        return capped;
    }
    let jitter = rand::thread_rng().gen_range(0..=jitter_range);
    capped + Duration::from_millis(jitter)
}

/// Streams likes from Jetstream and updates Redis graph.
pub struct LikeStreamer {
    redis: Arc<RedisClient>,
    interner: Arc<UriInterner>,
    config: Arc<Config>,
}

impl LikeStreamer {
    /// Create a new like streamer.
    pub fn new(redis: Arc<RedisClient>, interner: Arc<UriInterner>, config: Arc<Config>) -> Self {
        Self {
            redis,
            interner,
            config,
        }
    }

    /// Get the cursor to resume from, checking for pending cursor first.
    ///
    /// If a pending cursor exists, it means a batch was in progress when
    /// the process crashed. Resume from the pending cursor to avoid data loss.
    async fn get_resume_cursor(&self) -> Result<Option<String>> {
        // Check for pending cursor first (indicates incomplete batch)
        if let Some(pending) = self
            .redis
            .get_string(Keys::JETSTREAM_CURSOR_PENDING)
            .await?
        {
            warn!(
                cursor = %pending,
                "Found pending cursor from incomplete batch, resuming from there"
            );
            return Ok(Some(pending));
        }

        // Otherwise use the committed cursor
        self.redis.get_string(Keys::JETSTREAM_CURSOR).await
    }

    /// Run the like streamer.
    ///
    /// Connects to Jetstream WebSocket and processes like events indefinitely.
    pub async fn run(&self) -> Result<()> {
        info!(
            jetstream_url = %self.config.jetstream_url,
            "Starting like streamer"
        );

        let mut base_reconnect_delay = Duration::from_secs(1);
        let max_reconnect_delay = Duration::from_secs(60);
        let mut consecutive_failures: u32 = 0;

        loop {
            match self.connect_and_stream().await {
                Ok(()) => {
                    info!("Jetstream connection closed normally");
                    base_reconnect_delay = Duration::from_secs(1);
                    consecutive_failures = 0;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    error!(
                        error = %e,
                        consecutive_failures = consecutive_failures,
                        "Jetstream connection error"
                    );
                    base_reconnect_delay = (base_reconnect_delay * 2).min(max_reconnect_delay);

                    // Alert on repeated failures
                    if consecutive_failures == 5 {
                        error!(
                            consecutive_failures = consecutive_failures,
                            "Jetstream experiencing repeated connection failures"
                        );
                    }
                }
            }

            let delay = backoff_with_jitter(base_reconnect_delay, max_reconnect_delay);
            info!(
                delay_ms = delay.as_millis() as u64,
                base_delay_secs = base_reconnect_delay.as_secs(),
                "Reconnecting to Jetstream"
            );
            tokio::time::sleep(delay).await;
        }
    }

    /// Connect to Jetstream and process messages.
    async fn connect_and_stream(&self) -> Result<()> {
        // Get cursor position - check for pending cursor first (indicates incomplete batch)
        let cursor = self.get_resume_cursor().await?;

        // Build connection URL
        let mut url = format!(
            "{}?wantedCollections=app.bsky.feed.like",
            self.config.jetstream_url
        );
        if let Some(ref c) = cursor {
            url.push_str(&format!("&cursor={}", c));
            info!(cursor = c, "Resuming from cursor");
        }

        // Connect to WebSocket
        let (ws_stream, _) = connect_async(&url).await?;
        info!("Connected to Jetstream");

        let (_write, mut read) = ws_stream.split();

        // Create channels for communication and shutdown coordination
        let (event_tx, mut event_rx) = mpsc::channel::<LikeEvent>(10000);
        let (flush_tx, mut flush_rx) = mpsc::channel::<()>(1);
        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        // Spawn message processing task with read timeout
        let event_tx_clone = event_tx.clone();
        drop(event_tx); // Drop original so event_rx.recv() returns None when task exits
        let read_timeout = Duration::from_secs(self.config.jetstream_read_timeout_seconds);
        let mut shutdown_rx_msg = shutdown_tx.subscribe();
        let mut msg_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = tokio::time::timeout(read_timeout, read.next()) => {
                        match result {
                            Ok(Some(Ok(Message::Text(text)))) => {
                                if let Some(event) = parse_like_event(&text) {
                                    if event_tx_clone.send(event).await.is_err() {
                                        warn!("Event channel closed, stopping message task");
                                        break;
                                    }
                                }
                                // Non-like messages (identity, account, etc.) are silently skipped
                            }
                            Ok(Some(Ok(Message::Close(_)))) => {
                                info!("WebSocket closed by server");
                                break;
                            }
                            Ok(Some(Ok(Message::Ping(_data)))) => {
                                // Pong is handled automatically by tungstenite
                            }
                            Ok(Some(Ok(_))) => {
                                // Binary or other message types, skip
                            }
                            Ok(Some(Err(e))) => {
                                error!(error = %e, "WebSocket error");
                                break;
                            }
                            Ok(None) => {
                                info!("WebSocket stream ended");
                                break;
                            }
                            Err(_) => {
                                warn!(
                                    timeout_secs = read_timeout.as_secs(),
                                    "WebSocket read timeout, reconnecting"
                                );
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx_msg.recv() => {
                        info!("Message task received shutdown signal");
                        break;
                    }
                }
            }
        });

        // Spawn periodic flush task
        let batch_interval = Duration::from_millis(self.config.like_batch_interval_ms);
        let mut shutdown_rx_flush = shutdown_tx.subscribe();
        let flush_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(batch_interval) => {
                        if flush_tx.send(()).await.is_err() {
                            break;
                        }
                    }
                    _ = shutdown_rx_flush.recv() => {
                        info!("Flush task received shutdown signal");
                        break;
                    }
                }
            }
        });

        // Main processing loop
        let mut batch: Vec<LikeEvent> = Vec::with_capacity(1000);
        let mut last_flush = Instant::now();
        let mut last_event_time = Instant::now();
        let mut ttl_refresh_times: HashMap<String, Instant> = HashMap::new();
        let stale_timeout = Duration::from_secs(self.config.jetstream_stale_timeout_seconds);

        loop {
            tokio::select! {
                biased; // Prioritize event processing

                event = event_rx.recv() => {
                    match event {
                        Some(event) => {
                            last_event_time = Instant::now();
                            batch.push(event);

                            // Flush if batch is full
                            if batch.len() >= self.config.like_batch_size {
                                self.flush_batch(&mut batch, &mut ttl_refresh_times).await?;
                                last_flush = Instant::now();
                            }
                        }
                        None => {
                            info!("Event channel closed, message task has exited");
                            break;
                        }
                    }
                }
                flush = flush_rx.recv() => {
                    if flush.is_none() {
                        info!("Flush channel closed, flush task has exited");
                        break;
                    }
                    // Periodic flush
                    if !batch.is_empty() && last_flush.elapsed() >= batch_interval {
                        self.flush_batch(&mut batch, &mut ttl_refresh_times).await?;
                        last_flush = Instant::now();
                    }

                    // Check for stale connection (no events for stale_timeout)
                    if last_event_time.elapsed() > stale_timeout {
                        warn!(
                            elapsed_secs = last_event_time.elapsed().as_secs(),
                            threshold_secs = stale_timeout.as_secs(),
                            "Connection appears stale, no events received"
                        );
                        break;
                    }
                }
                result = &mut msg_task => {
                    match result {
                        Ok(()) => info!("Message task completed, will reconnect"),
                        Err(e) => warn!(error = %e, "Message task panicked"),
                    }
                    break;
                }
            }
        }

        // Flush remaining events
        if !batch.is_empty() {
            self.flush_batch(&mut batch, &mut ttl_refresh_times).await?;
        }

        // Signal all tasks to shutdown gracefully
        drop(shutdown_tx);

        // Wait for tasks to complete (with timeout to avoid hanging)
        let shutdown_timeout = Duration::from_secs(5);
        let _ = tokio::time::timeout(shutdown_timeout, async {
            let _ = msg_task.await;
            let _ = flush_task.await;
        })
        .await;

        Ok(())
    }

    /// Flush the current batch to Redis.
    ///
    /// Uses two-phase cursor management for crash recovery:
    /// 1. Save pending cursor (earliest timestamp) before processing
    /// 2. Process the batch
    /// 3. Save committed cursor (latest timestamp) after success
    /// 4. Clear pending cursor
    async fn flush_batch(
        &self,
        batch: &mut Vec<LikeEvent>,
        ttl_refresh_times: &mut HashMap<String, Instant>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let events: Vec<LikeEvent> = std::mem::take(batch);
        let earliest_timestamp = events.iter().map(|e| e.timestamp).min().unwrap_or(0);
        let latest_timestamp = events.iter().map(|e| e.timestamp).max().unwrap_or(0);

        debug!(batch_size = events.len(), "Flushing like batch");

        // Phase 1: Save pending cursor before processing
        // If we crash during processing, we'll resume from here on restart
        self.redis
            .set_ex(
                Keys::JETSTREAM_CURSOR_PENDING,
                &earliest_timestamp.to_string(),
                300, // 5-minute TTL
            )
            .await?;

        // Convert URIs to numeric IDs using the interner (batch operation)
        let unique_uris: Vec<String> = events.iter().map(|e| e.post_uri.clone()).collect();
        let uri_to_id = self.interner.get_or_create_ids_batch(&unique_uris).await?;

        // Track unique keys to avoid redundant commands
        let mut user_likes_keys: HashSet<String> = HashSet::new();
        let mut post_likers_keys: HashSet<String> = HashSet::new();
        let mut user_liked_authors_keys: HashSet<String> = HashSet::new();
        let mut author_likers_keys: HashSet<String> = HashSet::new();

        // Build ZADD items for each key
        let mut user_likes_items: HashMap<String, Vec<(f64, String)>> = HashMap::new();
        let mut post_likers_items: HashMap<String, Vec<(f64, String)>> = HashMap::new();
        let mut author_likers_items: HashMap<String, Vec<(f64, String)>> = HashMap::new();
        // Track (user_hash, author_hash) pairs for user_liked_authors increment
        let mut user_liked_authors_increments: HashMap<(String, String), i64> = HashMap::new();

        for event in &events {
            let user_hash = hash_did(&event.user_did);
            let timestamp_sec = event.timestamp as f64 / 1_000_000.0;

            // Get the date (YYYYMMDD) for this event's timestamp
            let date = date_from_timestamp(timestamp_sec);

            // Use numeric post ID from interner (matches algo posts format)
            let post_id = match uri_to_id.get(&event.post_uri) {
                Some(id) => id.to_string(),
                None => {
                    // Skip if we couldn't get an ID (shouldn't happen normally)
                    warn!(uri = %event.post_uri, "Failed to get post ID from interner");
                    continue;
                }
            };

            // User likes (date-based: ul:{hash}:{YYYYMMDD})
            let user_likes_key = Keys::user_likes_date(&user_hash, &date);
            user_likes_keys.insert(user_likes_key.clone());
            user_likes_items
                .entry(user_likes_key)
                .or_default()
                .push((timestamp_sec, post_id.clone()));

            // Post likers (date-based: pl:{id}:{YYYYMMDD})
            let post_likers_key = Keys::post_likers_date(&post_id, &date);
            post_likers_keys.insert(post_likers_key.clone());
            post_likers_items
                .entry(post_likers_key)
                .or_default()
                .push((timestamp_sec, user_hash.clone()));

            // Author-level tracking
            if let Some(author_did) = extract_author_did(&event.post_uri) {
                let author_hash = hash_did(author_did);

                // User liked authors - track for increment (NOT date-based, accumulates counts)
                let ula_key = Keys::user_liked_authors(&user_hash);
                user_liked_authors_keys.insert(ula_key);
                *user_liked_authors_increments
                    .entry((user_hash.clone(), author_hash.clone()))
                    .or_insert(0) += 1;

                // Author likers (date-based: authl:{hash}:{YYYYMMDD})
                let al_key = Keys::author_likers_date(&author_hash, &date);
                author_likers_keys.insert(al_key.clone());
                author_likers_items
                    .entry(al_key)
                    .or_default()
                    .push((timestamp_sec, user_hash.clone()));
            }
        }

        // Execute Redis commands
        let ttl_refresh_interval =
            Duration::from_secs(self.config.like_ttl_refresh_interval_seconds);
        let now = Instant::now();

        // Collect unique user hashes for like count increment
        // Extract from date-based keys: ul:{hash}:{YYYYMMDD} -> {hash}
        let unique_user_hashes: Vec<&str> = user_likes_keys
            .iter()
            .filter_map(|k| {
                k.strip_prefix("ul:")
                    .and_then(|rest| rest.rsplit_once(':'))
                    .map(|(hash, _)| hash)
            })
            .collect();

        // Increment user like counts in the ulc hash (for LinkLonk normalization)
        if !unique_user_hashes.is_empty() {
            self.redis
                .hincrby_multi(Keys::USER_LIKE_COUNTS, &unique_user_hashes)
                .await?;
        }

        // ═══════════════════════════════════════════════════════════════════
        // PHASE 1: Batch all ZADD operations (single pipeline per type)
        // ═══════════════════════════════════════════════════════════════════

        // User likes ZADDs
        let ul_zadd: Vec<(String, Vec<(f64, String)>)> = user_likes_items.into_iter().collect();
        let ul_zadd_refs: Vec<(&str, Vec<(f64, &str)>)> = ul_zadd
            .iter()
            .map(|(k, items)| {
                let items_ref: Vec<(f64, &str)> =
                    items.iter().map(|(s, m)| (*s, m.as_str())).collect();
                (k.as_str(), items_ref)
            })
            .collect();
        let ul_zadd_final: Vec<(&str, &[(f64, &str)])> = ul_zadd_refs
            .iter()
            .map(|(k, items)| (*k, items.as_slice()))
            .collect();
        self.redis.zadd_multi(&ul_zadd_final).await?;

        // Post likers ZADDs
        let pl_zadd: Vec<(String, Vec<(f64, String)>)> = post_likers_items.into_iter().collect();
        let pl_zadd_refs: Vec<(&str, Vec<(f64, &str)>)> = pl_zadd
            .iter()
            .map(|(k, items)| {
                let items_ref: Vec<(f64, &str)> =
                    items.iter().map(|(s, m)| (*s, m.as_str())).collect();
                (k.as_str(), items_ref)
            })
            .collect();
        let pl_zadd_final: Vec<(&str, &[(f64, &str)])> = pl_zadd_refs
            .iter()
            .map(|(k, items)| (*k, items.as_slice()))
            .collect();
        self.redis.zadd_multi(&pl_zadd_final).await?;

        // Author likers ZADDs
        let al_zadd: Vec<(String, Vec<(f64, String)>)> = author_likers_items.into_iter().collect();
        let al_zadd_refs: Vec<(&str, Vec<(f64, &str)>)> = al_zadd
            .iter()
            .map(|(k, items)| {
                let items_ref: Vec<(f64, &str)> =
                    items.iter().map(|(s, m)| (*s, m.as_str())).collect();
                (k.as_str(), items_ref)
            })
            .collect();
        let al_zadd_final: Vec<(&str, &[(f64, &str)])> = al_zadd_refs
            .iter()
            .map(|(k, items)| (*k, items.as_slice()))
            .collect();
        self.redis.zadd_multi(&al_zadd_final).await?;

        // ═══════════════════════════════════════════════════════════════════
        // PHASE 2: Date-based TTL refresh (TTL handles expiration automatically)
        // ═══════════════════════════════════════════════════════════════════

        // Helper to extract date from key (e.g., "ul:abc:20260204" -> "20260204")
        fn extract_date_from_key(key: &str) -> Option<&str> {
            key.rsplit_once(':')
                .map(|(_, date)| date)
                .filter(|d| d.len() == 8 && d.chars().all(|c| c.is_ascii_digit()))
        }

        // Collect keys that need TTL refresh, grouped by TTL value
        let mut ttl_groups: HashMap<i64, Vec<String>> = HashMap::new();

        // Check which user_likes keys need TTL refresh
        for (key, _) in &ul_zadd {
            let should_refresh = ttl_refresh_times
                .get(key)
                .map(|t| now.duration_since(*t) > ttl_refresh_interval)
                .unwrap_or(true);
            if should_refresh {
                if let Some(date) = extract_date_from_key(key) {
                    let ttl = ttl_for_date(date, self.config.like_ttl_days);
                    if ttl > 0 {
                        ttl_groups.entry(ttl).or_default().push(key.clone());
                    }
                }
                ttl_refresh_times.insert(key.clone(), now);
            }
        }

        // Check which post_likers keys need TTL refresh
        for (key, _) in &pl_zadd {
            let should_refresh = ttl_refresh_times
                .get(key)
                .map(|t| now.duration_since(*t) > ttl_refresh_interval)
                .unwrap_or(true);
            if should_refresh {
                if let Some(date) = extract_date_from_key(key) {
                    let ttl = ttl_for_date(date, self.config.like_ttl_days);
                    if ttl > 0 {
                        ttl_groups.entry(ttl).or_default().push(key.clone());
                    }
                }
                ttl_refresh_times.insert(key.clone(), now);
            }
        }

        // Check which author_likers keys need TTL refresh
        for (key, _) in &al_zadd {
            let should_refresh = ttl_refresh_times
                .get(key)
                .map(|t| now.duration_since(*t) > ttl_refresh_interval)
                .unwrap_or(true);
            if should_refresh {
                if let Some(date) = extract_date_from_key(key) {
                    let ttl = ttl_for_date(date, self.config.like_ttl_days);
                    if ttl > 0 {
                        ttl_groups.entry(ttl).or_default().push(key.clone());
                    }
                }
                ttl_refresh_times.insert(key.clone(), now);
            }
        }

        // Batch EXPIRE by TTL group (most keys will have the same TTL for today's date)
        for (ttl, keys) in ttl_groups {
            let expire_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            self.redis.expire_multi(&expire_refs, ttl).await?;
        }

        // No ZREMRANGEBYSCORE needed - TTL handles expiration automatically!

        // ═══════════════════════════════════════════════════════════════════
        // PHASE 3: Batch size capping (ZREMRANGEBYRANK)
        // ═══════════════════════════════════════════════════════════════════

        let mut rank_ops: Vec<(String, isize, isize)> = Vec::new();

        // User likes size cap
        let ul_max = self.config.max_likes_per_user as isize;
        for (key, _) in &ul_zadd {
            rank_ops.push((key.clone(), 0, -(ul_max + 1)));
        }

        // Post likers size cap
        let pl_max = self.config.max_likers_per_post as isize;
        for (key, _) in &pl_zadd {
            rank_ops.push((key.clone(), 0, -(pl_max + 1)));
        }

        // Author likers size cap
        let al_max = self.config.max_likers_per_author as isize;
        for (key, _) in &al_zadd {
            rank_ops.push((key.clone(), 0, -(al_max + 1)));
        }

        if !rank_ops.is_empty() {
            let rank_refs: Vec<(&str, isize, isize)> = rank_ops
                .iter()
                .map(|(k, s, e)| (k.as_str(), *s, *e))
                .collect();
            self.redis.zremrangebyrank_multi(&rank_refs).await?;
        }

        // ═══════════════════════════════════════════════════════════════════
        // PHASE 4: User liked authors (ZINCRBY + maintenance)
        // ═══════════════════════════════════════════════════════════════════

        if !user_liked_authors_increments.is_empty() {
            // Batch ZINCRBY for user_liked_authors
            let mut ula_incr_items: Vec<(String, String, i64)> = Vec::new();
            let mut ula_keys: Vec<String> = Vec::new();

            // Group by user
            let mut user_author_counts: HashMap<String, Vec<(String, i64)>> = HashMap::new();
            for ((user_hash, author_hash), count) in user_liked_authors_increments {
                user_author_counts
                    .entry(user_hash)
                    .or_default()
                    .push((author_hash, count));
            }

            for (user_hash, author_counts) in &user_author_counts {
                let ula_key = Keys::user_liked_authors(user_hash);
                ula_keys.push(ula_key.clone());

                for (author, count) in author_counts {
                    ula_incr_items.push((ula_key.clone(), author.clone(), *count));
                }
            }

            // Batch ZINCRBY
            let ula_incr_refs: Vec<(&str, &str, i64)> = ula_incr_items
                .iter()
                .map(|(k, m, c)| (k.as_str(), m.as_str(), *c))
                .collect();
            self.redis.zincrby_multi(&ula_incr_refs).await?;

            // Batch EXPIRE for ula keys that need refresh
            // Note: ula keys are NOT day-tranched, so they get the full TTL
            let ula_ttl_seconds = self.config.like_ttl_days as i64 * 24 * 60 * 60;
            let mut ula_expire: Vec<String> = Vec::new();
            for ula_key in &ula_keys {
                let should_refresh = ttl_refresh_times
                    .get(ula_key)
                    .map(|t| now.duration_since(*t) > ttl_refresh_interval)
                    .unwrap_or(true);
                if should_refresh {
                    ula_expire.push(ula_key.clone());
                    ttl_refresh_times.insert(ula_key.clone(), now);
                }
            }
            if !ula_expire.is_empty() {
                let ula_expire_refs: Vec<&str> = ula_expire.iter().map(|s| s.as_str()).collect();
                self.redis
                    .expire_multi(&ula_expire_refs, ula_ttl_seconds)
                    .await?;
            }

            // Batch ZREMRANGEBYRANK for ula size cap
            let ula_max = self.config.max_liked_authors_per_user as isize;
            let ula_rank_ops: Vec<(&str, isize, isize)> = ula_keys
                .iter()
                .map(|k| (k.as_str(), 0isize, -(ula_max + 1)))
                .collect();
            self.redis.zremrangebyrank_multi(&ula_rank_ops).await?;
        }

        // Phase 2: Save committed cursor after successful processing
        self.redis
            .set_ex(
                Keys::JETSTREAM_CURSOR,
                &latest_timestamp.to_string(),
                86400 * 30,
            )
            .await?;

        // Phase 3: Clear pending cursor to indicate successful completion
        // Ignore errors here - worst case we reprocess some events on restart
        let _ = self.redis.del(Keys::JETSTREAM_CURSOR_PENDING).await;

        // Cleanup stale TTL refresh times
        if ttl_refresh_times.len() > 100000 {
            let cutoff = now - (ttl_refresh_interval * 2);
            ttl_refresh_times.retain(|_, v| *v > cutoff);
        }

        Ok(())
    }
}

/// Parse a Jetstream message into a LikeEvent.
fn parse_like_event(message: &str) -> Option<LikeEvent> {
    let msg: JetstreamMessage = serde_json::from_str(message).ok()?;

    if msg.kind != "commit" {
        return None;
    }

    let commit = msg.commit?;
    if commit.collection.as_deref() != Some("app.bsky.feed.like") {
        return None;
    }

    if commit.operation.as_deref() != Some("create") {
        return None;
    }

    let record = commit.record?;
    let subject = record.subject?;
    let post_uri = subject.uri?;
    let user_did = msg.did?;
    let timestamp = msg.time_us.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    });

    Some(LikeEvent {
        user_did,
        post_uri,
        timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_with_jitter_bounds() {
        let base = Duration::from_secs(10);
        let max = Duration::from_secs(60);

        // Run multiple times to test randomness bounds
        for _ in 0..100 {
            let result = backoff_with_jitter(base, max);
            // Result should be between base and base + 25% jitter
            assert!(result >= base);
            assert!(result <= base + Duration::from_millis(2500)); // 25% of 10s
        }
    }

    #[test]
    fn test_backoff_with_jitter_respects_max() {
        let base = Duration::from_secs(100);
        let max = Duration::from_secs(60);

        for _ in 0..100 {
            let result = backoff_with_jitter(base, max);
            // Should be capped to max, plus up to 25% jitter
            assert!(result >= max);
            assert!(result <= max + Duration::from_millis(15000)); // 25% of 60s
        }
    }

    #[test]
    fn test_backoff_with_jitter_small_values() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(60);

        for _ in 0..100 {
            let result = backoff_with_jitter(base, max);
            // Small values should also work
            assert!(result >= base);
            assert!(result <= base + Duration::from_millis(25)); // 25% of 100ms
        }
    }

    #[test]
    fn test_backoff_with_jitter_zero() {
        let base = Duration::ZERO;
        let max = Duration::from_secs(60);

        let result = backoff_with_jitter(base, max);
        // Zero should return zero (no jitter range)
        assert_eq!(result, Duration::ZERO);
    }

    #[test]
    fn test_extract_author_did_valid_uri() {
        let uri = "at://did:plc:author123/app.bsky.feed.post/rkey456";
        let result = extract_author_did(uri);
        assert_eq!(result, Some("did:plc:author123"));
    }

    #[test]
    fn test_extract_author_did_different_collection() {
        let uri = "at://did:plc:author123/app.bsky.feed.generator/abc";
        let result = extract_author_did(uri);
        assert_eq!(result, Some("did:plc:author123"));
    }

    #[test]
    fn test_extract_author_did_web_did() {
        let uri = "at://did:web:example.com/app.bsky.feed.post/xyz";
        let result = extract_author_did(uri);
        assert_eq!(result, Some("did:web:example.com"));
    }

    #[test]
    fn test_extract_author_did_invalid_prefix() {
        let uri = "https://did:plc:author123/app.bsky.feed.post/rkey456";
        let result = extract_author_did(uri);
        assert_eq!(result, None);
    }

    #[test]
    fn test_extract_author_did_empty_string() {
        let result = extract_author_did("");
        assert_eq!(result, None);
    }

    #[test]
    fn test_extract_author_did_no_did() {
        let uri = "at://notadid/app.bsky.feed.post/rkey456";
        let result = extract_author_did(uri);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_like_event_valid() {
        let message = r#"{
            "kind": "commit",
            "did": "did:plc:testuser",
            "time_us": 1706140800000000,
            "commit": {
                "collection": "app.bsky.feed.like",
                "operation": "create",
                "record": {
                    "subject": {
                        "uri": "at://did:plc:author/app.bsky.feed.post/123"
                    }
                }
            }
        }"#;

        let event = parse_like_event(message).unwrap();
        assert_eq!(event.user_did, "did:plc:testuser");
        assert_eq!(event.post_uri, "at://did:plc:author/app.bsky.feed.post/123");
        assert_eq!(event.timestamp, 1706140800000000);
    }

    #[test]
    fn test_parse_like_event_non_like_collection() {
        let message = r#"{
            "kind": "commit",
            "did": "did:plc:testuser",
            "time_us": 1706140800000000,
            "commit": {
                "collection": "app.bsky.feed.post",
                "operation": "create",
                "record": {}
            }
        }"#;

        let event = parse_like_event(message);
        assert!(event.is_none());
    }

    #[test]
    fn test_parse_like_event_non_create_operation() {
        let message = r#"{
            "kind": "commit",
            "did": "did:plc:testuser",
            "time_us": 1706140800000000,
            "commit": {
                "collection": "app.bsky.feed.like",
                "operation": "delete",
                "record": {
                    "subject": {
                        "uri": "at://did:plc:author/app.bsky.feed.post/123"
                    }
                }
            }
        }"#;

        let event = parse_like_event(message);
        assert!(event.is_none());
    }

    #[test]
    fn test_parse_like_event_non_commit_kind() {
        let message = r#"{
            "kind": "identity",
            "did": "did:plc:testuser"
        }"#;

        let event = parse_like_event(message);
        assert!(event.is_none());
    }

    #[test]
    fn test_parse_like_event_invalid_json() {
        let message = "not valid json{";
        let event = parse_like_event(message);
        assert!(event.is_none());
    }

    #[test]
    fn test_parse_like_event_missing_subject() {
        let message = r#"{
            "kind": "commit",
            "did": "did:plc:testuser",
            "time_us": 1706140800000000,
            "commit": {
                "collection": "app.bsky.feed.like",
                "operation": "create",
                "record": {}
            }
        }"#;

        let event = parse_like_event(message);
        assert!(event.is_none());
    }

    #[test]
    fn test_like_event_struct() {
        let event = LikeEvent {
            user_did: "did:plc:testuser".to_string(),
            post_uri: "at://did:plc:author/app.bsky.feed.post/123".to_string(),
            timestamp: 1706140800000000,
        };

        assert_eq!(event.user_did, "did:plc:testuser");
        assert_eq!(event.post_uri, "at://did:plc:author/app.bsky.feed.post/123");
        assert_eq!(event.timestamp, 1706140800000000);
    }
}
