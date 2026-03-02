//! Standalone backfill tool for likes data.
//!
//! Connects to Jetstream with a historical cursor and processes likes
//! until reaching the end timestamp. Does NOT update the main cursor,
//! so it can run safely alongside the live like-streamer.
//!
//! Usage:
//!   graze-backfill --start <micros> --end <micros>
//!   graze-backfill --hours 48              # Last 48 hours
//!   graze-backfill --hours 48 --dry-run    # Count events without writing

use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::EnvFilter;

use graze_common::services::UriInterner;
use graze_common::{hash_did, Keys, RedisClient};
use graze_like_streamer::config::Config;

// ============================================================================
// Jetstream message types (same as like_streamer)
// ============================================================================

#[derive(Debug, Clone)]
struct LikeEvent {
    user_did: String,
    post_uri: String,
    timestamp: i64,
}

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
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    });

    Some(LikeEvent {
        user_did,
        post_uri,
        timestamp,
    })
}

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

// ============================================================================
// Backfill configuration
// ============================================================================

struct BackfillConfig {
    start_cursor: i64,
    end_cursor: i64,
    dry_run: bool,
    batch_size: usize,
}

fn parse_args() -> Result<BackfillConfig, String> {
    let args: Vec<String> = env::args().collect();

    let mut start_cursor: Option<i64> = None;
    let mut end_cursor: Option<i64> = None;
    let mut hours: Option<u64> = None;
    let mut dry_run = false;
    let batch_size = 5000;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--start" => {
                i += 1;
                if i >= args.len() {
                    return Err("--start requires a value".to_string());
                }
                start_cursor = Some(args[i].parse().map_err(|_| "Invalid --start value")?);
            }
            "--end" => {
                i += 1;
                if i >= args.len() {
                    return Err("--end requires a value".to_string());
                }
                end_cursor = Some(args[i].parse().map_err(|_| "Invalid --end value")?);
            }
            "--hours" => {
                i += 1;
                if i >= args.len() {
                    return Err("--hours requires a value".to_string());
                }
                hours = Some(args[i].parse().map_err(|_| "Invalid --hours value")?);
            }
            "--dry-run" => {
                dry_run = true;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {
                return Err(format!("Unknown argument: {}", args[i]));
            }
        }
        i += 1;
    }

    let now_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    // Handle --hours shorthand
    if let Some(h) = hours {
        let h_micros = h as i64 * 3600 * 1_000_000;
        start_cursor = Some(now_micros - h_micros);
        if end_cursor.is_none() {
            end_cursor = Some(now_micros);
        }
    }

    let start = start_cursor.ok_or("Missing --start or --hours argument")?;
    let end = end_cursor.unwrap_or(now_micros);

    if start >= end {
        return Err("Start cursor must be before end cursor".to_string());
    }

    Ok(BackfillConfig {
        start_cursor: start,
        end_cursor: end,
        dry_run,
        batch_size,
    })
}

fn print_usage() {
    eprintln!(
        r#"
graze-backfill - Backfill historical likes from Jetstream

USAGE:
    graze-backfill --hours <HOURS>
    graze-backfill --start <MICROS> --end <MICROS>

OPTIONS:
    --hours <N>       Backfill last N hours (shorthand for --start)
    --start <MICROS>  Start timestamp in microseconds
    --end <MICROS>    End timestamp in microseconds (default: now)
    --dry-run         Count events without writing to Redis
    -h, --help        Show this help message

EXAMPLES:
    # Backfill last 24 hours
    graze-backfill --hours 24

    # Backfill specific range
    graze-backfill --start 1769734290703866 --end 1769907090703866

    # Dry run to see how many events
    graze-backfill --hours 12 --dry-run

ENVIRONMENT:
    REDIS_URL        Redis connection URL (required)
    JETSTREAM_URL    Jetstream URL (default: wss://jetstream2.us-west.bsky.network/subscribe)
"#
    );
}

fn micros_to_datetime(micros: i64) -> String {
    let secs = micros / 1_000_000;
    let dt = chrono::DateTime::from_timestamp(secs, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "invalid".to_string());
    dt
}

// ============================================================================
// Backfill runner
// ============================================================================

struct BackfillRunner {
    redis: Arc<RedisClient>,
    interner: Arc<UriInterner>,
    config: Arc<Config>,
    backfill_config: BackfillConfig,
}

impl BackfillRunner {
    fn new(
        redis: Arc<RedisClient>,
        interner: Arc<UriInterner>,
        config: Arc<Config>,
        backfill_config: BackfillConfig,
    ) -> Self {
        Self {
            redis,
            interner,
            config,
            backfill_config,
        }
    }

    async fn run(&self) -> anyhow::Result<()> {
        let start_time = Instant::now();

        info!(
            start = %micros_to_datetime(self.backfill_config.start_cursor),
            end = %micros_to_datetime(self.backfill_config.end_cursor),
            dry_run = self.backfill_config.dry_run,
            "Starting backfill"
        );

        // Build connection URL with start cursor
        let url = format!(
            "{}?wantedCollections=app.bsky.feed.like&cursor={}",
            self.config.jetstream_url, self.backfill_config.start_cursor
        );

        info!(url = %url, "Connecting to Jetstream");

        let (ws_stream, _) = connect_async(&url).await?;
        info!("Connected to Jetstream");

        let (_write, mut read) = ws_stream.split();

        let mut batch: Vec<LikeEvent> = Vec::with_capacity(self.backfill_config.batch_size);
        let mut total_events = 0u64;
        let mut total_batches = 0u64;
        #[allow(unused_assignments)]
        let mut current_timestamp: i64 = self.backfill_config.start_cursor;
        let mut ttl_refresh_times: HashMap<String, Instant> = HashMap::new();

        let read_timeout = Duration::from_secs(30);
        let stale_timeout = Duration::from_secs(60);
        let mut last_event_time = Instant::now();

        loop {
            // Check for stale connection
            if last_event_time.elapsed() > stale_timeout {
                warn!("Connection appears stale, reconnecting...");
                break;
            }

            let result = tokio::time::timeout(read_timeout, read.next()).await;

            match result {
                Ok(Some(Ok(Message::Text(text)))) => {
                    last_event_time = Instant::now();

                    if let Some(event) = parse_like_event(&text) {
                        // Check if we've reached the end
                        if event.timestamp >= self.backfill_config.end_cursor {
                            info!(
                                timestamp = %micros_to_datetime(event.timestamp),
                                "Reached end cursor, flushing final batch"
                            );

                            // Flush remaining batch
                            if !batch.is_empty() {
                                if self.backfill_config.dry_run {
                                    total_events += batch.len() as u64;
                                } else {
                                    self.flush_batch(&mut batch, &mut ttl_refresh_times).await?;
                                    total_events += batch.len() as u64;
                                }
                                total_batches += 1;
                            }

                            break;
                        }

                        current_timestamp = event.timestamp;
                        batch.push(event);

                        // Flush when batch is full
                        if batch.len() >= self.backfill_config.batch_size {
                            if self.backfill_config.dry_run {
                                total_events += batch.len() as u64;
                                batch.clear();
                            } else {
                                let batch_len = batch.len();
                                self.flush_batch(&mut batch, &mut ttl_refresh_times).await?;
                                total_events += batch_len as u64;
                            }
                            total_batches += 1;

                            // Progress update
                            let progress_pct =
                                ((current_timestamp - self.backfill_config.start_cursor) as f64
                                    / (self.backfill_config.end_cursor
                                        - self.backfill_config.start_cursor)
                                        as f64
                                    * 100.0) as u32;

                            info!(
                                batches = total_batches,
                                events = total_events,
                                progress = %format!("{}%", progress_pct),
                                cursor = %micros_to_datetime(current_timestamp),
                                "Progress"
                            );
                        }
                    }
                }
                Ok(Some(Ok(Message::Close(_)))) => {
                    info!("WebSocket closed by server");
                    break;
                }
                Ok(Some(Ok(Message::Ping(_)))) => {
                    // Pong handled automatically
                }
                Ok(Some(Ok(_))) => {
                    // Other message types, skip
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
                    // Timeout - continue and check stale
                    continue;
                }
            }
        }

        let elapsed = start_time.elapsed();

        info!(
            total_events = total_events,
            total_batches = total_batches,
            elapsed_secs = elapsed.as_secs(),
            dry_run = self.backfill_config.dry_run,
            "Backfill complete"
        );

        Ok(())
    }

    /// Flush a batch of likes to Redis.
    ///
    /// OPTIMIZED VERSION: Uses pipelining to batch all Redis operations.
    /// This reduces thousands of round trips to just a handful.
    async fn flush_batch(
        &self,
        batch: &mut Vec<LikeEvent>,
        ttl_refresh_times: &mut HashMap<String, Instant>,
    ) -> anyhow::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let events: Vec<LikeEvent> = std::mem::take(batch);

        debug!(batch_size = events.len(), "Flushing backfill batch");

        // Convert URIs to numeric IDs
        let unique_uris: Vec<String> = events.iter().map(|e| e.post_uri.clone()).collect();
        let uri_to_id = self.interner.get_or_create_ids_batch(&unique_uris).await?;

        // Build ZADD items
        let mut user_likes_items: HashMap<String, Vec<(f64, String)>> = HashMap::new();
        let mut post_likers_items: HashMap<String, Vec<(f64, String)>> = HashMap::new();
        let mut author_likers_items: HashMap<String, Vec<(f64, String)>> = HashMap::new();
        let mut user_author_counts: HashMap<String, HashMap<String, i64>> = HashMap::new();
        let mut user_likes_keys: HashSet<String> = HashSet::new();

        for event in &events {
            let user_hash = hash_did(&event.user_did);
            let timestamp_sec = event.timestamp as f64 / 1_000_000.0;

            let post_id = match uri_to_id.get(&event.post_uri) {
                Some(id) => id.to_string(),
                None => continue,
            };

            // User likes
            let user_likes_key = Keys::user_likes(&user_hash);
            user_likes_keys.insert(user_likes_key.clone());
            user_likes_items
                .entry(user_likes_key)
                .or_default()
                .push((timestamp_sec, post_id.clone()));

            // Post likers
            let post_likers_key = Keys::post_likers(&post_id);
            post_likers_items
                .entry(post_likers_key)
                .or_default()
                .push((timestamp_sec, user_hash.clone()));

            // Author-level tracking
            if let Some(author_did) = extract_author_did(&event.post_uri) {
                let author_hash = hash_did(author_did);

                // Author likers
                let al_key = Keys::author_likers(&author_hash);
                author_likers_items
                    .entry(al_key)
                    .or_default()
                    .push((timestamp_sec, user_hash.clone()));

                // User liked authors count
                *user_author_counts
                    .entry(user_hash.clone())
                    .or_default()
                    .entry(author_hash)
                    .or_insert(0) += 1;
            }
        }

        // Timing and config
        let ttl_seconds = self.config.like_ttl_days as i64 * 24 * 60 * 60;
        let ttl_refresh_interval =
            Duration::from_secs(self.config.like_ttl_refresh_interval_seconds);
        let now = Instant::now();

        let current_time_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let prune_cutoff = current_time_sec - (self.config.like_ttl_days as f64 * 24.0 * 3600.0);

        // ═══════════════════════════════════════════════════════════════════
        // PHASE 1: Batch all ZADD operations (single pipeline)
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
        // PHASE 2: Batch TTL refresh (EXPIRE + ZREMRANGEBYSCORE)
        // ═══════════════════════════════════════════════════════════════════

        let mut keys_to_expire: Vec<String> = Vec::new();
        let mut keys_to_prune: Vec<String> = Vec::new();

        // Check which user_likes keys need TTL refresh
        for (key, _) in &ul_zadd {
            let should_refresh = ttl_refresh_times
                .get(key)
                .map(|t| now.duration_since(*t) > ttl_refresh_interval)
                .unwrap_or(true);
            if should_refresh {
                keys_to_expire.push(key.clone());
                keys_to_prune.push(key.clone());
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
                keys_to_expire.push(key.clone());
                keys_to_prune.push(key.clone());
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
                keys_to_expire.push(key.clone());
                keys_to_prune.push(key.clone());
                ttl_refresh_times.insert(key.clone(), now);
            }
        }

        // Batch EXPIRE
        if !keys_to_expire.is_empty() {
            let expire_refs: Vec<&str> = keys_to_expire.iter().map(|s| s.as_str()).collect();
            self.redis.expire_multi(&expire_refs, ttl_seconds).await?;
        }

        // Batch ZREMRANGEBYSCORE for time-based pruning
        if !keys_to_prune.is_empty() {
            let prune_refs: Vec<&str> = keys_to_prune.iter().map(|s| s.as_str()).collect();
            self.redis
                .zremrangebyscore_multi(&prune_refs, f64::NEG_INFINITY, prune_cutoff)
                .await?;
        }

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

        if self.config.linklonk_normalization_enabled && !user_author_counts.is_empty() {
            // Increment user like counts
            let unique_user_hashes: Vec<&str> = user_likes_keys
                .iter()
                .filter_map(|k| k.strip_prefix("ul:"))
                .collect();

            if !unique_user_hashes.is_empty() {
                self.redis
                    .hincrby_multi(Keys::USER_LIKE_COUNTS, &unique_user_hashes)
                    .await?;
            }

            // Batch ZINCRBY for user_liked_authors
            let mut ula_incr_items: Vec<(String, String, i64)> = Vec::new();
            let mut ula_keys: Vec<String> = Vec::new();

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
                    .expire_multi(&ula_expire_refs, ttl_seconds)
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

        // NOTE: We intentionally do NOT update cursor:jetstream here
        // This allows the backfill to run alongside the live streamer

        Ok(())
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .json()
        .init();

    // Parse arguments
    let backfill_config = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            eprintln!();
            print_usage();
            std::process::exit(1);
        }
    };

    info!("Starting Graze Backfill");
    info!(
        start = %micros_to_datetime(backfill_config.start_cursor),
        end = %micros_to_datetime(backfill_config.end_cursor),
        dry_run = backfill_config.dry_run,
        "Backfill configuration"
    );

    // Load configuration
    let config = Config::from_env();

    // Create Redis client
    let redis = Arc::new(RedisClient::new(&config.redis_config()).await?);
    info!("Connected to Redis");

    // Create URI interner
    let interner = Arc::new(UriInterner::new(redis.clone()));

    // Run backfill
    let runner = BackfillRunner::new(redis, interner, Arc::new(config), backfill_config);

    runner.run().await?;

    info!("Backfill shutdown complete");
    Ok(())
}
