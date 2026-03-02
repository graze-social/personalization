//! Backfill user_liked_authors from existing user_likes data.
//!
//! This tool scans all `ul:{user_hash}` keys and builds the corresponding
//! `ula:{user_hash}` keys by extracting author DIDs from post URIs.
//!
//! OPTIMIZED VERSION: Uses parallel processing and pipelining for high throughput.
//! Expected: 1000-5000 users/second (vs ~100 users/sec in sequential version).
//!
//! Usage:
//!   cargo run --release --bin graze-backfill-ula
//!
//! Environment variables:
//!   REDIS_URL - Redis connection URL (required)
//!   BACKFILL_CONCURRENCY - Number of concurrent user processors (default: 200)
//!   BACKFILL_SCAN_BATCH - Keys per SCAN iteration (default: 5000)

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Semaphore;
use tracing::{info, warn, Level};
use tracing_subscriber::EnvFilter;

use graze_common::services::UriInterner;
use graze_common::{hash_did, Keys, RedisClient};
use graze_like_streamer::config::Config;

/// Extract author DID from AT-URI.
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

/// Atomic counters for thread-safe stats.
#[derive(Debug, Default)]
struct BackfillStats {
    users_processed: AtomicUsize,
    users_skipped: AtomicUsize,
    total_authors_written: AtomicUsize,
    errors: AtomicUsize,
}

impl BackfillStats {
    fn processed(&self) -> usize {
        self.users_processed.load(Ordering::Relaxed)
    }
    fn skipped(&self) -> usize {
        self.users_skipped.load(Ordering::Relaxed)
    }
    fn authors(&self) -> usize {
        self.total_authors_written.load(Ordering::Relaxed)
    }
    fn errors(&self) -> usize {
        self.errors.load(Ordering::Relaxed)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

    info!("Starting user_liked_authors backfill (PARALLEL VERSION)");

    // Load configuration
    let config = Arc::new(Config::from_env());

    // Configurable parallelism
    let concurrency: usize = std::env::var("BACKFILL_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let scan_batch: usize = std::env::var("BACKFILL_SCAN_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);

    info!(concurrency, scan_batch, "Configuration loaded");

    // Create Redis client with larger pool for concurrency
    let redis = Arc::new(RedisClient::new(&config.redis_config()).await?);
    info!("Connected to Redis");

    // Create URI interner
    let interner = Arc::new(UriInterner::new(redis.clone()));

    // Run backfill
    let stats =
        backfill_user_liked_authors(redis, interner, config, concurrency, scan_batch).await?;

    info!(
        users_processed = stats.processed(),
        users_skipped = stats.skipped(),
        total_authors_written = stats.authors(),
        errors = stats.errors(),
        "Backfill completed"
    );

    Ok(())
}

async fn backfill_user_liked_authors(
    redis: Arc<RedisClient>,
    interner: Arc<UriInterner>,
    config: Arc<Config>,
    concurrency: usize,
    scan_batch: usize,
) -> anyhow::Result<Arc<BackfillStats>> {
    let stats = Arc::new(BackfillStats::default());
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let start_time = Instant::now();
    let ttl_seconds = config.like_ttl_days as i64 * 24 * 60 * 60;
    let max_authors = config.max_liked_authors_per_user as isize;

    info!("Scanning ul:* keys with batch size {}...", scan_batch);

    let mut cursor = 0u64;
    let mut total_keys_scanned = 0usize;
    let mut active_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        // Scan for user_likes keys
        let (new_cursor, keys) = redis.scan(cursor, "ul:*", scan_batch).await?;
        total_keys_scanned += keys.len();

        if !keys.is_empty() {
            let elapsed = start_time.elapsed().as_secs();
            let rate = if elapsed > 0 {
                stats.processed() as f64 / elapsed as f64
            } else {
                0.0
            };
            info!(
                keys_in_batch = keys.len(),
                total_scanned = total_keys_scanned,
                cursor = new_cursor,
                users_processed = stats.processed(),
                users_skipped = stats.skipped(),
                errors = stats.errors(),
                elapsed_secs = elapsed,
                rate_per_sec = format!("{:.1}", rate),
                "Processing batch"
            );
        }

        // Spawn parallel tasks for each user in this batch
        for ul_key in keys {
            let user_hash = match ul_key.strip_prefix("ul:") {
                Some(h) => h.to_string(),
                None => continue,
            };

            let permit = semaphore.clone().acquire_owned().await?;
            let redis = redis.clone();
            let interner = interner.clone();
            let stats = stats.clone();

            let handle = tokio::spawn(async move {
                let result =
                    process_single_user(&redis, &interner, &user_hash, ttl_seconds, max_authors)
                        .await;

                match result {
                    Ok(ProcessResult::Processed(author_count)) => {
                        stats.users_processed.fetch_add(1, Ordering::Relaxed);
                        stats
                            .total_authors_written
                            .fetch_add(author_count, Ordering::Relaxed);
                    }
                    Ok(ProcessResult::Skipped) => {
                        stats.users_skipped.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        warn!(user_hash, error = %e, "Failed to process user");
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }

                drop(permit);
            });

            active_tasks.push(handle);
        }

        // Periodically clean up completed tasks to avoid memory buildup
        if active_tasks.len() > concurrency * 10 {
            let mut remaining = Vec::new();
            for handle in active_tasks {
                if handle.is_finished() {
                    let _ = handle.await;
                } else {
                    remaining.push(handle);
                }
            }
            active_tasks = remaining;
        }

        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }

    // Wait for all remaining tasks to complete
    info!(
        remaining_tasks = active_tasks.len(),
        "Waiting for remaining tasks to complete"
    );
    for handle in active_tasks {
        let _ = handle.await;
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    let total = stats.processed() + stats.skipped();
    info!(
        total_users = total,
        elapsed_secs = format!("{:.1}", elapsed),
        rate_per_sec = format!("{:.1}", total as f64 / elapsed),
        "All tasks completed"
    );

    Ok(stats)
}

enum ProcessResult {
    Processed(usize),
    Skipped,
}

async fn process_single_user(
    redis: &RedisClient,
    interner: &UriInterner,
    user_hash: &str,
    ttl_seconds: i64,
    max_authors: isize,
) -> anyhow::Result<ProcessResult> {
    let ula_key = Keys::user_liked_authors(user_hash);
    let ul_key = format!("ul:{}", user_hash);

    // Check if ula: key already exists with data
    let existing_count = redis.zcard(&ula_key).await.unwrap_or(0);
    if existing_count > 0 {
        return Ok(ProcessResult::Skipped);
    }

    // Get all posts this user liked
    let posts = redis.zrevrange(&ul_key, 0, -1).await?;
    if posts.is_empty() {
        return Ok(ProcessResult::Skipped);
    }

    // Convert post IDs to URIs
    let post_ids: Vec<i64> = posts.iter().filter_map(|p| p.parse().ok()).collect();
    if post_ids.is_empty() {
        return Ok(ProcessResult::Skipped);
    }

    let id_to_uri = interner.get_uris_batch(&post_ids).await?;

    // Count likes per author
    let mut author_counts: HashMap<String, f64> = HashMap::new();
    for (_id, uri) in id_to_uri {
        if let Some(author_did) = extract_author_did(&uri) {
            let author_hash = hash_did(author_did);
            *author_counts.entry(author_hash).or_insert(0.0) += 1.0;
        }
    }

    if author_counts.is_empty() {
        return Ok(ProcessResult::Skipped);
    }

    let author_count = author_counts.len();

    // Write to ula:{user_hash}
    let items: Vec<(f64, &str)> = author_counts
        .iter()
        .map(|(author, count)| (*count, author.as_str()))
        .collect();

    redis.zadd(&ula_key, &items).await?;

    // Set TTL
    let _ = redis.expire(&ula_key, ttl_seconds).await;

    // Size cap
    let _ = redis.zremrangebyrank(&ula_key, 0, -(max_authors + 1)).await;

    Ok(ProcessResult::Processed(author_count))
}
