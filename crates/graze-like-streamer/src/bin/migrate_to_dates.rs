//! Migration script to convert d0-d7 format keys to YYYYMMDD date format.
//!
//! This script:
//! 1. Scans all d0-d7 format keys (ul:*:d*, pl:*:d*, authl:*:d*)
//! 2. For each entry, uses the timestamp to determine the date
//! 3. Writes entries to YYYYMMDD format keys
//! 4. Sets TTL on each date key based on retention period
//!
//! Usage: REDIS_URL=rediss://... graze-migrate-dates [OPTIONS]
//!
//! Options:
//!   --dry-run           Don't write anything, just show what would be done
//!   --workers N         Number of parallel workers (default: 8)
//!   --prefix PREFIX     Only migrate one prefix: ul, pl, or authl
//!   --chars CHARS       Only migrate keys starting with these chars (e.g., --chars 0123)
//!   --depth N           Shard depth: 1=first char (16 shards), 2=first 2 chars (256 shards)
//!
//! Examples:
//!   graze-migrate-dates --workers 16 --depth 2    # More parallelism
//!   graze-migrate-dates --prefix pl --workers 16  # Target slow prefix
//!
//! The script is idempotent and can be re-run safely.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use graze_common::{date_from_timestamp, ttl_for_date, RedisClient};
use graze_like_streamer::config::Config;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

/// Migration statistics (thread-safe).
#[derive(Debug, Default)]
struct MigrationStats {
    keys_scanned: AtomicUsize,
    keys_migrated: AtomicUsize,
    entries_migrated: AtomicUsize,
    keys_skipped: AtomicUsize,
    errors: AtomicUsize,
}

impl MigrationStats {
    fn add_scanned(&self, n: usize) {
        self.keys_scanned.fetch_add(n, Ordering::Relaxed);
    }
    fn add_migrated(&self, n: usize) {
        self.keys_migrated.fetch_add(n, Ordering::Relaxed);
    }
    fn add_entries(&self, n: usize) {
        self.entries_migrated.fetch_add(n, Ordering::Relaxed);
    }
    fn add_skipped(&self, n: usize) {
        self.keys_skipped.fetch_add(n, Ordering::Relaxed);
    }
    fn add_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    fn scanned(&self) -> usize {
        self.keys_scanned.load(Ordering::Relaxed)
    }
    fn migrated(&self) -> usize {
        self.keys_migrated.load(Ordering::Relaxed)
    }
    fn entries(&self) -> usize {
        self.entries_migrated.load(Ordering::Relaxed)
    }
    fn skipped(&self) -> usize {
        self.keys_skipped.load(Ordering::Relaxed)
    }
    fn errors(&self) -> usize {
        self.errors.load(Ordering::Relaxed)
    }
}

/// Parsed command line arguments.
struct Args {
    dry_run: bool,
    workers: usize,
    prefix: Option<String>,
    chars: Option<Vec<char>>,
    depth: usize,
}

/// Parse command line arguments.
fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();

    let dry_run = args.iter().any(|a| a == "--dry-run");

    let workers = args
        .iter()
        .position(|a| a == "--workers")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);

    let prefix = args
        .iter()
        .position(|a| a == "--prefix")
        .and_then(|i| args.get(i + 1))
        .map(|s| {
            let p = s.to_lowercase();
            if !p.ends_with(':') {
                format!("{}:", p)
            } else {
                p
            }
        });

    let chars = args
        .iter()
        .position(|a| a == "--chars")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.chars().collect());

    let depth = args
        .iter()
        .position(|a| a == "--depth")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    Args {
        dry_run,
        workers,
        prefix,
        chars,
        depth,
    }
}

/// Generate shard patterns for a given depth.
fn generate_shard_patterns(depth: usize, char_filter: Option<&[char]>) -> Vec<String> {
    let hex_chars: Vec<char> = "0123456789abcdef".chars().collect();
    let base_chars: Vec<char> = match char_filter {
        Some(filter) => hex_chars
            .iter()
            .filter(|c| filter.contains(c))
            .copied()
            .collect(),
        None => hex_chars.clone(),
    };

    if depth <= 1 {
        return base_chars.iter().map(|c| c.to_string()).collect();
    }

    // For depth 2, generate all combinations
    let mut patterns = Vec::new();
    for &first in &base_chars {
        for &second in &hex_chars {
            patterns.push(format!("{}{}", first, second));
        }
    }
    patterns
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args();

    if args.dry_run {
        info!("DRY RUN MODE - no changes will be made");
    }

    let prefixes: Vec<&str> = match &args.prefix {
        Some(p) => vec![p.as_str()],
        None => vec!["ul:", "pl:", "authl:"],
    };

    let shard_patterns = generate_shard_patterns(args.depth, args.chars.as_deref());

    info!(
        workers = args.workers,
        prefixes = ?prefixes,
        shard_count = shard_patterns.len(),
        depth = args.depth,
        "Starting migration"
    );

    let config = Arc::new(Config::from_env());
    let redis_config = config.redis_config();
    let redis = Arc::new(RedisClient::new(&redis_config).await?);
    let retention_days = config.like_ttl_days;

    let stats = Arc::new(MigrationStats::default());
    let start = Instant::now();

    // Build all work items: (prefix, shard_pattern)
    let mut work_items: Vec<(String, String)> = Vec::new();
    for prefix in &prefixes {
        for shard in &shard_patterns {
            work_items.push((prefix.to_string(), shard.clone()));
        }
    }

    info!(total_work_items = work_items.len(), "Work items generated");

    // Spawn worker tasks
    let mut join_set = JoinSet::new();
    let items_per_worker = work_items.len().div_ceil(args.workers);

    for (worker_id, chunk) in work_items.chunks(items_per_worker).enumerate() {
        let redis = redis.clone();
        let stats = stats.clone();
        let patterns: Vec<(String, String)> = chunk.to_vec();
        let dry_run = args.dry_run;

        join_set.spawn(async move {
            for (prefix, shard) in patterns {
                let pattern = format!("{}{}*:d*", prefix, shard);
                info!(worker = worker_id, pattern = %pattern, "Starting pattern");
                if let Err(e) =
                    migrate_pattern(&redis, &prefix, &pattern, retention_days, dry_run, &stats)
                        .await
                {
                    warn!(worker = worker_id, pattern = %pattern, error = %e, "Worker error");
                }
            }
            worker_id
        });
    }

    // Progress reporter task
    let stats_for_progress = stats.clone();
    let progress_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            info!(
                keys_scanned = stats_for_progress.scanned(),
                keys_migrated = stats_for_progress.migrated(),
                entries_migrated = stats_for_progress.entries(),
                errors = stats_for_progress.errors(),
                "Migration progress"
            );
        }
    });

    // Wait for all workers to complete
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(worker_id) => info!(worker = worker_id, "Worker completed"),
            Err(e) => warn!(error = %e, "Worker task failed"),
        }
    }

    // Stop progress reporter
    progress_handle.abort();

    let duration_secs = start.elapsed().as_secs_f64();

    info!("Migration complete!");
    info!(
        keys_scanned = stats.scanned(),
        keys_migrated = stats.migrated(),
        entries_migrated = stats.entries(),
        keys_skipped = stats.skipped(),
        errors = stats.errors(),
        duration_secs = format!("{:.1}", duration_secs),
        keys_per_sec = format!("{:.0}", stats.scanned() as f64 / duration_secs),
        dry_run = args.dry_run,
        "Final stats"
    );

    Ok(())
}

/// Check if a key is in d0-d7 format.
fn is_d_format_key(key: &str) -> bool {
    // Check if key ends with :d followed by a single digit
    if let Some(pos) = key.rfind(":d") {
        let suffix = &key[pos + 2..];
        suffix.len() == 1 && suffix.chars().all(|c| c.is_ascii_digit())
    } else {
        false
    }
}

/// Check if a key is in YYYYMMDD format.
fn is_date_format_key(key: &str) -> bool {
    if let Some(pos) = key.rfind(':') {
        let suffix = &key[pos + 1..];
        suffix.len() == 8 && suffix.chars().all(|c| c.is_ascii_digit())
    } else {
        false
    }
}

/// Extract entity ID from a d-format key.
/// e.g., "ul:abc123:d0" -> "abc123"
fn extract_entity_id(key: &str, prefix: &str) -> Option<String> {
    let stripped = key.strip_prefix(prefix)?;
    let parts: Vec<&str> = stripped.rsplitn(2, ':').collect();
    if parts.len() == 2 {
        Some(parts[1].to_string())
    } else {
        None
    }
}

/// Migrate all keys matching a specific pattern.
async fn migrate_pattern(
    redis: &RedisClient,
    prefix: &str,
    pattern: &str,
    retention_days: u32,
    dry_run: bool,
    stats: &Arc<MigrationStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch_size = 1000;
    let mut cursor = 0u64;

    loop {
        // Scan for keys matching pattern
        let (new_cursor, keys) = redis.scan(cursor, pattern, batch_size).await?;

        for key in &keys {
            // Skip if already in date format (YYYYMMDD)
            if is_date_format_key(key) {
                stats.add_skipped(1);
                continue;
            }

            // Only process d-format keys
            if !is_d_format_key(key) {
                continue;
            }

            stats.add_scanned(1);

            // Read all entries from the d-format key
            match migrate_single_key(redis, key, prefix, retention_days, dry_run).await {
                Ok(entries) => {
                    if entries > 0 {
                        stats.add_migrated(1);
                        stats.add_entries(entries);
                    }
                }
                Err(e) => {
                    warn!(key = %key, error = %e, "Failed to migrate key");
                    stats.add_error();
                }
            }
        }

        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }

    Ok(())
}

/// Migrate a single d-format key to date format keys.
async fn migrate_single_key(
    redis: &RedisClient,
    key: &str,
    prefix: &str,
    retention_days: u32,
    dry_run: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    // Read all entries with scores (timestamps)
    let entries: Vec<(String, f64)> = redis
        .zrevrangebyscore_with_scores(key, f64::INFINITY, 0.0, 100000)
        .await?;

    if entries.is_empty() {
        return Ok(0);
    }

    // Extract entity ID from key
    let entity_id = match extract_entity_id(key, prefix) {
        Some(id) => id,
        None => {
            warn!(key = %key, "Could not extract entity ID");
            return Ok(0);
        }
    };

    // Group entries by date (YYYYMMDD)
    let mut date_buckets: HashMap<String, Vec<(f64, String)>> = HashMap::new();

    for (member, score) in &entries {
        let date = date_from_timestamp(*score);
        date_buckets
            .entry(date)
            .or_default()
            .push((*score, member.clone()));
    }

    if dry_run {
        // Just log what would happen
        for (date, items) in &date_buckets {
            let new_key = format!("{}{}:{}", prefix, entity_id, date);
            debug!(
                d_format_key = %key,
                new_key = %new_key,
                entries = items.len(),
                "Would migrate"
            );
        }
        return Ok(entries.len());
    }

    // Write to date format keys
    let mut total_written = 0;

    for (date, items) in date_buckets {
        let new_key = format!("{}{}:{}", prefix, entity_id, date);
        let ttl = ttl_for_date(&date, retention_days);

        // Skip if TTL would be too small (date is too old)
        if ttl <= 60 {
            debug!(
                key = %new_key,
                date = %date,
                ttl = ttl,
                "Skipping - date too old, TTL too short"
            );
            continue;
        }

        // Convert items for zadd
        let zadd_items: Vec<(f64, &str)> = items.iter().map(|(s, m)| (*s, m.as_str())).collect();

        // Write entries
        redis.zadd(&new_key, &zadd_items).await?;

        // Set TTL
        redis.expire(&new_key, ttl).await?;

        total_written += items.len();
    }

    Ok(total_written)
}
