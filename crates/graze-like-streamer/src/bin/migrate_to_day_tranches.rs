//! Migration script to convert legacy like graph keys to day-tranched format.
//!
//! This script:
//! 1. Scans all legacy keys (ul:*, pl:*, authl:*)
//! 2. For each key, reads all entries with their timestamps
//! 3. Distributes entries to appropriate day-tranche keys based on timestamp
//! 4. Sets TTL on each day-tranche key based on remaining time
//!
//! Usage: REDIS_URL=rediss://... graze-migrate-tranches [--dry-run]
//!
//! The script is idempotent and can be re-run safely.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use graze_common::{day_offset_from_timestamp, ttl_for_day, RedisClient};
use graze_like_streamer::config::Config;
use tracing::{info, warn};

/// Migration statistics.
#[derive(Debug, Default)]
struct MigrationStats {
    keys_scanned: usize,
    keys_migrated: usize,
    entries_migrated: usize,
    keys_skipped_already_tranched: usize,
    errors: usize,
    duration_secs: f64,
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

    let args: Vec<String> = std::env::args().collect();
    let dry_run = args.iter().any(|a| a == "--dry-run");

    if dry_run {
        info!("DRY RUN MODE - no changes will be made");
    }

    info!("Starting migration to day-tranched keys");

    let config = Arc::new(Config::from_env());
    let redis_config = config.redis_config();
    let redis = Arc::new(RedisClient::new(&redis_config).await?);

    let mut total_stats = MigrationStats::default();
    let start = Instant::now();

    // Migrate user likes (ul:*)
    info!("Migrating user likes (ul:*)...");
    let ul_stats = migrate_pattern(&redis, &config, "ul:", dry_run).await?;
    info!(
        keys_scanned = ul_stats.keys_scanned,
        keys_migrated = ul_stats.keys_migrated,
        entries_migrated = ul_stats.entries_migrated,
        "User likes migration complete"
    );
    total_stats.keys_scanned += ul_stats.keys_scanned;
    total_stats.keys_migrated += ul_stats.keys_migrated;
    total_stats.entries_migrated += ul_stats.entries_migrated;
    total_stats.keys_skipped_already_tranched += ul_stats.keys_skipped_already_tranched;
    total_stats.errors += ul_stats.errors;

    // Migrate post likers (pl:*)
    info!("Migrating post likers (pl:*)...");
    let pl_stats = migrate_pattern(&redis, &config, "pl:", dry_run).await?;
    info!(
        keys_scanned = pl_stats.keys_scanned,
        keys_migrated = pl_stats.keys_migrated,
        entries_migrated = pl_stats.entries_migrated,
        "Post likers migration complete"
    );
    total_stats.keys_scanned += pl_stats.keys_scanned;
    total_stats.keys_migrated += pl_stats.keys_migrated;
    total_stats.entries_migrated += pl_stats.entries_migrated;
    total_stats.keys_skipped_already_tranched += pl_stats.keys_skipped_already_tranched;
    total_stats.errors += pl_stats.errors;

    // Migrate author likers (authl:*)
    info!("Migrating author likers (authl:*)...");
    let authl_stats = migrate_pattern(&redis, &config, "authl:", dry_run).await?;
    info!(
        keys_scanned = authl_stats.keys_scanned,
        keys_migrated = authl_stats.keys_migrated,
        entries_migrated = authl_stats.entries_migrated,
        "Author likers migration complete"
    );
    total_stats.keys_scanned += authl_stats.keys_scanned;
    total_stats.keys_migrated += authl_stats.keys_migrated;
    total_stats.entries_migrated += authl_stats.entries_migrated;
    total_stats.keys_skipped_already_tranched += authl_stats.keys_skipped_already_tranched;
    total_stats.errors += authl_stats.errors;

    total_stats.duration_secs = start.elapsed().as_secs_f64();

    info!("Migration complete!",);
    info!(
        keys_scanned = total_stats.keys_scanned,
        keys_migrated = total_stats.keys_migrated,
        entries_migrated = total_stats.entries_migrated,
        keys_skipped = total_stats.keys_skipped_already_tranched,
        errors = total_stats.errors,
        duration_secs = total_stats.duration_secs,
        dry_run = dry_run,
        "Final stats"
    );

    Ok(())
}

/// Migrate all keys matching a pattern to day-tranched format.
async fn migrate_pattern(
    redis: &RedisClient,
    config: &Config,
    prefix: &str,
    dry_run: bool,
) -> Result<MigrationStats, Box<dyn std::error::Error>> {
    let mut stats = MigrationStats::default();
    let mut cursor = 0u64;
    let batch_size = 500;
    let pattern = format!("{}*", prefix);
    let yield_interval = Duration::from_millis(50);
    let ttl_days = config.like_ttl_days;

    loop {
        // Scan for keys matching pattern
        let (new_cursor, keys) = redis.scan(cursor, &pattern, batch_size).await?;

        for key in &keys {
            // Skip if already a day-tranched key (contains :d followed by a number)
            if key.contains(":d")
                && key
                    .chars()
                    .last()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false)
            {
                stats.keys_skipped_already_tranched += 1;
                continue;
            }

            stats.keys_scanned += 1;

            // Read all entries from the legacy key
            match migrate_single_key(redis, key, prefix, ttl_days, dry_run).await {
                Ok(entries) => {
                    if entries > 0 {
                        stats.keys_migrated += 1;
                        stats.entries_migrated += entries;
                    }
                }
                Err(e) => {
                    warn!(key = %key, error = %e, "Failed to migrate key");
                    stats.errors += 1;
                }
            }

            // Progress logging
            if stats.keys_scanned % 1000 == 0 {
                info!(
                    prefix = prefix,
                    keys_scanned = stats.keys_scanned,
                    keys_migrated = stats.keys_migrated,
                    entries_migrated = stats.entries_migrated,
                    "Migration progress"
                );
            }
        }

        // Yield to prevent blocking
        if stats.keys_scanned % 5000 == 0 && stats.keys_scanned > 0 {
            tokio::time::sleep(yield_interval).await;
        }

        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }

    Ok(stats)
}

/// Migrate a single legacy key to day-tranched keys.
async fn migrate_single_key(
    redis: &RedisClient,
    key: &str,
    prefix: &str,
    ttl_days: u32,
    dry_run: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    // Read all entries with scores (timestamps)
    let entries: Vec<(String, f64)> = redis
        .zrevrangebyscore_with_scores(key, f64::INFINITY, 0.0, 100000)
        .await?;

    if entries.is_empty() {
        return Ok(0);
    }

    // Group entries by day bucket
    let mut day_buckets: HashMap<u8, Vec<(f64, String)>> = HashMap::new();

    for (member, score) in &entries {
        let day = day_offset_from_timestamp(*score);
        day_buckets
            .entry(day)
            .or_default()
            .push((*score, member.clone()));
    }

    // Extract the hash/id from the key (e.g., "ul:abc123" -> "abc123")
    let entity_id = key.strip_prefix(prefix).unwrap_or(key);

    if dry_run {
        // Just log what would happen
        for (day, items) in &day_buckets {
            let new_key = format!("{}{}:d{}", prefix, entity_id, day);
            info!(
                legacy_key = %key,
                new_key = %new_key,
                entries = items.len(),
                "Would migrate"
            );
        }
        return Ok(entries.len());
    }

    // Write to day-tranched keys
    let mut total_written = 0;

    for (day, items) in day_buckets {
        let new_key = format!("{}{}:d{}", prefix, entity_id, day);
        let ttl = ttl_for_day(day, ttl_days);

        // Skip if TTL would be 0 or negative
        if ttl <= 0 {
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
