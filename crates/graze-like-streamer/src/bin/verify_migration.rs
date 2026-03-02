//! Verification script for day-tranche migration.
//!
//! This script verifies that the migration from legacy keys to day-tranched keys
//! was successful by:
//!
//! 1. Checking that day-tranched keys exist for all three types (ul, pl, authl)
//! 2. Sampling legacy keys and comparing counts to day-tranched equivalents
//! 3. Verifying recent writes are going to day-tranched keys
//! 4. Checking TTLs are set correctly on day-tranched keys
//!
//! Usage: REDIS_URL=rediss://... graze-verify-migration

use std::sync::Arc;

use graze_common::RedisClient;
use graze_like_streamer::config::Config;
use tracing::{info, warn};

/// Verification results
#[derive(Debug, Default)]
struct VerificationResults {
    // Key existence
    ul_tranched_keys_found: usize,
    pl_tranched_keys_found: usize,
    authl_tranched_keys_found: usize,

    // Sample comparisons
    ul_samples_checked: usize,
    ul_samples_matched: usize,
    ul_samples_close: usize, // Within 10%

    pl_samples_checked: usize,
    pl_samples_matched: usize,
    pl_samples_close: usize,

    authl_samples_checked: usize,
    authl_samples_matched: usize,
    authl_samples_close: usize,

    // Recent activity
    recent_ul_writes_to_tranched: usize,
    recent_pl_writes_to_tranched: usize,
    recent_authl_writes_to_tranched: usize,

    // TTL checks
    keys_with_ttl: usize,
    keys_without_ttl: usize,

    // Errors
    errors: Vec<String>,
}

impl VerificationResults {
    fn is_success(&self) -> bool {
        // Success criteria:
        // 1. Found day-tranched keys for all types
        // 2. At least 90% of samples match or are close
        // 3. Recent writes are going to tranched keys
        // 4. No critical errors

        let has_tranched_keys = self.ul_tranched_keys_found > 0
            && self.pl_tranched_keys_found > 0
            && self.authl_tranched_keys_found > 0;

        let ul_ok = self.ul_samples_checked == 0
            || (self.ul_samples_matched + self.ul_samples_close) as f64
                / self.ul_samples_checked as f64
                >= 0.9;
        let pl_ok = self.pl_samples_checked == 0
            || (self.pl_samples_matched + self.pl_samples_close) as f64
                / self.pl_samples_checked as f64
                >= 0.9;
        let authl_ok = self.authl_samples_checked == 0
            || (self.authl_samples_matched + self.authl_samples_close) as f64
                / self.authl_samples_checked as f64
                >= 0.9;

        let recent_writes_ok = self.recent_ul_writes_to_tranched > 0;

        has_tranched_keys
            && ul_ok
            && pl_ok
            && authl_ok
            && recent_writes_ok
            && self.errors.is_empty()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Starting migration verification");

    let config = Arc::new(Config::from_env());
    let redis_config = config.redis_config();
    let redis = Arc::new(RedisClient::new(&redis_config).await?);

    let mut results = VerificationResults::default();

    // 1. Check for existence of day-tranched keys
    info!("=== Phase 1: Checking day-tranched key existence ===");
    check_tranched_key_existence(&redis, &mut results).await?;

    // 2. Sample and compare legacy vs day-tranched counts
    info!("=== Phase 2: Sampling and comparing counts ===");
    sample_and_compare(&redis, &mut results).await?;

    // 3. Check recent writes are going to tranched keys
    info!("=== Phase 3: Verifying recent writes ===");
    check_recent_writes(&redis, &mut results).await?;

    // 4. Check TTLs
    info!("=== Phase 4: Checking TTLs ===");
    check_ttls(&redis, &mut results).await?;

    // Print results
    println!("\n{}", "=".repeat(60));
    println!("MIGRATION VERIFICATION RESULTS");
    println!("{}\n", "=".repeat(60));

    println!("Day-tranched keys found:");
    println!("  ul:*:d* keys: {}", results.ul_tranched_keys_found);
    println!("  pl:*:d* keys: {}", results.pl_tranched_keys_found);
    println!("  authl:*:d* keys: {}", results.authl_tranched_keys_found);

    println!("\nSample comparisons (legacy vs day-tranched):");
    println!(
        "  ul: {}/{} exact match, {}/{} within 10%",
        results.ul_samples_matched,
        results.ul_samples_checked,
        results.ul_samples_close,
        results.ul_samples_checked
    );
    println!(
        "  pl: {}/{} exact match, {}/{} within 10%",
        results.pl_samples_matched,
        results.pl_samples_checked,
        results.pl_samples_close,
        results.pl_samples_checked
    );
    println!(
        "  authl: {}/{} exact match, {}/{} within 10%",
        results.authl_samples_matched,
        results.authl_samples_checked,
        results.authl_samples_close,
        results.authl_samples_checked
    );

    println!("\nRecent writes to day-tranched keys:");
    println!(
        "  ul:*:d0 with recent data: {}",
        results.recent_ul_writes_to_tranched
    );
    println!(
        "  pl:*:d0 with recent data: {}",
        results.recent_pl_writes_to_tranched
    );
    println!(
        "  authl:*:d0 with recent data: {}",
        results.recent_authl_writes_to_tranched
    );

    println!("\nTTL status:");
    println!("  Keys with TTL set: {}", results.keys_with_ttl);
    println!("  Keys without TTL: {}", results.keys_without_ttl);

    if !results.errors.is_empty() {
        println!("\nErrors encountered:");
        for err in &results.errors {
            println!("  - {}", err);
        }
    }

    println!("\n{}", "=".repeat(60));
    if results.is_success() {
        println!("VERIFICATION PASSED");
        println!("{}", "=".repeat(60));
        Ok(())
    } else {
        println!("VERIFICATION FAILED");
        println!("{}", "=".repeat(60));
        std::process::exit(1);
    }
}

/// Check that day-tranched keys exist for all three types.
async fn check_tranched_key_existence(
    redis: &RedisClient,
    results: &mut VerificationResults,
) -> Result<(), Box<dyn std::error::Error>> {
    // Sample day-tranched keys
    let (_, ul_keys) = redis.scan(0, "ul:*:d0", 100).await?;
    results.ul_tranched_keys_found = ul_keys.len();
    info!("Found {} ul:*:d0 keys in sample", ul_keys.len());

    let (_, pl_keys) = redis.scan(0, "pl:*:d0", 100).await?;
    results.pl_tranched_keys_found = pl_keys.len();
    info!("Found {} pl:*:d0 keys in sample", pl_keys.len());

    let (_, authl_keys) = redis.scan(0, "authl:*:d0", 100).await?;
    results.authl_tranched_keys_found = authl_keys.len();
    info!("Found {} authl:*:d0 keys in sample", authl_keys.len());

    Ok(())
}

/// Sample legacy keys and compare counts to day-tranched equivalents.
async fn sample_and_compare(
    redis: &RedisClient,
    results: &mut VerificationResults,
) -> Result<(), Box<dyn std::error::Error>> {
    let sample_size = 50;

    // Sample ul: keys (legacy format without :d suffix)
    info!("Sampling ul: keys...");
    let (_, ul_legacy_keys) = redis.scan(0, "ul:*", 500).await?;
    let ul_legacy_only: Vec<&String> = ul_legacy_keys
        .iter()
        .filter(|k| !k.contains(":d"))
        .take(sample_size)
        .collect();

    for key in ul_legacy_only {
        let legacy_count = redis.zcard(key).await?;
        let hash = key.strip_prefix("ul:").unwrap_or(key);

        // Sum counts from all day tranches
        let mut tranched_total = 0usize;
        for d in 0..8 {
            let tranched_key = format!("ul:{}:d{}", hash, d);
            tranched_total += redis.zcard(&tranched_key).await?;
        }

        results.ul_samples_checked += 1;
        if legacy_count == tranched_total {
            results.ul_samples_matched += 1;
        } else if tranched_total > 0 {
            let diff_pct = ((legacy_count as f64 - tranched_total as f64).abs()
                / legacy_count.max(1) as f64)
                * 100.0;
            if diff_pct <= 10.0 {
                results.ul_samples_close += 1;
            } else {
                warn!(
                    key = %key,
                    legacy_count = legacy_count,
                    tranched_total = tranched_total,
                    diff_pct = diff_pct,
                    "Count mismatch"
                );
            }
        }
    }
    info!("ul: {} samples checked", results.ul_samples_checked);

    // Sample pl: keys
    info!("Sampling pl: keys...");
    let (_, pl_legacy_keys) = redis.scan(0, "pl:*", 500).await?;
    let pl_legacy_only: Vec<&String> = pl_legacy_keys
        .iter()
        .filter(|k| !k.contains(":d"))
        .take(sample_size)
        .collect();

    for key in pl_legacy_only {
        let legacy_count = redis.zcard(key).await?;
        let id = key.strip_prefix("pl:").unwrap_or(key);

        let mut tranched_total = 0usize;
        for d in 0..8 {
            let tranched_key = format!("pl:{}:d{}", id, d);
            tranched_total += redis.zcard(&tranched_key).await?;
        }

        results.pl_samples_checked += 1;
        if legacy_count == tranched_total {
            results.pl_samples_matched += 1;
        } else if tranched_total > 0 {
            let diff_pct = ((legacy_count as f64 - tranched_total as f64).abs()
                / legacy_count.max(1) as f64)
                * 100.0;
            if diff_pct <= 10.0 {
                results.pl_samples_close += 1;
            }
        }
    }
    info!("pl: {} samples checked", results.pl_samples_checked);

    // Sample authl: keys
    info!("Sampling authl: keys...");
    let (_, authl_legacy_keys) = redis.scan(0, "authl:*", 500).await?;
    let authl_legacy_only: Vec<&String> = authl_legacy_keys
        .iter()
        .filter(|k| !k.contains(":d"))
        .take(sample_size)
        .collect();

    for key in authl_legacy_only {
        let legacy_count = redis.zcard(key).await?;
        let hash = key.strip_prefix("authl:").unwrap_or(key);

        let mut tranched_total = 0usize;
        for d in 0..8 {
            let tranched_key = format!("authl:{}:d{}", hash, d);
            tranched_total += redis.zcard(&tranched_key).await?;
        }

        results.authl_samples_checked += 1;
        if legacy_count == tranched_total {
            results.authl_samples_matched += 1;
        } else if tranched_total > 0 {
            let diff_pct = ((legacy_count as f64 - tranched_total as f64).abs()
                / legacy_count.max(1) as f64)
                * 100.0;
            if diff_pct <= 10.0 {
                results.authl_samples_close += 1;
            }
        }
    }
    info!("authl: {} samples checked", results.authl_samples_checked);

    Ok(())
}

/// Check that recent writes are going to day-tranched keys.
async fn check_recent_writes(
    redis: &RedisClient,
    results: &mut VerificationResults,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();

    // Recent = within last hour
    let recent_cutoff = now - 3600.0;

    // Check ul:*:d0 keys for recent data
    let (_, ul_d0_keys) = redis.scan(0, "ul:*:d0", 100).await?;
    for key in ul_d0_keys.iter().take(20) {
        let entries = redis
            .zrevrangebyscore_with_scores(key, now, recent_cutoff, 1)
            .await?;
        if !entries.is_empty() {
            results.recent_ul_writes_to_tranched += 1;
        }
    }
    info!(
        "Found {} ul:*:d0 keys with recent writes",
        results.recent_ul_writes_to_tranched
    );

    // Check pl:*:d0 keys
    let (_, pl_d0_keys) = redis.scan(0, "pl:*:d0", 100).await?;
    for key in pl_d0_keys.iter().take(20) {
        let entries = redis
            .zrevrangebyscore_with_scores(key, now, recent_cutoff, 1)
            .await?;
        if !entries.is_empty() {
            results.recent_pl_writes_to_tranched += 1;
        }
    }
    info!(
        "Found {} pl:*:d0 keys with recent writes",
        results.recent_pl_writes_to_tranched
    );

    // Check authl:*:d0 keys
    let (_, authl_d0_keys) = redis.scan(0, "authl:*:d0", 100).await?;
    for key in authl_d0_keys.iter().take(20) {
        let entries = redis
            .zrevrangebyscore_with_scores(key, now, recent_cutoff, 1)
            .await?;
        if !entries.is_empty() {
            results.recent_authl_writes_to_tranched += 1;
        }
    }
    info!(
        "Found {} authl:*:d0 keys with recent writes",
        results.recent_authl_writes_to_tranched
    );

    Ok(())
}

/// Check that TTLs are set on day-tranched keys.
async fn check_ttls(
    redis: &RedisClient,
    results: &mut VerificationResults,
) -> Result<(), Box<dyn std::error::Error>> {
    // Sample d0 keys and check TTL
    let (_, keys) = redis.scan(0, "ul:*:d0", 50).await?;

    for key in keys.iter().take(20) {
        let ttl = redis.ttl(key).await?;
        if ttl > 0 {
            results.keys_with_ttl += 1;
        } else {
            results.keys_without_ttl += 1;
            if ttl == -1 {
                warn!(key = %key, "Key has no TTL set");
            }
        }
    }

    info!(
        "TTL check: {}/{} keys have TTL set",
        results.keys_with_ttl,
        results.keys_with_ttl + results.keys_without_ttl
    );

    Ok(())
}
