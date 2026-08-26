//! Build durable co-liker taste profiles (`ucl:{hash}`) from long-range like history.
//!
//! Phase A of `DESIGN-durable-coliker-profiles.md`: this writes `ucl:` keys and nothing
//! else. No read path is wired up, so it cannot change what any user is served.
//!
//! Usage:
//!   cargo run --release --bin graze-build-coliker-profiles
//!
//! Start with a dry run to see profile counts and sizes without writing:
//!   PROFILE_DRY_RUN=1 PROFILE_ONLY_BUCKET=0 cargo run --release --bin graze-build-coliker-profiles
//!
//! Environment variables:
//!   REDIS_URL                     Redis connection URL (required)
//!   CLICKHOUSE_HOST/_PORT/...     ClickHouse connection (see graze-candidate-sync config)
//!   PROFILE_DRY_RUN               Compute and report, write nothing (default: false)
//!   PROFILE_ONLY_BUCKET           Build a single bucket instead of all (default: all)
//!   PROFILE_CHUNK_COUNT           cityHash64 buckets to split the job into (default: 8)
//!   PROFILE_MAX_COLIKERS          Profile size cap (default: 128)
//!   PROFILE_MIN_SIZE              Skip profiles smaller than this (default: 10)
//!   PROFILE_HISTORY_DAYS          Like history window for the seed (default: 365)
//!   PROFILE_REQUESTER_WINDOW_DAYS Only profile users who requested a feed since (default: 30)
//!   PROFILE_MIN_HISTORY_LIKES     Minimum lifetime likes (default: 20)
//!   PROFILE_MAX_HISTORY_LIKES     Hyper-liker exclusion (default: 5000)
//!   PROFILE_MAX_SEED_POSTS        Recent liked posts used as seed (default: 128)
//!   PROFILE_MAX_SEED_POST_LIKERS  Drop seed posts above this liker count (default: 500)
//!   PROFILE_TTL_DAYS              TTL on ucl: keys (default: 7)
//!   PROFILE_WRITE_BATCH           Redis writes per pipeline (default: 500)
//!   PROFILE_QUERY_TIMEOUT_SECS    Per-chunk ClickHouse timeout (default: 1800)

use std::sync::Arc;

use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

use graze_candidate_sync::coliker_profiles::{ProfileBuilder, ProfileBuilderConfig};
use graze_candidate_sync::config::Config;
use graze_common::clickhouse::ClickHouseConfig;
use graze_common::RedisClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let config = Config::from_env();
    let builder_config = ProfileBuilderConfig::from_env();

    let clickhouse = Arc::new(ClickHouseConfig {
        host: config.clickhouse_host.clone(),
        port: config.clickhouse_port,
        database: config.clickhouse_database.clone(),
        user: config.clickhouse_user.clone(),
        password: config.clickhouse_password.clone(),
        secure: config.clickhouse_secure,
    });

    let redis = Arc::new(RedisClient::new(&config.redis_config()).await?);
    info!(
        clickhouse_host = %clickhouse.host,
        database = %clickhouse.database,
        "connected"
    );

    let stats = ProfileBuilder::new(clickhouse, redis, builder_config)
        .run()
        .await?;

    // Phase A verification numbers: compare against the design's 118 MB / ~124-mean
    // projections before wiring up any read path.
    info!(
        profiles_written = stats.profiles_written,
        skipped_too_small = stats.skipped_too_small,
        mean_profile_size = stats.mean_profile_size(),
        bytes_written = stats.bytes_written,
        megabytes_written = stats.bytes_written as f64 / 1_048_576.0,
        chunks_failed = stats.chunks_failed,
        "phase_a_summary"
    );

    if stats.chunks_failed > 0 {
        anyhow::bail!(
            "{} of {} chunks failed",
            stats.chunks_failed,
            stats.chunks_failed + stats.chunks_run
        );
    }

    Ok(())
}
