//! Build follow-graph seeds (`uf:{hash}`) for users the co-liker engine cannot reach.
//!
//! Phase A of the follow-seeding plan in `DESIGN-coverage-next-lever-2026-08.md`: this writes `uf:`
//! keys and nothing else. No read path consumes them, so it cannot change what any user is served.
//!
//! Targets are selected from ClickHouse as the DIDs actually turned away with
//! `fallback_reason='no_user_data'` — which is only selectable because that reason now reaches the
//! provenance blob rather than living in a log line.
//!
//! Usage — always rehearse first:
//!   FOLLOW_SEED_DRY_RUN=1 FOLLOW_SEED_MAX_USERS=50 \
//!     cargo run --release --bin graze-build-follow-seeds
//!
//! Environment variables:
//!   REDIS_URL                        Redis connection URL (required)
//!   CLICKHOUSE_HOST/_PORT/...        ClickHouse connection (graze-candidate-sync config)
//!   FOLLOW_SEED_DRY_RUN              Report only, write nothing (default: false)
//!   FOLLOW_SEED_APPVIEW_BASE         AppView base (default: https://public.api.bsky.app)
//!   FOLLOW_SEED_MAX_FOLLOWS          Follows stored per user (default: 100)
//!   FOLLOW_SEED_MIN_FOLLOWS          Below this, store a miss marker instead (default: 10)
//!   FOLLOW_SEED_TTL_DAYS             TTL on uf: keys (default: 30)
//!   FOLLOW_SEED_MISS_TTL_HOURS       Retry cadence for unusable accounts (default: 24)
//!   FOLLOW_SEED_MAX_USERS            Per-run target cap (default: 5000)
//!   FOLLOW_SEED_REQUEST_DELAY_MS     Delay between AppView calls (default: 150)
//!   FOLLOW_SEED_LOOKBACK_DAYS        How far back to select targets (default: 7)
//!   FOLLOW_SEED_QUERY_TIMEOUT_SECS   ClickHouse timeout (default: 300)

use std::sync::Arc;

use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

use graze_candidate_sync::config::Config;
use graze_candidate_sync::follow_seeds::{FollowSeedBuilder, FollowSeedConfig};
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
    let seed_config = FollowSeedConfig::from_env();

    // Constructed exactly as build_coliker_profiles does, so both binaries read the same
    // graze-candidate-sync config rather than diverging on connection details.
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
        dry_run = seed_config.dry_run,
        max_users = seed_config.max_users,
        min_follows = seed_config.min_follows,
        ttl_days = seed_config.ttl_days,
        lookback_days = seed_config.lookback_days,
        "build_follow_seeds_starting"
    );

    let builder = FollowSeedBuilder::new(clickhouse, redis, seed_config)?;
    let stats = builder.run().await?;

    info!(
        targets = stats.targets,
        written = stats.written,
        mean_follows = stats.mean_follows(),
        already_seeded = stats.already_seeded,
        already_missed = stats.already_missed,
        too_few_follows = stats.too_few_follows,
        fetch_errors = stats.fetch_errors,
        "build_follow_seeds_done"
    );
    Ok(())
}
