//! graze-lens-refresh: re-enqueue builds for every facet a viewer already has.
//!
//! Blobs are built on demand and live for the TTL (7 days). That is the right
//! lifecycle for `follows` — deltas keep it live between builds — but wrong for
//! the facets whose *content* is time-shaped: `velocity` claims "what my
//! network discovered this week" and would happily serve week-old "this week"
//! until its TTL lapsed. A nightly re-enqueue keeps every published facet as
//! fresh as the tables under it, which the projection job rebuilds nightly.
//!
//! Only facets that already exist are refreshed. Enqueuing all six for every
//! active viewer would make this job the biggest source of build load in the
//! system for blobs nobody asked for; refreshing what is published keeps the
//! cost proportional to actual use.

use anyhow::Context;
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::{Config as RedisConfig, Runtime};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const ACTIVE_KEY: &str = "lens:active";
const QUEUE: &str = "queue:lens";
const FACETS: &[&str] = &[
    "follows",
    "follows2",
    "niche",
    "popular",
    "velocity",
    "community",
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let url = std::env::var("LENS_REDIS_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .context("LENS_REDIS_URL is required")?;
    let pool = RedisConfig::from_url(url)
        .builder()?
        .max_size(4)
        .runtime(Runtime::Tokio1)
        .build()
        .context("redis pool")?;
    let mut conn = pool.get().await.context("redis conn")?;
    // How long to spread the enqueues over, and how many at most.
    //
    // 🔴 THIS USED TO ENQUEUE EVERYTHING AT ONCE. Measured 2026-09-17: 16.5k
    // active viewers × their published facets = ~45k builds dropped on the
    // queue at 12:13 UTC. The builder does about one build a second per pod,
    // so a NEW reader's first build — the one that decides whether their
    // first lensed page is lensed — queued behind hours of refresh work, and
    // ClickHouse answered the burst with 60 s query timeouts. Pacing the
    // enqueues over a window keeps the queue shallow so on-demand builds are
    // served within seconds all night. The window defaults to four hours;
    // the cronjob's deadline must exceed it (kube/lens-refresh-cronjob.yaml).
    //
    // LENS_REFRESH_LIMIT caps how many are enqueued — for a canary run of this
    // binary, where re-enqueuing the whole fleet is not the point.
    let spread = std::time::Duration::from_secs(env_u64("LENS_REFRESH_SPREAD_SECONDS", 4 * 3600));
    let limit = env_u64("LENS_REFRESH_LIMIT", 0) as usize;
    let viewers: Vec<String> = conn.smembers(ACTIVE_KEY).await.context("lens:active")?;
    // Count first, so the pace is right from the first enqueue rather than
    // discovered at the end.
    let mut work: Vec<(String, &'static str)> = Vec::new();
    'count: for viewer in &viewers {
        for facet in FACETS {
            let exists: bool = conn
                .exists(format!("lens:v2:{facet}:{viewer}"))
                .await
                .unwrap_or(false);
            if exists {
                work.push((viewer.clone(), facet));
                if limit > 0 && work.len() >= limit {
                    break 'count;
                }
            }
        }
    }
    let pace = if work.is_empty() {
        std::time::Duration::ZERO
    } else {
        spread / work.len() as u32
    };
    info!(
        viewers = viewers.len(),
        builds = work.len(),
        spread_seconds = spread.as_secs(),
        pace_ms = pace.as_millis(),
        limit,
        "refreshing published facets"
    );
    let mut enqueued = 0usize;
    for (i, (viewer, facet)) in work.iter().enumerate() {
        let payload = serde_json::json!({ "viewer_did": viewer, "facet": facet }).to_string();
        let result: Result<(), _> = deadpool_redis::redis::cmd("XADD")
            .arg(QUEUE)
            .arg("MAXLEN")
            .arg("~")
            .arg(100_000)
            .arg("*")
            .arg("data")
            .arg(&payload)
            .query_async(&mut conn)
            .await;
        match result {
            Ok(()) => enqueued += 1,
            Err(e) => warn!(viewer, facet, error = %e, "enqueue failed"),
        }
        if (i + 1) % 1000 == 0 {
            info!(enqueued, remaining = work.len() - i - 1, "refresh progress");
        }
        if !pace.is_zero() {
            tokio::time::sleep(pace).await;
        }
    }

    // Feed-scoped domain blobs, keyed by algorithm id rather than viewer.
    // SCAN is fine at this scale: one key per lens-enabled feed.
    let mut domains = 0usize;
    let mut cursor: u64 = 0;
    loop {
        let (next, keys): (u64, Vec<String>) = deadpool_redis::redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("lens:v2:domain:*")
            .arg("COUNT")
            .arg(500)
            .query_async(&mut conn)
            .await
            .context("scan domain blobs")?;
        for key in keys {
            let Some(algo_id) = key.rsplit(':').next().and_then(|v| v.parse::<u32>().ok()) else {
                continue;
            };
            let payload =
                serde_json::json!({ "facet": "domain", "feed_algo_id": algo_id }).to_string();
            let result: Result<(), _> = deadpool_redis::redis::cmd("XADD")
                .arg(QUEUE)
                .arg("MAXLEN")
                .arg("~")
                .arg(100_000)
                .arg("*")
                .arg("data")
                .arg(&payload)
                .query_async(&mut conn)
                .await;
            match result {
                Ok(()) => domains += 1,
                Err(e) => warn!(algo_id, error = %e, "domain enqueue failed"),
            }
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }

    info!(enqueued, domains, "refresh complete");
    Ok(())
}

/// A numeric env var, or its default when unset, empty or unparseable.
fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
