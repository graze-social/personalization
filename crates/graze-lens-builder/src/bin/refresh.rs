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

    let viewers: Vec<String> = conn.smembers(ACTIVE_KEY).await.context("lens:active")?;
    info!(viewers = viewers.len(), "refreshing published facets");

    let mut enqueued = 0usize;
    for viewer in &viewers {
        for facet in FACETS {
            let exists: bool = conn
                .exists(format!("lens:v2:{facet}:{viewer}"))
                .await
                .unwrap_or(false);
            if !exists {
                continue;
            }
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
        }
    }

    info!(enqueued, "refresh complete");
    Ok(())
}
