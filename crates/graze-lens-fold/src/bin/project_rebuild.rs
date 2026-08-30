//! graze-lens-project: rebuilds the u32 traversal projection for 2-hop queries.
//!
//! Runs on a timer, after the follow graph has moved enough to matter. Safe at
//! any time: it builds into staging and swaps, so a run that dies partway
//! leaves the live projection untouched.

use anyhow::Context;
use deadpool_redis::{Config as RedisConfig, Runtime};
use graze_common::ClickHouseConfig;
use graze_lens_fold::project::Projector;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let clickhouse = ClickHouseConfig {
        host: require("CLICKHOUSE_HOST")?,
        port: parse("CLICKHOUSE_PORT", 443)?,
        database: default_env("CLICKHOUSE_DATABASE", "default"),
        user: require("CLICKHOUSE_USER")?,
        password: require("CLICKHOUSE_PASSWORD")?,
        secure: parse("CLICKHOUSE_SECURE", true)?,
    };

    // The lens id space lives on the LENS redis, beside the blobs whose ids it
    // issues — not the shared space on the cache redis. Falls back so an unset
    // LENS_REDIS_URL keeps working, but the two must agree with the builder or
    // every id in the projection means a different account than the blobs do.
    // Required, with no fallback. A fallback here is not a convenience, it is a
    // silent corruption: the ids this issues are stamped into blobs as the lens
    // space, so interning them against a different instance produces a
    // perfectly healthy-looking run whose ids belong to nobody. That happened —
    // 50,000 ids landed on the 1.1 GiB instance and the logs said "interner
    // extended" throughout.
    let redis_url = require("LENS_REDIS_URL")
        .context("LENS_REDIS_URL is required: the lens id space must not be built against a fallback instance")?;
    if let Some(host) = redis_url
        .split('@')
        .nth(1)
        .and_then(|h| h.split(':').next())
    {
        info!(redis_host = host, "interning the lens id space");
    }
    let pool = RedisConfig::from_url(redis_url)
        .builder()?
        .max_size(parse("LENS_PROJECT_REDIS_POOL", 4)?)
        .runtime(Runtime::Tokio1)
        .build()
        .context("interner redis pool")?;

    let max_execution = parse("LENS_PROJECT_MAX_EXECUTION_SECONDS", 1_800)?;
    let projector = Projector::new(
        clickhouse,
        pool,
        Duration::from_secs(max_execution + 60),
        max_execution,
        parse("LENS_PROJECT_MAX_INTERN", 2_000_000)?,
        parse("LENS_PROJECT_MIN_RATIO", 0.5)?,
        parse("LENS_PROJECT_FORCE", false)?,
    )
    .context("projector")?;

    info!("rebuilding traversal projection");
    let r = projector.run().await.context("rebuild")?;
    info!(
        interned = r.interned,
        before = r.before,
        after = r.after,
        "traversal projection rebuild complete"
    );
    Ok(())
}

fn optional(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}
fn default_env(name: &str, fallback: &str) -> String {
    optional(name).unwrap_or_else(|| fallback.to_string())
}
fn require(name: &str) -> anyhow::Result<String> {
    optional(name).ok_or_else(|| anyhow::anyhow!("{name} is required but unset"))
}
fn parse<T>(name: &str, fallback: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match optional(name) {
        None => Ok(fallback),
        Some(raw) => raw
            .parse()
            .map_err(|e| anyhow::anyhow!("{name} is not a valid value ({raw}): {e}")),
    }
}
