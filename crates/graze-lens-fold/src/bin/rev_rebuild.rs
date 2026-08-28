//! graze-lens-rev-rebuild: rebuilds the reverse follow index.
//!
//! Runs on a timer, not on a stream. `mutuals` reads `follow_edges_rev`, whose
//! semantics do not change minute to minute, so a periodic rebuild is far
//! cheaper than resolving every unfollow's followee at ingest time — deletes are
//! ~40% of live follow traffic, and none of them name their subject.
//!
//! Safe to run at any time: it builds into a staging table and swaps, so a run
//! that fails partway leaves the live index untouched.

use anyhow::Context;
use graze_common::ClickHouseConfig;
use graze_lens_fold::rev::RevRebuilder;
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

    let max_execution_seconds = parse("LENS_REV_MAX_EXECUTION_SECONDS", 1_800)?;
    let rebuilder = RevRebuilder::new(
        clickhouse,
        // Client waits past the server's own limit so a query that hits
        // max_execution_time returns its error rather than tripping our timeout.
        Duration::from_secs(max_execution_seconds + 60),
        max_execution_seconds,
        parse("LENS_REV_MIN_RATIO", 0.5)?,
        parse("LENS_REV_FORCE", false)?,
    )
    .context("rebuilder")?;

    info!("starting reverse index rebuild");
    let report = rebuilder.run().await.context("rebuild")?;
    info!(
        before = report.before,
        after = report.after,
        "reverse index rebuild complete"
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
