//! graze-lens-lpa: label-propagation community detection over the follow graph.
//!
//! Weekly, not nightly: communities are coarse structure and drift slowly, and
//! each run is several full passes over a sampled adjacency. Prereq: the
//! projection job has run (this reads `follow_graph_int` and `account_stats`).

use anyhow::Context;
use graze_common::ClickHouseConfig;
use graze_lens_fold::lpa::{Lpa, LpaConfig};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let clickhouse = ClickHouseConfig {
        host: require("CLICKHOUSE_HOST")?,
        port: parse("CLICKHOUSE_PORT", 8443)?,
        database: default_env("CLICKHOUSE_DATABASE", "default"),
        user: default_env("CLICKHOUSE_USER", "default"),
        password: require("CLICKHOUSE_PASSWORD")?,
        secure: parse("CLICKHOUSE_SECURE", true)?,
    };

    let max_execution: u64 = parse("LENS_LPA_MAX_EXECUTION_SECONDS", 3_600)?;
    let lpa = Lpa::new(LpaConfig {
        clickhouse,
        timeout: std::time::Duration::from_secs(max_execution + 60),
        max_execution_seconds: max_execution,
        sample_pct: parse("LENS_LPA_SAMPLE_PCT", 5)?,
        iterations: parse("LENS_LPA_ITERATIONS", 4)?,
        max_dominant_share: parse("LENS_LPA_MAX_DOMINANT_SHARE", 0.5)?,
    })
    .context("lpa")?;

    info!("label propagation starting");
    let r = lpa.run().await.context("lpa run")?;
    info!(
        accounts = r.accounts,
        communities = r.communities,
        largest = r.largest,
        "community detection complete"
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
