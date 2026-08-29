//! Configuration for the backfill job.

use std::time::Duration;

use graze_common::ClickHouseConfig;

#[derive(Debug, Clone)]
pub struct Config {
    pub clickhouse: ClickHouseConfig,
    pub insert_timeout: Duration,
    /// Rows per ClickHouse insert. Backfill is bulk, so this is far larger than
    /// the fold's live batch.
    pub insert_batch: usize,

    /// Accounts backfilled concurrently. Kept low by default: this fans out to
    /// other people's PDS hosts, and a backfill is never urgent.
    pub concurrency: usize,
    pub request_timeout: Duration,
    /// Delay between pages against one PDS.
    pub page_delay: Duration,
    /// Cap on pages per account (100 records each). 500 pages covers a 50k-follow
    /// account; beyond that the account is logged and truncated rather than
    /// paging indefinitely.
    pub max_pages: usize,

    pub plc_directory: Option<String>,
    /// Resolve and fetch, but write nothing.
    pub dry_run: bool,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let clickhouse = ClickHouseConfig {
            host: require("CLICKHOUSE_HOST")?,
            port: parse("CLICKHOUSE_PORT", 443)?,
            database: default("CLICKHOUSE_DATABASE", "default"),
            user: require("CLICKHOUSE_USER")?,
            password: require("CLICKHOUSE_PASSWORD")?,
            secure: parse("CLICKHOUSE_SECURE", true)?,
        };

        Ok(Self {
            clickhouse,
            insert_timeout: Duration::from_secs(parse(
                "LENS_BOOTSTRAP_INSERT_TIMEOUT_SECONDS",
                60,
            )?),
            insert_batch: parse("LENS_BOOTSTRAP_INSERT_BATCH", 20_000)?,

            concurrency: parse("LENS_BOOTSTRAP_CONCURRENCY", 4)?,
            request_timeout: Duration::from_secs(parse("LENS_BOOTSTRAP_REQUEST_TIMEOUT", 20)?),
            page_delay: Duration::from_millis(parse("LENS_BOOTSTRAP_PAGE_DELAY_MS", 150)?),
            max_pages: parse("LENS_BOOTSTRAP_MAX_PAGES", 600)?,

            plc_directory: optional("LENS_BOOTSTRAP_PLC_DIRECTORY"),
            dry_run: parse("LENS_BOOTSTRAP_DRY_RUN", false)?,
        })
    }
}

fn optional(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn default(name: &str, fallback: &str) -> String {
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
