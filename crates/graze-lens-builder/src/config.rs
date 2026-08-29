//! Configuration, read from the environment at process start.
//!
//! Follows the house convention (bare `std::env` helpers, no config crate) used
//! by `graze-like-streamer/src/config.rs`.

use std::time::Duration;

use graze_common::ClickHouseConfig;

#[derive(Debug, Clone)]
pub struct Config {
    /// Where lens sets and build state are published. In M0 this is the
    /// personalization instance feeder-rs already pools; M1 moves it to a
    /// dedicated `volatile-lru` Valkey.
    pub redis_url: String,
    pub redis_pool_size: usize,

    pub clickhouse: ClickHouseConfig,
    /// Bound on the follow query, kept below `query_timeout` so ClickHouse
    /// cancels the query itself rather than leaving it running after we give up
    /// (an abandoned-but-running query is what drove a past cost incident).
    pub max_execution_seconds: u64,
    pub query_timeout: Duration,

    /// Consumer group name on the build stream.
    pub consumer_group: String,
    /// This worker's identity within the group; must be unique per replica.
    pub consumer_name: String,
    /// Messages to claim per read.
    pub batch_size: usize,
    /// How long to block waiting for work before looping.
    pub block: Duration,

    /// TTL on a built lens set.
    ///
    /// Long, because graze-lens-fold now applies the follow stream to live sets
    /// and extends this on every delta. It is a garbage-collection horizon for
    /// readers who stopped reading, not a freshness bound — the short TTL this
    /// used to carry meant an active reader kept falling off the end of their
    /// own lens and seeing an unfiltered feed while it rebuilt.
    pub set_ttl: Duration,
    /// Refuse to publish a set larger than this. A pathological account should
    /// degrade to "no lens" rather than push a multi-megabyte value into Redis
    /// on the serve path's critical read.
    pub max_set_size: usize,

    pub metrics_port: u16,

    /// Builds allowed to run at once. Almost all of a build is waiting -- on
    /// someone else's PDS during a backfill, or on ClickHouse -- so overlapping
    /// them costs little and stops one slow viewer stalling the queue.
    pub concurrency: usize,
    /// How long to let in-flight builds finish on shutdown.
    pub drain_timeout: Duration,

    /// Backfill settings, used when a viewer has no follow history on record.
    /// Deliberately polite: this fans out to other people's PDS hosts, and a
    /// backfill is never urgent — the feed serves unlensed until it lands.
    pub backfill_request_timeout: Duration,
    pub backfill_page_delay: Duration,
    pub backfill_max_pages: usize,
    pub plc_directory: Option<String>,
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

        let max_execution_seconds = parse("LENS_QUERY_MAX_EXECUTION_SECONDS", 20)?;

        Ok(Self {
            redis_url: require("LENS_REDIS_URL")
                .or_else(|_| require("PERSONALIZATION_REDIS_URL"))?,
            redis_pool_size: parse("LENS_REDIS_POOL_SIZE", 8)?,

            clickhouse,
            max_execution_seconds,
            // Client waits slightly longer than the server's own limit, so a
            // query that hits `max_execution_time` returns its error to us
            // instead of tripping the client timeout first.
            query_timeout: Duration::from_secs(max_execution_seconds + 5),

            consumer_group: default("LENS_CONSUMER_GROUP", "builders"),
            consumer_name: std::env::var("HOSTNAME")
                .ok()
                .filter(|h| !h.is_empty())
                .unwrap_or_else(|| "lens-builder-0".to_string()),
            batch_size: parse("LENS_BATCH_SIZE", 16)?,
            block: Duration::from_millis(parse("LENS_BLOCK_MS", 5_000)?),

            set_ttl: Duration::from_secs(parse("LENS_SET_TTL_SECONDS", 604_800)?),
            max_set_size: parse("LENS_MAX_SET_SIZE", 200_000)?,

            metrics_port: parse("METRICS_PORT", 9090)?,

            concurrency: parse("LENS_BUILD_CONCURRENCY", 8)?,
            drain_timeout: Duration::from_secs(parse("LENS_BUILD_DRAIN_SECONDS", 30)?),

            backfill_request_timeout: Duration::from_secs(parse(
                "LENS_BOOTSTRAP_REQUEST_TIMEOUT",
                20,
            )?),
            backfill_page_delay: Duration::from_millis(parse("LENS_BOOTSTRAP_PAGE_DELAY_MS", 150)?),
            backfill_max_pages: parse("LENS_BOOTSTRAP_MAX_PAGES", 600)?,
            plc_directory: optional("LENS_BOOTSTRAP_PLC_DIRECTORY"),
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
