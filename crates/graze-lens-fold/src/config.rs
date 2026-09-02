//! Configuration for the follow-graph fold.

use std::time::Duration;

use graze_common::ClickHouseConfig;

#[derive(Debug, Clone)]
pub struct Config {
    /// Jetstream endpoint. Pin this to a specific known-good host rather than a
    /// round-robin name: `jetstream2.us-east` has served records ~6.8h stale
    /// before (documented in turbo-deploy's Pulumi config), and a stale host
    /// looks identical to a healthy one except in the cursor-age gauge.
    pub jetstream_url: String,
    pub redis_url: String,
    pub redis_pool_size: usize,
    pub clickhouse: ClickHouseConfig,

    pub batch_size: usize,
    pub batch_interval: Duration,
    pub insert_timeout: Duration,
    /// Rows to hold in memory while inserts are failing before dropping them and
    /// resuming from the stored cursor instead.
    pub max_pending_rows: usize,
    /// Reconnect if no frame arrives within this window.
    pub read_timeout_seconds: u64,

    /// Apply the follow stream to lens sets that already exist, so they stay
    /// correct and their TTL can be long. Off means sets rot until they expire.
    pub deltas_enabled: bool,
    /// How often to reload the set of viewers worth tracking.
    pub active_refresh_interval: Duration,
    /// How often the sweeper turns dirty viewers into rebuild requests.
    ///
    /// This IS the propagation latency for a reader's own follows: a change is
    /// visible once the sweep fires and the builder drains the queue. Lower is
    /// fresher and costs more ClickHouse work per changed viewer; the coalescing
    /// means it costs nothing extra per *event*.
    pub dirty_sweep_interval: Duration,
    /// Life extension applied to a set each time a delta lands. Long on
    /// purpose: with deltas keeping it correct, an actively-read set should
    /// never expire, and expiry becomes a garbage-collector for readers who
    /// have gone away rather than a correctness mechanism.
    pub set_ttl_seconds: u64,

    pub metrics_port: u16,
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
            jetstream_url: default(
                "LENS_JETSTREAM_URL",
                "wss://jetstream.us-east.bsky.network/subscribe",
            ),
            redis_url: require("LENS_REDIS_URL")
                .or_else(|_| require("PERSONALIZATION_REDIS_URL"))?,
            redis_pool_size: parse("LENS_REDIS_POOL_SIZE", 4)?,
            clickhouse,

            batch_size: parse("LENS_FOLD_BATCH_SIZE", 5_000)?,
            batch_interval: Duration::from_millis(parse("LENS_FOLD_BATCH_INTERVAL_MS", 5_000)?),
            insert_timeout: Duration::from_secs(parse("LENS_FOLD_INSERT_TIMEOUT_SECONDS", 30)?),
            max_pending_rows: parse("LENS_FOLD_MAX_PENDING_ROWS", 250_000)?,
            read_timeout_seconds: parse("LENS_FOLD_READ_TIMEOUT_SECONDS", 45)?,

            deltas_enabled: parse("LENS_FOLD_DELTAS_ENABLED", true)?,
            dirty_sweep_interval: Duration::from_secs(parse("LENS_FOLD_DIRTY_SWEEP_SECONDS", 30)?),
            active_refresh_interval: Duration::from_secs(parse(
                "LENS_FOLD_ACTIVE_REFRESH_SECONDS",
                60,
            )?),
            // 7 days. With deltas keeping the set correct, this is a GC horizon
            // for readers who stopped reading, not a freshness bound.
            set_ttl_seconds: parse("LENS_SET_TTL_SECONDS", 604_800)?,

            metrics_port: parse("METRICS_PORT", 9090)?,
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
