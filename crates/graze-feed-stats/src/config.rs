//! Configuration for the Graze Feed Stats worker.
//!
//! All configuration is loaded from environment variables. This worker is a
//! drop-in replacement for the Python `feed_stats_runner.py` and preserves its
//! Redis / ClickHouse / Postgres contract exactly (see the crate docs).

use std::collections::HashSet;
use std::sync::Arc;

use graze_common::{exclusion_set_from_env_opt, ClickHouseConfig, RedisConfig};

/// Application settings loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    // ─── Redis ──────────────────────────────────────────────────────────────
    pub redis_url: String,
    pub redis_pool_size: usize,
    pub redis_connect_max_retries: u32,
    pub redis_connect_initial_delay_ms: u64,

    // ─── ClickHouse (analytics sink) ────────────────────────────────────────
    pub clickhouse_host: String,
    pub clickhouse_port: u16,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub clickhouse_secure: bool,

    // ─── Postgres (billing sink) ────────────────────────────────────────────
    pub database_url: String,
    pub pg_max_connections: u32,

    // ─── Worker tuning ──────────────────────────────────────────────────────
    /// Max log_tasks pulled per batch before flushing (Python: 500).
    pub log_batch_size: usize,
    /// Max seconds to spend filling a batch (Python: 60).
    pub log_batch_timeout_secs: u64,
    /// feed_requests -> last_delivered_time flush cadence (Python default 60).
    pub feed_requests_flush_interval_secs: u64,

    // ─── Metrics ────────────────────────────────────────────────────────────
    pub metrics_port: u16,

    // ─── Privacy / opt-out (EXCLUSION_LIST) ─────────────────────────────────
    pub exclusion_dids: Arc<HashSet<String>>,

    // ─── Shadow mode ────────────────────────────────────────────────────────
    /// When true every sink is redirected to an isolated target so no
    /// production data is mutated. See [`ShadowConfig`].
    pub shadow: ShadowConfig,
}

/// Shadow-mode isolation knobs. Off by default (real production sinks).
#[derive(Debug, Clone)]
pub struct ShadowConfig {
    /// Master switch. When false, everything else here is ignored.
    pub enabled: bool,
    /// Redis list consumed for feed-render logs. Real: `log_tasks`.
    /// Shadow default: `log_tasks_shadow`.
    pub log_tasks_key: String,
    /// Redis list consumed for feed-request pings. Real: `feed_requests`.
    /// Shadow default: `feed_requests_shadow`.
    pub feed_requests_key: String,
    /// Prefix prepended to ClickHouse table names. Real: "" -> `post_render_log`.
    /// Shadow default: `shadow_` -> `shadow_post_render_log`.
    pub ch_table_prefix: String,
    /// Prefix prepended to the four Redis counter keys. Real: "".
    /// Shadow default: `shadow:` -> `shadow:campaign_algo:{id}` etc.
    pub redis_key_prefix: String,
    /// When true, Postgres billing mutations are NOT applied — the intended
    /// statement + params are recorded for golden-diffing instead. Reads
    /// (algorithm/account lookups) still hit the real DB (they are read-only).
    pub pg_dry_run: bool,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log_tasks_key: "log_tasks".to_string(),
            feed_requests_key: "feed_requests".to_string(),
            ch_table_prefix: String::new(),
            redis_key_prefix: String::new(),
            pg_dry_run: false,
        }
    }
}

impl Config {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let shadow_enabled = parse_bool_env("SHADOW_MODE", false);

        // In shadow mode the queue keys / sink prefixes default to isolated
        // targets, but each is still individually overridable so the golden-diff
        // harness can point them wherever it needs.
        let shadow = ShadowConfig {
            enabled: shadow_enabled,
            log_tasks_key: default_env(
                "LOG_TASKS_KEY",
                if shadow_enabled {
                    "log_tasks_shadow"
                } else {
                    "log_tasks"
                },
            ),
            feed_requests_key: default_env(
                "FEED_REQUESTS_KEY",
                if shadow_enabled {
                    "feed_requests_shadow"
                } else {
                    "feed_requests"
                },
            ),
            ch_table_prefix: default_env(
                "SHADOW_CH_TABLE_PREFIX",
                if shadow_enabled { "shadow_" } else { "" },
            ),
            redis_key_prefix: default_env(
                "SHADOW_REDIS_KEY_PREFIX",
                if shadow_enabled { "shadow:" } else { "" },
            ),
            pg_dry_run: parse_bool_env("SHADOW_PG_DRY_RUN", shadow_enabled),
        };

        Self {
            redis_url: default_env("REDIS_URL", "redis://localhost:6379"),
            redis_pool_size: parse_usize_env("REDIS_POOL_SIZE", 100),
            redis_connect_max_retries: parse_u32_env("REDIS_CONNECT_MAX_RETRIES", 10),
            redis_connect_initial_delay_ms: parse_u64_env("REDIS_CONNECT_INITIAL_DELAY_MS", 500),

            // ClickHouse Cloud is HTTPS on 443 (Python posts to `https://$HOST`
            // with no explicit port). Keep it env-driven so local/dev can point
            // at a plain-HTTP instance on 8123.
            clickhouse_host: default_env("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port: parse_u16_env("CLICKHOUSE_PORT", 443),
            clickhouse_database: default_env("CLICKHOUSE_DATABASE", "default"),
            clickhouse_user: default_env("CLICKHOUSE_USER", "default"),
            clickhouse_password: default_env("CLICKHOUSE_PASSWORD", ""),
            clickhouse_secure: parse_bool_env("CLICKHOUSE_SECURE", true),

            database_url: default_env("DATABASE_URL", "postgres://localhost/graze"),
            pg_max_connections: parse_u32_env("PG_MAX_CONNECTIONS", 8),

            log_batch_size: parse_usize_env("LOG_BATCH_SIZE", 500),
            log_batch_timeout_secs: parse_u64_env("LOG_BATCH_TIMEOUT_SECS", 60),
            feed_requests_flush_interval_secs: parse_u64_env("FEED_REQUESTS_FLUSH_INTERVAL", 60),

            metrics_port: parse_u16_env("METRICS_PORT", 0),

            exclusion_dids: exclusion_set_from_env_opt(std::env::var("EXCLUSION_LIST").ok()),

            shadow,
        }
    }

    /// Convert to `RedisConfig` for graze-common.
    pub fn redis_config(&self) -> RedisConfig {
        RedisConfig {
            url: self.redis_url.clone(),
            pool_size: self.redis_pool_size,
            connect_max_retries: self.redis_connect_max_retries,
            connect_initial_delay_ms: self.redis_connect_initial_delay_ms,
        }
    }

    /// Convert to `ClickHouseConfig` for the analytics sink.
    pub fn clickhouse_config(&self) -> ClickHouseConfig {
        ClickHouseConfig {
            host: self.clickhouse_host.clone(),
            port: self.clickhouse_port,
            database: self.clickhouse_database.clone(),
            user: self.clickhouse_user.clone(),
            password: self.clickhouse_password.clone(),
            secure: self.clickhouse_secure,
        }
    }
}

// ─── Environment variable helpers (copied from graze-candidate-sync) ────────

fn default_env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_usize_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_u16_env(name: &str, default: u16) -> u16 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_u32_env(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_bool_env(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => matches!(v.to_lowercase().as_str(), "true" | "1" | "yes"),
        Err(_) => default,
    }
}
