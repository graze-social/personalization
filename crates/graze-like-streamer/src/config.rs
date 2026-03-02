//! Configuration for the Graze Like Streamer service.
//!
//! All configuration is loaded from environment variables.

use graze_common::RedisConfig;

/// Application settings loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    // ═══════════════════════════════════════════════════════════════════════════════
    // Redis Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub redis_url: String,
    pub redis_pool_size: usize,
    pub redis_connect_max_retries: u32,
    pub redis_connect_initial_delay_ms: u64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Jetstream Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub jetstream_url: String,
    pub jetstream_read_timeout_seconds: u64,
    pub jetstream_stale_timeout_seconds: u64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Like Graph Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub like_ttl_days: u32,
    pub like_batch_size: usize,
    pub like_batch_interval_ms: u64,
    pub like_ttl_refresh_interval_seconds: u64,
    pub max_likes_per_user: usize,
    pub max_likers_per_post: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Author-Level Affinity Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub max_liked_authors_per_user: usize,
    pub max_likers_per_author: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Bot Filtering Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub bot_filter_enabled: bool,
    pub bot_like_threshold: u64,
    pub bot_cache_refresh_seconds: u64,
    pub bot_log_ttl_days: u32,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Cleanup Worker Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub cleanup_interval_hours: u64,
    pub cleanup_worker_enabled: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Algorithm Feature Flags
    // ═══════════════════════════════════════════════════════════════════════════════
    pub linklonk_normalization_enabled: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Metrics Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub metrics_port: u16,
}

impl Config {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        Self {
            // Redis
            redis_url: default_env("REDIS_URL", "redis://localhost:6379"),
            redis_pool_size: parse_usize_env("REDIS_POOL_SIZE", 100),
            redis_connect_max_retries: parse_u32_env("REDIS_CONNECT_MAX_RETRIES", 10),
            redis_connect_initial_delay_ms: parse_u64_env("REDIS_CONNECT_INITIAL_DELAY_MS", 500),

            // Jetstream
            jetstream_url: default_env(
                "JETSTREAM_URL",
                "wss://jetstream2.us-west.bsky.network/subscribe",
            ),
            jetstream_read_timeout_seconds: parse_u64_env("JETSTREAM_READ_TIMEOUT_SECONDS", 45),
            jetstream_stale_timeout_seconds: parse_u64_env("JETSTREAM_STALE_TIMEOUT_SECONDS", 60),

            // Like Graph
            like_ttl_days: parse_u32_env("LIKE_TTL_DAYS", 8),
            like_batch_size: parse_usize_env("LIKE_BATCH_SIZE", 5000),
            like_batch_interval_ms: parse_u64_env("LIKE_BATCH_INTERVAL_MS", 5000),
            like_ttl_refresh_interval_seconds: parse_u64_env(
                "LIKE_TTL_REFRESH_INTERVAL_SECONDS",
                3600,
            ),
            max_likes_per_user: parse_usize_env("MAX_LIKES_PER_USER", 5000),
            max_likers_per_post: parse_usize_env("MAX_LIKERS_PER_POST", 10000),

            // Author-Level Affinity
            max_liked_authors_per_user: parse_usize_env("MAX_LIKED_AUTHORS_PER_USER", 500),
            max_likers_per_author: parse_usize_env("MAX_LIKERS_PER_AUTHOR", 1000),

            // Bot Filtering
            bot_filter_enabled: parse_bool_env("BOT_FILTER_ENABLED", true),
            bot_like_threshold: parse_u64_env("BOT_LIKE_THRESHOLD", 5000),
            bot_cache_refresh_seconds: parse_u64_env("BOT_CACHE_REFRESH_SECONDS", 300),
            bot_log_ttl_days: parse_u32_env("BOT_LOG_TTL_DAYS", 30),

            // Cleanup Worker
            cleanup_interval_hours: parse_u64_env("CLEANUP_INTERVAL_HOURS", 8),
            cleanup_worker_enabled: parse_bool_env("CLEANUP_WORKER_ENABLED", true),

            // Algorithm Feature Flags
            linklonk_normalization_enabled: parse_bool_env("LINKLONK_NORMALIZATION_ENABLED", true),

            // Metrics
            metrics_port: parse_u16_env("METRICS_PORT", 0),
        }
    }

    /// Convert to RedisConfig for graze-common.
    pub fn redis_config(&self) -> RedisConfig {
        RedisConfig {
            url: self.redis_url.clone(),
            pool_size: self.redis_pool_size,
            connect_max_retries: self.redis_connect_max_retries,
            connect_initial_delay_ms: self.redis_connect_initial_delay_ms,
        }
    }
}

// Environment variable helper functions

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
