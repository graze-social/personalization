//! Configuration for the Graze Candidate Sync service.
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
    // Candidate Source Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Where to fetch candidates: `clickhouse`, `http`, or `admin_only`.
    pub candidate_source: String,
    /// Base URL for GET /v1/feeds/:algo_id/candidates when candidate_source is `http`.
    pub candidate_http_url: Option<String>,

    // ═══════════════════════════════════════════════════════════════════════════════
    // ClickHouse Configuration (used when candidate_source is clickhouse)
    // ═══════════════════════════════════════════════════════════════════════════════
    pub clickhouse_host: String,
    pub clickhouse_port: u16,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub clickhouse_secure: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Feed Access Rolling Sync Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub feed_access_sync_enabled: bool,
    pub feed_access_scan_interval_seconds: u64,
    pub feed_access_window_seconds: u64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Sync Rate Limiting and Timing
    // ═══════════════════════════════════════════════════════════════════════════════
    pub sync_rate_limit_seconds: u64,
    pub sync_refresh_threshold_minutes: u64,
    pub sync_batch_delay_ms: u64,
    pub sync_yield_every_n_batches: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Algorithm Post Sync Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub sync_preferred_max_age_hours: u32,
    pub sync_fallback_max_age_hours: u32,
    pub sync_minimum_posts: usize,
    pub algo_posts_limit: u32,
    pub algo_posts_ttl_hours: u32,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Algo Likers Sync Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub algo_likers_enabled: bool,
    pub algo_likers_batch_size: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Trending Tranche Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub trending_recency_decay_hours: f64,
    pub trending_posts_limit: usize,
    pub trending_posts_ttl_hours: u32,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Popular Tranche Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub popular_decay_hours: f64,
    pub popular_min_likes: usize,
    pub popular_posts_limit: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Velocity Tranche Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub velocity_window_hours: f64,
    pub velocity_min_likes: usize,
    pub velocity_posts_limit: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Discovery Tranche Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub author_success_decay_hours: f64,
    pub author_success_min_posts: usize,
    pub discovery_max_post_likes: usize,
    pub discovery_posts_limit: usize,

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

            // Candidate source
            candidate_source: default_env("CANDIDATE_SOURCE", "clickhouse")
                .to_lowercase()
                .trim()
                .to_string(),
            candidate_http_url: std::env::var("CANDIDATE_HTTP_URL")
                .ok()
                .and_then(|s| if s.trim().is_empty() { None } else { Some(s.trim().to_string()) }),

            // ClickHouse (when candidate_source is clickhouse)
            clickhouse_host: default_env("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port: parse_u16_env("CLICKHOUSE_PORT", 8123),
            clickhouse_database: default_env("CLICKHOUSE_DATABASE", "graze"),
            clickhouse_user: default_env("CLICKHOUSE_USER", "default"),
            clickhouse_password: default_env("CLICKHOUSE_PASSWORD", ""),
            clickhouse_secure: parse_bool_env("CLICKHOUSE_SECURE", false),

            // Feed Access Rolling Sync
            feed_access_sync_enabled: parse_bool_env("FEED_ACCESS_SYNC_ENABLED", true),
            feed_access_scan_interval_seconds: parse_u64_env(
                "FEED_ACCESS_SCAN_INTERVAL_SECONDS",
                300,
            ),
            feed_access_window_seconds: parse_u64_env(
                "FEED_ACCESS_WINDOW_SECONDS",
                86400, // 24 hours
            ),

            // Sync Rate Limiting
            sync_rate_limit_seconds: parse_u64_env("SYNC_RATE_LIMIT_SECONDS", 300),
            sync_refresh_threshold_minutes: parse_u64_env("SYNC_REFRESH_THRESHOLD_MINUTES", 30),
            sync_batch_delay_ms: parse_u64_env("SYNC_BATCH_DELAY_MS", 10),
            sync_yield_every_n_batches: parse_usize_env("SYNC_YIELD_EVERY_N_BATCHES", 10),

            // Algorithm Post Sync: up to 40k posts from the last 3 days (whichever is first)
            sync_preferred_max_age_hours: parse_u32_env("SYNC_PREFERRED_MAX_AGE_HOURS", 72), // 3 days
            sync_fallback_max_age_hours: parse_u32_env("SYNC_FALLBACK_MAX_AGE_HOURS", 72),
            sync_minimum_posts: parse_usize_env("SYNC_MINIMUM_POSTS", 1000),
            algo_posts_limit: parse_u32_env("ALGO_POSTS_LIMIT", 40000),
            algo_posts_ttl_hours: parse_u32_env("ALGO_POSTS_TTL_HOURS", 4),

            // Algo Likers Sync
            algo_likers_enabled: parse_bool_env("ALGO_LIKERS_ENABLED", true),
            algo_likers_batch_size: parse_usize_env("ALGO_LIKERS_BATCH_SIZE", 200),

            // Trending Tranche
            trending_recency_decay_hours: parse_f64_env("TRENDING_RECENCY_DECAY_HOURS", 6.0),
            trending_posts_limit: parse_usize_env("TRENDING_POSTS_LIMIT", 1000),
            trending_posts_ttl_hours: parse_u32_env("TRENDING_POSTS_TTL_HOURS", 4),

            // Popular Tranche
            popular_decay_hours: parse_f64_env("POPULAR_DECAY_HOURS", 168.0), // 7 days
            popular_min_likes: parse_usize_env("POPULAR_MIN_LIKES", 10),
            popular_posts_limit: parse_usize_env("POPULAR_POSTS_LIMIT", 500),

            // Velocity Tranche
            velocity_window_hours: parse_f64_env("VELOCITY_WINDOW_HOURS", 6.0),
            velocity_min_likes: parse_usize_env("VELOCITY_MIN_LIKES", 5),
            velocity_posts_limit: parse_usize_env("VELOCITY_POSTS_LIMIT", 300),

            // Discovery Tranche
            author_success_decay_hours: parse_f64_env("AUTHOR_SUCCESS_DECAY_HOURS", 168.0),
            author_success_min_posts: parse_usize_env("AUTHOR_SUCCESS_MIN_POSTS", 3),
            discovery_max_post_likes: parse_usize_env("DISCOVERY_MAX_POST_LIKES", 5),
            discovery_posts_limit: parse_usize_env("DISCOVERY_POSTS_LIMIT", 500),

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

fn parse_f64_env(name: &str, default: f64) -> f64 {
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
