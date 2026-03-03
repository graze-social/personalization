//! Configuration for the Graze API service.
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
    /// Optional Redis URL for post-render / request logging (e.g. log_tasks queue).
    /// When unset, post-render logging is skipped quietly.
    pub redis_requests_logger_url: Option<String>,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Inverted Algorithm Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub inverted_algorithm_enabled: bool,
    pub inverted_min_post_likes: usize,
    pub inverted_max_likers_per_post: usize,
    pub inverted_max_posts_to_score: usize,
    pub min_overlapping_colikers: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Liker Cache Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub liker_cache_enabled: bool,
    pub liker_cache_max_size: usize,
    pub liker_cache_ttl_seconds: u64,
    pub liker_cache_prewarm_count: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Local Algo Cache Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub local_algo_cache_ttl_seconds: u64,
    pub local_algo_cache_max_algos: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Bloom Filter Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub seen_bloom_enabled: bool,
    pub seen_bloom_expected_items: usize,
    pub seen_bloom_false_positive_rate: f64,
    pub seen_bloom_max_users: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Trending Posts Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub trending_posts_limit: usize,
    pub trending_posts_ttl_hours: u32,
    pub trending_recency_decay_hours: f64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Fallback Tranches Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub fallback_personalization_ratio: f64,
    pub fallback_popular_ratio: f64,
    pub fallback_trending_ratio: f64,
    pub fallback_discovery_ratio: f64,
    pub fallback_stagger_factor: f64,
    pub popular_posts_limit: usize,
    pub popular_decay_hours: f64,
    pub popular_min_likes: usize,
    pub velocity_window_hours: f64,
    pub velocity_min_likes: usize,
    pub velocity_posts_limit: usize,
    pub author_success_min_posts: usize,
    pub author_success_decay_hours: f64,
    pub discovery_posts_limit: usize,
    pub discovery_max_post_likes: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Progressive Blending Thresholds
    // ═══════════════════════════════════════════════════════════════════════════════
    pub cold_user_max_likes: usize,
    pub warm_user_max_likes: usize,
    pub cold_user_trending_ratio: f64,
    pub warm_user_trending_ratio: f64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Liked Posts Filter Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    /// When true, filter out already-liked posts from every feed response.
    pub liked_posts_filter_enabled: bool,
    /// Max number of liked posts to load for the universal filter (higher than scorer's cap).
    pub liked_posts_filter_max: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Seen Posts Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub seen_posts_ttl_hours: u32,
    pub seen_posts_enabled: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Interactions Logging Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Enable logging of all interactions (queue + worker).
    pub interactions_logging_enabled: bool,
    /// Backend for interaction writes: `clickhouse` or `none`.
    pub interactions_writer: String,
    /// Interval for batched ClickHouse writes (ms).
    pub interactions_batch_interval_ms: u64,
    /// Max interactions to batch before flushing.
    pub interactions_batch_size: usize,
    /// Channel capacity for the interaction queue.
    pub interactions_queue_capacity: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Special Posts Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Source for special posts: `remote` (fetch from API) or `local` (Redis only, admin CRUD).
    pub special_posts_source: String,
    /// API base URL when special_posts_source is remote.
    pub special_posts_api_base: String,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Feed Access Sync Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub feed_access_sync_enabled: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Feed Cache Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub feed_cache_ttl_seconds: u64,
    pub feed_cache_size: usize,
    pub feed_cache_enabled: bool,
    pub feed_cache_stale_threshold_seconds: u64,
    pub feed_cache_batch_size: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Co-liker Pre-computation Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub coliker_ttl_seconds: u64,
    pub coliker_refresh_threshold_seconds: u64,
    pub coliker_max_sources: usize,
    pub coliker_enabled: bool,
    pub linklonk_normalization_enabled: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Author-Affinity Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub author_affinity_enabled: bool,
    pub max_liked_authors_per_user: usize,
    pub max_likers_per_author: usize,
    pub author_affinity_max_authors: usize,
    pub author_affinity_max_colikers: usize,
    pub author_affinity_max_likers_per_author: usize,
    pub author_affinity_time_window_hours: u32,
    pub author_affinity_ttl_seconds: u64,
    pub author_affinity_refresh_threshold_seconds: u64,
    pub author_affinity_max_posts_to_score: usize,
    pub author_affinity_min_author_likes: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Personalization Defaults
    // ═══════════════════════════════════════════════════════════════════════════════
    pub stale_refresh_threshold_seconds: u64,
    pub default_max_user_likes: usize,
    pub default_max_sources_per_post: usize,
    pub default_min_co_likes: usize,
    pub default_time_window_hours: f64,
    pub default_recency_half_life_hours: f64,
    pub default_specificity_power: f64,
    pub default_popularity_power: f64,
    pub default_num_paths_power: f64,
    pub max_coliker_weight: f64,
    pub prove_max_posts_to_sample: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Author Diversity Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub diversity_enabled: bool,
    pub max_posts_per_author: usize,
    pub author_diminishing_factor: f64,
    pub diversity_mmr_lambda: f64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // HTTP Server Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub http_host: String,
    pub http_port: u16,
    pub http_external: String,
    pub http_workers: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Feed Generator Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub feed_generator_did: String,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Metrics Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub metrics_enabled: bool,
    pub metrics_port: u16,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Read-Only Mode Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub read_only_mode: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Admin API Authentication
    // ═══════════════════════════════════════════════════════════════════════════════
    /// When set, all non-ATProto / non-well-known endpoints require this key via Authorization: Bearer or X-API-Key.
    pub admin_api_key: Option<String>,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Personalization A/B Test Holdout
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Fraction of first-page requests (0.0–1.0) served with non-personalized fallback blend
    /// for A/B testing. Default 0.5 = 50/50 split. Enables downstream comparison of engagement vs personalized.
    pub personalization_holdout_rate: f64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Audit Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub audit_enabled: bool,
    pub audit_all_users: bool,
    pub audit_sample_rate: f64,
    pub audit_log_full_breakdown: bool,
    pub audit_max_contributors: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // ClickHouse Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub clickhouse_host: String,
    pub clickhouse_port: u16,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub clickhouse_database: String,
    pub clickhouse_secure: bool,
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
            redis_requests_logger_url: std::env::var("REDIS_REQUESTS_LOGGER").ok().and_then(|s| {
                if s.trim().is_empty() {
                    None
                } else {
                    Some(s)
                }
            }),

            // Inverted Algorithm
            inverted_algorithm_enabled: parse_bool_env("INVERTED_ALGORITHM_ENABLED", true),
            inverted_min_post_likes: parse_usize_env("INVERTED_MIN_POST_LIKES", 10),
            inverted_max_likers_per_post: parse_usize_env("INVERTED_MAX_LIKERS_PER_POST", 30),
            inverted_max_posts_to_score: parse_usize_env("INVERTED_MAX_POSTS_TO_SCORE", 0),
            min_overlapping_colikers: parse_usize_env("MIN_OVERLAPPING_COLIKERS", 1),

            // Liker Cache
            liker_cache_enabled: parse_bool_env("LIKER_CACHE_ENABLED", true),
            liker_cache_max_size: parse_usize_env("LIKER_CACHE_MAX_SIZE", 400000),
            liker_cache_ttl_seconds: parse_u64_env("LIKER_CACHE_TTL_SECONDS", 600),
            liker_cache_prewarm_count: parse_usize_env("LIKER_CACHE_PREWARM_COUNT", 10000),

            // Local Algo Cache
            local_algo_cache_ttl_seconds: parse_u64_env("LOCAL_ALGO_CACHE_TTL_SECONDS", 120),
            local_algo_cache_max_algos: parse_usize_env("LOCAL_ALGO_CACHE_MAX_ALGOS", 100),

            // Bloom Filter
            seen_bloom_enabled: parse_bool_env("SEEN_BLOOM_ENABLED", true),
            seen_bloom_expected_items: parse_usize_env("SEEN_BLOOM_EXPECTED_ITEMS", 10000),
            seen_bloom_false_positive_rate: parse_f64_env("SEEN_BLOOM_FALSE_POSITIVE_RATE", 0.01),
            seen_bloom_max_users: parse_usize_env("SEEN_BLOOM_MAX_USERS", 20000),

            // Trending Posts
            trending_posts_limit: parse_usize_env("TRENDING_POSTS_LIMIT", 500),
            trending_posts_ttl_hours: parse_u32_env("TRENDING_POSTS_TTL_HOURS", 1),
            trending_recency_decay_hours: parse_f64_env("TRENDING_RECENCY_DECAY_HOURS", 24.0),

            // Fallback Tranches
            fallback_personalization_ratio: parse_f64_env("FALLBACK_PERSONALIZATION_RATIO", 0.80),
            fallback_popular_ratio: parse_f64_env("FALLBACK_POPULAR_RATIO", 0.34),
            fallback_trending_ratio: parse_f64_env("FALLBACK_TRENDING_RATIO", 0.33),
            fallback_discovery_ratio: parse_f64_env("FALLBACK_DISCOVERY_RATIO", 0.33),
            fallback_stagger_factor: parse_f64_env("FALLBACK_STAGGER_FACTOR", 0.3),
            popular_posts_limit: parse_usize_env("POPULAR_POSTS_LIMIT", 500),
            popular_decay_hours: parse_f64_env("POPULAR_DECAY_HOURS", 48.0),
            popular_min_likes: parse_usize_env("POPULAR_MIN_LIKES", 10),
            velocity_window_hours: parse_f64_env("VELOCITY_WINDOW_HOURS", 6.0),
            velocity_min_likes: parse_usize_env("VELOCITY_MIN_LIKES", 3),
            velocity_posts_limit: parse_usize_env("VELOCITY_POSTS_LIMIT", 500),
            author_success_min_posts: parse_usize_env("AUTHOR_SUCCESS_MIN_POSTS", 3),
            author_success_decay_hours: parse_f64_env("AUTHOR_SUCCESS_DECAY_HOURS", 48.0),
            discovery_posts_limit: parse_usize_env("DISCOVERY_POSTS_LIMIT", 500),
            discovery_max_post_likes: parse_usize_env("DISCOVERY_MAX_POST_LIKES", 5),

            // Progressive Blending
            cold_user_max_likes: parse_usize_env("COLD_USER_MAX_LIKES", 5),
            warm_user_max_likes: parse_usize_env("WARM_USER_MAX_LIKES", 20),
            cold_user_trending_ratio: parse_f64_env("COLD_USER_TRENDING_RATIO", 0.8),
            warm_user_trending_ratio: parse_f64_env("WARM_USER_TRENDING_RATIO", 0.5),

            // Liked Posts Filter
            liked_posts_filter_enabled: parse_bool_env("LIKED_POSTS_FILTER_ENABLED", true),
            liked_posts_filter_max: parse_usize_env("LIKED_POSTS_FILTER_MAX", 2000),

            // Seen Posts
            seen_posts_ttl_hours: parse_u32_env("SEEN_POSTS_TTL_HOURS", 48),
            seen_posts_enabled: parse_bool_env("SEEN_POSTS_ENABLED", true),

            // Interactions Logging
            interactions_logging_enabled: parse_bool_env("INTERACTIONS_LOGGING_ENABLED", true),
            interactions_writer: default_env("INTERACTIONS_WRITER", "clickhouse")
                .to_lowercase()
                .trim()
                .to_string(),
            interactions_batch_interval_ms: parse_u64_env("INTERACTIONS_BATCH_INTERVAL_MS", 3000),
            interactions_batch_size: parse_usize_env("INTERACTIONS_BATCH_SIZE", 200),
            interactions_queue_capacity: parse_usize_env("INTERACTIONS_QUEUE_CAPACITY", 5000),

            // Special Posts
            special_posts_source: default_env("SPECIAL_POSTS_SOURCE", "remote")
                .to_lowercase()
                .trim()
                .to_string(),
            special_posts_api_base: default_env(
                "SPECIAL_POSTS_API_BASE",
                "https://api.graze.social/app/my_feeds",
            )
            .trim()
            .to_string(),

            // Feed Access Sync
            feed_access_sync_enabled: parse_bool_env("FEED_ACCESS_SYNC_ENABLED", true),

            // Feed Cache
            feed_cache_ttl_seconds: parse_u64_env("FEED_CACHE_TTL_SECONDS", 600),
            feed_cache_size: parse_usize_env("FEED_CACHE_SIZE", 1000),
            feed_cache_enabled: parse_bool_env("FEED_CACHE_ENABLED", true),
            feed_cache_stale_threshold_seconds: parse_u64_env(
                "FEED_CACHE_STALE_THRESHOLD_SECONDS",
                120,
            ),
            feed_cache_batch_size: parse_usize_env("FEED_CACHE_BATCH_SIZE", 300),

            // Co-liker Pre-computation
            coliker_ttl_seconds: parse_u64_env("COLIKER_TTL_SECONDS", 21600),
            coliker_refresh_threshold_seconds: parse_u64_env(
                "COLIKER_REFRESH_THRESHOLD_SECONDS",
                3600,
            ),
            coliker_max_sources: parse_usize_env("COLIKER_MAX_SOURCES", 1000),
            coliker_enabled: parse_bool_env("COLIKER_ENABLED", true),
            linklonk_normalization_enabled: parse_bool_env("LINKLONK_NORMALIZATION_ENABLED", true),

            // Author-Affinity
            author_affinity_enabled: parse_bool_env("AUTHOR_AFFINITY_ENABLED", true),
            max_liked_authors_per_user: parse_usize_env("MAX_LIKED_AUTHORS_PER_USER", 500),
            max_likers_per_author: parse_usize_env("MAX_LIKERS_PER_AUTHOR", 1000),
            author_affinity_max_authors: parse_usize_env("AUTHOR_AFFINITY_MAX_AUTHORS", 100),
            author_affinity_max_colikers: parse_usize_env("AUTHOR_AFFINITY_MAX_COLIKERS", 300),
            author_affinity_max_likers_per_author: parse_usize_env(
                "AUTHOR_AFFINITY_MAX_LIKERS_PER_AUTHOR",
                100,
            ),
            author_affinity_time_window_hours: parse_u32_env(
                "AUTHOR_AFFINITY_TIME_WINDOW_HOURS",
                168,
            ),
            author_affinity_ttl_seconds: parse_u64_env("AUTHOR_AFFINITY_TTL_SECONDS", 3600),
            author_affinity_refresh_threshold_seconds: parse_u64_env(
                "AUTHOR_AFFINITY_REFRESH_THRESHOLD_SECONDS",
                600,
            ),
            author_affinity_max_posts_to_score: parse_usize_env(
                "AUTHOR_AFFINITY_MAX_POSTS_TO_SCORE",
                500,
            ),
            author_affinity_min_author_likes: parse_usize_env(
                "AUTHOR_AFFINITY_MIN_AUTHOR_LIKES",
                2,
            ),

            // Personalization Defaults
            stale_refresh_threshold_seconds: parse_u64_env("STALE_REFRESH_THRESHOLD_SECONDS", 60),
            default_max_user_likes: parse_usize_env("DEFAULT_MAX_USER_LIKES", 750),
            default_max_sources_per_post: parse_usize_env("DEFAULT_MAX_SOURCES_PER_POST", 100),
            default_min_co_likes: parse_usize_env("DEFAULT_MIN_CO_LIKES", 1),
            default_time_window_hours: parse_f64_env("DEFAULT_TIME_WINDOW_HOURS", 168.0),
            default_recency_half_life_hours: parse_f64_env("DEFAULT_RECENCY_HALF_LIFE_HOURS", 24.0),
            default_specificity_power: parse_f64_env("DEFAULT_SPECIFICITY_POWER", 1.0),
            default_popularity_power: parse_f64_env("DEFAULT_POPULARITY_POWER", 0.6),
            default_num_paths_power: parse_f64_env("DEFAULT_NUM_PATHS_POWER", 0.3),
            max_coliker_weight: parse_f64_env("MAX_COLIKER_WEIGHT", 0.000001),
            prove_max_posts_to_sample: parse_usize_env("PROVE_MAX_POSTS_TO_SAMPLE", 0),

            // Author Diversity
            diversity_enabled: parse_bool_env("DIVERSITY_ENABLED", true),
            max_posts_per_author: parse_usize_env("MAX_POSTS_PER_AUTHOR", 3),
            author_diminishing_factor: parse_f64_env("AUTHOR_DIMINISHING_FACTOR", 0.5),
            diversity_mmr_lambda: parse_f64_env("DIVERSITY_MMR_LAMBDA", 0.3),

            // HTTP Server
            http_host: default_env("HTTP_HOST", "0.0.0.0"),
            http_port: parse_u16_env("HTTP_PORT", 8080),
            http_external: default_env("HTTP_EXTERNAL", ""),
            http_workers: parse_usize_env("HTTP_WORKERS", 4),

            // Feed Generator
            feed_generator_did: default_env("FEED_GENERATOR_DID", "did:web:labs.graze.social"),

            // Metrics
            metrics_enabled: parse_bool_env("METRICS_ENABLED", true),
            metrics_port: parse_u16_env("METRICS_PORT", 9090),

            // Read-Only Mode
            read_only_mode: parse_bool_env("READ_ONLY_MODE", false),

            // Admin API key (empty string = auth disabled)
            admin_api_key: std::env::var("ADMIN_API_KEY").ok().and_then(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(s)
                }
            }),

            // Personalization Holdout (A/B test)
            personalization_holdout_rate: parse_f64_env("PERSONALIZATION_HOLDOUT_RATE", 0.5),

            // Audit
            audit_enabled: parse_bool_env("AUDIT_ENABLED", false),
            audit_all_users: parse_bool_env("AUDIT_ALL_USERS", false),
            audit_sample_rate: parse_f64_env("AUDIT_SAMPLE_RATE", 0.0),
            audit_log_full_breakdown: parse_bool_env("AUDIT_LOG_FULL_BREAKDOWN", false),
            audit_max_contributors: parse_usize_env("AUDIT_MAX_CONTRIBUTORS", 10),

            // ClickHouse
            clickhouse_host: default_env("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port: parse_u16_env("CLICKHOUSE_PORT", 8123),
            clickhouse_user: default_env("CLICKHOUSE_USER", "default"),
            clickhouse_password: default_env("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database: default_env("CLICKHOUSE_DATABASE", "default"),
            clickhouse_secure: parse_bool_env("CLICKHOUSE_SECURE", false),
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
