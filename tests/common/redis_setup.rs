//! Redis test setup utilities.
//!
//! Provides helpers for setting up Redis connections for testing:
//! - Real Redis via REDIS_URL environment variable
//! - Docker-based Redis via testcontainers
//!
//! To run integration tests with real Redis:
//!   REDIS_URL=redis://localhost:6379 cargo test --features integration-tests
//!
//! To run with Docker (testcontainers):
//!   INTEGRATION_USE_DOCKER=1 cargo test --features integration-tests

use std::env;
use std::sync::Arc;

use graze::config::Config;
use graze::redis::RedisClient;

/// Test context holding shared resources.
pub struct TestContext {
    pub redis: Arc<RedisClient>,
    pub config: Arc<Config>,
}

impl TestContext {
    /// Create a new test context.
    ///
    /// This will:
    /// 1. Use REDIS_URL if set
    /// 2. Use Docker container if INTEGRATION_USE_DOCKER is set
    /// 3. Skip if neither is available
    pub async fn new() -> Option<Self> {
        let redis_url = get_redis_url()?;

        // Load config with defaults, then override redis_url
        let config = create_test_config(redis_url);

        let redis = match RedisClient::new(&config).await {
            Ok(client) => Arc::new(client),
            Err(e) => {
                eprintln!("Failed to connect to Redis: {}", e);
                return None;
            }
        };

        // Flush the database for a clean test state
        if let Err(e) = flush_db(&redis).await {
            eprintln!("Failed to flush Redis: {}", e);
            return None;
        }

        Some(Self {
            redis,
            config: Arc::new(config),
        })
    }

    /// Flush the test database.
    #[allow(dead_code)]
    pub async fn flush(&self) -> graze::error::Result<()> {
        flush_db(&self.redis).await
    }
}

/// Create a test configuration with the given Redis URL.
fn create_test_config(redis_url: String) -> Config {
    Config {
        // Redis
        redis_url,
        redis_pool_size: 4,
        redis_socket_timeout: 5.0,
        redis_socket_connect_timeout: 3.0,
        redis_health_check_interval: 15,
        redis_connect_max_retries: 3,
        redis_connect_initial_delay_ms: 100,

        // ClickHouse
        clickhouse_host: "localhost".to_string(),
        clickhouse_port: 8123,
        clickhouse_database: "default".to_string(),
        clickhouse_user: "default".to_string(),
        clickhouse_password: String::new(),
        clickhouse_secure: false,

        // Jetstream
        jetstream_url: "wss://jetstream2.us-west.bsky.network/subscribe".to_string(),
        jetstream_collections: vec!["app.bsky.feed.like".to_string()],
        jetstream_read_timeout_seconds: 45,
        jetstream_stale_timeout_seconds: 60,

        // Like Graph
        like_ttl_days: 8,
        like_batch_size: 5000,
        like_batch_interval_ms: 5000,
        like_ttl_refresh_interval_seconds: 3600,
        max_likes_per_user: 5000,
        max_likers_per_post: 10000,

        // Algorithm Sync
        algo_posts_ttl_hours: 1,
        algo_posts_limit: 20000,
        sync_rate_limit_seconds: 300,
        sync_refresh_threshold_minutes: 30,
        algo_likers_enabled: true,
        algo_likers_batch_size: 500,
        sync_batch_delay_ms: 50,
        sync_yield_every_n_batches: 5,
        feed_access_sync_enabled: true,
        feed_access_window_seconds: 10800,
        feed_access_scan_interval_seconds: 60,

        // Inverted Algorithm
        inverted_algorithm_enabled: true,
        inverted_min_post_likes: 5,
        inverted_max_likers_per_post: 30,
        inverted_max_posts_to_score: 0,
        min_overlapping_colikers: 2,

        // Python/Rust Scorer
        python_scorer_enabled: true,
        python_scorer_use_rust: true,
        python_scorer_batch_size: 2000,

        // Liker Cache
        liker_cache_enabled: true,
        liker_cache_max_size: 400000,
        liker_cache_ttl_seconds: 600,
        liker_cache_prewarm_count: 10000,

        // Local Algo Cache
        local_algo_cache_enabled: true,
        local_algo_cache_ttl_seconds: 120,
        local_algo_cache_max_algos: 100,

        // Bloom Filter
        seen_bloom_enabled: true,
        seen_bloom_expected_items: 10000,
        seen_bloom_false_positive_rate: 0.01,
        seen_bloom_max_users: 20000,

        // Trending Posts
        trending_posts_limit: 500,
        trending_posts_ttl_hours: 1,
        trending_recency_decay_hours: 24.0,

        // Fallback Tranches
        fallback_personalization_ratio: 0.80,
        fallback_popular_ratio: 0.34,
        fallback_trending_ratio: 0.33,
        fallback_discovery_ratio: 0.33,
        fallback_stagger_factor: 0.3,
        popular_posts_limit: 500,
        popular_decay_hours: 48.0,
        popular_min_likes: 10,
        velocity_window_hours: 6.0,
        velocity_min_likes: 3,
        velocity_posts_limit: 500,
        author_success_min_posts: 3,
        author_success_decay_hours: 48.0,
        discovery_posts_limit: 500,
        discovery_max_post_likes: 5,
        sync_preferred_max_age_hours: 48,
        sync_minimum_posts: 2000,
        sync_fallback_max_age_hours: 72,

        // Progressive Blending
        cold_user_max_likes: 5,
        warm_user_max_likes: 20,
        cold_user_trending_ratio: 0.8,
        warm_user_trending_ratio: 0.5,

        // Seen Posts
        seen_posts_ttl_hours: 48,
        seen_posts_enabled: true,

        // Interactions Logging
        interactions_logging_enabled: false, // Disabled in tests by default

        // Bot Filtering
        bot_filter_enabled: true,
        bot_like_threshold: 5000,
        bot_cache_refresh_seconds: 300,
        bot_log_ttl_days: 30,

        // Feed Cache
        feed_cache_ttl_seconds: 600,
        feed_cache_size: 1000,
        feed_cache_enabled: true,
        feed_cache_stale_threshold_seconds: 120,
        feed_cache_batch_size: 300,

        // Co-liker
        coliker_ttl_seconds: 3600,
        coliker_refresh_threshold_seconds: 600,
        coliker_max_sources: 1000,
        coliker_enabled: true,
        linklonk_normalization_enabled: true,

        // Author-Affinity
        author_affinity_enabled: true,
        max_liked_authors_per_user: 500,
        max_likers_per_author: 1000,
        author_affinity_max_authors: 100,
        author_affinity_max_colikers: 300,
        author_affinity_max_likers_per_author: 100,
        author_affinity_time_window_hours: 168,
        author_affinity_ttl_seconds: 3600,
        author_affinity_refresh_threshold_seconds: 600,
        author_affinity_max_posts_to_score: 500,
        author_affinity_min_author_likes: 1,

        // Background Tasks
        max_background_refresh_tasks: 20,

        // Cleanup Worker
        cleanup_interval_hours: 8,
        cleanup_worker_enabled: false,

        // Thompson Sampling
        lints_learner_enabled: true,

        // Personalization Defaults
        default_result_ttl_seconds: 300,
        stale_refresh_threshold_seconds: 60,
        default_max_user_likes: 750,
        default_max_sources_per_post: 100,
        default_max_total_sources: 2000,
        default_min_co_likes: 1,
        default_time_window_hours: 168.0,
        default_recency_half_life_hours: 24.0,
        default_specificity_power: 1.0,
        default_popularity_power: 0.6,
        default_num_paths_power: 0.3,
        max_coliker_weight: 3.0,
        prove_max_posts_to_sample: 0,

        // Author Diversity
        diversity_enabled: true,
        max_posts_per_author: 3,
        author_diminishing_factor: 0.5,
        diversity_mmr_lambda: 0.3,

        // API
        api_host: "0.0.0.0".to_string(),
        api_port: 8080,
        api_workers: 4,

        // Feed Generator
        feed_generator_did: "did:web:test.graze.social".to_string(),

        // Request Coalescing
        computation_timeout_seconds: 30.0,

        // Metrics
        metrics_enabled: true,
        metrics_port: 9090,

        // Read-Only Mode
        read_only_mode: false,

        // Audit
        audit_enabled: false,
        audit_all_users: false,
        audit_sample_rate: 0.0,
        audit_log_full_breakdown: false,
        audit_max_contributors: 10,
    }
}

/// Get Redis URL from environment or Docker container.
fn get_redis_url() -> Option<String> {
    // Check for explicit Redis URL
    if let Ok(url) = env::var("REDIS_URL") {
        if !url.is_empty() {
            return Some(url);
        }
    }

    // Check for Docker mode
    let use_docker = env::var("INTEGRATION_USE_DOCKER")
        .map(|v| v.to_lowercase())
        .map(|v| v == "1" || v == "true" || v == "yes")
        .unwrap_or(false);

    if use_docker {
        // In CI or when explicitly requested, try to use testcontainers
        // For now, return None and let the test be skipped
        // TODO: Implement testcontainers setup
        return None;
    }

    None
}

/// Flush the Redis database.
async fn flush_db(redis: &RedisClient) -> graze::error::Result<()> {
    // Use FLUSHDB to clear the current database
    let script = "return redis.call('FLUSHDB')";
    let _: String = redis.eval(script, &[], &[]).await?;
    Ok(())
}

/// Skip test if Redis is not available.
#[macro_export]
macro_rules! skip_without_redis {
    () => {
        let ctx = match $crate::common::TestContext::new().await {
            Some(ctx) => ctx,
            None => {
                eprintln!("Skipping test: Redis not available");
                return;
            }
        };
        ctx
    };
}
