//! Candidate sync binary - syncs algorithm posts from a configurable source (ClickHouse, HTTP, or admin-only).

use std::sync::Arc;

use tokio::signal;
use tracing::{debug, info, Level};
use tracing_subscriber::EnvFilter;

use graze_candidate_sync::{CandidateSync, Config, SyncMetrics};
use graze_common::clickhouse::{
    AdminOnlyCandidateSource, CandidateQueryParams, CandidateSource, ClickHouseCandidateSource,
    ClickHouseConfig, HttpCandidateSource,
};
use graze_common::services::UriInterner;
use graze_common::{maybe_run_metrics_server, RedisClient};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .json()
        .init();

    info!("Starting Graze Candidate Sync");

    // Load configuration
    let config = Config::from_env();

    // Capture metrics port before wrapping config
    let metrics_port = config.metrics_port;

    let config = Arc::new(config);

    // Log configuration (redact sensitive values)
    debug!(
        redis_url = %redact_url(&config.redis_url),
        redis_pool_size = config.redis_pool_size,
        redis_connect_max_retries = config.redis_connect_max_retries,
        redis_connect_initial_delay_ms = config.redis_connect_initial_delay_ms,
        candidate_source = %config.candidate_source,
        clickhouse_host = %config.clickhouse_host,
        clickhouse_port = config.clickhouse_port,
        metrics_port = metrics_port,
        "Configuration loaded"
    );

    // Create Redis client
    let redis = Arc::new(RedisClient::new(&config.redis_config()).await?);
    info!("Connected to Redis");

    // Create URI interner
    let interner = Arc::new(UriInterner::new(redis.clone()));

    // Build candidate source from config
    let source: Arc<dyn CandidateSource> = match config.candidate_source.as_str() {
        "clickhouse" => {
            let ch_config = Arc::new(ClickHouseConfig {
                host: config.clickhouse_host.clone(),
                port: config.clickhouse_port,
                database: config.clickhouse_database.clone(),
                user: config.clickhouse_user.clone(),
                password: config.clickhouse_password.clone(),
                secure: config.clickhouse_secure,
            });
            let query_params = CandidateQueryParams {
                limit: config.algo_posts_limit,
                preferred_max_age_hours: config.sync_preferred_max_age_hours,
                fallback_max_age_hours: config.sync_fallback_max_age_hours,
                minimum_posts: config.sync_minimum_posts,
            };
            Arc::new(ClickHouseCandidateSource::new(ch_config, query_params))
        }
        "http" => {
            let base_url = config
                .candidate_http_url
                .as_deref()
                .unwrap_or_else(|| {
                    tracing::warn!("CANDIDATE_SOURCE=http but CANDIDATE_HTTP_URL unset; using http://localhost:8080");
                    "http://localhost:8080"
                })
                .trim_end_matches('/')
                .to_string();
            Arc::new(HttpCandidateSource::new(base_url))
        }
        _ => Arc::new(AdminOnlyCandidateSource),
    };

    // Create metrics
    let metrics = Arc::new(SyncMetrics::new());

    // Create and run sync worker
    let sync = CandidateSync::new(redis, interner, config, source);

    // Run with graceful shutdown (and optional metrics server)
    tokio::select! {
        result = sync.run() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Candidate sync error");
            }
        }
        _ = shutdown_signal() => {
            info!("Shutdown signal received");
        }
        result = maybe_run_metrics_server(metrics_port, "0.0.0.0", metrics) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Metrics server error");
            }
        }
    }

    info!("Candidate sync shutdown complete");
    Ok(())
}

/// Redact password from URL for safe logging.
fn redact_url(url: &str) -> String {
    if let Some(at_pos) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let scheme = &url[..scheme_end + 3];
            let after_at = &url[at_pos..];
            return format!("{}[REDACTED]{}", scheme, after_at);
        }
    }
    url.to_string()
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
