//! Like streamer binary - consumes likes from Jetstream.
//!
//! Also runs the background cleanup worker for maintaining rolling time windows.

use std::sync::Arc;

use tokio::signal;
use tracing::{debug, info, warn, Level};
use tracing_subscriber::EnvFilter;

use graze_common::{maybe_run_metrics_server, RedisClient, UriInterner};
use graze_like_streamer::config::Config;
use graze_like_streamer::{CleanupWorker, LikeStreamer, StreamerMetrics};

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

    info!("Starting Graze Like Streamer");

    // Load configuration
    let config = Config::from_env();

    // Capture metrics port before wrapping config
    let metrics_port = config.metrics_port;

    // Log configuration (redact sensitive values)
    debug!(
        redis_url = %redact_url(&config.redis_url),
        redis_pool_size = config.redis_pool_size,
        jetstream_url = %config.jetstream_url,
        cleanup_worker_enabled = config.cleanup_worker_enabled,
        cleanup_interval_hours = config.cleanup_interval_hours,
        like_ttl_days = config.like_ttl_days,
        metrics_port = metrics_port,
        "Configuration loaded"
    );

    // Create Redis client
    let redis_config = config.redis_config();
    let redis = Arc::new(RedisClient::new(&redis_config).await?);
    info!("Connected to Redis");

    // Create URI interner (shares post ID mappings with candidate_sync)
    let interner = Arc::new(UriInterner::new(redis.clone()));

    // Create metrics
    let metrics = Arc::new(StreamerMetrics::new());

    // Create workers
    let config = Arc::new(config);
    let streamer = LikeStreamer::new(redis.clone(), interner, config.clone());
    let cleanup = CleanupWorker::new(redis.clone(), config.clone());

    // Spawn cleanup worker if enabled
    let cleanup_handle = if config.cleanup_worker_enabled {
        let interval = config.cleanup_interval_hours;
        info!(
            interval_hours = interval,
            "Starting background cleanup worker"
        );
        Some(tokio::spawn(async move {
            if let Err(e) = cleanup.run(interval).await {
                warn!(error = %e, "Cleanup worker error");
            }
        }))
    } else {
        info!("Cleanup worker disabled");
        None
    };

    // Run streamer with graceful shutdown (and optional metrics server)
    tokio::select! {
        result = streamer.run() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Like streamer error");
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

    // Abort cleanup worker on shutdown
    if let Some(handle) = cleanup_handle {
        handle.abort();
    }

    info!("Like streamer shutdown complete");
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
