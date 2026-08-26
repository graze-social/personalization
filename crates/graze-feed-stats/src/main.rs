//! Feed Stats worker binary — Rust drop-in for `feed_stats_runner.py`.

use std::sync::Arc;

use tokio::signal;
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

use graze_common::{maybe_run_metrics_server, RedisClient};
use graze_feed_stats::clickhouse_sink::ClickHouseSink;
use graze_feed_stats::pg::PgStore;
use graze_feed_stats::redis_counters::RedisCounters;
use graze_feed_stats::{Config, FeedRequestsWorker, FeedStatsMetrics, LogWorker};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .json()
        .init();

    info!("Starting Graze Feed Stats worker");

    let config = Config::from_env();
    let metrics_port = config.metrics_port;
    info!(
        shadow_mode = config.shadow.enabled,
        log_tasks_key = %config.shadow.log_tasks_key,
        feed_requests_key = %config.shadow.feed_requests_key,
        ch_table_prefix = %config.shadow.ch_table_prefix,
        redis_key_prefix = %config.shadow.redis_key_prefix,
        pg_dry_run = config.shadow.pg_dry_run,
        clickhouse_host = %config.clickhouse_host,
        "Configuration loaded"
    );
    let config = Arc::new(config);

    // Shared clients.
    let redis = Arc::new(RedisClient::new(&config.redis_config()).await?);
    info!("Connected to Redis");
    let pg = Arc::new(
        PgStore::connect(
            &config.database_url,
            config.pg_max_connections,
            config.shadow.pg_dry_run,
        )
        .await?,
    );
    info!(dry_run = pg.dry_run(), "Connected to Postgres");
    let ch = Arc::new(ClickHouseSink::new(
        config.clickhouse_config(),
        config.shadow.ch_table_prefix.clone(),
    ));
    let counters = RedisCounters::new(config.shadow.redis_key_prefix.clone());
    let metrics = Arc::new(FeedStatsMetrics::new());

    // Main log_tasks worker.
    let log_worker = LogWorker::new(
        redis.clone(),
        ch.clone(),
        pg.clone(),
        counters,
        metrics.clone(),
        config.clone(),
    );

    // Background feed_requests flusher (Python's daemon thread).
    let feed_requests =
        FeedRequestsWorker::new(redis.clone(), pg.clone(), metrics.clone(), config.clone());
    let feed_requests_handle = tokio::spawn(async move {
        if let Err(e) = feed_requests.run().await {
            tracing::error!(error = %e, "feed_requests worker exited");
        }
    });

    tokio::select! {
        result = log_worker.run() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "log worker error");
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

    feed_requests_handle.abort();
    info!("Feed Stats worker shutdown complete");
    Ok(())
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
