//! graze-lens-fold: Jetstream follow events → ClickHouse `follow_edges`.

use anyhow::Context;
use deadpool_redis::{Config as RedisConfig, Runtime};
use graze_lens_fold::{Config, Cursor, DeltaApplier, Metrics, Sink, Streamer};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = Config::from_env().context("configuration")?;
    info!(jetstream = %config.jetstream_url, "starting lens fold");

    let pool = RedisConfig::from_url(config.redis_url.clone())
        .builder()?
        .max_size(config.redis_pool_size)
        .runtime(Runtime::Tokio1)
        .build()
        .context("redis pool")?;

    let metrics = Metrics::new();
    let sink = Sink::new(config.clickhouse.clone(), config.insert_timeout).context("sink")?;
    let cursor = Cursor::new(pool.clone());

    let deltas = if config.deltas_enabled {
        let applier = DeltaApplier::new(pool, metrics.clone(), config.set_ttl_seconds);
        // Warm the active list before the first event, so a restart does not
        // spend an interval ignoring deltas for viewers who are already live.
        match applier.refresh_active().await {
            Ok(n) => info!(active_viewers = n, "live lens maintenance enabled"),
            Err(e) => warn!(error = %e, "could not load active viewers at startup"),
        }
        Some(applier)
    } else {
        info!("live lens maintenance disabled; sets will expire on their TTL");
        None
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    serve_metrics(config.metrics_port, metrics.clone());

    let streamer = Streamer::new(config, cursor, sink, metrics, deltas);
    let run = streamer.run(shutdown_rx);

    tokio::select! {
        _ = run => {}
        _ = tokio::signal::ctrl_c() => {
            info!("SIGINT received; draining");
            let _ = shutdown_tx.send(true);
            // Give the streamer a moment to flush its batch and commit.
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }

    Ok(())
}

/// Metrics on their own port, so scraping never contends with the stream.
fn serve_metrics(port: u16, metrics: Metrics) {
    tokio::spawn(async move {
        let app = axum::Router::new()
            .route(
                "/metrics",
                axum::routing::get({
                    let m = metrics.clone();
                    move || {
                        let m = m.clone();
                        async move { m.encode() }
                    }
                }),
            )
            .route("/healthz", axum::routing::get(|| async { "ok" }));

        match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
            Ok(listener) => {
                let _ = axum::serve(listener, app).await;
            }
            Err(e) => tracing::error!(error = %e, port, "metrics server failed to bind"),
        }
    });
}
