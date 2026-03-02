//! Reusable metrics HTTP server for Graze services.
//!
//! Provides a simple HTTP server that exposes Prometheus metrics at `/metrics`.

use std::future::Future;
use std::sync::Arc;

use axum::{
    body::Body, extract::State, http::StatusCode, response::Response, routing::get, Router,
};
use tokio::net::TcpListener;
use tracing::info;

/// Trait for types that can encode themselves as Prometheus metrics text.
pub trait MetricsEncodable: Send + Sync + 'static {
    /// Encode metrics in Prometheus text format.
    fn encode(&self) -> String;
}

/// State wrapper for the metrics handler.
struct MetricsState<M> {
    metrics: Arc<M>,
}

impl<M> Clone for MetricsState<M> {
    fn clone(&self) -> Self {
        Self {
            metrics: self.metrics.clone(),
        }
    }
}

/// Start a metrics-only HTTP server on the given address.
///
/// Returns a future that runs the server until it's terminated.
/// The server exposes a single endpoint: `GET /metrics`
///
/// # Arguments
/// * `addr` - Address to bind to (e.g., "0.0.0.0:9090")
/// * `metrics` - Arc-wrapped metrics implementation that can encode to Prometheus format
pub async fn run_metrics_server<M>(
    addr: &str,
    metrics: Arc<M>,
) -> std::result::Result<(), std::io::Error>
where
    M: MetricsEncodable,
{
    let state = MetricsState { metrics };

    let app = Router::new()
        .route("/metrics", get(metrics_handler::<M>))
        .route("/internal/started", get(internal_probe))
        .route("/internal/ready", get(internal_probe))
        .route("/internal/alive", get(internal_probe))
        .with_state(state);

    let listener = TcpListener::bind(addr).await?;
    info!(address = %addr, "Metrics server listening");

    axum::serve(listener, app).await
}

/// Spawn a metrics server as a background task if the port is non-zero.
///
/// Returns a future that can be used with `tokio::select!` to run the metrics
/// server alongside other tasks.
///
/// # Arguments
/// * `port` - Port to bind to. If 0, returns a future that never completes.
/// * `host` - Host to bind to (e.g., "0.0.0.0")
/// * `metrics` - Arc-wrapped metrics implementation
pub fn maybe_run_metrics_server<M>(
    port: u16,
    host: &str,
    metrics: Arc<M>,
) -> impl Future<Output = std::result::Result<(), std::io::Error>>
where
    M: MetricsEncodable,
{
    let addr = format!("{}:{}", host, port);
    let should_run = port > 0;

    async move {
        if should_run {
            run_metrics_server(&addr, metrics).await
        } else {
            // Never complete - let other tasks drive the select!
            std::future::pending().await
        }
    }
}

async fn metrics_handler<M>(State(state): State<MetricsState<M>>) -> Response
where
    M: MetricsEncodable,
{
    let body = state.metrics.encode();
    Response::builder()
        .status(200)
        .header("Content-Type", "text/plain; version=0.0.4")
        .body(Body::from(body))
        .unwrap()
}

/// Health probe handler for /internal/* endpoints.
///
/// Returns 200 OK to indicate the service is running.
/// Used for Kubernetes startup, readiness, and liveness probes.
pub async fn internal_probe() -> StatusCode {
    StatusCode::OK
}
