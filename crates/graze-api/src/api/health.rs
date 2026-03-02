//! Health and metrics endpoints.

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::AppState;

/// Prometheus metrics endpoint.
pub async fn metrics(State(state): State<Arc<AppState>>) -> String {
    state.metrics.encode()
}

/// Root endpoint with service info.
pub async fn root() -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "service": "Graze Personalization Service",
            "version": "0.2.0",
            "docs": "/docs",
            "health": "/internal/ready"
        })),
    )
        .into_response()
}

/// Started probe - indicates service has started.
pub async fn started() -> StatusCode {
    StatusCode::OK
}

/// Ready probe - indicates service is ready for load balancing.
///
/// Returns 200 OK if Redis is connected, 503 Service Unavailable otherwise.
pub async fn ready(State(state): State<Arc<AppState>>) -> StatusCode {
    if state.redis.ping().await {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Alive probe - indicates service is operating.
pub async fn alive() -> StatusCode {
    StatusCode::OK
}
