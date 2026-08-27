//! Shared-secret authentication for non-ATProto endpoints.
//!
//! When ADMIN_API_KEY is set, all requests except allowlisted ATProto/well-known
//! paths must supply the key via Authorization: Bearer or X-API-Key.

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::Arc;

use crate::AppState;

/// Paths that do not require the admin API key (ATProto, well-known, and kubelet probes).
///
/// The `/internal/*` probes are here because **a kubelet probe cannot authenticate**. Without this
/// they return 401, which a probe reads as failure. That is not theoretical: `kube/api-deployment.yaml`
/// declares a readinessProbe on `/internal/ready`, and applying it would have failed readiness on
/// every replica at once and emptied the Service — a self-inflicted total outage. The live deployment
/// consequently ran with no probes at all, so Kubernetes marked pods Ready the instant the container
/// started and routed traffic to them before the HTTP listener was accepting connections.
///
/// Nothing here discloses anything sensitive: `started` and `alive` are static 200s, and `ready`
/// reports only whether Redis answers a ping — which is precisely the fact a load balancer needs and
/// no more than an unauthenticated caller learns by observing whether requests succeed.
const ALLOWLISTED_PATHS: &[&str] = &[
    "/.well-known/did.json",
    "/xrpc/app.bsky.feed.describeFeedGenerator",
    "/xrpc/app.bsky.feed.getFeedSkeleton",
    "/xrpc/app.bsky.feed.sendInteractions",
    "/internal/started",
    "/internal/alive",
    "/internal/ready",
];

/// Constant-time equality to reduce timing side channels.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Extract the API key from request headers.
/// Tries Authorization: Bearer <key> first, then X-API-Key: <key>.
fn extract_api_key(request: &Request) -> Option<String> {
    let headers = request.headers();
    if let Some(auth) = headers.get("Authorization") {
        if let Ok(s) = auth.to_str() {
            let s = s.trim();
            if let Some(stripped) = s.strip_prefix("Bearer ") {
                return Some(stripped.trim().to_string());
            }
        }
    }
    if let Some(key) = headers.get("X-API-Key") {
        if let Ok(s) = key.to_str() {
            return Some(s.trim().to_string());
        }
    }
    None
}

/// Middleware that requires ADMIN_API_KEY for all non-allowlisted paths.
pub async fn require_admin_api_key(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if ALLOWLISTED_PATHS.contains(&path) {
        return next.run(request).await;
    }
    let Some(configured_key) = &state.config.admin_api_key else {
        return next.run(request).await;
    };
    let Some(provided) = extract_api_key(&request) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "Unauthorized",
                "message": "Missing or invalid API key"
            })),
        )
            .into_response();
    };
    if !constant_time_eq(&provided, configured_key) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "Unauthorized",
                "message": "Missing or invalid API key"
            })),
        )
            .into_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod probe_allowlist_tests {
    use super::ALLOWLISTED_PATHS;

    /// A kubelet probe carries no credentials, so every probe path must be allowlisted or the probe
    /// fails closed and takes the replica out of the Service. Measured before this change: all
    /// three returned 401, which is why the live deployment ran with no probes at all.
    #[test]
    fn every_kubelet_probe_path_is_allowlisted() {
        for p in ["/internal/started", "/internal/alive", "/internal/ready"] {
            assert!(
                ALLOWLISTED_PATHS.contains(&p),
                "{p} must not require the admin key: a probe cannot supply one"
            );
        }
    }

    /// The allowlist is exact-match, and must stay that way. This guards against someone later
    /// replacing the entries with a prefix like "/internal" and exposing admin surfaces with it.
    #[test]
    fn admin_and_scoring_paths_are_not_allowlisted() {
        for p in [
            "/v1/personalize",
            "/v1/invalidate",
            "/v1/thompson/stats",
            "/metrics",
            "/internal",
        ] {
            assert!(
                !ALLOWLISTED_PATHS.contains(&p),
                "{p} must keep requiring the admin key"
            );
        }
    }
}
