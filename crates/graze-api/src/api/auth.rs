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

/// Paths that do not require the admin API key (ATProto and well-known).
const ALLOWLISTED_PATHS: &[&str] = &[
    "/.well-known/did.json",
    "/xrpc/app.bsky.feed.describeFeedGenerator",
    "/xrpc/app.bsky.feed.getFeedSkeleton",
    "/xrpc/app.bsky.feed.sendInteractions",
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
