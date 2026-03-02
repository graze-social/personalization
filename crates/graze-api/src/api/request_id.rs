//! Request ID middleware for request tracing.
//!
//! Generates a UUIDv7 for each request and attaches it to:
//! 1. A tracing span (for structured logging)
//! 2. Request extensions (for handler extraction)

use axum::{extract::Request, middleware::Next, response::Response};
use std::fmt;
use tracing::{info_span, Instrument};
use uuid::Uuid;

/// Unique request identifier (UUIDv7).
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

impl RequestId {
    /// Generate a new UUIDv7 request ID.
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    /// Get the request ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<RequestId> for String {
    fn from(id: RequestId) -> Self {
        id.0
    }
}

/// Middleware that generates a UUIDv7 request ID and creates a tracing span.
///
/// The request ID is available to handlers via `Extension<RequestId>`.
/// All logs within the request will include `request_id` in their context.
pub async fn request_id_middleware(mut request: Request, next: Next) -> Response {
    let request_id = RequestId::new();

    // Insert into request extensions for handler extraction
    request.extensions_mut().insert(request_id.clone());

    // Create span with request_id field
    let span = info_span!(
        "request",
        request_id = %request_id,
        method = %request.method(),
        uri = %request.uri(),
    );

    // Execute the rest of the request within this span
    next.run(request).instrument(span).await
}
