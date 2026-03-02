//! Error types for the Graze API service with Axum integration.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use thiserror::Error;

// Re-export common error type for convenience
pub use graze_common::GrazeError;

/// API-specific error wrapper that implements Axum's IntoResponse.
#[derive(Error, Debug)]
#[error(transparent)]
pub struct ApiError(#[from] pub GrazeError);

/// Result type alias for API operations
pub type Result<T> = std::result::Result<T, ApiError>;

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_code, message) = match &self.0 {
            GrazeError::NotReady => (
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailable",
                "Service not ready".to_string(),
            ),
            GrazeError::UnknownFeed(feed) => (
                StatusCode::BAD_REQUEST,
                "UnknownFeed",
                format!("Feed not found: {}", feed),
            ),
            GrazeError::RateLimited => (
                StatusCode::TOO_MANY_REQUESTS,
                "RateLimited",
                "Too many requests".to_string(),
            ),
            GrazeError::AuthRequired => (
                StatusCode::UNAUTHORIZED,
                "AuthenticationRequired",
                "Authentication required".to_string(),
            ),
            GrazeError::InvalidCursor => (
                StatusCode::BAD_REQUEST,
                "InvalidCursor",
                "Invalid cursor format".to_string(),
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                self.0.to_string(),
            ),
        };

        let body = Json(serde_json::json!({
            "error": error_code,
            "message": message,
        }));

        (status, body).into_response()
    }
}

// Implement From for common error types to enable ? operator
impl From<deadpool_redis::redis::RedisError> for ApiError {
    fn from(err: deadpool_redis::redis::RedisError) -> Self {
        ApiError(GrazeError::Redis(err))
    }
}

impl From<deadpool_redis::PoolError> for ApiError {
    fn from(err: deadpool_redis::PoolError) -> Self {
        ApiError(GrazeError::RedisPool(err))
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(err: serde_json::Error) -> Self {
        ApiError(GrazeError::Json(err))
    }
}

impl From<std::io::Error> for ApiError {
    fn from(err: std::io::Error) -> Self {
        ApiError(GrazeError::Io(err))
    }
}
