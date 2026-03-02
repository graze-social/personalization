//! Error types for the Graze service.

use thiserror::Error;

/// Main error type for the Graze service
#[derive(Error, Debug)]
pub enum GrazeError {
    #[error("Redis error: {0}")]
    Redis(#[from] deadpool_redis::redis::RedisError),

    #[error("Redis pool error: {0}")]
    RedisPool(#[from] deadpool_redis::PoolError),

    #[error("HTTP client error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Service not ready")]
    NotReady,

    #[error("Unknown feed: {0}")]
    UnknownFeed(String),

    #[error("Rate limited")]
    RateLimited,

    #[error("Authentication required")]
    AuthRequired,

    #[error("Invalid cursor")]
    InvalidCursor,

    #[error("Script not found: {0}")]
    ScriptNotFound(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type alias for Graze operations
pub type Result<T> = std::result::Result<T, GrazeError>;
