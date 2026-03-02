//! API response models.

use serde::Serialize;

/// A scored post in the personalization response.
#[derive(Debug, Clone, Serialize)]
pub struct ScoredPost {
    /// Post AT-URI or interned ID.
    #[serde(rename = "uri", skip_serializing_if = "String::is_empty")]
    pub uri: String,

    /// Post ID (internal, used when URI is not resolved).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub post_id: String,

    /// Personalization score.
    pub score: f64,

    /// Reasons why this post was recommended.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<String>,
}

impl ScoredPost {
    /// Create a new scored post with just post_id.
    pub fn from_id(post_id: String, score: f64) -> Self {
        Self {
            uri: String::new(),
            post_id,
            score,
            reasons: Vec::new(),
        }
    }

    /// Create a new scored post with URI.
    pub fn from_uri(uri: String, score: f64) -> Self {
        Self {
            uri,
            post_id: String::new(),
            score,
            reasons: Vec::new(),
        }
    }
}

/// Metadata about the personalization response.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ResponseMeta {
    /// Whether the result was served from cache.
    pub cached: bool,

    /// Age of cached result in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_age_seconds: Option<u32>,

    /// Total number of posts scored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_scored: Option<usize>,

    /// Computation time in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute_time_ms: Option<f64>,

    /// Whether a sync is in progress.
    #[serde(default)]
    pub syncing: bool,

    /// Suggested retry time if rate limited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,

    /// Whether the server is in read-only mode (shadow traffic).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,

    /// Number of candidate posts checked during scoring.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub posts_checked: Option<usize>,

    /// Number of co-likers used in scoring.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub colikers_used: Option<usize>,

    /// Scoring time in milliseconds (excluding cache/URI resolution).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scoring_time_ms: Option<f64>,
}

/// Response from the personalize endpoint.
#[derive(Debug, Serialize)]
pub struct PersonalizeResponse {
    /// Scored posts.
    pub posts: Vec<ScoredPost>,

    /// Cursor for pagination.
    pub cursor: Option<String>,

    /// Response metadata.
    pub meta: ResponseMeta,
}

/// Response from the trace/debug endpoint.
#[derive(Debug, Serialize)]
pub struct TraceResponse {
    /// User DID.
    pub user_did: String,

    /// Hashed user DID.
    pub user_hash: String,

    /// Algorithm ID.
    pub algo_id: i32,

    /// Preset used.
    pub preset: String,

    /// Step-by-step trace information.
    pub steps: Vec<TraceStep>,

    /// Total computation time in milliseconds.
    pub total_time_ms: f64,
}

/// A single step in the trace.
#[derive(Debug, Serialize)]
pub struct TraceStep {
    /// Step name.
    pub name: String,

    /// Step number.
    pub step: usize,

    /// Duration in milliseconds.
    pub duration_ms: f64,

    /// Step-specific data.
    pub data: serde_json::Value,
}

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    /// Service status.
    pub status: String,

    /// Redis connectivity.
    pub redis: bool,

    /// Version.
    pub version: String,

    /// Whether running in read-only mode.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
}

/// Sync status response.
#[derive(Debug, Serialize)]
pub struct SyncResponse {
    /// Whether sync was queued.
    pub queued: bool,

    /// Message.
    pub message: String,
}

/// Invalidation response.
#[derive(Debug, Serialize)]
pub struct InvalidateResponse {
    /// Number of keys invalidated.
    pub invalidated: usize,

    /// Message.
    pub message: String,
}
