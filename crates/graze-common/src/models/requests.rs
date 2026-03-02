//! API request models.

use serde::Deserialize;

/// Personalization parameters override.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PersonalizationParams {
    /// Preset name (default, discovery, stable, fast).
    pub preset: Option<String>,

    /// Maximum user likes to consider.
    pub max_user_likes: Option<usize>,

    /// Maximum sources per post.
    pub max_sources_per_post: Option<usize>,

    /// Maximum total sources.
    pub max_total_sources: Option<usize>,

    /// Minimum co-likes threshold.
    pub min_co_likes: Option<usize>,

    /// Time window in hours.
    pub time_window_hours: Option<f64>,

    /// Recency half-life in hours.
    pub recency_half_life_hours: Option<f64>,

    /// Specificity power (penalize prolific likers).
    pub specificity_power: Option<f64>,

    /// Popularity power (penalize viral posts).
    pub popularity_power: Option<f64>,

    /// Num paths power (boost posts with more distinct co-liker paths).
    pub num_paths_power: Option<f64>,

    /// Use author-level co-likers instead of post-level (coarse linklonk).
    /// This creates denser connections by matching on authors rather than exact posts.
    #[serde(default)]
    pub use_author_affinity: bool,

    /// Seed sample pool size. When > 0, fetch this many likes then randomly sample max_user_likes.
    /// 0 = disabled (deterministic, current behavior).
    pub seed_sample_pool: Option<usize>,

    /// Corater decay: per-rank decay factor for co-liker contributions (0.0 to 1.0).
    /// 0.0 = disabled. 0.2 = each successive co-liker like gets 20% less weight.
    pub corater_decay: Option<f64>,
}

/// Request for the personalize endpoint.
#[derive(Debug, Deserialize)]
pub struct PersonalizeRequest {
    /// User DID to personalize for.
    pub user_did: String,

    /// Algorithm ID.
    pub algo_id: i32,

    /// Maximum number of posts to return.
    #[serde(default = "default_limit")]
    pub limit: usize,

    /// Cursor for pagination.
    pub cursor: Option<String>,

    /// Optional parameter overrides.
    pub params: Option<PersonalizationParams>,
}

fn default_limit() -> usize {
    30
}

/// Request for algorithm tracing/debugging.
#[derive(Debug, Deserialize)]
pub struct TraceRequest {
    /// User DID to trace.
    pub user_did: String,

    /// Algorithm ID.
    pub algo_id: i32,

    /// Preset name.
    pub preset: Option<String>,
}

/// Request for cache invalidation.
#[derive(Debug, Deserialize)]
pub struct InvalidateRequest {
    /// Optional user DID to invalidate (if not provided, invalidates all).
    pub user_did: Option<String>,

    /// Optional algorithm ID to invalidate.
    pub algo_id: Option<i32>,
}

/// Request for algorithm sync.
#[derive(Debug, Deserialize)]
pub struct SyncRequest {
    /// Algorithm ID to sync.
    pub algo_id: i32,

    /// Force sync even if recently synced.
    #[serde(default)]
    pub force: bool,
}

/// Request to register a feed.
#[derive(Debug, Deserialize)]
pub struct RegisterFeedRequest {
    /// Feed AT-URI.
    pub feed_uri: String,

    /// Algorithm ID.
    pub algo_id: i32,
}

/// Request for sendInteractions endpoint.
#[derive(Debug, Deserialize)]
pub struct SendInteractionsRequest {
    /// Interactions to record.
    pub interactions: Vec<Interaction>,
}

/// A single user interaction.
#[derive(Debug, Clone, Deserialize)]
pub struct Interaction {
    /// Post URI that was interacted with.
    /// Field name is "item" in the AT Protocol spec.
    #[serde(alias = "uri")]
    pub item: String,

    /// Type of interaction (e.g., "app.bsky.feed.defs#interactionSeen").
    #[serde(rename = "event")]
    pub event_type: String,

    /// Base64-encoded JSON context from the feed response.
    /// Contains feed_uri and attribution fields.
    #[serde(rename = "feedContext")]
    pub feed_context: Option<String>,

    /// Request ID from the original feed request.
    #[serde(rename = "reqId")]
    pub req_id: Option<String>,
}
