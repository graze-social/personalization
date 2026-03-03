//! Row types for analytics persistence (e.g. ClickHouse).
//!
//! Used by the interaction writer backends and the interactions service.

use chrono::{DateTime, Utc};

/// A row for the feed_interactions_buffer table.
#[derive(Debug, Clone)]
pub struct FeedInteractionRow {
    pub did: String,
    /// Join key back to feed_impressions. Empty string when ML impressions are disabled.
    pub impression_id: String,
    pub interaction_feed_context: String,
    pub feed_uri: String,
    pub attribution: String,
    pub interaction_item: String,
    pub interaction_event: String,
    pub interaction_request_id: String,
    pub occurred: DateTime<Utc>,
}

/// A row for the feed_impressions_buffer table.
///
/// One row per (request, post-position) pair. Feature vector logged at serve
/// time; joined to `feed_interactions` on `impression_id` for training labels.
#[derive(Debug, Clone, Default)]
pub struct FeedImpressionRow {
    // --- Identity ---
    pub impression_id: String,
    pub user_hash: String,
    pub post_id: String,
    pub algo_id: i32,
    pub served_at: DateTime<Utc>,

    // --- Feed context ---
    pub depth: u8,
    pub source: String,
    pub is_holdout: bool,
    pub is_exploration: bool,
    pub response_time_ms: f32,
    pub is_first_page: bool,

    // --- Scoring features (from scorer hot loop) ---
    pub raw_score: f32,
    pub final_score: f32,
    pub num_paths: u16,
    pub liker_count: u32,
    pub popularity_penalty: f32,
    pub paths_boost: f32,
    pub max_contribution: f32,
    pub score_concentration: f32,
    pub newest_like_age_hours: f32,
    pub oldest_like_age_hours: f32,
    pub was_liker_cache_hit: bool,

    // --- Network features (from source_weights before scoring) ---
    pub coliker_count: u16,
    pub top_coliker_weight: f32,
    pub top5_weight_sum: f32,
    pub mean_coliker_weight: f32,
    pub weight_concentration: f32,

    // --- User features ---
    pub user_like_count: u32,
    pub user_segment: String,

    // --- Request quality ---
    pub richness_ratio: f32,
    pub hour_of_day: u8,
    pub day_of_week: u8,
}

/// A row for the user_action_logs_buffer table.
#[derive(Debug, Clone)]
pub struct UserActionLogRow {
    pub algo_id: i32,
    pub user_did: String,
    pub action_type: String,
    pub action_identifier: String,
    pub action_time: DateTime<Utc>,
    pub action_count: u32,
}
