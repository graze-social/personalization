//! Row types for analytics persistence (e.g. ClickHouse).
//!
//! Used by the interaction writer backends and the interactions service.

use chrono::{DateTime, Utc};

/// A row for the feed_interactions_buffer table.
#[derive(Debug, Clone)]
pub struct FeedInteractionRow {
    pub did: String,
    pub interaction_feed_context: String,
    pub feed_uri: String,
    pub attribution: String,
    pub interaction_item: String,
    pub interaction_event: String,
    pub interaction_request_id: String,
    pub occurred: DateTime<Utc>,
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
