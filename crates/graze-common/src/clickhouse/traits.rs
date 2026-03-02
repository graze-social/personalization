//! Pluggable backends for analytics writes and candidate reads.
//!
//! Implement these traits to use ClickHouse, HTTP, or no-op backends without
//! tying the rest of the codebase to a specific storage.

use crate::error::Result;
use crate::models::{FeedInteractionRow, UserActionLogRow};

/// Writes interaction and action-log rows to a backend (e.g. ClickHouse).
///
/// Use `ClickHouseInteractionWriter` when `INTERACTIONS_WRITER=clickhouse`;
/// use `NoOpInteractionWriter` when `INTERACTIONS_WRITER=none`.
#[async_trait::async_trait]
pub trait InteractionWriter: Send + Sync {
    /// Persist a batch of feed interaction rows and user action log rows.
    async fn persist_batch(
        &self,
        feed_rows: Vec<FeedInteractionRow>,
        action_rows: Vec<UserActionLogRow>,
    ) -> Result<()>;
}

/// Source of algorithm candidate post URIs (e.g. ClickHouse or HTTP API).
///
/// - `ClickHouseCandidateSource`: `CANDIDATE_SOURCE=clickhouse`
/// - `HttpCandidateSource`: `CANDIDATE_SOURCE=http` (set `CANDIDATE_HTTP_URL`)
/// - `AdminOnlyCandidateSource`: `CANDIDATE_SOURCE=admin_only` (candidates only via admin API)
#[async_trait::async_trait]
pub trait CandidateSource: Send + Sync {
    /// Fetch candidate post URIs for an algorithm.
    ///
    /// The caller (e.g. candidate-sync worker) will intern URIs and write to Redis.
    async fn fetch_candidates(&self, algo_id: i32) -> Result<Vec<String>>;
}
