//! No-op candidate source for admin-only mode.
//!
//! Returns no candidates; the worker only runs fallback tranches.
//! Use when `CANDIDATE_SOURCE=admin_only`. Candidates are populated only via HTTP admin API.

use crate::clickhouse::traits::CandidateSource;
use crate::error::Result;

/// Candidate source that always returns an empty list.
///
/// Use when `CANDIDATE_SOURCE=admin_only`. Candidates are managed solely via
/// POST/DELETE /v1/feeds/:algo_id/candidates; the sync worker does not overwrite them.
pub struct AdminOnlyCandidateSource;

#[async_trait::async_trait]
impl CandidateSource for AdminOnlyCandidateSource {
    async fn fetch_candidates(&self, _algo_id: i32) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}
