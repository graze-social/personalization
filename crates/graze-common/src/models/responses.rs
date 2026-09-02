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

    /// Which ranker contributed this post, when an interleaving experiment is running.
    ///
    /// `None` outside experiments, and also for items both rankers offered (which carry no
    /// preference information and must not be credited to either side).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ranker: Option<String>,
}

impl ScoredPost {
    /// Create a new scored post with just post_id.
    pub fn from_id(post_id: String, score: f64) -> Self {
        Self {
            uri: String::new(),
            post_id,
            score,
            reasons: Vec::new(),
            ranker: None,
        }
    }

    /// Create a new scored post with URI.
    pub fn from_uri(uri: String, score: f64) -> Self {
        Self {
            uri,
            post_id: String::new(),
            score,
            reasons: Vec::new(),
            ranker: None,
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

    /// Why scoring was skipped entirely before it ran: `pool_density` | `pool_size` |
    /// `no_colikers`. Absent when scoring ran, including when it ran and returned nothing.
    ///
    /// Exists so `api::feed` can turn a bail-out that happened before scoring into a
    /// `fallback_reason` on the provenance blob. Before this, each of them returned an empty
    /// `ScoringResult` that was indistinguishable from "scored and found nothing", so the response
    /// was served as fallback with no reason recorded at all and the coverage failure could not be
    /// decomposed in ClickHouse. Verified on feed 6445 on 2026-09-01: 22 of 24 items carried
    /// `source=fallback` with no `fallback_reason`.
    ///
    /// `pool_density` and `pool_size` are feed-level config gates. `no_colikers` is not: it is the
    /// co-liker walk returning nothing for a user who did have seed, which is why it is a separate
    /// value from `no_user_data` rather than folded into it.
    ///
    /// `Option<String>` rather than `&'static str` because this crosses the wire on
    /// `/v1/personalize`; the reasons themselves are constants on `ScoringResult::skip_reason`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `skip_reason` is absent from the wire unless a gate actually fired.
    ///
    /// Same additive property the provenance blob's own fields rely on: a consumer that has never
    /// heard of this field sees exactly the payload it saw before, and a present value therefore
    /// means something happened rather than being a default that is always there.
    #[test]
    fn skip_reason_is_omitted_unless_a_gate_fired() {
        let mut meta = ResponseMeta::default();
        let json = serde_json::to_string(&meta).expect("serialize");
        assert!(
            !json.contains("skip_reason"),
            "a response that scored must not carry skip_reason: {json}"
        );

        meta.skip_reason = Some("pool_density".to_string());
        let json = serde_json::to_string(&meta).expect("serialize");
        assert!(
            json.contains("\"skip_reason\":\"pool_density\""),
            "a gated response must carry the reason: {json}"
        );
    }
}
