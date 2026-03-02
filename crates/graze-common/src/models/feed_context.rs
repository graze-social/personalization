//! Feed context provenance for getFeedSkeleton responses.
//!
//! Encoded as base64(URL_SAFE) JSON and sent as the feedContext string on each
//! skeleton item so downstream can trace why a post was shown.

use base64::{engine::general_purpose::URL_SAFE, Engine};
use serde::{Deserialize, Serialize};

/// Provenance payload encoded into each item's feedContext string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedContextProvenance {
    /// Full AT-URI of the feed (e.g. "at://did:plc:xxx/app.bsky.feed.generator/feedname").
    /// Required so we can deserialize it from interaction events—the client echoes this back.
    #[serde(rename = "feed_uri")]
    pub feed_uri: String,

    #[serde(rename = "algo_id")]
    pub algo_id: i32,

    /// 0-based index of this post in the response.
    #[serde(rename = "depth")]
    pub depth: usize,

    /// True if this slot came from post-level or author-affinity personalization.
    #[serde(rename = "personalized")]
    pub personalized: bool,

    /// Source of this post: personalized | author_affinity | fallback | pinned | rotating | sponsored | placeholder
    #[serde(rename = "source")]
    pub source: String,

    /// When personalized: post_level (exact post co-likers) | author_level (author-affinity co-likers).
    /// Omitted for non-personalized sources.
    #[serde(
        rename = "personalization_type",
        skip_serializing_if = "Option::is_none"
    )]
    pub personalization_type: Option<String>,

    /// When source is fallback: popular | velocity | discovery. Otherwise omitted.
    #[serde(rename = "fallback_tranche", skip_serializing_if = "Option::is_none")]
    pub fallback_tranche: Option<String>,

    /// Total number of posts in this response.
    #[serde(rename = "total")]
    pub total: usize,

    /// Number of items in this response that were personalized (post-level or author-affinity).
    #[serde(rename = "personalized_count")]
    pub personalized_count: usize,

    /// For special posts (pinned/rotating/sponsored); omitted otherwise.
    #[serde(rename = "attribution", skip_serializing_if = "Option::is_none")]
    pub attribution: Option<String>,

    /// Params used for personalization when available (e.g. from Thompson sampling).
    #[serde(rename = "params", skip_serializing_if = "Option::is_none")]
    pub params: Option<ProvenanceParams>,

    /// Response time in ms for Thompson speed gate (interaction-based learning).
    #[serde(rename = "response_time_ms", skip_serializing_if = "Option::is_none")]
    pub response_time_ms: Option<f64>,

    /// True if this request used holdout params (control group).
    #[serde(rename = "is_holdout", skip_serializing_if = "Option::is_none")]
    pub is_holdout: Option<bool>,

    /// True if this request was in the personalization holdout (control group for A/B test).
    /// These requests skip LinkLonk entirely and serve only the non-personalized fallback blend.
    #[serde(
        rename = "is_personalization_holdout",
        skip_serializing_if = "Option::is_none"
    )]
    pub is_personalization_holdout: Option<bool>,
}

/// Compact personalization params for provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvenanceParams {
    #[serde(rename = "min_post_likes", skip_serializing_if = "Option::is_none")]
    pub min_post_likes: Option<usize>,
    #[serde(
        rename = "max_likers_per_post",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_likers_per_post: Option<usize>,
    #[serde(rename = "max_total_sources", skip_serializing_if = "Option::is_none")]
    pub max_total_sources: Option<usize>,
    #[serde(rename = "max_algo_checks", skip_serializing_if = "Option::is_none")]
    pub max_algo_checks: Option<usize>,
    #[serde(rename = "min_co_likes", skip_serializing_if = "Option::is_none")]
    pub min_co_likes: Option<usize>,
    #[serde(rename = "max_user_likes", skip_serializing_if = "Option::is_none")]
    pub max_user_likes: Option<usize>,
    #[serde(
        rename = "max_sources_per_post",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_sources_per_post: Option<usize>,
    #[serde(rename = "seed_sample_pool", skip_serializing_if = "Option::is_none")]
    pub seed_sample_pool: Option<usize>,
    #[serde(rename = "corater_decay_pct", skip_serializing_if = "Option::is_none")]
    pub corater_decay_pct: Option<usize>,
}

impl ProvenanceParams {
    /// Build from Thompson SelectedParams (all 9 fields).
    #[allow(clippy::too_many_arguments)]
    pub fn from_selected(
        min_post_likes: usize,
        max_likers_per_post: usize,
        max_total_sources: usize,
        max_algo_checks: usize,
        min_co_likes: usize,
        max_user_likes: usize,
        max_sources_per_post: usize,
        seed_sample_pool: usize,
        corater_decay_pct: usize,
    ) -> Self {
        Self {
            min_post_likes: Some(min_post_likes),
            max_likers_per_post: Some(max_likers_per_post),
            max_total_sources: Some(max_total_sources),
            max_algo_checks: Some(max_algo_checks),
            min_co_likes: Some(min_co_likes),
            max_user_likes: Some(max_user_likes),
            max_sources_per_post: Some(max_sources_per_post),
            seed_sample_pool: Some(seed_sample_pool),
            corater_decay_pct: Some(corater_decay_pct),
        }
    }
}

impl FeedContextProvenance {
    /// Encode to the string value for feedContext: JSON then base64 URL-safe.
    pub fn encode(&self) -> Option<String> {
        let json = serde_json::to_vec(self).ok()?;
        Some(URL_SAFE.encode(&json))
    }

    /// Decode feedContext from base64 URL-safe JSON. Returns None if decoding fails.
    pub fn decode(feed_context: &str) -> Option<Self> {
        if feed_context.is_empty() {
            return None;
        }
        let decoded = URL_SAFE.decode(feed_context).ok()?;
        let json_str = String::from_utf8(decoded).ok()?;
        serde_json::from_str(&json_str).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_feed_context_provenance_full() {
        let ctx = FeedContextProvenance {
            feed_uri: "at://did:plc:abc/app.bsky.feed.generator/feed".to_string(),
            algo_id: 42,
            depth: 0,
            personalized: true,
            source: "personalized".to_string(),
            personalization_type: Some("post_level".to_string()),
            fallback_tranche: None,
            total: 30,
            personalized_count: 25,
            attribution: None,
            params: Some(ProvenanceParams::from_selected(
                5, 30, 1000, 500, 1, 500, 100, 0, 0,
            )),
            response_time_ms: Some(150.0),
            is_holdout: Some(false),
            is_personalization_holdout: None,
        };
        let encoded = ctx.encode().expect("encode");
        let decoded = FeedContextProvenance::decode(&encoded).expect("decode");
        assert_eq!(decoded.feed_uri, ctx.feed_uri);
        assert_eq!(decoded.algo_id, ctx.algo_id);
        assert_eq!(decoded.response_time_ms, Some(150.0));
        assert_eq!(decoded.is_holdout, Some(false));
        assert!(decoded.params.is_some());
        let p = decoded.params.unwrap();
        assert_eq!(p.min_post_likes, Some(5));
        assert_eq!(p.max_user_likes, Some(500));
    }

    #[test]
    fn test_decode_feed_context_empty() {
        assert!(FeedContextProvenance::decode("").is_none());
    }

    #[test]
    fn test_decode_feed_context_invalid_base64() {
        assert!(FeedContextProvenance::decode("!!!invalid!!!").is_none());
    }
}
