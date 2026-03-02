//! ATProto-specific models for the Bluesky feed generator protocol.

use serde::{Deserialize, Serialize};

/// A single post in the feed skeleton.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkeletonFeedPost {
    /// Post AT-URI.
    pub post: String,

    /// Optional reason for the post (e.g., repost info).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<SkeletonReason>,

    /// Optional base64-encoded provenance (algo_id, depth, source, etc.) for tracing.
    #[serde(rename = "feedContext", skip_serializing_if = "Option::is_none")]
    pub feed_context: Option<String>,
}

/// Reason a post appears in the feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "$type")]
pub enum SkeletonReason {
    #[serde(rename = "app.bsky.feed.defs#skeletonReasonRepost")]
    Repost { repost: String },
}

/// Response from getFeedSkeleton.
#[derive(Debug, Serialize)]
pub struct FeedSkeletonResponse {
    /// List of posts in the feed.
    pub feed: Vec<SkeletonFeedPost>,

    /// Cursor for pagination.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Feed generator description.
#[derive(Debug, Serialize)]
pub struct FeedGeneratorDescription {
    /// Feed URI.
    pub uri: String,

    /// Creator DID.
    #[serde(rename = "creatorDid")]
    pub creator_did: String,

    /// Display name.
    #[serde(rename = "displayName")]
    pub display_name: String,

    /// Description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Avatar image.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,

    /// Like count.
    #[serde(rename = "likeCount", skip_serializing_if = "Option::is_none")]
    pub like_count: Option<u64>,
}

/// Response from describeFeedGenerator.
#[derive(Debug, Serialize)]
pub struct DescribeFeedGeneratorResponse {
    /// DID of the feed generator service.
    pub did: String,

    /// List of feeds provided by this generator.
    pub feeds: Vec<FeedGeneratorDescription>,
}

/// ATProto error response.
#[derive(Debug, Serialize)]
pub struct ATProtoError {
    /// Error code.
    pub error: String,

    /// Human-readable message.
    pub message: String,
}

impl ATProtoError {
    /// Create an "UnknownFeed" error.
    pub fn unknown_feed(feed: &str) -> Self {
        Self {
            error: "UnknownFeed".to_string(),
            message: format!("Feed not found: {}", feed),
        }
    }

    /// Create an "AuthenticationRequired" error.
    pub fn auth_required() -> Self {
        Self {
            error: "AuthenticationRequired".to_string(),
            message: "Authentication required".to_string(),
        }
    }

    /// Create an "InternalError" error.
    pub fn internal(message: &str) -> Self {
        Self {
            error: "InternalError".to_string(),
            message: message.to_string(),
        }
    }
}

/// DID document for /.well-known/did.json.
#[derive(Debug, Serialize)]
pub struct DidDocument {
    #[serde(rename = "@context")]
    pub context: Vec<String>,

    pub id: String,

    pub service: Vec<DidService>,
}

/// Service endpoint in a DID document.
#[derive(Debug, Serialize)]
pub struct DidService {
    pub id: String,

    #[serde(rename = "type")]
    pub service_type: String,

    #[serde(rename = "serviceEndpoint")]
    pub service_endpoint: String,
}

impl DidDocument {
    /// Create a new DID document for a feed generator.
    pub fn new_feed_generator(did: &str, service_endpoint: &str) -> Self {
        Self {
            context: vec!["https://www.w3.org/ns/did/v1".to_string()],
            id: did.to_string(),
            service: vec![DidService {
                id: "#bsky_fg".to_string(),
                service_type: "BskyFeedGenerator".to_string(),
                service_endpoint: service_endpoint.to_string(),
            }],
        }
    }
}
