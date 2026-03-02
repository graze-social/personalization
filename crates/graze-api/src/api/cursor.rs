//! Feed cursor for pagination state tracking.
//!
//! Supports three cursor formats:
//! - v1: Legacy offset-based pagination
//! - v2: Score-based pagination with last_score/last_post_id
//! - v3: Fallback-only mode when personalization is exhausted

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Cursor state for feed pagination.
///
/// Supports both legacy offset-based (v1) and score-based (v2) pagination.
/// Score-based pagination uses (last_score, last_post_id) for O(log N) deep pagination.
///
/// When personalized content is exhausted, transitions to fallback_only mode (v3)
/// which serves from popular/velocity/discovery tranches.
///
/// Tracks which injectable posts have been shown to prevent duplicate posts
/// which would crash the Bluesky client.
#[derive(Debug, Clone, Default)]
pub struct FeedCursor {
    /// Legacy pagination offset (deprecated, for backwards compat).
    pub offset: i32,
    /// Score of the last item returned (for score-based pagination).
    pub last_score: Option<f64>,
    /// Post ID of the last item returned (tiebreaker for equal scores).
    pub last_post_id: Option<i64>,
    /// True when personalization is exhausted, serve fallback only.
    pub fallback_only: bool,
    /// Offset into fallback content when in fallback_only mode.
    pub fallback_offset: usize,
    /// Attribution IDs of pinned posts already shown.
    pub shown_pinned: HashSet<String>,
    /// Attribution IDs of rotating/sticky posts already shown.
    pub shown_rotating: HashSet<String>,
    /// Attribution IDs of sponsored posts already shown.
    pub shown_sponsored: HashSet<String>,
    /// URIs of fallback posts already shown (to avoid duplicates).
    pub shown_fallback: HashSet<String>,
    /// True when fallback_only is from personalization holdout (A/B test control).
    pub is_personalization_holdout: bool,
}

/// Compact cursor format for v1 (offset-based).
#[derive(Debug, Serialize, Deserialize)]
struct CursorV1 {
    #[serde(rename = "o")]
    offset: i32,
    #[serde(rename = "p", default, skip_serializing_if = "Vec::is_empty")]
    pinned: Vec<String>,
    #[serde(rename = "r", default, skip_serializing_if = "Vec::is_empty")]
    rotating: Vec<String>,
    #[serde(rename = "s", default, skip_serializing_if = "Vec::is_empty")]
    sponsored: Vec<String>,
}

/// Compact cursor format for v2 (score-based).
#[derive(Debug, Serialize, Deserialize)]
struct CursorV2 {
    #[serde(rename = "v")]
    version: u8,
    #[serde(rename = "ls", skip_serializing_if = "Option::is_none")]
    last_score: Option<f64>,
    #[serde(rename = "lp", skip_serializing_if = "Option::is_none")]
    last_post_id: Option<i64>,
    #[serde(rename = "p", default, skip_serializing_if = "Vec::is_empty")]
    pinned: Vec<String>,
    #[serde(rename = "r", default, skip_serializing_if = "Vec::is_empty")]
    rotating: Vec<String>,
    #[serde(rename = "s", default, skip_serializing_if = "Vec::is_empty")]
    sponsored: Vec<String>,
}

/// Compact cursor format for v3 (fallback-only mode).
#[derive(Debug, Serialize, Deserialize)]
struct CursorV3 {
    #[serde(rename = "v")]
    version: u8,
    #[serde(rename = "fo")]
    fallback_offset: usize,
    #[serde(rename = "p", default, skip_serializing_if = "Vec::is_empty")]
    pinned: Vec<String>,
    #[serde(rename = "r", default, skip_serializing_if = "Vec::is_empty")]
    rotating: Vec<String>,
    #[serde(rename = "s", default, skip_serializing_if = "Vec::is_empty")]
    sponsored: Vec<String>,
    #[serde(rename = "fb", default, skip_serializing_if = "Vec::is_empty")]
    fallback: Vec<String>,
    #[serde(rename = "ph", default)]
    is_personalization_holdout: bool,
}

/// Generic cursor container for deserialization.
#[derive(Debug, Deserialize)]
struct CursorContainer {
    #[serde(rename = "v", default)]
    version: u8,
    // v1 fields
    #[serde(rename = "o", default)]
    offset: i32,
    // v2 fields
    #[serde(rename = "ls")]
    last_score: Option<f64>,
    #[serde(rename = "lp")]
    last_post_id: Option<i64>,
    // v3 fields
    #[serde(rename = "fo", default)]
    fallback_offset: usize,
    #[serde(rename = "fb", default)]
    fallback: Vec<String>,
    #[serde(rename = "ph", default)]
    is_personalization_holdout: bool,
    // Common fields
    #[serde(rename = "p", default)]
    pinned: Vec<String>,
    #[serde(rename = "r", default)]
    rotating: Vec<String>,
    #[serde(rename = "s", default)]
    sponsored: Vec<String>,
}

impl FeedCursor {
    /// Create a new empty cursor for the first page.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if this is the first page.
    pub fn is_first_page(&self) -> bool {
        self.offset == 0 && self.last_score.is_none() && !self.fallback_only
    }

    /// Check if this cursor uses score-based pagination.
    pub fn is_score_based(&self) -> bool {
        self.last_score.is_some() || (self.offset == 0 && self.last_post_id.is_none())
    }

    /// Check if this is the end-of-feed marker.
    pub fn is_eof(&self) -> bool {
        self.offset == -1
    }

    /// Encode the cursor to a base64 string.
    ///
    /// Uses gzip compression to keep the cursor size small.
    /// Generates v3 format for fallback-only mode, v2 for score-based, v1 for legacy offset.
    pub fn encode(&self) -> String {
        let json_bytes = if self.fallback_only {
            // v3 format: fallback-only mode (personalization exhausted or holdout)
            let data = CursorV3 {
                version: 3,
                fallback_offset: self.fallback_offset,
                pinned: self.shown_pinned.iter().cloned().collect(),
                rotating: self.shown_rotating.iter().cloned().collect(),
                sponsored: self.shown_sponsored.iter().cloned().collect(),
                fallback: self.shown_fallback.iter().cloned().collect(),
                is_personalization_holdout: self.is_personalization_holdout,
            };
            serde_json::to_vec(&data).unwrap_or_default()
        } else if self.last_score.is_some() {
            // v2 format: score-based pagination
            let data = CursorV2 {
                version: 2,
                last_score: self.last_score,
                last_post_id: self.last_post_id,
                pinned: self.shown_pinned.iter().cloned().collect(),
                rotating: self.shown_rotating.iter().cloned().collect(),
                sponsored: self.shown_sponsored.iter().cloned().collect(),
            };
            serde_json::to_vec(&data).unwrap_or_default()
        } else {
            // v1 format: offset-based pagination (legacy)
            let data = CursorV1 {
                offset: self.offset,
                pinned: self.shown_pinned.iter().cloned().collect(),
                rotating: self.shown_rotating.iter().cloned().collect(),
                sponsored: self.shown_sponsored.iter().cloned().collect(),
            };
            serde_json::to_vec(&data).unwrap_or_default()
        };

        let compressed = miniz_oxide::deflate::compress_to_vec(&json_bytes, 6);
        URL_SAFE_NO_PAD.encode(&compressed)
    }

    /// Decode a cursor string to a FeedCursor.
    ///
    /// Handles:
    /// - None: First page
    /// - "eof": End of feed
    /// - Legacy integer: Old offset-based cursor
    /// - v1/v2/v3 formats: Gzip-compressed JSON
    pub fn decode(cursor_str: Option<&str>) -> Self {
        let cursor_str = match cursor_str {
            Some(s) if !s.is_empty() => s,
            _ => return Self::new(),
        };

        // Handle "eof" cursor
        if cursor_str == "eof" {
            return Self {
                offset: -1, // Signal end of feed
                ..Default::default()
            };
        }

        // Try to parse as legacy integer offset first
        if let Ok(offset) = cursor_str.parse::<i32>() {
            return Self {
                offset,
                ..Default::default()
            };
        }

        // Try to decode as base64+gzip+json format
        let compressed = match URL_SAFE_NO_PAD.decode(cursor_str) {
            Ok(c) => c,
            Err(_) => return Self::new(),
        };

        let json_bytes = match miniz_oxide::inflate::decompress_to_vec(&compressed) {
            Ok(j) => j,
            Err(_) => return Self::new(),
        };

        let container: CursorContainer = match serde_json::from_slice(&json_bytes) {
            Ok(c) => c,
            Err(_) => return Self::new(),
        };

        match container.version {
            3 => {
                // v3 format: fallback-only mode
                Self {
                    fallback_only: true,
                    fallback_offset: container.fallback_offset,
                    shown_pinned: container.pinned.into_iter().collect(),
                    shown_rotating: container.rotating.into_iter().collect(),
                    shown_sponsored: container.sponsored.into_iter().collect(),
                    shown_fallback: container.fallback.into_iter().collect(),
                    is_personalization_holdout: container.is_personalization_holdout,
                    ..Default::default()
                }
            }
            2 => {
                // v2 format: score-based pagination
                Self {
                    last_score: container.last_score,
                    last_post_id: container.last_post_id,
                    shown_pinned: container.pinned.into_iter().collect(),
                    shown_rotating: container.rotating.into_iter().collect(),
                    shown_sponsored: container.sponsored.into_iter().collect(),
                    ..Default::default()
                }
            }
            _ => {
                // v1 format: offset-based pagination
                Self {
                    offset: container.offset,
                    shown_pinned: container.pinned.into_iter().collect(),
                    shown_rotating: container.rotating.into_iter().collect(),
                    shown_sponsored: container.sponsored.into_iter().collect(),
                    ..Default::default()
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_cursor() {
        let cursor = FeedCursor::new();
        assert!(cursor.is_first_page());
        assert!(!cursor.is_eof());
    }

    #[test]
    fn test_decode_none() {
        let cursor = FeedCursor::decode(None);
        assert!(cursor.is_first_page());
    }

    #[test]
    fn test_decode_empty() {
        let cursor = FeedCursor::decode(Some(""));
        assert!(cursor.is_first_page());
    }

    #[test]
    fn test_decode_eof() {
        let cursor = FeedCursor::decode(Some("eof"));
        assert!(cursor.is_eof());
        assert_eq!(cursor.offset, -1);
    }

    #[test]
    fn test_decode_legacy_integer() {
        let cursor = FeedCursor::decode(Some("30"));
        assert_eq!(cursor.offset, 30);
        assert!(!cursor.is_first_page());
    }

    #[test]
    fn test_encode_decode_v1() {
        let mut cursor = FeedCursor::new();
        cursor.offset = 60;
        cursor.shown_pinned.insert("pin1".to_string());

        let encoded = cursor.encode();
        let decoded = FeedCursor::decode(Some(&encoded));

        assert_eq!(decoded.offset, 60);
        assert!(decoded.shown_pinned.contains("pin1"));
    }

    #[test]
    fn test_encode_decode_v2() {
        let mut cursor = FeedCursor::new();
        cursor.last_score = Some(0.95);
        cursor.last_post_id = Some(12345);
        cursor.shown_rotating.insert("rot1".to_string());

        let encoded = cursor.encode();
        let decoded = FeedCursor::decode(Some(&encoded));

        assert_eq!(decoded.last_score, Some(0.95));
        assert_eq!(decoded.last_post_id, Some(12345));
        assert!(decoded.shown_rotating.contains("rot1"));
    }

    #[test]
    fn test_encode_decode_v3() {
        let mut cursor = FeedCursor::new();
        cursor.fallback_only = true;
        cursor.fallback_offset = 100;
        cursor
            .shown_fallback
            .insert("at://did:plc:abc/post/123".to_string());
        cursor.shown_sponsored.insert("spon1".to_string());

        let encoded = cursor.encode();
        let decoded = FeedCursor::decode(Some(&encoded));

        assert!(decoded.fallback_only);
        assert_eq!(decoded.fallback_offset, 100);
        assert!(decoded.shown_fallback.contains("at://did:plc:abc/post/123"));
        assert!(decoded.shown_sponsored.contains("spon1"));
    }

    #[test]
    fn test_is_score_based() {
        let cursor1 = FeedCursor::new();
        assert!(cursor1.is_score_based()); // New cursor defaults to score-based

        let mut cursor2 = FeedCursor::new();
        cursor2.last_score = Some(0.5);
        assert!(cursor2.is_score_based());

        let mut cursor3 = FeedCursor::new();
        cursor3.offset = 30;
        // If offset is set without last_score, it's offset-based (v1), not score-based
        assert!(!cursor3.is_score_based());
    }
}
