//! Interactions client for persisting user feedback.
//!
//! Prepares interaction and action-log rows and delegates to an `InteractionWriter`
//! (e.g. ClickHouse or no-op). Configure via `INTERACTIONS_WRITER=clickhouse` or `none`.

use base64::{engine::general_purpose::URL_SAFE, Engine};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::clickhouse::traits::InteractionWriter;
use crate::models::{FeedInteractionRow, Interaction, UserActionLogRow};
use crate::redis::{Keys, RedisClient};

/// Configuration for the ClickHouse interactions client.
#[derive(Debug, Clone)]
pub struct InteractionsConfig {
    pub clickhouse_host: String,
    pub clickhouse_port: u16,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub clickhouse_database: String,
    pub clickhouse_secure: bool,
}

/// Decoded feed context from the base64-encoded feedContext field.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct FeedContext {
    /// Feed URI (e.g., "at://did:plc:xxx/app.bsky.feed.generator/feedname")
    pub feed_uri: Option<String>,
    /// Custom attribution string
    pub attribution: Option<String>,
    /// ML impression ID (16-char hex). Set when ML_IMPRESSIONS_ENABLED=true.
    /// Key is "iid" to keep feedContext payloads compact.
    #[serde(rename = "iid")]
    pub impression_id: Option<String>,
}

/// Client that prepares interaction rows and writes them via an `InteractionWriter`.
pub struct InteractionsClient {
    #[allow(dead_code)] // retained for compatibility and optional logging
    config: Arc<InteractionsConfig>,
    redis: Arc<RedisClient>,
    writer: Arc<dyn InteractionWriter>,
}

impl InteractionsClient {
    /// Create a new interactions client with the given writer backend.
    pub fn new(
        config: Arc<InteractionsConfig>,
        redis: Arc<RedisClient>,
        writer: Arc<dyn InteractionWriter>,
    ) -> Self {
        Self {
            config,
            redis,
            writer,
        }
    }

    /// Decode the base64 feedContext string into a FeedContext struct.
    pub fn decode_feed_context(feed_context: &str) -> Option<FeedContext> {
        if feed_context.is_empty() {
            return None;
        }

        // Decode base64 (URL-safe variant)
        let decoded = match URL_SAFE.decode(feed_context) {
            Ok(bytes) => bytes,
            Err(e) => {
                debug!(error = %e, "failed to decode feedContext base64");
                return None;
            }
        };

        // Parse as JSON
        let json_str = match String::from_utf8(decoded) {
            Ok(s) => s,
            Err(e) => {
                debug!(error = %e, "feedContext is not valid UTF-8");
                return None;
            }
        };

        match serde_json::from_str::<FeedContext>(&json_str) {
            Ok(ctx) => Some(ctx),
            Err(e) => {
                debug!(error = %e, json = %json_str, "failed to parse feedContext JSON");
                None
            }
        }
    }

    /// Look up algo_id from feed_uri. Checks SUPPORTED_FEEDS first (active feeds), then
    /// FEED_URI_TO_ALGO_WRITES so interaction writes are still accepted for recently
    /// deregistered feeds (late-arriving sendInteractions).
    pub async fn get_algo_id_from_feed_uri(&self, feed_uri: &str) -> Option<i32> {
        let from_supported = self.redis.hget(Keys::SUPPORTED_FEEDS, feed_uri).await;
        match from_supported {
            Ok(Some(algo_id_str)) => match algo_id_str.parse::<i32>() {
                Ok(id) => return Some(id),
                Err(_) => {
                    warn!(feed_uri = %feed_uri, algo_id_str = %algo_id_str, "invalid algo_id in SUPPORTED_FEEDS");
                }
            },
            Ok(None) => {}
            Err(e) => {
                warn!(feed_uri = %feed_uri, error = %e, "failed to lookup feed_uri in Redis");
                return None;
            }
        }
        // Fallback: durable write-through map (accept interactions after deregister)
        match self
            .redis
            .hget(Keys::FEED_URI_TO_ALGO_WRITES, feed_uri)
            .await
        {
            Ok(Some(algo_id_str)) => match algo_id_str.parse::<i32>() {
                Ok(id) => {
                    debug!(feed_uri = %feed_uri, algo_id = id, "algo_id from FEED_URI_TO_ALGO_WRITES (deregistered feed)");
                    Some(id)
                }
                Err(_) => None,
            },
            Ok(None) => {
                debug!(feed_uri = %feed_uri, "feed_uri not found in SUPPORTED_FEEDS or FEED_URI_TO_ALGO_WRITES");
                None
            }
            Err(e) => {
                warn!(feed_uri = %feed_uri, error = %e, "failed to lookup feed_uri in FEED_URI_TO_ALGO_WRITES");
                None
            }
        }
    }

    /// Process interactions and prepare rows for ClickHouse insertion.
    pub async fn prepare_rows(
        &self,
        user_did: &str,
        interactions: &[Interaction],
        timestamp: DateTime<Utc>,
    ) -> (Vec<FeedInteractionRow>, Vec<UserActionLogRow>) {
        let mut interaction_rows = Vec::with_capacity(interactions.len());
        let mut action_log_rows = Vec::new();

        for interaction in interactions {
            // Decode feedContext
            let (feed_uri, attribution, impression_id) = match &interaction.feed_context {
                Some(ctx) => {
                    let decoded = Self::decode_feed_context(ctx);
                    (
                        decoded.as_ref().and_then(|d| d.feed_uri.clone()),
                        decoded.as_ref().and_then(|d| d.attribution.clone()),
                        decoded.as_ref().and_then(|d| d.impression_id.clone()),
                    )
                }
                None => (None, None, None),
            };

            // Build feed_interactions_buffer row
            interaction_rows.push(FeedInteractionRow {
                did: user_did.to_string(),
                impression_id: impression_id.unwrap_or_default(),
                interaction_feed_context: interaction.feed_context.clone().unwrap_or_default(),
                feed_uri: feed_uri.clone().unwrap_or_default(),
                attribution: attribution.clone().unwrap_or_default(),
                interaction_item: interaction.item.clone(),
                interaction_event: interaction.event_type.clone(),
                interaction_request_id: interaction.req_id.clone().unwrap_or_default(),
                occurred: timestamp,
            });

            // Build user_action_logs_buffer row (only if we can resolve algo_id)
            if let Some(ref uri) = feed_uri {
                if let Some(algo_id) = self.get_algo_id_from_feed_uri(uri).await {
                    action_log_rows.push(UserActionLogRow {
                        algo_id,
                        user_did: user_did.to_string(),
                        action_type: interaction.event_type.clone(),
                        action_identifier: interaction.item.clone(),
                        action_time: timestamp,
                        action_count: 1,
                    });
                }
            }
        }

        (interaction_rows, action_log_rows)
    }

    /// Process and persist all interactions via the configured writer.
    ///
    /// This is the main entry point for the interactions logging pipeline.
    pub async fn persist_interactions(
        &self,
        user_did: &str,
        interactions: &[Interaction],
    ) -> Result<(), String> {
        if interactions.is_empty() {
            return Ok(());
        }

        let timestamp = Utc::now();

        // Prepare rows for both tables
        let (interaction_rows, action_log_rows) =
            self.prepare_rows(user_did, interactions, timestamp).await;

        let feed_count = interaction_rows.len();
        let action_count = action_log_rows.len();
        self.writer
            .persist_batch(interaction_rows, action_log_rows)
            .await
            .map_err(|e| e.to_string())?;

        info!(
            user_did = %user_did,
            feed_interactions_count = feed_count,
            user_action_logs_count = action_count,
            "interactions_persisted"
        );

        Ok(())
    }

    /// Persist a batch of (user_did, interactions) pairs in a single ClickHouse write.
    /// Used by the interaction queue worker for efficient batched inserts.
    pub async fn persist_interactions_batch(
        &self,
        batch: &[(String, Vec<Interaction>)],
    ) -> Result<(), String> {
        if batch.is_empty() {
            return Ok(());
        }

        let timestamp = Utc::now();
        let mut all_feed_rows = Vec::new();
        let mut all_action_rows = Vec::new();

        for (user_did, interactions) in batch {
            if interactions.is_empty() {
                continue;
            }
            let (feed_rows, action_rows) =
                self.prepare_rows(user_did, interactions, timestamp).await;
            all_feed_rows.extend(feed_rows);
            all_action_rows.extend(action_rows);
        }

        if all_feed_rows.is_empty() && all_action_rows.is_empty() {
            return Ok(());
        }

        let feed_count = all_feed_rows.len();
        let action_count = all_action_rows.len();
        self.writer
            .persist_batch(all_feed_rows, all_action_rows)
            .await
            .map_err(|e| e.to_string())?;

        info!(
            batch_count = batch.len(),
            feed_interactions_count = feed_count,
            user_action_logs_count = action_count,
            "interactions_batch_persisted"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_feed_context_valid() {
        // {"feed_uri": "at://did:plc:xxx/app.bsky.feed.generator/myfeed", "attribution": "test"}
        let encoded = "eyJmZWVkX3VyaSI6ICJhdDovL2RpZDpwbGM6eHh4L2FwcC5ic2t5LmZlZWQuZ2VuZXJhdG9yL215ZmVlZCIsICJhdHRyaWJ1dGlvbiI6ICJ0ZXN0In0=";
        let decoded = InteractionsClient::decode_feed_context(encoded);
        assert!(decoded.is_some());
        let ctx = decoded.unwrap();
        assert_eq!(
            ctx.feed_uri,
            Some("at://did:plc:xxx/app.bsky.feed.generator/myfeed".to_string())
        );
        assert_eq!(ctx.attribution, Some("test".to_string()));
    }

    #[test]
    fn test_decode_feed_context_provenance_format() {
        // Full FeedContextProvenance format: feed_uri + algo_id, depth, source, etc.
        let json = serde_json::json!({
            "feed_uri": "at://did:plc:abc123/app.bsky.feed.generator/graze",
            "algo_id": 32,
            "depth": 0,
            "personalized": true,
            "source": "personalized",
            "total": 30,
            "personalized_count": 25
        });
        let encoded = URL_SAFE.encode(json.to_string());
        let decoded = InteractionsClient::decode_feed_context(&encoded);
        assert!(decoded.is_some());
        let ctx = decoded.unwrap();
        assert_eq!(
            ctx.feed_uri,
            Some("at://did:plc:abc123/app.bsky.feed.generator/graze".to_string())
        );
    }

    #[test]
    fn test_decode_feed_context_empty() {
        let decoded = InteractionsClient::decode_feed_context("");
        assert!(decoded.is_none());
    }

    #[test]
    fn test_decode_feed_context_invalid_base64() {
        let decoded = InteractionsClient::decode_feed_context("not-valid-base64!!!");
        assert!(decoded.is_none());
    }

    #[test]
    fn test_decode_feed_context_invalid_json() {
        // Valid base64 but not valid JSON
        let encoded = URL_SAFE.encode("not json");
        let decoded = InteractionsClient::decode_feed_context(&encoded);
        assert!(decoded.is_none());
    }
}
