//! ClickHouse HTTP INSERT client for interaction and action-log tables.
//!
//! Implements `InteractionWriter` for `INTERACTIONS_WRITER=clickhouse`.

use std::sync::Arc;

use reqwest::Client;
use tracing::error;

use crate::clickhouse::config::ClickHouseConfig;
use crate::clickhouse::traits::InteractionWriter;
use crate::error::{GrazeError, Result};
use crate::models::{FeedInteractionRow, UserActionLogRow};

/// ClickHouse-backed interaction writer.
///
/// Use when `INTERACTIONS_WRITER=clickhouse`. Requires ClickHouse config.
pub struct ClickHouseInteractionWriter {
    http_client: Client,
    config: Arc<ClickHouseConfig>,
}

impl ClickHouseInteractionWriter {
    pub fn new(config: Arc<ClickHouseConfig>) -> Self {
        Self {
            http_client: Client::new(),
            config,
        }
    }

    async fn insert_tabseparated(
        &self,
        table_name: &str,
        columns: &str,
        data: Vec<Vec<String>>,
    ) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let query = format!(
            "INSERT INTO {}.{} ({}) FORMAT TabSeparated",
            self.config.database, table_name, columns
        );

        let data_payload = data
            .iter()
            .map(|row| row.join("\t"))
            .collect::<Vec<_>>()
            .join("\n");

        let url = self.config.base_url();
        let response = self
            .http_client
            .post(&url)
            .basic_auth(&self.config.user, Some(&self.config.password))
            .header("Content-Type", "text/plain")
            .query(&[("query", query.as_str())])
            .body(data_payload)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| GrazeError::Internal(format!("ClickHouse connection error: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            error!(
                table = %table_name,
                status = %status,
                body = &body[..body.len().min(500)],
                "ClickHouse insert failed"
            );
            return Err(GrazeError::Internal(format!(
                "ClickHouse insert failed: {}",
                status
            )));
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl InteractionWriter for ClickHouseInteractionWriter {
    async fn persist_batch(
        &self,
        feed_rows: Vec<FeedInteractionRow>,
        action_rows: Vec<UserActionLogRow>,
    ) -> Result<()> {
        if !feed_rows.is_empty() {
            let columns = "did, interaction_feed_context, feed_uri, attribution, interaction_item, interaction_event, interaction_request_id, occurred";
            let data: Vec<Vec<String>> = feed_rows
                .into_iter()
                .map(|row| {
                    vec![
                        escape_tab_value(&row.did),
                        escape_tab_value(&row.interaction_feed_context),
                        escape_tab_value(&row.feed_uri),
                        escape_tab_value(&row.attribution),
                        escape_tab_value(&row.interaction_item),
                        escape_tab_value(&row.interaction_event),
                        escape_tab_value(&row.interaction_request_id),
                        row.occurred.format("%Y-%m-%d %H:%M:%S").to_string(),
                    ]
                })
                .collect();
            self.insert_tabseparated("feed_interactions_buffer", columns, data)
                .await?;
        }

        if !action_rows.is_empty() {
            let columns =
                "algo_id, user_did, action_type, action_identifier, action_time, action_count";
            let data: Vec<Vec<String>> = action_rows
                .into_iter()
                .map(|row| {
                    vec![
                        row.algo_id.to_string(),
                        escape_tab_value(&row.user_did),
                        escape_tab_value(&row.action_type),
                        escape_tab_value(&row.action_identifier),
                        row.action_time.format("%Y-%m-%d %H:%M:%S").to_string(),
                        row.action_count.to_string(),
                    ]
                })
                .collect();
            self.insert_tabseparated("user_action_logs_buffer", columns, data)
                .await?;
        }

        Ok(())
    }
}

/// No-op interaction writer (drops all rows).
///
/// Use when `INTERACTIONS_WRITER=none` or analytics writes are disabled.
pub struct NoOpInteractionWriter;

#[async_trait::async_trait]
impl InteractionWriter for NoOpInteractionWriter {
    async fn persist_batch(
        &self,
        _feed_rows: Vec<FeedInteractionRow>,
        _action_rows: Vec<UserActionLogRow>,
    ) -> Result<()> {
        Ok(())
    }
}

fn escape_tab_value(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}
