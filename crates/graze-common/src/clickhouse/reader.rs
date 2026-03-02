//! ClickHouse HTTP SELECT client for algorithm candidate posts.
//!
//! Implements `CandidateSource` for `CANDIDATE_SOURCE=clickhouse`.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, error, info};

use crate::clickhouse::config::ClickHouseConfig;
use crate::clickhouse::traits::CandidateSource;
use crate::error::{GrazeError, Result};

/// ClickHouse JSONCompact response format for SELECT uri.
#[derive(Debug, Deserialize)]
struct ClickHouseResponse {
    data: Vec<Vec<String>>,
}

/// Params for candidate query (time window and limit).
/// Two-pass: try preferred_hours first; if fewer than minimum_posts, try fallback_hours.
#[derive(Debug, Clone)]
pub struct CandidateQueryParams {
    pub limit: u32,
    pub preferred_max_age_hours: u32,
    pub fallback_max_age_hours: u32,
    pub minimum_posts: usize,
}

/// ClickHouse-backed candidate source.
///
/// Use when `CANDIDATE_SOURCE=clickhouse`. Queries `algorithm_posts_v2`.
pub struct ClickHouseCandidateSource {
    http_client: Client,
    config: Arc<ClickHouseConfig>,
    query_params: CandidateQueryParams,
}

impl ClickHouseCandidateSource {
    pub fn new(config: Arc<ClickHouseConfig>, query_params: CandidateQueryParams) -> Self {
        Self {
            http_client: Client::builder()
                .timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(50)
                .build()
                .expect("HTTP client"),
            config,
            query_params,
        }
    }

    /// Fetch algorithm post URIs for a time window (for use when extending window).
    pub async fn fetch_candidates_with_age(
        &self,
        algo_id: i32,
        max_age_hours: u32,
    ) -> Result<Vec<String>> {
        let min_time = Utc::now() - chrono::Duration::hours(max_age_hours as i64);
        let min_time_str = min_time.format("%Y-%m-%d %H:%M:%S").to_string();

        debug!(
            algo_id,
            max_age_hours,
            min_time = %min_time_str,
            "clickhouse_query_starting"
        );

        let query = r#"
            SELECT uri
            FROM {database:Identifier}.algorithm_posts_v2
            WHERE algo_id = {algo_id:Int32}
              AND bluesky_created_at >= {min_time:DateTime}
              AND bluesky_created_at <= created_at + INTERVAL 1 HOUR
              AND bluesky_created_at <= now()
            ORDER BY bluesky_created_at DESC
            LIMIT {limit:Int32}
            FORMAT JSONCompact
        "#;

        let url = self.config.base_url();
        let response = self
            .http_client
            .post(&url)
            .basic_auth(&self.config.user, Some(&self.config.password))
            .header("Content-Type", "text/plain")
            .query(&[
                ("param_database", self.config.database.as_str()),
                ("param_algo_id", &algo_id.to_string()),
                ("param_min_time", &min_time_str),
                ("param_limit", &self.query_params.limit.to_string()),
            ])
            .body(query.to_string())
            .send()
            .await
            .map_err(|e| GrazeError::Internal(format!("ClickHouse connection error: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            error!(
                algo_id = algo_id,
                status = %status,
                body = &body[..body.len().min(500)],
                "ClickHouse query failed"
            );
            return Err(GrazeError::Internal(format!(
                "ClickHouse query failed: {}",
                status
            )));
        }

        let data: ClickHouseResponse = response
            .json()
            .await
            .map_err(|e| GrazeError::Internal(format!("ClickHouse response parse error: {}", e)))?;

        let posts: Vec<String> = data
            .data
            .into_iter()
            .filter_map(|row| row.into_iter().next())
            .collect();

        debug!(
            algo_id,
            max_age_hours,
            row_count = posts.len(),
            "clickhouse_query_complete"
        );

        Ok(posts)
    }
}

#[async_trait::async_trait]
impl CandidateSource for ClickHouseCandidateSource {
    async fn fetch_candidates(&self, algo_id: i32) -> Result<Vec<String>> {
        let posts = self
            .fetch_candidates_with_age(algo_id, self.query_params.preferred_max_age_hours)
            .await?;

        if posts.len() < self.query_params.minimum_posts {
            info!(
                algo_id = algo_id,
                preferred_posts = posts.len(),
                preferred_hours = self.query_params.preferred_max_age_hours,
                fallback_hours = self.query_params.fallback_max_age_hours,
                minimum_posts = self.query_params.minimum_posts,
                "Sync extending time window"
            );
            return self
                .fetch_candidates_with_age(algo_id, self.query_params.fallback_max_age_hours)
                .await;
        }

        Ok(posts)
    }
}
