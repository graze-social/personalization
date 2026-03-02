//! HTTP-backed candidate source.
//!
//! Fetches candidate URIs from the graze-api GET /v1/feeds/:algo_id/candidates endpoint.
//! Use when `CANDIDATE_SOURCE=http`.

use std::time::Duration;

use reqwest::Client;
use tracing::debug;

use crate::clickhouse::traits::CandidateSource;
use crate::error::{GrazeError, Result};

/// Response shape from GET /v1/feeds/:algo_id/candidates.
#[derive(Debug, serde::Deserialize)]
struct CandidatesResponse {
    uris: Vec<String>,
}

/// Fetches candidates via HTTP from the admin API.
///
/// Use when `CANDIDATE_SOURCE=http`. Set `CANDIDATE_HTTP_URL` to the base URL
/// of the graze-api (e.g. `http://graze-api:8080`).
pub struct HttpCandidateSource {
    http_client: Client,
    base_url: String,
}

impl HttpCandidateSource {
    pub fn new(base_url: String) -> Self {
        Self {
            http_client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("HTTP client"),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }
}

#[async_trait::async_trait]
impl CandidateSource for HttpCandidateSource {
    async fn fetch_candidates(&self, algo_id: i32) -> Result<Vec<String>> {
        let url = format!("{}/v1/feeds/{}/candidates", self.base_url, algo_id);
        debug!(algo_id, url = %url, "http_candidate_source_fetch");

        let response = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| GrazeError::Internal(format!("HTTP candidate fetch error: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(GrazeError::Internal(format!(
                "HTTP candidates returned {}",
                status
            )));
        }

        let data: CandidatesResponse = response
            .json()
            .await
            .map_err(|e| GrazeError::Internal(format!("HTTP candidates parse error: {}", e)))?;

        debug!(algo_id, count = data.uris.len(), "http_candidate_source_ok");
        Ok(data.uris)
    }
}
