//! Special posts fetching and caching.
//!
//! This module handles fetching injectable posts (pinned, rotating, sponsored)
//! from the Graze API and caching them in Redis.

use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::error::Result;
use crate::redis::{Keys, RedisClient};

/// Cache TTL in seconds (1 minute). Used when source is Remote.
const SPECIAL_POSTS_CACHE_TTL: u64 = 60;

/// Default API base URL for fetching special posts when source is Remote.
pub const DEFAULT_SPECIAL_POSTS_API_BASE: &str = "https://api.graze.social/app/my_feeds";

/// Where special posts are loaded from.
#[derive(Debug, Clone)]
pub enum SpecialPostsSource {
    /// Read only from Redis (populated via admin API). No external fetch.
    Local,
    /// Fetch from external API, cache in Redis with TTL.
    Remote {
        /// Base URL (e.g. "https://api.graze.social/app/my_feeds").
        api_base_url: String,
        /// Bearer token for authenticating with the special posts API.
        api_token: String,
    },
}

/// A special post (pinned or sticky/rotating) to inject into feeds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpecialPost {
    /// Attribution ID for tracking (e.g., ns123).
    pub attribution: String,
    /// AT-URI of the post.
    pub post: String,
}

/// A sponsored post with CPM pricing information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SponsoredPost {
    /// Attribution ID for tracking.
    pub attribution: String,
    /// AT-URI of the post.
    pub post: String,
    /// Cost per thousand impressions in cents.
    pub cpm_cents: i32,
}

/// Response from the special posts API endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpecialPostsResponse {
    /// The feed/algorithm ID.
    pub algorithm_id: i32,
    /// Percentage of available sponsored slots to fill (0-100).
    #[serde(default = "default_saturation_rate")]
    pub sponsorship_saturation_rate: i32,
    /// Posts pinned to the top of the feed (first page only).
    #[serde(default)]
    pub pinned: Vec<SpecialPost>,
    /// Rotating/sticky posts to intersperse in the feed.
    #[serde(default)]
    pub sticky: Vec<SpecialPost>,
    /// Sponsored posts ordered by CPM (highest first).
    #[serde(default)]
    pub sponsored: Vec<SponsoredPost>,
}

fn default_saturation_rate() -> i32 {
    20
}

impl Default for SpecialPostsResponse {
    fn default() -> Self {
        Self {
            algorithm_id: 0,
            sponsorship_saturation_rate: 20,
            pinned: Vec::new(),
            sticky: Vec::new(),
            sponsored: Vec::new(),
        }
    }
}

impl SpecialPostsResponse {
    /// Create an empty response for a given algorithm.
    pub fn empty(algo_id: i32) -> Self {
        Self {
            algorithm_id: algo_id,
            ..Default::default()
        }
    }
}

/// Client for fetching and caching special posts.
pub struct SpecialPostsClient {
    redis: Arc<RedisClient>,
    http_client: Client,
    source: SpecialPostsSource,
}

impl SpecialPostsClient {
    /// Create a new special posts client.
    pub fn new(redis: Arc<RedisClient>, source: SpecialPostsSource) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            redis,
            http_client,
            source: source.clone(),
        }
    }

    /// Get special posts for an algorithm.
    ///
    /// - **Local**: Read only from Redis; if missing, return empty. No external fetch.
    /// - **Remote**: Use cache if fresh (60s), otherwise fetch from API and cache.
    pub async fn get_special_posts(&self, algo_id: i32) -> Result<SpecialPostsResponse> {
        let cache_key = Keys::special_posts(algo_id);

        if let Some(cached) = self.get_from_cache(&cache_key).await? {
            debug!(algo_id, "special_posts_cache_hit");
            return Ok(cached);
        }

        match &self.source {
            SpecialPostsSource::Local => {
                debug!(algo_id, "special_posts_local_miss");
                Ok(SpecialPostsResponse::empty(algo_id))
            }
            SpecialPostsSource::Remote {
                api_base_url,
                api_token,
            } => {
                debug!(algo_id, "special_posts_cache_miss");
                let response = self.fetch_from_api(algo_id, api_base_url).await;

                self.store_in_cache(&cache_key, &response).await;

                Ok(response)
            }
        }
    }

    /// Get special posts from Redis cache.
    async fn get_from_cache(&self, cache_key: &str) -> Result<Option<SpecialPostsResponse>> {
        match self.redis.get_string(cache_key).await? {
            Some(cached_data) => match serde_json::from_str(&cached_data) {
                Ok(response) => Ok(Some(response)),
                Err(e) => {
                    warn!(error = %e, "special_posts_cache_deserialize_error");
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }

    /// Store special posts in Redis cache with TTL.
    async fn store_in_cache(&self, cache_key: &str, response: &SpecialPostsResponse) {
        match serde_json::to_string(response) {
            Ok(json) => {
                if let Err(e) = self
                    .redis
                    .set_ex(cache_key, &json, SPECIAL_POSTS_CACHE_TTL)
                    .await
                {
                    warn!(error = %e, "special_posts_cache_write_error");
                }
            }
            Err(e) => {
                warn!(error = %e, "special_posts_cache_serialize_error");
            }
        }
    }

    /// Fetch special posts from the external API.
    async fn fetch_from_api(
        &self,
        algo_id: i32,
        api_base_url: &str,
        api_token: &str,
    ) -> SpecialPostsResponse {
        let url = format!("{}/{}/special-posts", api_base_url, algo_id);
        let start = std::time::Instant::now();

        match self.http_client.get(&url).bearer_auth(api_token).send().await {
            Ok(response) => {
                let fetch_time_ms = start.elapsed().as_millis();

                if response.status().is_success() {
                    match response.json::<SpecialPostsResponse>().await {
                        Ok(mut data) => {
                            info!(
                                algo_id,
                                fetch_time_ms,
                                pinned_count = data.pinned.len(),
                                sticky_count = data.sticky.len(),
                                sponsored_count = data.sponsored.len(),
                                "special_posts_fetched"
                            );
                            // Ensure algorithm_id is set
                            data.algorithm_id = algo_id;
                            data
                        }
                        Err(e) => {
                            warn!(algo_id, error = %e, "special_posts_parse_error");
                            SpecialPostsResponse::empty(algo_id)
                        }
                    }
                } else {
                    warn!(
                        algo_id,
                        status = %response.status(),
                        fetch_time_ms,
                        "special_posts_api_error"
                    );
                    SpecialPostsResponse::empty(algo_id)
                }
            }
            Err(e) => {
                let fetch_time_ms = start.elapsed().as_millis();
                if e.is_timeout() {
                    warn!(algo_id, fetch_time_ms, "special_posts_api_timeout");
                } else {
                    warn!(algo_id, error = %e, "special_posts_api_exception");
                }
                SpecialPostsResponse::empty(algo_id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_special_posts_response_default() {
        let response = SpecialPostsResponse::default();
        assert_eq!(response.algorithm_id, 0);
        assert_eq!(response.sponsorship_saturation_rate, 20);
        assert!(response.pinned.is_empty());
        assert!(response.sticky.is_empty());
        assert!(response.sponsored.is_empty());
    }

    #[test]
    fn test_special_posts_response_empty() {
        let response = SpecialPostsResponse::empty(42);
        assert_eq!(response.algorithm_id, 42);
        assert_eq!(response.sponsorship_saturation_rate, 20);
    }

    #[test]
    fn test_special_post_serialization() {
        let post = SpecialPost {
            attribution: "test123".to_string(),
            post: "at://did:plc:abc/app.bsky.feed.post/123".to_string(),
        };
        let json = serde_json::to_string(&post).unwrap();
        assert!(json.contains("test123"));
        assert!(json.contains("at://"));
    }
}
