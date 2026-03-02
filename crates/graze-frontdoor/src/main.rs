//! Graze Frontdoor server binary.
//!
//! Routes requests to either the feed service or labs service based on path:
//! - GET requests to FILTERED_ENDPOINTS -> FEED_SERVICE_HOST (with DID allowlist check)
//! - All other requests -> LABS_SERVICE_HOST
//!
//! Feed requests are filtered by DID allowlist. Allowed DIDs are loaded from
//! Redis (`allowed_identities` SET) with periodic refresh, falling back to
//! ALLOWED_DIDS environment variable if Redis is unavailable.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use atproto_record::aturi::ATURI;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::{Config as RedisConfig, Pool, Runtime};
use graze_common::{internal_probe, run_metrics_server, MetricsEncodable};
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use reqwest::Client;
use tokio::signal;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::EnvFilter;

/// Frontdoor configuration loaded from environment variables.
#[derive(Debug, Clone)]
struct FrontdoorConfig {
    /// Host to bind the proxy server to.
    pub host: String,
    /// Port for the proxy server.
    pub port: u16,
    /// Port for the metrics server.
    pub metrics_port: u16,
    /// URL of the feed service backend.
    pub feed_service_host: String,
    /// URL of the labs service backend.
    pub labs_service_host: String,
    /// Request timeout in seconds.
    pub request_timeout_seconds: u64,
    /// Connection timeout in seconds.
    pub connect_timeout_seconds: u64,
    /// Redis URL for loading allowed identities (optional).
    pub redis_url: Option<String>,
    /// Interval in seconds to refresh allowed identities from Redis.
    pub allowlist_refresh_seconds: u64,
    /// Comma-separated list of allowed DIDs (fallback if Redis unavailable).
    pub allowed_dids: Vec<String>,
}

impl FrontdoorConfig {
    /// Load configuration from environment variables.
    fn from_env() -> Self {
        Self {
            host: default_env("HTTP_HOST", "0.0.0.0"),
            port: parse_u16_env("HTTP_PORT", 8081),
            metrics_port: parse_u16_env("METRICS_PORT", 9091),
            feed_service_host: default_env("FEED_SERVICE_HOST", "http://localhost:8080"),
            labs_service_host: default_env("LABS_SERVICE_HOST", "http://localhost:8082"),
            request_timeout_seconds: parse_u64_env("REQUEST_TIMEOUT_SECONDS", 30),
            connect_timeout_seconds: parse_u64_env("CONNECT_TIMEOUT_SECONDS", 5),
            redis_url: std::env::var("REDIS_URL").ok(),
            allowlist_refresh_seconds: parse_u64_env("ALLOWLIST_REFRESH_SECONDS", 60),
            allowed_dids: parse_string_list_env("ALLOWED_DIDS", vec![]),
        }
    }
}

// Environment variable helper functions

fn default_env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_u16_env(name: &str, default: u16) -> u16 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_string_list_env(name: &str, default: Vec<String>) -> Vec<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or(default)
}

/// Endpoints that require feed DID filtering.
const FILTERED_ENDPOINTS: &[&str] = &[
    "/xrpc/app.bsky.feed.getFeedSkeleton",
    "/xrpc/app.bsky.feed.describeFeedGenerator",
    "/xrpc/app.bsky.feed.sendInteractions",
];

/// Prometheus metrics for the frontdoor service.
struct FrontdoorMetrics {
    registry: Registry,
    /// Total HTTP requests served.
    requests_total: Counter,
    /// Size of the allowed identities list.
    allowed_identities_size: Gauge,
    /// Total allowlist check hits (DID was in allowlist).
    allowlist_hits: Counter,
    /// Total allowlist check misses (DID was not in allowlist).
    allowlist_misses: Counter,
}

impl FrontdoorMetrics {
    fn new(allowed_dids_count: usize) -> Self {
        let mut registry = Registry::default();

        let requests_total = Counter::default();
        registry.register(
            "frontdoor_requests_total",
            "Total number of HTTP requests served",
            requests_total.clone(),
        );

        let allowed_identities_size: Gauge = Gauge::default();
        registry.register(
            "frontdoor_allowed_identities_size",
            "Number of DIDs in the allowed identities list",
            allowed_identities_size.clone(),
        );
        // Set initial size
        allowed_identities_size.set(allowed_dids_count as i64);

        let allowlist_hits = Counter::default();
        registry.register(
            "frontdoor_allowlist_hits_total",
            "Total allowlist check hits (DID was allowed)",
            allowlist_hits.clone(),
        );

        let allowlist_misses = Counter::default();
        registry.register(
            "frontdoor_allowlist_misses_total",
            "Total allowlist check misses (DID was not allowed)",
            allowlist_misses.clone(),
        );

        Self {
            registry,
            requests_total,
            allowed_identities_size,
            allowlist_hits,
            allowlist_misses,
        }
    }

    fn encode_metrics(&self) -> String {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry).unwrap();
        buffer
    }
}

impl MetricsEncodable for FrontdoorMetrics {
    fn encode(&self) -> String {
        self.encode_metrics()
    }
}

/// Redis key for allowed identities SET.
const ALLOWED_IDENTITIES_KEY: &str = "allowed_identities";

/// Frontdoor application state.
struct FrontdoorState {
    config: Arc<FrontdoorConfig>,
    client: Client,
    /// Redis connection pool (if configured).
    redis_pool: Option<Pool>,
    /// Set of allowed DIDs for feed requests (thread-safe for background refresh).
    allowed_dids: Arc<RwLock<HashSet<String>>>,
    /// Prometheus metrics.
    metrics: Arc<FrontdoorMetrics>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .json()
        .init();

    info!("Starting Graze Frontdoor server");

    // Load configuration
    let config = FrontdoorConfig::from_env();
    let proxy_addr = format!("{}:{}", config.host, config.port);
    let metrics_addr = format!("{}:{}", config.host, config.metrics_port);

    debug!(
        host = %config.host,
        port = config.port,
        metrics_port = config.metrics_port,
        feed_service_host = %config.feed_service_host,
        labs_service_host = %config.labs_service_host,
        redis_url = ?config.redis_url,
        allowlist_refresh_seconds = config.allowlist_refresh_seconds,
        fallback_allowed_dids_count = config.allowed_dids.len(),
        "Configuration loaded"
    );

    // Initialize Redis pool if configured
    let (redis_pool, initial_dids) = if let Some(ref redis_url) = config.redis_url {
        match create_redis_pool(redis_url).await {
            Ok(pool) => {
                // Try to load initial allowed identities from Redis
                match load_allowed_identities_from_redis(&pool).await {
                    Ok(dids) => {
                        info!(
                            count = dids.len(),
                            source = "redis",
                            "Loaded allowed identities"
                        );
                        (Some(pool), dids)
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            fallback_count = config.allowed_dids.len(),
                            "Failed to load allowed identities from Redis, using env var fallback"
                        );
                        let fallback: HashSet<String> =
                            config.allowed_dids.iter().cloned().collect();
                        (Some(pool), fallback)
                    }
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    fallback_count = config.allowed_dids.len(),
                    "Failed to connect to Redis, using env var fallback"
                );
                let fallback: HashSet<String> = config.allowed_dids.iter().cloned().collect();
                (None, fallback)
            }
        }
    } else {
        info!(
            count = config.allowed_dids.len(),
            source = "env",
            "Using ALLOWED_DIDS from environment (no Redis configured)"
        );
        let fallback: HashSet<String> = config.allowed_dids.iter().cloned().collect();
        (None, fallback)
    };

    // Create thread-safe allowed DIDs set
    let allowed_dids = Arc::new(RwLock::new(initial_dids.clone()));

    // Create metrics
    let metrics = Arc::new(FrontdoorMetrics::new(initial_dids.len()));

    // Create HTTP client for proxying
    let client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_seconds))
        .connect_timeout(Duration::from_secs(config.connect_timeout_seconds))
        .build()?;

    let refresh_seconds = config.allowlist_refresh_seconds;
    let metrics_for_server = metrics.clone();
    let state = Arc::new(FrontdoorState {
        config: Arc::new(config),
        client,
        redis_pool,
        allowed_dids,
        metrics,
    });

    // Spawn background refresh task if Redis is configured
    if state.redis_pool.is_some() {
        let state_clone = state.clone();
        tokio::spawn(async move {
            refresh_allowed_dids_task(state_clone, refresh_seconds).await;
        });
    }

    // Build proxy router (catch-all handler)
    let proxy_app = Router::new()
        .route("/internal/started", get(internal_probe))
        .route("/internal/ready", get(internal_probe))
        .route("/internal/alive", get(internal_probe))
        .fallback(proxy_handler)
        .with_state(state);

    // Start proxy server
    let proxy_listener = tokio::net::TcpListener::bind(&proxy_addr).await?;
    info!(address = %proxy_addr, "Frontdoor proxy server listening");

    // Run both servers concurrently
    tokio::select! {
        result = axum::serve(proxy_listener, proxy_app).with_graceful_shutdown(shutdown_signal()) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Proxy server error");
            }
        }
        result = run_metrics_server(&metrics_addr, metrics_for_server) => {
            if let Err(e) = result {
                tracing::error!(error = %e, "Metrics server error");
            }
        }
    }

    info!("Frontdoor server shutdown complete");
    Ok(())
}

/// Determine the target host based on request method and path.
fn get_target_host<'a>(config: &'a FrontdoorConfig, method: &Method, path: &str) -> &'a str {
    // Route GET requests to filtered feed endpoints to feed service
    if method == Method::GET && FILTERED_ENDPOINTS.iter().any(|ep| path.starts_with(ep)) {
        &config.feed_service_host
    } else {
        &config.labs_service_host
    }
}

/// Build the full target URL from host and request URI.
fn build_target_url(host: &str, uri: &Uri) -> String {
    let host = host.trim_end_matches('/');
    let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    format!("{}{}", host, path_and_query)
}

/// Check if the path requires feed DID filtering.
fn requires_feed_filtering(path: &str) -> bool {
    FILTERED_ENDPOINTS.iter().any(|ep| path.starts_with(ep))
}

/// Extract the `feed` query parameter from a URI.
fn extract_feed_param(uri: &Uri) -> Option<String> {
    uri.query().and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == "feed")
            .map(|(_, value)| value.into_owned())
    })
}

/// Check if a feed AT-URI's DID is allowed.
async fn is_feed_allowed(
    feed_uri: &str,
    allowed_dids: &RwLock<HashSet<String>>,
    metrics: &FrontdoorMetrics,
) -> bool {
    // Parse the AT-URI and extract the authority (DID)
    match ATURI::from_str(feed_uri) {
        Ok(aturi) => {
            let allowed = {
                let dids = allowed_dids.read().await;
                dids.contains(&aturi.authority)
            };
            if allowed {
                metrics.allowlist_hits.inc();
            } else {
                metrics.allowlist_misses.inc();
                debug!(
                    feed_uri = %feed_uri,
                    did = %aturi.authority,
                    "Feed DID not in allowlist"
                );
            }
            allowed
        }
        Err(e) => {
            metrics.allowlist_misses.inc();
            warn!(
                feed_uri = %feed_uri,
                error = %e,
                "Failed to parse feed AT-URI"
            );
            false
        }
    }
}

/// Main proxy handler - forwards requests to appropriate backend.
async fn proxy_handler(State(state): State<Arc<FrontdoorState>>, request: Request) -> Response {
    // Count this request
    state.metrics.requests_total.inc();

    let method = request.method().clone();
    let uri = request.uri().clone();
    let path = uri.path();

    // Check feed DID allowlist for filtered endpoints
    if requires_feed_filtering(path) {
        if let Some(feed_uri) = extract_feed_param(&uri) {
            if !is_feed_allowed(&feed_uri, &state.allowed_dids, &state.metrics).await {
                return (StatusCode::FORBIDDEN, "Feed not allowed").into_response();
            }
        }
    }

    // Determine target host
    let target_host = get_target_host(&state.config, &method, path);

    // Build target URL (preserving path and query)
    let target_url = build_target_url(target_host, &uri);

    debug!(
        method = %method,
        path = %path,
        target = %target_url,
        "Proxying request"
    );

    // Forward the request
    match forward_request(&state.client, request, &target_url).await {
        Ok(response) => response,
        Err(e) => {
            warn!(
                error = %e,
                target = %target_url,
                "Proxy request failed"
            );
            (StatusCode::BAD_GATEWAY, "Upstream service unavailable").into_response()
        }
    }
}

/// Forward the request to the target URL and return the response.
async fn forward_request(
    client: &Client,
    request: Request,
    target_url: &str,
) -> Result<Response, reqwest::Error> {
    let (parts, body) = request.into_parts();

    // Build the outgoing request
    let mut builder = client.request(parts.method, target_url);

    // Copy headers (filtering out hop-by-hop headers)
    for (name, value) in parts.headers.iter() {
        let name_str = name.as_str().to_lowercase();
        if name_str != "host" && name_str != "connection" && name_str != "content-length" {
            builder = builder.header(name.clone(), value.clone());
        }
    }

    // Convert axum body to bytes for reqwest
    let body_bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_default();

    if !body_bytes.is_empty() {
        builder = builder.body(body_bytes);
    }

    // Send request
    let response = builder.send().await?;

    // Convert reqwest response to axum response
    let status = response.status();
    let headers = response.headers().clone();

    // Get the response body
    let body_bytes = response.bytes().await?;

    let mut builder = Response::builder().status(status);
    for (name, value) in headers.iter() {
        let name_str = name.as_str().to_lowercase();
        // Filter out transfer-encoding as we're not chunking
        if name_str != "transfer-encoding" {
            builder = builder.header(name.clone(), value.clone());
        }
    }

    Ok(builder.body(Body::from(body_bytes)).unwrap())
}

/// Create a Redis connection pool.
async fn create_redis_pool(redis_url: &str) -> anyhow::Result<Pool> {
    let config = RedisConfig::from_url(redis_url);
    let pool = config.create_pool(Some(Runtime::Tokio1))?;

    // Verify connection with a ping
    let mut conn = pool.get().await?;
    deadpool_redis::redis::cmd("PING")
        .query_async::<()>(&mut conn)
        .await?;

    Ok(pool)
}

/// Load allowed identities from Redis SET.
async fn load_allowed_identities_from_redis(pool: &Pool) -> anyhow::Result<HashSet<String>> {
    let mut conn = pool.get().await?;
    let members: Vec<String> = conn.smembers(ALLOWED_IDENTITIES_KEY).await?;
    Ok(members.into_iter().collect())
}

/// Background task that periodically refreshes allowed identities from Redis.
async fn refresh_allowed_dids_task(state: Arc<FrontdoorState>, interval_seconds: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_seconds));
    // Skip the first tick (we already loaded at startup)
    interval.tick().await;

    loop {
        interval.tick().await;

        if let Some(ref pool) = state.redis_pool {
            match load_allowed_identities_from_redis(pool).await {
                Ok(new_dids) => {
                    let count = new_dids.len();
                    {
                        let mut allowed = state.allowed_dids.write().await;
                        *allowed = new_dids;
                    }
                    state.metrics.allowed_identities_size.set(count as i64);
                    debug!(count = count, "Refreshed allowed identities from Redis");
                }
                Err(e) => {
                    error!(
                        error = %e,
                        "Failed to refresh allowed identities from Redis, keeping existing set"
                    );
                }
            }
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> FrontdoorConfig {
        FrontdoorConfig::from_env()
    }

    #[test]
    fn test_get_target_host_feed_routes() {
        let config = test_config();

        // GET requests to filtered endpoints should go to feed service
        assert_eq!(
            get_target_host(&config, &Method::GET, "/xrpc/app.bsky.feed.getFeedSkeleton"),
            &config.feed_service_host
        );
        assert_eq!(
            get_target_host(
                &config,
                &Method::GET,
                "/xrpc/app.bsky.feed.describeFeedGenerator"
            ),
            &config.feed_service_host
        );
        assert_eq!(
            get_target_host(
                &config,
                &Method::GET,
                "/xrpc/app.bsky.feed.sendInteractions"
            ),
            &config.feed_service_host
        );
    }

    #[test]
    fn test_get_target_host_labs_routes() {
        let config = test_config();

        // POST requests to filtered endpoints should go to labs service
        assert_eq!(
            get_target_host(
                &config,
                &Method::POST,
                "/xrpc/app.bsky.feed.sendInteractions"
            ),
            &config.labs_service_host
        );

        // Non-filtered feed endpoints should go to labs service
        assert_eq!(
            get_target_host(&config, &Method::GET, "/xrpc/app.bsky.feed.getActorFeeds"),
            &config.labs_service_host
        );
        assert_eq!(
            get_target_host(&config, &Method::GET, "/xrpc/app.bsky.feed.getLikes"),
            &config.labs_service_host
        );

        // Non-feed endpoints should go to labs service
        assert_eq!(
            get_target_host(&config, &Method::GET, "/internal/ready"),
            &config.labs_service_host
        );
        assert_eq!(
            get_target_host(&config, &Method::POST, "/v1/personalize"),
            &config.labs_service_host
        );
        assert_eq!(
            get_target_host(&config, &Method::GET, "/xrpc/app.bsky.actor.getProfile"),
            &config.labs_service_host
        );
    }

    #[test]
    fn test_build_target_url() {
        let uri: Uri = "/xrpc/app.bsky.feed.getFeedSkeleton?feed=test&limit=50"
            .parse()
            .unwrap();
        let url = build_target_url("http://localhost:8080", &uri);
        assert_eq!(
            url,
            "http://localhost:8080/xrpc/app.bsky.feed.getFeedSkeleton?feed=test&limit=50"
        );

        // Handle trailing slash in host
        let url = build_target_url("http://localhost:8080/", &uri);
        assert_eq!(
            url,
            "http://localhost:8080/xrpc/app.bsky.feed.getFeedSkeleton?feed=test&limit=50"
        );
    }

    #[test]
    fn test_build_target_url_simple_path() {
        let uri: Uri = "/internal/ready".parse().unwrap();
        let url = build_target_url("http://localhost:8082", &uri);
        assert_eq!(url, "http://localhost:8082/internal/ready");
    }

    #[test]
    fn test_requires_feed_filtering() {
        assert!(requires_feed_filtering(
            "/xrpc/app.bsky.feed.getFeedSkeleton"
        ));
        assert!(requires_feed_filtering(
            "/xrpc/app.bsky.feed.describeFeedGenerator"
        ));
        assert!(requires_feed_filtering(
            "/xrpc/app.bsky.feed.sendInteractions"
        ));

        // Should not filter other endpoints
        assert!(!requires_feed_filtering(
            "/xrpc/app.bsky.feed.getActorFeeds"
        ));
        assert!(!requires_feed_filtering("/internal/ready"));
        assert!(!requires_feed_filtering("/xrpc/app.bsky.actor.getProfile"));
    }

    #[test]
    fn test_extract_feed_param() {
        let uri: Uri = "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/app.bsky.feed.generator/test&limit=50"
            .parse()
            .unwrap();
        assert_eq!(
            extract_feed_param(&uri),
            Some("at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/app.bsky.feed.generator/test".to_string())
        );

        // URL-encoded feed parameter
        let uri: Uri = "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at%3A%2F%2Fdid%3Aplc%3Aewvi7nxzyoun6zhxrhs64oiz%2Fapp.bsky.feed.generator%2Ftest"
            .parse()
            .unwrap();
        assert_eq!(
            extract_feed_param(&uri),
            Some("at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/app.bsky.feed.generator/test".to_string())
        );

        // No feed parameter
        let uri: Uri = "/xrpc/app.bsky.feed.getFeedSkeleton?limit=50"
            .parse()
            .unwrap();
        assert_eq!(extract_feed_param(&uri), None);

        // No query string
        let uri: Uri = "/xrpc/app.bsky.feed.getFeedSkeleton".parse().unwrap();
        assert_eq!(extract_feed_param(&uri), None);
    }

    #[tokio::test]
    async fn test_is_feed_allowed_with_allowlist() {
        let metrics = FrontdoorMetrics::new(1);
        // Use valid 24-character DID:PLC identifiers
        let allowed_did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz";
        let disallowed_did = "did:plc:abcdefghijklmnopqrstuvwx";

        let mut allowed_set = HashSet::new();
        allowed_set.insert(allowed_did.to_string());
        let allowed_dids = RwLock::new(allowed_set);

        // Allowed DID
        assert!(
            is_feed_allowed(
                &format!("at://{}/app.bsky.feed.generator/test", allowed_did),
                &allowed_dids,
                &metrics
            )
            .await
        );

        // Disallowed DID
        assert!(
            !is_feed_allowed(
                &format!("at://{}/app.bsky.feed.generator/test", disallowed_did),
                &allowed_dids,
                &metrics
            )
            .await
        );
    }

    #[tokio::test]
    async fn test_is_feed_allowed_invalid_uri() {
        let metrics = FrontdoorMetrics::new(1);
        let mut allowed_set = HashSet::new();
        allowed_set.insert("did:plc:ewvi7nxzyoun6zhxrhs64oiz".to_string());
        let allowed_dids = RwLock::new(allowed_set);

        // Invalid AT-URI should be rejected
        assert!(!is_feed_allowed("not-an-aturi", &allowed_dids, &metrics).await);
        assert!(!is_feed_allowed("https://example.com", &allowed_dids, &metrics).await);
    }

    #[test]
    fn test_metrics_initialization() {
        let metrics = FrontdoorMetrics::new(5);
        // Verify the gauge was set correctly
        assert_eq!(metrics.allowed_identities_size.get(), 5);
    }
}
