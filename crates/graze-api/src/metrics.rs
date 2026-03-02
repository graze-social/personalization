//! Prometheus metrics for the Graze service.

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

/// Application metrics using prometheus-client.
pub struct Metrics {
    pub registry: Registry,

    // Request metrics
    pub requests_total: Counter,
    pub request_duration_seconds: Histogram,

    // Personalization metrics
    pub personalization_requests: Counter,
    pub personalization_cache_hits: Counter,
    pub personalization_cache_misses: Counter,
    pub personalization_duration_seconds: Histogram,
    pub personalization_posts_scored: Histogram,

    // Redis metrics
    pub redis_operations: Counter,
    pub redis_errors: Counter,
    pub redis_latency_seconds: Histogram,

    // Worker metrics
    pub likes_processed: Counter,
    pub likes_batch_size: Histogram,
    pub sync_operations: Counter,
    pub sync_duration_seconds: Histogram,
    pub coliker_computations: Counter,
    pub coliker_duration_seconds: Histogram,

    // Jetstream metrics
    pub jetstream_connections_total: Counter,
    pub jetstream_disconnections_total: Counter,
    pub jetstream_messages_received: Counter,
    pub jetstream_parse_errors: Counter,
    pub jetstream_consecutive_failures: Gauge,

    // Active connections
    pub active_connections: Gauge,
    pub background_tasks: Gauge,
}

impl Metrics {
    /// Create a new metrics instance with all metrics registered.
    pub fn new() -> Self {
        let mut registry = Registry::default();

        // Request metrics
        let requests_total = Counter::default();
        registry.register(
            "graze_requests_total",
            "Total number of HTTP requests",
            requests_total.clone(),
        );

        let request_duration_seconds = Histogram::new(exponential_buckets(0.001, 2.0, 15));
        registry.register(
            "graze_request_duration_seconds",
            "HTTP request duration in seconds",
            request_duration_seconds.clone(),
        );

        // Personalization metrics
        let personalization_requests = Counter::default();
        registry.register(
            "graze_personalization_requests_total",
            "Total personalization requests",
            personalization_requests.clone(),
        );

        let personalization_cache_hits = Counter::default();
        registry.register(
            "graze_personalization_cache_hits_total",
            "Personalization cache hits",
            personalization_cache_hits.clone(),
        );

        let personalization_cache_misses = Counter::default();
        registry.register(
            "graze_personalization_cache_misses_total",
            "Personalization cache misses",
            personalization_cache_misses.clone(),
        );

        let personalization_duration_seconds = Histogram::new(exponential_buckets(0.001, 2.0, 15));
        registry.register(
            "graze_personalization_duration_seconds",
            "Personalization computation duration",
            personalization_duration_seconds.clone(),
        );

        let personalization_posts_scored = Histogram::new(exponential_buckets(1.0, 2.0, 15));
        registry.register(
            "graze_personalization_posts_scored",
            "Number of posts scored per request",
            personalization_posts_scored.clone(),
        );

        // Redis metrics
        let redis_operations = Counter::default();
        registry.register(
            "graze_redis_operations_total",
            "Total Redis operations",
            redis_operations.clone(),
        );

        let redis_errors = Counter::default();
        registry.register(
            "graze_redis_errors_total",
            "Total Redis errors",
            redis_errors.clone(),
        );

        let redis_latency_seconds = Histogram::new(exponential_buckets(0.0001, 2.0, 15));
        registry.register(
            "graze_redis_latency_seconds",
            "Redis operation latency",
            redis_latency_seconds.clone(),
        );

        // Worker metrics
        let likes_processed = Counter::default();
        registry.register(
            "graze_likes_processed_total",
            "Total likes processed from Jetstream",
            likes_processed.clone(),
        );

        let likes_batch_size = Histogram::new(exponential_buckets(1.0, 2.0, 15));
        registry.register(
            "graze_likes_batch_size",
            "Size of like batches",
            likes_batch_size.clone(),
        );

        let sync_operations = Counter::default();
        registry.register(
            "graze_sync_operations_total",
            "Total sync operations",
            sync_operations.clone(),
        );

        let sync_duration_seconds = Histogram::new(exponential_buckets(0.1, 2.0, 12));
        registry.register(
            "graze_sync_duration_seconds",
            "Sync operation duration",
            sync_duration_seconds.clone(),
        );

        let coliker_computations = Counter::default();
        registry.register(
            "graze_coliker_computations_total",
            "Total co-liker computations",
            coliker_computations.clone(),
        );

        let coliker_duration_seconds = Histogram::new(exponential_buckets(0.001, 2.0, 15));
        registry.register(
            "graze_coliker_duration_seconds",
            "Co-liker computation duration",
            coliker_duration_seconds.clone(),
        );

        // Jetstream metrics
        let jetstream_connections_total = Counter::default();
        registry.register(
            "graze_jetstream_connections_total",
            "Total Jetstream connection attempts",
            jetstream_connections_total.clone(),
        );

        let jetstream_disconnections_total = Counter::default();
        registry.register(
            "graze_jetstream_disconnections_total",
            "Total Jetstream disconnections",
            jetstream_disconnections_total.clone(),
        );

        let jetstream_messages_received = Counter::default();
        registry.register(
            "graze_jetstream_messages_received_total",
            "Total messages received from Jetstream",
            jetstream_messages_received.clone(),
        );

        let jetstream_parse_errors = Counter::default();
        registry.register(
            "graze_jetstream_parse_errors_total",
            "Total Jetstream message parse errors",
            jetstream_parse_errors.clone(),
        );

        let jetstream_consecutive_failures: Gauge = Gauge::default();
        registry.register(
            "graze_jetstream_consecutive_failures",
            "Current count of consecutive Jetstream connection failures",
            jetstream_consecutive_failures.clone(),
        );

        // Gauges
        let active_connections: Gauge = Gauge::default();
        registry.register(
            "graze_active_connections",
            "Number of active connections",
            active_connections.clone(),
        );

        let background_tasks: Gauge = Gauge::default();
        registry.register(
            "graze_background_tasks",
            "Number of background tasks running",
            background_tasks.clone(),
        );

        Self {
            registry,
            requests_total,
            request_duration_seconds,
            personalization_requests,
            personalization_cache_hits,
            personalization_cache_misses,
            personalization_duration_seconds,
            personalization_posts_scored,
            redis_operations,
            redis_errors,
            redis_latency_seconds,
            likes_processed,
            likes_batch_size,
            sync_operations,
            sync_duration_seconds,
            coliker_computations,
            coliker_duration_seconds,
            jetstream_connections_total,
            jetstream_disconnections_total,
            jetstream_messages_received,
            jetstream_parse_errors,
            jetstream_consecutive_failures,
            active_connections,
            background_tasks,
        }
    }

    /// Encode all metrics as Prometheus text format.
    pub fn encode(&self) -> String {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry).unwrap();
        buffer
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl graze_common::MetricsEncodable for Metrics {
    fn encode(&self) -> String {
        self.encode()
    }
}
