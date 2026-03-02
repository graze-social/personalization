//! Prometheus metrics for the Like Streamer service.

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

/// Prometheus metrics for the Like Streamer.
pub struct StreamerMetrics {
    registry: Registry,

    /// Total likes processed from Jetstream.
    pub likes_processed: Counter,
    /// Size of like batches written to Redis.
    pub likes_batch_size: Histogram,
    /// Total Jetstream connection attempts.
    pub jetstream_connections: Counter,
    /// Total Jetstream disconnections.
    pub jetstream_disconnections: Counter,
    /// Total messages received from Jetstream.
    pub jetstream_messages: Counter,
    /// Total Jetstream message parse errors.
    pub jetstream_parse_errors: Counter,
    /// Current count of consecutive Jetstream connection failures.
    pub jetstream_consecutive_failures: Gauge,
    /// Total bots detected.
    pub bot_detections: Counter,
    /// Total cleanup worker runs.
    pub cleanup_runs: Counter,
    /// Duration of cleanup runs in seconds.
    pub cleanup_duration_seconds: Histogram,
}

impl StreamerMetrics {
    /// Create a new metrics instance with all metrics registered.
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let likes_processed = Counter::default();
        registry.register(
            "graze_streamer_likes_processed_total",
            "Total likes processed from Jetstream",
            likes_processed.clone(),
        );

        let likes_batch_size = Histogram::new(exponential_buckets(1.0, 2.0, 15));
        registry.register(
            "graze_streamer_likes_batch_size",
            "Size of like batches written to Redis",
            likes_batch_size.clone(),
        );

        let jetstream_connections = Counter::default();
        registry.register(
            "graze_streamer_jetstream_connections_total",
            "Total Jetstream connection attempts",
            jetstream_connections.clone(),
        );

        let jetstream_disconnections = Counter::default();
        registry.register(
            "graze_streamer_jetstream_disconnections_total",
            "Total Jetstream disconnections",
            jetstream_disconnections.clone(),
        );

        let jetstream_messages = Counter::default();
        registry.register(
            "graze_streamer_jetstream_messages_total",
            "Total messages received from Jetstream",
            jetstream_messages.clone(),
        );

        let jetstream_parse_errors = Counter::default();
        registry.register(
            "graze_streamer_jetstream_parse_errors_total",
            "Total Jetstream message parse errors",
            jetstream_parse_errors.clone(),
        );

        let jetstream_consecutive_failures: Gauge = Gauge::default();
        registry.register(
            "graze_streamer_jetstream_consecutive_failures",
            "Current count of consecutive Jetstream connection failures",
            jetstream_consecutive_failures.clone(),
        );

        let bot_detections = Counter::default();
        registry.register(
            "graze_streamer_bot_detections_total",
            "Total bots detected",
            bot_detections.clone(),
        );

        let cleanup_runs = Counter::default();
        registry.register(
            "graze_streamer_cleanup_runs_total",
            "Total cleanup worker runs",
            cleanup_runs.clone(),
        );

        let cleanup_duration_seconds = Histogram::new(exponential_buckets(0.1, 2.0, 12));
        registry.register(
            "graze_streamer_cleanup_duration_seconds",
            "Duration of cleanup runs in seconds",
            cleanup_duration_seconds.clone(),
        );

        Self {
            registry,
            likes_processed,
            likes_batch_size,
            jetstream_connections,
            jetstream_disconnections,
            jetstream_messages,
            jetstream_parse_errors,
            jetstream_consecutive_failures,
            bot_detections,
            cleanup_runs,
            cleanup_duration_seconds,
        }
    }

    /// Encode all metrics as Prometheus text format.
    pub fn encode(&self) -> String {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry).unwrap();
        buffer
    }
}

impl Default for StreamerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl graze_common::MetricsEncodable for StreamerMetrics {
    fn encode(&self) -> String {
        self.encode()
    }
}
