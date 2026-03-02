//! Prometheus metrics for the Candidate Sync service.

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

/// Prometheus metrics for the Candidate Sync worker.
pub struct SyncMetrics {
    registry: Registry,

    /// Total sync runs completed.
    pub sync_runs: Counter,
    /// Duration of sync runs in seconds.
    pub sync_duration_seconds: Histogram,
    /// Total posts synced to Redis.
    pub posts_synced: Counter,
    /// Total ClickHouse queries executed.
    pub clickhouse_queries: Counter,
    /// Total ClickHouse query errors.
    pub clickhouse_errors: Counter,
    /// Total tranche updates (trending, popular, velocity, discovery).
    pub tranche_updates: Counter,
    /// Total Redis write errors during sync.
    pub redis_errors: Counter,
}

impl SyncMetrics {
    /// Create a new metrics instance with all metrics registered.
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let sync_runs = Counter::default();
        registry.register(
            "graze_sync_runs_total",
            "Total sync runs completed",
            sync_runs.clone(),
        );

        let sync_duration_seconds = Histogram::new(exponential_buckets(0.1, 2.0, 12));
        registry.register(
            "graze_sync_duration_seconds",
            "Duration of sync runs in seconds",
            sync_duration_seconds.clone(),
        );

        let posts_synced = Counter::default();
        registry.register(
            "graze_sync_posts_synced_total",
            "Total posts synced to Redis",
            posts_synced.clone(),
        );

        let clickhouse_queries = Counter::default();
        registry.register(
            "graze_sync_clickhouse_queries_total",
            "Total ClickHouse queries executed",
            clickhouse_queries.clone(),
        );

        let clickhouse_errors = Counter::default();
        registry.register(
            "graze_sync_clickhouse_errors_total",
            "Total ClickHouse query errors",
            clickhouse_errors.clone(),
        );

        let tranche_updates = Counter::default();
        registry.register(
            "graze_sync_tranche_updates_total",
            "Total tranche updates (trending, popular, velocity, discovery)",
            tranche_updates.clone(),
        );

        let redis_errors = Counter::default();
        registry.register(
            "graze_sync_redis_errors_total",
            "Total Redis write errors during sync",
            redis_errors.clone(),
        );

        Self {
            registry,
            sync_runs,
            sync_duration_seconds,
            posts_synced,
            clickhouse_queries,
            clickhouse_errors,
            tranche_updates,
            redis_errors,
        }
    }

    /// Encode all metrics as Prometheus text format.
    pub fn encode(&self) -> String {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry).unwrap();
        buffer
    }
}

impl Default for SyncMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl graze_common::MetricsEncodable for SyncMetrics {
    fn encode(&self) -> String {
        self.encode()
    }
}
