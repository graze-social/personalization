//! Prometheus metrics for the Feed Stats worker.
//!
//! Mirrors the `telemetry.record_gauge(...)` call sites in feed_stats_runner.py
//! as counters, plus worker-health counters. Namespaced `graze_feed_stats_*`.

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::registry::Registry;

pub struct FeedStatsMetrics {
    registry: Registry,

    /// Rows inserted into post_render_log.
    pub post_render_log_inserts: Counter,
    /// Rows inserted into sponsored_feed_impressions.
    pub sponsored_feed_impressions_inserts: Counter,
    /// log_tasks batches processed.
    pub batches_processed: Counter,
    /// Individual log lines processed (successful expansions).
    pub logs_processed: Counter,
    /// Log lines skipped due to a parse error (Python per-log try/except).
    pub log_parse_errors: Counter,
    /// feed_requests flushed to last_delivered_time.
    pub feed_requests_flushed: Counter,
    /// ClickHouse insert errors.
    pub clickhouse_errors: Counter,
    /// Postgres errors.
    pub pg_errors: Counter,
    /// Redis errors.
    pub redis_errors: Counter,
}

impl FeedStatsMetrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        macro_rules! reg {
            ($name:expr, $help:expr) => {{
                let c = Counter::default();
                registry.register($name, $help, c.clone());
                c
            }};
        }

        let post_render_log_inserts = reg!(
            "graze_feed_stats_post_render_log_inserts_total",
            "Rows inserted into post_render_log"
        );
        let sponsored_feed_impressions_inserts = reg!(
            "graze_feed_stats_sponsored_feed_impressions_inserts_total",
            "Rows inserted into sponsored_feed_impressions"
        );
        let batches_processed = reg!(
            "graze_feed_stats_batches_processed_total",
            "log_tasks batches processed"
        );
        let logs_processed = reg!(
            "graze_feed_stats_logs_processed_total",
            "Log lines processed"
        );
        let log_parse_errors = reg!(
            "graze_feed_stats_log_parse_errors_total",
            "Log lines skipped on parse error"
        );
        let feed_requests_flushed = reg!(
            "graze_feed_stats_feed_requests_flushed_total",
            "feed_requests flushed to last_delivered_time"
        );
        let clickhouse_errors = reg!(
            "graze_feed_stats_clickhouse_errors_total",
            "ClickHouse insert errors"
        );
        let pg_errors = reg!("graze_feed_stats_pg_errors_total", "Postgres errors");
        let redis_errors = reg!("graze_feed_stats_redis_errors_total", "Redis errors");

        Self {
            registry,
            post_render_log_inserts,
            sponsored_feed_impressions_inserts,
            batches_processed,
            logs_processed,
            log_parse_errors,
            feed_requests_flushed,
            clickhouse_errors,
            pg_errors,
            redis_errors,
        }
    }

    pub fn encode(&self) -> String {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry).unwrap();
        buffer
    }
}

impl Default for FeedStatsMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl graze_common::MetricsEncodable for FeedStatsMetrics {
    fn encode(&self) -> String {
        self.encode()
    }
}
