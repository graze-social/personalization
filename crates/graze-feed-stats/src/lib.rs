//! Graze Feed Stats — Rust drop-in replacement for `feed_stats_runner.py`.
//!
//! Consumes feed-serve telemetry from Redis (`log_tasks`, `feed_requests`) and
//! fans it out to three sinks, preserving the Python worker's contract exactly:
//!   * ClickHouse analytics: `post_render_log`, `sponsored_feed_impressions`
//!   * Redis ad-spend counters (consumed by clickhouse_materializer.py)
//!   * Postgres billing: sticky-post credit decrement, `CreditUsage`,
//!     `Algorithm.settings.last_delivered_time`
//!
//! Shadow mode ([`config::ShadowConfig`]) redirects every sink to an isolated
//! target so it can run against mirrored production traffic with zero risk.

pub mod clickhouse_sink;
pub mod config;
pub mod feed_requests;
pub mod log_worker;
pub mod metrics;
pub mod parse;
pub mod pg;
pub mod redis_counters;

pub use config::{Config, ShadowConfig};
pub use feed_requests::FeedRequestsWorker;
pub use log_worker::LogWorker;
pub use metrics::FeedStatsMetrics;
