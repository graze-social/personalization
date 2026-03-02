//! Graze Common - Shared infrastructure for Graze personalization service.
//!
//! This crate provides common utilities used across all Graze applications:
//! - Redis client and key patterns
//! - Error types
//! - Service utilities (URI interner, special posts client)
//! - Shared models

pub mod clickhouse;
pub mod error;
pub mod metrics_server;
pub mod models;
pub mod redis;
pub mod services;

pub use error::{GrazeError, Result};
pub use redis::{
    // New date-based functions
    date_from_timestamp,
    // Legacy (deprecated) - keep for migration
    day_offset_from_timestamp,
    // Core exports
    hash_did,
    hash_uri,
    retention_dates,
    today_date,
    ttl_for_date,
    ttl_for_day,
    Keys,
    RedisClient,
    RedisConfig,
    ScriptManager,
    DAY_TRANCHES,
    DEFAULT_RETENTION_DAYS,
};
pub use clickhouse::{
    AdminOnlyCandidateSource, CandidateQueryParams, CandidateSource, ClickHouseCandidateSource,
    ClickHouseConfig, ClickHouseInteractionWriter, HttpCandidateSource, InteractionWriter,
    NoOpInteractionWriter,
};
pub use services::{
    InteractionsClient, InteractionsConfig, SpecialPost, SpecialPostsClient, SpecialPostsResponse,
    SpecialPostsSource, SponsoredPost, UriInterner,
};

pub use metrics_server::{
    internal_probe, maybe_run_metrics_server, run_metrics_server, MetricsEncodable,
};
