//! Graze Common - Shared infrastructure for Graze personalization service.
//!
//! This crate provides common utilities used across all Graze applications:
//! - Redis client and key patterns
//! - Error types
//! - Service utilities (URI interner, special posts client)
//! - Shared models

pub mod clickhouse;
pub mod coliker_profile;
pub mod error;
pub mod exclusion;
pub mod metrics_server;
pub mod models;
pub mod post_id;
pub mod redis;
pub mod services;

pub use clickhouse::{
    AdminOnlyCandidateSource, CandidateQueryParams, CandidateSource, ClickHouseCandidateSource,
    ClickHouseConfig, ClickHouseInteractionWriter, HttpCandidateSource, InteractionWriter,
    NoOpInteractionWriter,
};
pub use coliker_profile::{
    decode_profile, encode_profile, encode_profile_from_dids, profile_len, PROFILE_ENTRY_BYTES,
};
pub use error::{GrazeError, Result};
pub use exclusion::{
    author_did_from_at_uri, exclusion_set_from_env_opt, is_excluded_did, is_excluded_post_uri,
    parse_exclusion_list, should_log_interaction, should_process_like_event,
};
pub use post_id::{
    format_post_id, intern_date_from_post_id, is_dated, is_legacy_numeric, DATED_ID_LEN,
};
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
pub use services::{
    InteractionsClient, InteractionsConfig, SpecialPost, SpecialPostsClient, SpecialPostsResponse,
    SpecialPostsSource, SponsoredPost, UriInterner,
};

pub use metrics_server::{
    internal_probe, maybe_run_metrics_server, run_metrics_server, MetricsEncodable,
};
