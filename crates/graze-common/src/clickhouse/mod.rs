//! Single ClickHouse module for reads and writes.
//!
//! - **Writer**: HTTP INSERT into `feed_interactions_buffer` and `user_action_logs_buffer`.
//!   Use when `INTERACTIONS_WRITER=clickhouse`.
//! - **Reader**: HTTP SELECT from `algorithm_posts_v2` for candidate URIs.
//!   Use when `CANDIDATE_SOURCE=clickhouse`.
//!
//! Pluggable backends (see `traits`) allow using other implementations (e.g. no-op writer,
//! HTTP candidate source) without ClickHouse.

pub mod admin_only_source;
pub mod config;
pub mod http_source;
pub mod reader;
pub mod traits;
pub mod writer;

pub use admin_only_source::AdminOnlyCandidateSource;
pub use config::ClickHouseConfig;
pub use http_source::HttpCandidateSource;
pub use reader::{CandidateQueryParams, ClickHouseCandidateSource};
pub use traits::{CandidateSource, InteractionWriter};
pub use writer::{ClickHouseInteractionWriter, NoOpInteractionWriter};
