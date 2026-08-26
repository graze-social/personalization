//! Graze Candidate Sync - ClickHouse to Redis candidate synchronization.
//!
//! This crate provides the CandidateSync worker which synchronizes algorithm post
//! candidates from ClickHouse to Redis, along with fallback tranches for trending,
//! popular, velocity, and discovery posts.

pub mod coliker_profiles;
pub mod config;
pub mod metrics;
pub mod sync;

pub use coliker_profiles::{BuildStats, ProfileBuilder, ProfileBuilderConfig};
pub use config::Config;
pub use metrics::SyncMetrics;
pub use sync::CandidateSync;
