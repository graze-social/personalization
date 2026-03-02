//! Graze Like Streamer - Jetstream consumer and like graph processor.
//!
//! This crate provides workers for processing like events from Jetstream
//! and maintaining the like graph in Redis.

pub mod bot_filter;
pub mod cleanup;
pub mod config;
pub mod metrics;
pub mod streamer;

pub use bot_filter::{BotFilter, BotFilterStats, BotRecord};
pub use cleanup::{CleanupStats, CleanupWorker};
pub use config::Config;
pub use metrics::StreamerMetrics;
pub use streamer::LikeStreamer;
