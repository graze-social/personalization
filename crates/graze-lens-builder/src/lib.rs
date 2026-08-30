//! Builds per-viewer author sets ("lenses") from the follow graph.
//!
//! feeder-rs enqueues a build the first time it wants a lens it does not have;
//! this worker folds `follow_edges` for that viewer and publishes the resulting
//! DID set to Redis. Nothing here is on the serve path — the worker can be down
//! for an hour and feeds keep serving, unlensed.

pub mod builder;
pub mod config;
pub mod interner;
pub mod queue;
pub mod scored;
pub mod second_degree;

pub use builder::{BuildOutcome, Builder};
// Re-exported from graze-lens-bootstrap, where the backfill it records lives.
pub use config::Config;
pub use graze_lens_bootstrap::CompletenessStore;
pub use interner::Interner;
pub use queue::{BuildRequest, Delivery, Queue};
