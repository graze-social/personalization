//! Builds per-viewer author sets ("lenses") from the follow graph.
//!
//! feeder-rs enqueues a build the first time it wants a lens it does not have;
//! this worker folds `follow_edges` for that viewer and publishes the resulting
//! DID set to Redis. Nothing here is on the serve path — the worker can be down
//! for an hour and feeds keep serving, unlensed.

pub mod builder;
pub mod completeness;
pub mod config;
pub mod queue;

pub use builder::{BuildOutcome, Builder};
pub use completeness::CompletenessStore;
pub use config::Config;
pub use queue::{BuildRequest, Delivery, Queue};
