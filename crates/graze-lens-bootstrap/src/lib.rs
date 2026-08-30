//! Backfills `follow_edges` for a set of accounts.
//!
//! `graze-lens-fold` only sees follows from the moment it connects. This job
//! fills in history, by reading each account's follow records straight from its
//! PDS and writing them as `create` rows versioned by the record's own TID.
//!
//! # Why this rather than the Jetstream v2 archive
//!
//! The design brief calls for bootstrapping from Jetstream v2's Network Replay:
//! one filtered bulk download of the whole network's follow history. That is
//! still the right move for warming the *global* graph, and it remains blocked
//! on an archive API token plus a decoder for the `.jss` columnar segment format
//! (256-byte header, length-prefixed zstd blocks, raw CBOR payloads) — which is
//! not something to write blind against a format we cannot yet fetch and verify.
//!
//! This job needs neither. It is per-account, uses only public unauthenticated
//! endpoints, and covers exactly the accounts that actually asked for a lens —
//! which is all M0 requires, and matches the lazy-build shape of the rest of the
//! system. When the archive path lands it supersedes this for bulk warming;
//! this remains the repair path for an account whose history predates whatever
//! the archive holds.
//!
//! Idempotent: rows are versioned by TID, so re-running writes the same
//! (follower, rkey, seq) tuples and ReplacingMergeTree collapses them.

pub mod backfill;
pub mod completeness;
pub mod config;
pub mod resolve;

pub use backfill::{Backfilled, Backfiller};
pub use completeness::{CompletenessStore, SOURCE_BOOTSTRAP, SOURCE_PDS};
pub use config::Config;
pub use resolve::Resolver;
