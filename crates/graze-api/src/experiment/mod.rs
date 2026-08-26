//! Experiment harnesses for comparing ranking approaches.
//!
//! Measurement, not ranking, is the binding constraint on this system: within-user clustering
//! inflates variance 6.2×, which at current traffic means ~48 days to detect a 20% effect. These
//! harnesses exist to shrink that.

pub mod interleave;

pub use interleave::{control_drafts_first, team_draft, Draft, DraftedItem, Ranker};
