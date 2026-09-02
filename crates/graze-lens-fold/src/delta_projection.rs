//! Keeping the traversal projection current between nightly rebuilds.
//!
//! `follow_graph_int` is what every second-degree facet actually reads, and it is
//! rebuilt once a night. The cadence is not arbitrary: the bulk rebuild needs
//! `follow_edges FINAL` over 2.79 billion rows and a `grace_hash` join against
//! the 42M-row id map, and the default hash join blew ClickHouse's 21.6 GiB
//! per-query ceiling. See `project.rs` for the full account.
//!
//! But that is an argument about the *bulk* job, not about incremental work. A
//! delta never needs `FINAL` over the whole table — only over the window since
//! the last rebuild, which is roughly 19M edges a day against a 2.78B base.
//!
//! # The read cost is nil, and that is measured
//!
//! The worry with a second table is the hot-path query: the projection's speed
//! rests on u32 keys, `index_granularity = 1024`, no partitioning, and literal
//! seed lists. Adding a `UNION` could have undone it. Measured on production
//! against a real viewer's 1,187 seeds, three runs each:
//!
//! | read | timings |
//! |---|---|
//! | `follow_graph_int` alone | 0.70 / 0.57 / 0.56 s |
//! | the same, `UNION ALL` a 131M-row second table | 0.60 / 0.55 / 0.47 s |
//!
//! Identical within noise, because a seed-bounded `follower_int IN (<literals>)`
//! uses the sparse index on both tables — the delta costs a few granule seeks,
//! not a scan. So the delta mirrors the base's properties exactly: same
//! `ORDER BY`, same granularity, unpartitioned.
//!
//! # Look up ids; never allocate them
//!
//! Ids come from `lookup_many`, which allocates nothing. Interning every account
//! that flies past would add ~19M entries a day to a hash already holding ~42M,
//! and it would also be *wrong*: the base projection joins against the id map
//! with INNER joins, so an account without an id is already absent there. A delta
//! that invented ids would claim edges the base cannot express. Unknown accounts
//! wait for the nightly job's bounded interning, exactly as before.
//!
//! # Removals, and the lookup they need
//!
//! A jetstream follow-delete carries `(repo, rkey)` and no followee, so a
//! retraction cannot be written from the event alone — the base rebuild
//! sidesteps this by letting `FINAL` collapse the create. Here each batch's
//! deletes are resolved to their subject in one point lookup and written as
//! `op = 'delete'` tombstones, which the reach queries exclude.
//!
//! Two sources, cheapest first. A follow that was undone inside the same batch
//! is resolved from the batch itself, because its create row may not have
//! reached the base table yet — that is the one case ClickHouse cannot answer.
//! Everything else is one query per flush, roughly four deletes a second at
//! measured rates.
//!
//! An unresolvable delete leaves the addition standing until the nightly
//! compaction. That is the honest failure: the rkey names a follow this
//! projection never carried, or one whose create predates the delta entirely, so
//! there is nothing to retract.

use std::collections::HashMap;

use graze_common::lens_interner::Interner;
use tracing::{debug, warn};

use crate::event::FollowEdge;
use crate::metrics::Metrics;
use crate::sink::Sink;

/// The table the facets `UNION` in. Mirrors `follow_graph_int`'s properties.
pub const DELTA_TABLE: &str = "follow_graph_int_delta";

pub struct DeltaProjector {
    interner: Interner,
    sink: Sink,
    metrics: Metrics,
}

impl DeltaProjector {
    pub fn new(interner: Interner, sink: Sink, metrics: Metrics) -> Self {
        Self {
            interner,
            sink,
            metrics,
        }
    }

    /// Project one flushed batch of edges into the delta.
    ///
    /// Runs on the batch the sink already assembled rather than per event: a row
    /// per follow would be the tiny-insert storm that drives this instance's
    /// cost, and one id lookup per event would be 27 Redis round trips a second
    /// for no reason.
    pub async fn project(&self, batch: &[FollowEdge]) {
        let creates: Vec<&FollowEdge> = batch.iter().filter(|e| e.op == "create").collect();
        let deletes: Vec<&FollowEdge> = batch.iter().filter(|e| e.op == "delete").collect();
        if creates.is_empty() && deletes.is_empty() {
            return;
        }

        // Resolve removals first, so their subjects join the same id lookup.
        let retractions = self.resolve_retractions(batch, &deletes).await;

        // One lookup for the whole batch, both sides of every edge at once.
        let mut dids: Vec<String> = Vec::with_capacity(creates.len() * 2 + retractions.len() * 2);
        for edge in &creates {
            dids.push(edge.follower.clone());
            dids.push(edge.followee.clone());
        }
        for (follower, followee, _) in &retractions {
            dids.push(follower.clone());
            dids.push(followee.clone());
        }
        dids.sort_unstable();
        dids.dedup();

        let ids: HashMap<String, u32> = match self.interner.lookup_many(&dids).await {
            Ok(ids) => ids,
            Err(e) => {
                warn!(error = %e, "delta projection could not resolve ids");
                self.metrics.delta_projection_failures.inc();
                return;
            }
        };

        let mut rows: Vec<(u32, u32, &'static str, u64)> =
            Vec::with_capacity(creates.len() + retractions.len());
        let mut skipped = 0u64;
        for edge in &creates {
            match (ids.get(&edge.follower), ids.get(&edge.followee)) {
                (Some(&follower), Some(&followee)) => {
                    rows.push((follower, followee, "create", edge.seq))
                }
                // One side has no id yet, so the base projection does not contain
                // this edge either. Waiting for the nightly pass keeps the two
                // tables describing the same population.
                _ => skipped += 1,
            }
        }
        let mut tombstones = 0u64;
        for (follower, followee, seq) in &retractions {
            match (ids.get(follower), ids.get(followee)) {
                (Some(&follower), Some(&followee)) => {
                    rows.push((follower, followee, "delete", *seq));
                    tombstones += 1;
                }
                _ => skipped += 1,
            }
        }
        self.metrics.delta_rows_skipped.inc_by(skipped);
        self.metrics.delta_tombstones.inc_by(tombstones);

        if rows.is_empty() {
            return;
        }
        let projected = rows.len() as u64;
        match self.sink.insert_delta(&rows).await {
            Ok(()) => {
                self.metrics.delta_rows_projected.inc_by(projected);
                debug!(rows = projected, skipped, "projected edges into the delta");
            }
            Err(e) => {
                warn!(error = %e, rows = projected, "delta insert failed");
                self.metrics.delta_projection_failures.inc();
            }
        }
    }
    /// Subjects for this batch's deletes, as `(follower, followee, seq)`.
    ///
    /// The in-batch pass matters more than it looks: a follow undone within the
    /// same flush has a create row that may not be in the base table yet, so
    /// ClickHouse would answer "no such record" and the addition would stand
    /// until the nightly compaction.
    async fn resolve_retractions(
        &self,
        batch: &[FollowEdge],
        deletes: &[&FollowEdge],
    ) -> Vec<(String, String, u64)> {
        if deletes.is_empty() {
            return Vec::new();
        }

        let in_batch: HashMap<(&str, &str), &str> = batch
            .iter()
            .filter(|e| e.op == "create" && !e.followee.is_empty())
            .map(|e| ((e.follower.as_str(), e.rkey.as_str()), e.followee.as_str()))
            .collect();

        let mut out: Vec<(String, String, u64)> = Vec::with_capacity(deletes.len());
        let mut ask: Vec<(String, String)> = Vec::new();
        let mut seq_of: HashMap<(String, String), u64> = HashMap::new();
        for edge in deletes {
            let key = (edge.follower.as_str(), edge.rkey.as_str());
            match in_batch.get(&key) {
                Some(followee) => {
                    out.push((edge.follower.clone(), (*followee).to_string(), edge.seq))
                }
                None => {
                    ask.push((edge.follower.clone(), edge.rkey.clone()));
                    seq_of.insert((edge.follower.clone(), edge.rkey.clone()), edge.seq);
                }
            }
        }

        if !ask.is_empty() {
            match self.sink.resolve_followees(&ask).await {
                Ok(found) => {
                    for (follower, rkey, followee) in found {
                        if let Some(&seq) = seq_of.get(&(follower.clone(), rkey)) {
                            out.push((follower, followee, seq));
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, deletes = ask.len(), "could not resolve unfollows");
                    self.metrics.delta_resolve_failures.inc();
                }
            }
        }

        let unresolved = deletes.len().saturating_sub(out.len()) as u64;
        self.metrics.delta_unresolved_deletes.inc_by(unresolved);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The delta must name the table the facet queries read. A typo here is a
    /// silent no-op: rows land somewhere nothing unions in, and freshness looks
    /// implemented while nothing changes.
    #[test]
    fn delta_table_name_is_stable() {
        assert_eq!(DELTA_TABLE, "follow_graph_int_delta");
    }
}
