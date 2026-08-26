//! Team-draft interleaving for ranker comparison.
//!
//! # Why interleaving
//!
//! An A/B test splits users between two rankers, so the estimate carries between-user variance.
//! Measured here: within-user clustering inflates variance **6.2×** (naive pooled z=3.59 vs
//! cluster-correct permutation z=1.44), which at ~3,600 usable impressions/day means **48 days to
//! detect a 20% effect** — about 4–8 well-powered experiments per year.
//!
//! Interleaving blends both rankers into a single response, so each user is their own control and
//! the between-user variance disappears. Netflix reports up to **100×** sensitivity; Airbnb reached
//! the same conclusion as the corresponding A/B using **0.5% of the running time and 4% of the
//! traffic**, agreeing directionally 82% of the time.
//!
//! # Design: team draft with competitive pairs
//!
//! Following Airbnb: a single coin flip, derived from the user's DID so it is stable across
//! requests and pages, decides which ranker drafts first. Then we repeatedly take the next
//! not-yet-placed item from each ranker:
//!
//! - if the two items **differ**, they form a **competitive pair** and both are added, each tagged
//!   with the ranker that contributed it;
//! - if they are the **same** item, it is added once and left **untagged** — neither ranker earns
//!   credit for something both would have shown anyway.
//!
//! The per-user statistic is `τᵢ = wins(treatment) − wins(control)` over that user's competitive
//! pairs, aggregated as a proportion test across users.
//!
//! # Known limitations (respect these)
//!
//! Interleaving is **not valid** for set-level objectives such as diversity, for results reused by
//! other surfaces, or for continuous metrics. Anything that changes diversity behaviour still needs
//! a full A/B. Airbnb also observed a treatment looking *worse* under interleaving than in the
//! following A/B, caused by comparative advantage when control had a higher base rate.

use std::collections::HashSet;

use graze_common::hash_did;

/// A ranking strategy that can take part in an interleaving experiment.
///
/// Every later ranking approach plugs in here rather than forking the serving path, so each is
/// measured the same way against the same control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ranker {
    /// Current production path: iterate candidates, fetch `pl:{post}` per candidate.
    PostFirst,
    /// Co-liker-first traversal. Measured at 0.94× coverage and 5× latency; kept behind a flag
    /// because it becomes competitive if co-liker sets shrink.
    Inverted,
    /// Seed restricted to the user's likes relevant to *this* feed (LinkLonk facets at Step 1).
    PerFeedSeeded,
    /// Offline author-author similarity.
    ItemItem,
    /// Pixie-style sampled random walk.
    SampledWalk,
    /// Learned two-tower embeddings.
    TwoTower,
}

impl Ranker {
    /// Stable wire name, used in the `feedContext` provenance and in config.
    pub fn as_str(&self) -> &'static str {
        match self {
            Ranker::PostFirst => "post_first",
            Ranker::Inverted => "inverted",
            Ranker::PerFeedSeeded => "per_feed_seeded",
            Ranker::ItemItem => "item_item",
            Ranker::SampledWalk => "sampled_walk",
            Ranker::TwoTower => "two_tower",
        }
    }

    /// Parse from config. Unknown names are **rejected** rather than defaulted, so a typo in an
    /// experiment definition fails loudly instead of quietly measuring the control against itself.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "post_first" => Some(Ranker::PostFirst),
            "inverted" => Some(Ranker::Inverted),
            "per_feed_seeded" => Some(Ranker::PerFeedSeeded),
            "item_item" => Some(Ranker::ItemItem),
            "sampled_walk" => Some(Ranker::SampledWalk),
            "two_tower" => Some(Ranker::TwoTower),
            _ => None,
        }
    }
}

/// One item in an interleaved result.
#[derive(Debug, Clone)]
pub struct DraftedItem {
    pub score: f64,
    pub post_id: String,
    /// Which ranker contributed this item. `None` when both rankers offered it, or when one list
    /// was exhausted and the remainder came from the other — in both cases attribution would be
    /// misleading, so the item is excluded from the preference statistic.
    pub ranker: Option<Ranker>,
}

/// The result of a team draft.
#[derive(Debug, Clone, Default)]
pub struct Draft {
    pub items: Vec<DraftedItem>,
    /// Number of competitive pairs formed — the unit the preference statistic is computed over.
    pub competitive_pairs: usize,
    /// Items both rankers offered, added untagged.
    pub shared_items: usize,
    /// True when the control ranker drafted first for this user.
    pub control_first: bool,
}

/// Decide deterministically, from the user, which ranker drafts first.
///
/// Stable per user so a feed does not reshuffle between requests or pages, and derived from a
/// different hash input than experiment enrolment so the two are independent.
pub fn control_drafts_first(user_did: &str, salt: &str) -> bool {
    let h = hash_did(&format!("{}|draft|{}", salt, user_did));
    u64::from_str_radix(&h, 16).unwrap_or(0).is_multiple_of(2)
}

/// Advance `idx` to the next item not already placed, returning it.
fn next_unused<'a>(
    list: &'a [(f64, String)],
    idx: &mut usize,
    placed: &HashSet<String>,
) -> Option<&'a (f64, String)> {
    while *idx < list.len() {
        if !placed.contains(&list[*idx].1) {
            return Some(&list[*idx]);
        }
        *idx += 1;
    }
    None
}

/// Interleave two ranked lists using team draft with competitive pairs.
///
/// `control` and `treatment` are `(score, post_id)` ranked best-first.
///
/// **The draft order *is* the ranking.** Each item keeps the score its contributing ranker gave it,
/// and those scores are not comparable across rankers, so anything downstream that re-sorts by
/// score destroys the assignment. The caller must therefore run diversity in `preserve_order` mode
/// (`DIVERSITY_PRESERVE_ORDER`).
pub fn team_draft(
    control: &[(f64, String)],
    treatment: &[(f64, String)],
    control_ranker: Ranker,
    treatment_ranker: Ranker,
    control_first: bool,
    limit: usize,
) -> Draft {
    let mut items: Vec<DraftedItem> = Vec::new();
    let mut placed: HashSet<String> = HashSet::new();
    let mut competitive_pairs = 0usize;
    let mut shared_items = 0usize;
    let (mut ci, mut ti) = (0usize, 0usize);

    while items.len() < limit {
        let c = next_unused(control, &mut ci, &placed).cloned();
        let t = next_unused(treatment, &mut ti, &placed).cloned();

        match (c, t) {
            (None, None) => break,

            // One list is exhausted. Keep filling from the other so the feed stays full, but do
            // not tag: an unopposed item is not evidence of preference.
            (Some(item), None) | (None, Some(item)) => {
                placed.insert(item.1.clone());
                items.push(DraftedItem {
                    score: item.0,
                    post_id: item.1,
                    ranker: None,
                });
            }

            (Some(c), Some(t)) if c.1 == t.1 => {
                // Both rankers want the same post — add once, credit neither.
                placed.insert(c.1.clone());
                items.push(DraftedItem {
                    score: c.0,
                    post_id: c.1,
                    ranker: None,
                });
                shared_items += 1;
            }

            (Some(c), Some(t)) => {
                // A competitive pair. The per-user coin flip decides which side leads, so
                // position bias does not systematically favour one ranker.
                competitive_pairs += 1;
                let mut pair = [
                    DraftedItem {
                        score: c.0,
                        post_id: c.1,
                        ranker: Some(control_ranker),
                    },
                    DraftedItem {
                        score: t.0,
                        post_id: t.1,
                        ranker: Some(treatment_ranker),
                    },
                ];
                if !control_first {
                    pair.swap(0, 1);
                }
                for item in pair {
                    if items.len() >= limit {
                        break;
                    }
                    placed.insert(item.post_id.clone());
                    items.push(item);
                }
            }
        }
    }

    Draft {
        items,
        competitive_pairs,
        shared_items,
        control_first,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(ids: &[&str]) -> Vec<(f64, String)> {
        ids.iter()
            .enumerate()
            .map(|(i, id)| (1.0 - i as f64 * 0.01, id.to_string()))
            .collect()
    }

    #[test]
    fn disjoint_lists_alternate_and_both_are_credited() {
        let c = list(&["c1", "c2", "c3"]);
        let t = list(&["t1", "t2", "t3"]);
        let d = team_draft(&c, &t, Ranker::PostFirst, Ranker::SampledWalk, true, 6);
        let ids: Vec<&str> = d.items.iter().map(|i| i.post_id.as_str()).collect();
        assert_eq!(ids, vec!["c1", "t1", "c2", "t2", "c3", "t3"]);
        assert_eq!(d.competitive_pairs, 3);
        assert_eq!(d.shared_items, 0);
        // Each side credited equally — the harness must not bias the count.
        let control_count = d
            .items
            .iter()
            .filter(|i| i.ranker == Some(Ranker::PostFirst))
            .count();
        assert_eq!(control_count, 3);
    }

    #[test]
    fn coin_flip_controls_which_side_leads() {
        let c = list(&["c1"]);
        let t = list(&["t1"]);
        let first = team_draft(&c, &t, Ranker::PostFirst, Ranker::SampledWalk, true, 2);
        let second = team_draft(&c, &t, Ranker::PostFirst, Ranker::SampledWalk, false, 2);
        assert_eq!(first.items[0].post_id, "c1");
        assert_eq!(second.items[0].post_id, "t1");
    }

    /// Items both rankers would show carry no preference information, so neither may be credited.
    /// Getting this wrong inflates whichever ranker happens to draft first.
    #[test]
    fn identical_items_are_added_once_and_untagged() {
        let c = list(&["same1", "c2", "same2"]);
        let t = list(&["same1", "t2", "same2"]);
        let d = team_draft(&c, &t, Ranker::PostFirst, Ranker::SampledWalk, true, 10);
        let ids: Vec<&str> = d.items.iter().map(|i| i.post_id.as_str()).collect();
        assert_eq!(ids.iter().filter(|id| **id == "same1").count(), 1);
        assert_eq!(ids.iter().filter(|id| **id == "same2").count(), 1);
        assert_eq!(d.shared_items, 2);
        assert_eq!(d.competitive_pairs, 1, "only c2/t2 compete");
        for item in &d.items {
            if item.post_id.starts_with("same") {
                assert!(item.ranker.is_none(), "shared item must be untagged");
            }
        }
    }

    /// No post may appear twice in one feed, even if the two rankers order it differently.
    #[test]
    fn no_duplicates_across_the_whole_draft() {
        let c = list(&["a", "b", "c", "d"]);
        let t = list(&["d", "c", "b", "a"]);
        let d = team_draft(&c, &t, Ranker::PostFirst, Ranker::SampledWalk, true, 10);
        let ids: Vec<&str> = d.items.iter().map(|i| i.post_id.as_str()).collect();
        let unique: HashSet<&&str> = ids.iter().collect();
        assert_eq!(ids.len(), unique.len(), "duplicate in draft: {:?}", ids);
    }

    #[test]
    fn respects_the_limit_including_mid_pair() {
        let c = list(&["c1", "c2", "c3"]);
        let t = list(&["t1", "t2", "t3"]);
        for limit in 0..7 {
            let d = team_draft(&c, &t, Ranker::PostFirst, Ranker::SampledWalk, true, limit);
            assert!(d.items.len() <= limit, "limit {} exceeded", limit);
        }
        // An odd limit must be allowed to split a pair rather than overshoot.
        let d = team_draft(&c, &t, Ranker::PostFirst, Ranker::SampledWalk, true, 3);
        assert_eq!(d.items.len(), 3);
    }

    #[test]
    fn exhausted_list_keeps_filling_but_stops_crediting() {
        let c = list(&["c1", "c2", "c3", "c4"]);
        let t = list(&["t1"]);
        let d = team_draft(&c, &t, Ranker::PostFirst, Ranker::SampledWalk, true, 6);
        assert_eq!(d.competitive_pairs, 1);
        // Everything after the single pair is unopposed and therefore untagged.
        let tagged = d.items.iter().filter(|i| i.ranker.is_some()).count();
        assert_eq!(tagged, 2, "only the one pair is credited");
        assert_eq!(d.items.len(), 5);
    }

    #[test]
    fn empty_inputs_are_handled() {
        let empty: Vec<(f64, String)> = vec![];
        let d = team_draft(
            &empty,
            &empty,
            Ranker::PostFirst,
            Ranker::SampledWalk,
            true,
            10,
        );
        assert!(d.items.is_empty());
        assert_eq!(d.competitive_pairs, 0);

        let c = list(&["c1"]);
        let d = team_draft(&c, &empty, Ranker::PostFirst, Ranker::SampledWalk, true, 10);
        assert_eq!(d.items.len(), 1);
        assert!(d.items[0].ranker.is_none());
    }

    /// **The harness's own negative control.** Interleaving a ranker against itself must produce no
    /// competitive pairs at all — every item is shared. If this ever fails, any measured preference
    /// is an artifact of the harness rather than of the rankers.
    #[test]
    fn self_interleaving_yields_no_competitive_pairs() {
        let c = list(&["a", "b", "c", "d", "e"]);
        let d = team_draft(&c, &c, Ranker::PostFirst, Ranker::PostFirst, true, 10);
        assert_eq!(
            d.competitive_pairs, 0,
            "a ranker cannot compete with itself"
        );
        assert_eq!(d.shared_items, 5);
        assert!(d.items.iter().all(|i| i.ranker.is_none()));
        let ids: Vec<&str> = d.items.iter().map(|i| i.post_id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c", "d", "e"], "order preserved");
    }

    #[test]
    fn draft_order_is_stable_per_user_and_split_evenly() {
        let mut firsts = 0;
        for i in 0..2000 {
            let did = format!("did:plc:user{}", i);
            let a = control_drafts_first(&did, "v1");
            // Stability: repeated calls agree.
            assert_eq!(a, control_drafts_first(&did, "v1"));
            if a {
                firsts += 1;
            }
        }
        let frac = firsts as f64 / 2000.0;
        assert!((0.45..0.55).contains(&frac), "coin flip skewed: {}", frac);
    }

    #[test]
    fn ranker_names_roundtrip_and_reject_typos() {
        for r in [
            Ranker::PostFirst,
            Ranker::Inverted,
            Ranker::PerFeedSeeded,
            Ranker::ItemItem,
            Ranker::SampledWalk,
            Ranker::TwoTower,
        ] {
            assert_eq!(Ranker::parse(r.as_str()), Some(r));
        }
        assert_eq!(Ranker::parse("post-first"), None);
        assert_eq!(Ranker::parse(""), None);
        assert_eq!(Ranker::parse("PostFirst"), None);
    }
}
