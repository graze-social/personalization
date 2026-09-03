//! The `community` facet must rank the reader's STRONGEST community first.
//!
//! Written while chasing an identical-for-every-reader `community` blob in prod.
//! The affinity math turned out to be correct — this test is what proved it, by
//! running the real parsers over the exact TSVs prod's builder had received and
//! showing the top-20,000 come from the reader's strongest community. The actual
//! fault was upstream: the builder interned its *seeds* against a private
//! 9,499-entry `lensdid` map on the cache Redis instead of the 42M-entry lens
//! space the ClickHouse tables are keyed by, so every seed id named an unrelated
//! account. Keeping this test guards the half that was never broken, so a future
//! change to the weighting cannot quietly invert it.
//!
//! Env-gated like the other live-data tests in this crate: point it at TSVs
//! captured from prod with the builder's own two queries and it reports what
//! `parse_affinity_tsv` + `parse_members_tsv` actually do with them.
//!
//!   COMMUNITY_AFFINITY_TSV=affinity.tsv \
//!   COMMUNITY_MEMBERS_TSV=members.tsv \
//!   COMMUNITY_SEEDS=1187 \
//!   cargo test -p graze-lens-builder --test community_repro -- --nocapture

use std::collections::HashMap;

use graze_lens_builder::priors;

#[test]
fn community_entries_reflect_the_viewers_affinity() {
    let (Ok(aff_path), Ok(mem_path)) = (
        std::env::var("COMMUNITY_AFFINITY_TSV"),
        std::env::var("COMMUNITY_MEMBERS_TSV"),
    ) else {
        eprintln!("skipping: COMMUNITY_AFFINITY_TSV / COMMUNITY_MEMBERS_TSV unset");
        return;
    };
    let seeds: usize = std::env::var("COMMUNITY_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .expect("COMMUNITY_SEEDS required");

    let aff_text = std::fs::read_to_string(&aff_path).expect("read affinity tsv");
    let mem_text = std::fs::read_to_string(&mem_path).expect("read members tsv");

    let affinity = priors::parse_affinity_tsv(&aff_text, seeds);
    println!("\n=== affinity as the code parses it (seeds={seeds}) ===");
    for (c, s) in &affinity {
        println!("  community {c:>10}  share {s:.6}");
    }

    // account -> community, straight from the same TSV the builder consumed.
    let mut community_of: HashMap<u32, u32> = HashMap::new();
    let mut followers_of: HashMap<u32, u64> = HashMap::new();
    for line in mem_text.lines() {
        let mut cols = line.split('\t');
        if let (Some(a), Some(c), Some(f)) = (cols.next(), cols.next(), cols.next()) {
            if let (Ok(a), Ok(c), Ok(f)) = (a.trim().parse(), c.trim().parse(), f.trim().parse()) {
                community_of.insert(a, c);
                followers_of.insert(a, f);
            }
        }
    }
    println!("members rows parsed: {}", community_of.len());

    let top_k = 20_000;
    let map = priors::parse_members_tsv(&mem_text, &affinity, seeds, top_k);
    println!(
        "\n=== parse_members_tsv -> entries={} all_ids={} ===",
        map.entries.len(),
        map.all_ids.len()
    );

    let mut by_community: HashMap<u32, usize> = HashMap::new();
    for (id, _, _) in &map.entries {
        *by_community
            .entry(community_of.get(id).copied().unwrap_or(u32::MAX))
            .or_default() += 1;
    }
    let mut rows: Vec<_> = by_community.into_iter().collect();
    rows.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    println!("community breakdown of the top-{top_k} entries:");
    for (c, n) in &rows {
        let share = affinity
            .iter()
            .find(|(k, _)| k == c)
            .map(|(_, s)| *s)
            .unwrap_or(f32::NAN);
        println!("  community {c:>10}  entries {n:>6}  share {share:.6}");
    }

    // Which account set the normalisation max, and what it looks like.
    if let Some((top_id, top_w, _)) = map.entries.iter().max_by_key(|(_, w, _)| *w) {
        println!(
            "\nhighest-weight entry: account {top_id} weight {top_w} community {:?} followers {:?}",
            community_of.get(top_id),
            followers_of.get(top_id)
        );
    }
    let scores: Vec<u16> = map.entries.iter().map(|(_, w, _)| *w).collect();
    println!(
        "weight range: {:?} .. {:?}",
        scores.iter().min(),
        scores.iter().max()
    );

    // Dump entries so the prod blob can be diffed against them.
    if let Ok(out) = std::env::var("COMMUNITY_DUMP_ENTRIES") {
        let body: String = map
            .entries
            .iter()
            .map(|(id, w, _)| format!("{id}\t{w}\n"))
            .collect();
        std::fs::write(&out, body).expect("write dump");
        println!("dumped {} entries to {out}", map.entries.len());
    }

    // The claim under test: a reader's strongest community should dominate.
    let strongest = affinity
        .iter()
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .map(|(c, _)| *c)
        .expect("affinity non-empty");
    let winner = rows.first().map(|(c, _)| *c).expect("entries non-empty");
    println!(
        "\nstrongest-affinity community = {strongest}; community dominating entries = {winner}"
    );
    assert_eq!(
        strongest, winner,
        "the facet ranked a community the reader cares about LESS above their strongest one"
    );
}
