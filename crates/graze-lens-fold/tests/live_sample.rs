//! Parser validation against a captured sample of live Jetstream traffic.
//!
//! Unit tests pin two hand-picked frames; this runs the parser over thousands of
//! real ones to catch shapes nobody thought to write down. Capture a fresh
//! sample with `scratchpad/capture_follows.py` and point `LENS_FOLD_SAMPLE` at
//! the resulting NDJSON file; the test no-ops without it, so CI stays hermetic.

use graze_lens_fold::parse;
use std::io::BufRead;

#[test]
fn parses_live_follow_traffic() {
    let Some(path) = std::env::var("LENS_FOLD_SAMPLE")
        .ok()
        .filter(|p| !p.is_empty())
    else {
        eprintln!("skipping: LENS_FOLD_SAMPLE unset");
        return;
    };

    let file = std::fs::File::open(&path).expect("sample file");
    let reader = std::io::BufReader::new(file);

    let (mut frames, mut creates, mut deletes, mut skipped) = (0, 0, 0, 0);
    let mut bad_created_at = 0;

    for line in reader.lines() {
        let line = line.expect("read line");
        if line.trim().is_empty() {
            continue;
        }
        frames += 1;

        // Only commits on the follow collection should yield an edge; anything
        // else (identity, account, other collections) must be skipped, not
        // mis-parsed into a bogus row.
        let is_follow_commit = line.contains(r#""kind":"commit""#)
            && line.contains(r#""collection":"app.bsky.graph.follow""#);

        match parse(&line) {
            Some(edge) => {
                assert!(is_follow_commit, "parsed a non-follow frame: {line}");
                assert!(!edge.follower.is_empty(), "follower must be set: {line}");
                assert!(!edge.rkey.is_empty(), "rkey must be set: {line}");
                assert!(edge.seq > 0, "seq must be set: {line}");

                match edge.op {
                    "create" => {
                        creates += 1;
                        assert!(
                            edge.followee.starts_with("did:"),
                            "a create must name a DID followee: {line}"
                        );
                    }
                    "delete" => {
                        deletes += 1;
                        assert!(
                            edge.followee.is_empty(),
                            "a delete cannot know the followee: {line}"
                        );
                    }
                    other => panic!("unexpected op {other}"),
                }

                // Every row must render a ClickHouse-parseable DateTime64(3).
                if !looks_like_ch_datetime(&edge.created_at) {
                    bad_created_at += 1;
                    eprintln!("bad created_at: {:?} from {line}", edge.created_at);
                }
            }
            None => skipped += 1,
        }
    }

    eprintln!(
        "frames={frames} creates={creates} deletes={deletes} skipped={skipped} bad_created_at={bad_created_at}"
    );

    assert!(frames > 0, "sample was empty");
    assert_eq!(
        bad_created_at, 0,
        "some rows rendered an unusable timestamp"
    );
    assert!(creates > 0 && deletes > 0, "sample lacked both operations");

    // Deletes are a large minority of live follow traffic (~40% when this was
    // written). That is precisely why the schema is keyed on rkey rather than
    // resolving each delete's followee: a lookup per delete would mean a
    // ClickHouse point query for two of every five events.
    let parsed = creates + deletes;
    assert!(
        parsed * 100 / frames >= 90,
        "parsed only {parsed}/{frames} frames; the wire format may have changed"
    );
}

/// `YYYY-MM-DD HH:MM:SS.mmm`
fn looks_like_ch_datetime(s: &str) -> bool {
    let bytes = s.as_bytes();
    bytes.len() == 23
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b' '
        && bytes[13] == b':'
        && bytes[16] == b':'
        && bytes[19] == b'.'
        && bytes
            .iter()
            .enumerate()
            .all(|(i, b)| matches!(i, 4 | 7 | 10 | 13 | 16 | 19) || b.is_ascii_digit())
}
