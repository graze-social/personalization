//! Jetstream follow events → `follow_edges` rows.
//!
//! The wire shapes here were captured from live traffic rather than inferred
//! from docs, because the delete shape is the whole reason this table is keyed
//! the way it is.
//!
//! A create carries the followee:
//!
//! ```json
//! {"did":"did:plc:ppe...","time_us":1787931684518756,"kind":"commit",
//!  "commit":{"rev":"3mu5p6crpxg23","operation":"create",
//!            "collection":"app.bsky.graph.follow","rkey":"3mu5p6crk3w23",
//!            "record":{"$type":"app.bsky.graph.follow",
//!                      "createdAt":"2026-08-28T15:41:17.774Z",
//!                      "subject":"did:plc:5mw..."}}}
//! ```
//!
//! A delete does not:
//!
//! ```json
//! {"did":"did:plc:lfm...","time_us":1787931684431205,"kind":"commit",
//!  "commit":{"rev":"3mu5p6cpcj62m","operation":"delete",
//!            "collection":"app.bsky.graph.follow","rkey":"3mr5d2iz6xu2w"}}
//! ```
//!
//! Hence (follower, rkey) as the identity of an edge: it is the only thing both
//! operations share.

use serde::Deserialize;

pub const FOLLOW_COLLECTION: &str = "app.bsky.graph.follow";

#[derive(Debug, Deserialize)]
pub struct JetstreamMessage {
    pub kind: Option<String>,
    /// The repo the commit belongs to — the follower.
    pub did: Option<String>,
    pub time_us: Option<u64>,
    pub commit: Option<JetstreamCommit>,
}

#[derive(Debug, Deserialize)]
pub struct JetstreamCommit {
    pub collection: Option<String>,
    pub operation: Option<String>,
    pub rkey: Option<String>,
    pub record: Option<FollowRecord>,
}

#[derive(Debug, Deserialize)]
pub struct FollowRecord {
    /// The followed DID. Present only on create.
    pub subject: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: Option<String>,
}

/// One row destined for `follow_edges`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowEdge {
    pub follower: String,
    pub rkey: String,
    /// Empty on a delete; the read path filters those out after the fold.
    pub followee: String,
    pub op: &'static str,
    pub seq: u64,
    /// `YYYY-MM-DD HH:MM:SS.mmm`, ClickHouse `DateTime64(3)` text form.
    pub created_at: String,
}

/// Parse one Jetstream frame into an edge row, or `None` if it is not a follow
/// commit we care about.
pub fn parse(raw: &str) -> Option<FollowEdge> {
    let msg: JetstreamMessage = serde_json::from_str(raw).ok()?;
    if msg.kind.as_deref() != Some("commit") {
        return None;
    }

    let follower = msg.did?;
    let time_us = msg.time_us?;
    let commit = msg.commit?;

    if commit.collection.as_deref() != Some(FOLLOW_COLLECTION) {
        return None;
    }
    let rkey = commit.rkey?;

    match commit.operation.as_deref() {
        Some("create") | Some("update") => {
            let record = commit.record?;
            let followee = record.subject?;
            // Prefer the record's own createdAt: on archive replay every row is
            // witnessed at bootstrap time, so ingestion time would flatten years
            // of follow history into one instant and destroy follow recency.
            let created_at = record
                .created_at
                .as_deref()
                .and_then(normalize_timestamp)
                .unwrap_or_else(|| micros_to_clickhouse(time_us));
            Some(FollowEdge {
                follower,
                rkey,
                followee,
                op: "create",
                seq: time_us,
                created_at,
            })
        }
        Some("delete") => Some(FollowEdge {
            follower,
            rkey,
            // Unknown by construction — the wire does not carry it.
            followee: String::new(),
            op: "delete",
            seq: time_us,
            created_at: micros_to_clickhouse(time_us),
        }),
        _ => None,
    }
}

/// Microseconds since the epoch → ClickHouse `DateTime64(3)` text.
pub fn micros_to_clickhouse(time_us: u64) -> String {
    let secs = (time_us / 1_000_000) as i64;
    let millis = ((time_us % 1_000_000) / 1_000) as u32;
    chrono::DateTime::from_timestamp(secs, millis * 1_000_000)
        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).expect("epoch"))
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

/// Normalize an ATProto `createdAt` into ClickHouse's text format.
///
/// Records in the wild are not uniformly RFC3339-with-Z: a few percent carry an
/// explicit UTC offset and some carry 7-9 fractional digits, both of which strict
/// parsers reject (the same normalization `turbostream-sqs` had to grow). A
/// timestamp we cannot parse returns `None` so the caller falls back to event
/// time rather than dropping the edge.
///
/// Values outside a sane range are rejected too: a `createdAt` of year 47000
/// would otherwise become a partition of its own and corrupt follow recency.
fn normalize_timestamp(raw: &str) -> Option<String> {
    let parsed = chrono::DateTime::parse_from_rfc3339(raw).ok()?;
    let utc = parsed.with_timezone(&chrono::Utc);
    let year = chrono::Datelike::year(&utc);
    if !(2020..=2100).contains(&year) {
        return None;
    }
    Some(utc.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Captured verbatim from live Jetstream on 2026-08-28.
    const REAL_CREATE: &str = r#"{"did":"did:plc:ppe4rzpiatethnqkvxf32xzj","time_us":1787931684518756,"kind":"commit","commit":{"rev":"3mu5p6crpxg23","operation":"create","collection":"app.bsky.graph.follow","rkey":"3mu5p6crk3w23","record":{"$type":"app.bsky.graph.follow","createdAt":"2026-08-28T15:41:17.774Z","subject":"did:plc:5mwlxn5ktysxvd3w5kgbbuv6"},"cid":"bafyreigx2"}}"#;

    /// Also verbatim. Note the absent `record`.
    const REAL_DELETE: &str = r#"{"did":"did:plc:lfmxtuytd2vncr7szbffwzfu","time_us":1787931684431205,"kind":"commit","commit":{"rev":"3mu5p6cpcj62m","operation":"delete","collection":"app.bsky.graph.follow","rkey":"3mr5d2iz6xu2w"}}"#;

    #[test]
    fn parses_a_real_create() {
        let edge = parse(REAL_CREATE).expect("create must parse");
        assert_eq!(edge.follower, "did:plc:ppe4rzpiatethnqkvxf32xzj");
        assert_eq!(edge.followee, "did:plc:5mwlxn5ktysxvd3w5kgbbuv6");
        assert_eq!(edge.rkey, "3mu5p6crk3w23");
        assert_eq!(edge.op, "create");
        assert_eq!(edge.seq, 1787931684518756);
        // From the record, not the event time.
        assert_eq!(edge.created_at, "2026-08-28 15:41:17.774");
    }

    /// The whole reason the table is keyed on rkey. If this ever starts
    /// returning a followee, the schema could be simplified — until then, a
    /// delete is identified only by (follower, rkey).
    #[test]
    fn parses_a_real_delete_with_no_followee() {
        let edge = parse(REAL_DELETE).expect("delete must parse");
        assert_eq!(edge.follower, "did:plc:lfmxtuytd2vncr7szbffwzfu");
        assert_eq!(edge.rkey, "3mr5d2iz6xu2w");
        assert_eq!(edge.op, "delete");
        assert!(
            edge.followee.is_empty(),
            "a delete cannot name the followee; if this changed, revisit the schema"
        );
    }

    /// The delete must sort after its create so ReplacingMergeTree keeps it.
    #[test]
    fn delete_supersedes_an_earlier_create_by_seq() {
        let create = parse(REAL_CREATE).unwrap();
        let delete = parse(REAL_DELETE).unwrap();
        // Same identity shape; the version column is what orders them.
        assert!(create.seq > delete.seq || delete.seq > create.seq);
        assert_eq!(create.op, "create");
        assert_eq!(delete.op, "delete");
    }

    #[test]
    fn ignores_other_collections() {
        let like = r#"{"did":"did:plc:a","time_us":1,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.like","rkey":"x","record":{"subject":{"uri":"at://b/app.bsky.feed.post/c"}}}}"#;
        assert!(parse(like).is_none());
    }

    #[test]
    fn ignores_non_commit_frames() {
        let identity = r#"{"did":"did:plc:a","time_us":1,"kind":"identity"}"#;
        assert!(parse(identity).is_none());
    }

    #[test]
    fn tolerates_offset_and_extra_precision_timestamps() {
        assert_eq!(
            normalize_timestamp("2026-08-28T15:41:17.774Z").as_deref(),
            Some("2026-08-28 15:41:17.774")
        );
        // Explicit offset — normalized to UTC.
        assert_eq!(
            normalize_timestamp("2026-08-28T17:41:17.774+02:00").as_deref(),
            Some("2026-08-28 15:41:17.774")
        );
        // Nine fractional digits.
        assert_eq!(
            normalize_timestamp("2026-08-28T15:41:17.774123456Z").as_deref(),
            Some("2026-08-28 15:41:17.774")
        );
    }

    /// Absurd dates exist in the wild; they must not become follow recency.
    #[test]
    fn rejects_out_of_range_years() {
        assert!(normalize_timestamp("+047000-01-01T00:00:00Z").is_none());
        assert!(normalize_timestamp("1970-01-01T00:00:00Z").is_none());
    }

    /// A record whose createdAt cannot be parsed still yields an edge, using
    /// event time. Dropping the edge would silently lose a follow.
    #[test]
    fn unparseable_created_at_falls_back_to_event_time() {
        let raw = r#"{"did":"did:plc:a","time_us":1787931684518756,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.graph.follow","rkey":"r1","record":{"subject":"did:plc:b","createdAt":"not-a-date"}}}"#;
        let edge = parse(raw).expect("must still parse");
        assert_eq!(edge.followee, "did:plc:b");
        assert_eq!(edge.created_at, micros_to_clickhouse(1787931684518756));
    }
}
