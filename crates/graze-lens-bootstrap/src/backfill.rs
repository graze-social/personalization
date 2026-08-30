//! Paginate one repo's follow records into `follow_edges` rows.
//!
//! Uses `com.atproto.repo.listRecords` rather than a repo CAR fetch. The CAR is
//! one request instead of N, but the record key lives in the MST, not in the
//! record — and the rkey is exactly what this table is keyed on, so a CAR path
//! would mean walking the MST correctly to recover it. listRecords hands back
//! `uri` (rkey included) and the decoded record as JSON, which is the same data
//! with none of the binary parsing risk. Backfill is a one-time, background
//! operation per viewer, so the extra round trips are affordable.

use std::time::Duration;

use graze_lens_fold::FollowEdge;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::resolve::Resolver;

const COLLECTION: &str = "app.bsky.graph.follow";
/// listRecords caps at 100.
const PAGE_LIMIT: u32 = 100;

#[derive(Deserialize)]
struct ListRecordsResponse {
    records: Vec<Record>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct Record {
    uri: String,
    value: FollowValue,
}

#[derive(Deserialize)]
struct FollowValue {
    subject: Option<String>,
    #[serde(rename = "createdAt")]
    created_at: Option<String>,
}

/// The result of backfilling one account.
///
/// `truncated` is the field that matters to callers: it means we stopped at
/// `max_pages` with records still unread, so these edges are a *prefix* of the
/// account's follows, not their graph. A caller that records completeness must
/// not do so for a truncated result — that would pin the account to a partial
/// graph permanently, which is the same silent-wrong-lens failure the
/// completeness marker exists to prevent, just harder to spot because the
/// backfill genuinely ran.
pub struct Backfilled {
    pub edges: Vec<FollowEdge>,
    pub truncated: bool,
}

pub struct Backfiller {
    http: reqwest::Client,
    resolver: Resolver,
    request_timeout: Duration,
    /// Politeness delay between pages against one PDS.
    page_delay: Duration,
    max_pages: usize,
}

impl Backfiller {
    pub fn new(
        http: reqwest::Client,
        resolver: Resolver,
        request_timeout: Duration,
        page_delay: Duration,
        max_pages: usize,
    ) -> Self {
        Self {
            http,
            resolver,
            request_timeout,
            page_delay,
            max_pages,
        }
    }

    /// Every current follow edge for one account.
    ///
    /// Only creates are produced: listRecords reflects present state, so a
    /// record that is absent was either never created or already deleted, and
    /// in both cases there is nothing to write. Live deletes come from the fold.
    pub async fn edges_for(&self, did: &str) -> anyhow::Result<Backfilled> {
        let pds = self.resolver.pds_for(did).await?;
        let url = format!("{pds}/xrpc/com.atproto.repo.listRecords");

        let mut edges = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0usize;
        let mut truncated = false;

        loop {
            let mut query: Vec<(&str, String)> = vec![
                ("repo", did.to_string()),
                ("collection", COLLECTION.to_string()),
                ("limit", PAGE_LIMIT.to_string()),
            ];
            if let Some(ref c) = cursor {
                query.push(("cursor", c.clone()));
            }

            let response = self
                .http
                .get(&url)
                .query(&query)
                .timeout(self.request_timeout)
                .send()
                .await?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();

                // An account that has been deleted or deactivated is not a
                // backfill failure — it is an answer. The PDS is telling us
                // this repo has no records, and treating that as an error makes
                // a bulk run over thousands of accounts report failure for
                // doing exactly the right thing. Seen constantly at hop-2
                // scale, where a slice of anyone's follow list has since left.
                if is_repo_gone(status, &body) {
                    debug!(did, %status, "repo unavailable; treating as no records");
                    // Not truncated: this is a complete answer about an account
                    // with nothing to read, not a partial read of one that has
                    // records left.
                    return Ok(Backfilled {
                        edges: Vec::new(),
                        truncated: false,
                    });
                }

                anyhow::bail!(
                    "listRecords failed for {did} ({}): {}",
                    status,
                    &body[..body.len().min(300)]
                );
            }

            let page: ListRecordsResponse = response.json().await?;
            let returned = page.records.len();
            for record in page.records {
                match edge_from_record(did, &record) {
                    Some(edge) => edges.push(edge),
                    None => debug!(uri = %record.uri, "skipping unusable follow record"),
                }
            }

            pages += 1;
            cursor = page.cursor;

            // An empty page still carries a cursor on some PDS versions, so
            // stop on "no cursor" OR "nothing returned" — trusting the cursor
            // alone can spin forever on an account whose records were deleted
            // mid-pagination.
            if cursor.is_none() || returned == 0 {
                break;
            }
            if pages >= self.max_pages {
                warn!(
                    did,
                    pages,
                    edges = edges.len(),
                    "hit max_pages; backfill truncated for this account"
                );
                truncated = true;
                break;
            }
            if !self.page_delay.is_zero() {
                tokio::time::sleep(self.page_delay).await;
            }
        }

        Ok(Backfilled { edges, truncated })
    }
}

/// Build a row from one listRecords entry.
fn edge_from_record(did: &str, record: &Record) -> Option<FollowEdge> {
    let rkey = rkey_from_uri(&record.uri)?;
    let followee = record.value.subject.clone()?;
    if !followee.starts_with("did:") {
        return None;
    }

    // The rkey is a TID, which encodes the record's creation time in
    // microseconds. That is the right version column: it is monotonic, it comes
    // from the repo rather than the record body, and it cannot be forged by a
    // bogus `createdAt`. A live unfollow always carries a much larger
    // jetstream `time_us`, so it reliably supersedes a backfilled create.
    let seq = atproto_record::tid::Tid::decode(rkey)
        .map(|tid| tid.timestamp_micros())
        .unwrap_or(0);

    let created_at = record
        .value
        .created_at
        .as_deref()
        .and_then(normalize_timestamp)
        .unwrap_or_else(|| graze_lens_fold::event::micros_to_clickhouse(seq));

    Some(FollowEdge {
        follower: did.to_string(),
        rkey: rkey.to_string(),
        followee,
        op: "create",
        seq,
        created_at,
    })
}

/// Is this PDS response "that account is gone" rather than "something broke"?
///
/// Matched on the response body rather than the status alone, because a bare
/// 400 also covers genuine client errors we do want to hear about — a malformed
/// DID, a bad cursor. Only the specific repo-missing shapes are treated as an
/// empty answer.
fn is_repo_gone(status: reqwest::StatusCode, body: &str) -> bool {
    if status != reqwest::StatusCode::BAD_REQUEST && status != reqwest::StatusCode::NOT_FOUND {
        return false;
    }
    let b = body.to_ascii_lowercase();
    b.contains("could not find repo")
        || b.contains("reponotfound")
        || b.contains("repodeactivated")
        || b.contains("repotakendown")
        || b.contains("account is deactivated")
}

/// `at://did/app.bsky.graph.follow/<rkey>` → `<rkey>`.
fn rkey_from_uri(uri: &str) -> Option<&str> {
    let rkey = uri.rsplit('/').next()?;
    if rkey.is_empty() || rkey == uri {
        return None;
    }
    Some(rkey)
}

/// Same normalization the fold applies, kept in step deliberately: a record
/// backfilled here and the same record seen live must produce the same row.
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

    fn record(uri: &str, subject: Option<&str>, created: Option<&str>) -> Record {
        Record {
            uri: uri.to_string(),
            value: FollowValue {
                subject: subject.map(str::to_string),
                created_at: created.map(str::to_string),
            },
        }
    }

    /// Captured from a real listRecords response, for the same account and rkey
    /// observed on the firehose — the two paths must agree.
    #[test]
    fn builds_an_edge_matching_the_firehose_row() {
        let r = record(
            "at://did:plc:ppe4rzpiatethnqkvxf32xzj/app.bsky.graph.follow/3mu5p6crk3w23",
            Some("did:plc:5mwlxn5ktysxvd3w5kgbbuv6"),
            Some("2026-08-28T15:41:17.774Z"),
        );
        let edge = edge_from_record("did:plc:ppe4rzpiatethnqkvxf32xzj", &r).expect("edge");
        assert_eq!(edge.rkey, "3mu5p6crk3w23");
        assert_eq!(edge.followee, "did:plc:5mwlxn5ktysxvd3w5kgbbuv6");
        assert_eq!(edge.op, "create");
        assert_eq!(edge.created_at, "2026-08-28 15:41:17.774");
    }

    /// The seq must come from the rkey's TID, not the record body — otherwise a
    /// forged `createdAt` far in the future would outrank a real unfollow and
    /// the edge could never be retracted.
    #[test]
    fn seq_comes_from_the_rkey_not_the_record() {
        let honest = record(
            "at://did:plc:a/app.bsky.graph.follow/3mu5p6crk3w23",
            Some("did:plc:b"),
            Some("2026-08-28T15:41:17.774Z"),
        );
        let forged = record(
            "at://did:plc:a/app.bsky.graph.follow/3mu5p6crk3w23",
            Some("did:plc:b"),
            Some("2099-01-01T00:00:00.000Z"),
        );
        let a = edge_from_record("did:plc:a", &honest).unwrap();
        let b = edge_from_record("did:plc:a", &forged).unwrap();
        assert_eq!(a.seq, b.seq, "seq must not depend on createdAt");
        assert!(a.seq > 0, "a valid TID must yield a timestamp");
    }

    /// A backfilled create must lose to any later live event for the same edge.
    #[test]
    fn backfilled_seq_is_below_present_day_event_time() {
        let r = record(
            "at://did:plc:a/app.bsky.graph.follow/3mu5p6crk3w23",
            Some("did:plc:b"),
            None,
        );
        let edge = edge_from_record("did:plc:a", &r).unwrap();
        let now_us = 1_787_931_684_518_756u64; // the captured live event time
        assert!(
            edge.seq <= now_us,
            "backfill seq {} must not exceed live event time",
            edge.seq
        );
    }

    #[test]
    fn records_without_a_subject_are_skipped() {
        let r = record("at://did:plc:a/app.bsky.graph.follow/3mu5", None, None);
        assert!(edge_from_record("did:plc:a", &r).is_none());
    }

    /// A subject that is a handle rather than a DID would never match an author
    /// DID at serve time; drop it rather than publish an unmatchable member.
    #[test]
    fn non_did_subjects_are_skipped() {
        let r = record(
            "at://did:plc:a/app.bsky.graph.follow/3mu5",
            Some("alice.bsky.social"),
            None,
        );
        assert!(edge_from_record("did:plc:a", &r).is_none());
    }

    /// A deleted or deactivated account is an answer, not an error. At hop-2
    /// scale a slice of anyone's follow list has since left, and failing the
    /// whole run for that would report failure for correct behaviour.
    #[test]
    fn gone_repos_are_not_failures() {
        use reqwest::StatusCode;
        assert!(is_repo_gone(
            StatusCode::BAD_REQUEST,
            r#"{"error":"InvalidRequest","message":"Could not find repo: did:plc:x"}"#
        ));
        assert!(is_repo_gone(
            StatusCode::BAD_REQUEST,
            r#"{"error":"RepoNotFound"}"#
        ));
        assert!(is_repo_gone(
            StatusCode::BAD_REQUEST,
            r#"{"error":"RepoDeactivated"}"#
        ));
        assert!(is_repo_gone(StatusCode::NOT_FOUND, "RepoTakendown"));
    }

    /// But a genuine client error must still surface. A bare 400 is not on its
    /// own evidence the account is gone.
    #[test]
    fn other_client_errors_still_fail() {
        use reqwest::StatusCode;
        assert!(!is_repo_gone(
            StatusCode::BAD_REQUEST,
            r#"{"error":"InvalidRequest","message":"Invalid cursor"}"#
        ));
        assert!(!is_repo_gone(StatusCode::BAD_REQUEST, "malformed did"));
        assert!(!is_repo_gone(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not find repo"
        ));
        assert!(!is_repo_gone(StatusCode::TOO_MANY_REQUESTS, "slow down"));
    }

    #[test]
    fn rkey_is_the_last_uri_segment() {
        assert_eq!(
            rkey_from_uri("at://did:plc:a/app.bsky.graph.follow/3mu5p6crk3w23"),
            Some("3mu5p6crk3w23")
        );
        assert!(rkey_from_uri("nonsense").is_none());
    }
}
