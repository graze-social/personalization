//! Parsing of `log_tasks` payloads — a faithful port of the field reads in
//! `feed_stats_runner.py::process_logs`.
//!
//! Parity notes (replicated bug-for-bug; see crate docs):
//!  * `cpm_cents` is gated on the presence of `_c` in the attribution string,
//!    even though it is split on `_p` (feed_stats_runner.py:313-314, :340-341).
//!  * `limit` is coerced to an integer for `post_render_log` (`int(limit)`).
//!  * `reqId` missing → a fresh uuid4; `iss`-less / missing auth → "anonymous".
//!  * A record that fails any of these conversions aborts processing of that
//!    single log (Python wraps each log in try/except), so callers should treat
//!    a parse error as "skip this one log".

use base64::Engine;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Deserialize;

/// Raw `log_tasks` JSON payload. Every field is optional at the JSON layer;
/// semantic requirements (e.g. `feed_uri`) are enforced during expansion.
#[derive(Debug, Clone, Deserialize)]
pub struct RawLog {
    #[serde(default, rename = "reqId")]
    pub req_id: Option<String>,
    pub feed_uri: Option<String>,
    #[serde(default)]
    pub post_ids: Vec<String>,
    #[serde(default)]
    pub attributions: Option<Vec<Option<String>>>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<serde_json::Value>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub authorization_header: Option<String>,
}

/// A parsed attribution tag: `s{attribution_id}_c{campaign_id}_p{cpm_cents}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attribution {
    pub attribution_id: i64,
    pub campaign_id: Option<i64>,
    pub cpm_cents: Option<i64>,
}

/// One row of `post_render_log` (a fully-expanded post impression).
#[derive(Debug, Clone)]
pub struct PostView {
    pub attribution_id: Option<i64>,
    pub campaign_id: Option<i64>,
    pub post_id: String,
    pub position: usize,
    pub uuid: String,
    pub algorithm_id: i64,
    pub user_did: String,
    pub cpm_cents: Option<i64>,
    pub created_at: NaiveDateTime,
    pub feed_operator_did: String,
    pub slug: String,
    pub cursor: Option<String>,
    pub limit: i64,
    pub post_count: usize,
}

/// One attributable (paid) impression → a row of `sponsored_feed_impressions`
/// and a Redis-counter / credit event.
#[derive(Debug, Clone)]
pub struct AttributableRow {
    pub attribution_id: i64,
    pub campaign_id: Option<i64>,
    pub post_id: String,
    pub uuid: String,
    pub algorithm_id: i64,
    pub user_did: Option<String>,
    pub cpm_cents: Option<i64>,
    pub created_at: NaiveDateTime,
}

/// The result of expanding a single log line.
#[derive(Debug, Clone, Default)]
pub struct ExpandedLog {
    pub post_views: Vec<PostView>,
    pub attributable_rows: Vec<AttributableRow>,
    /// (post_uri, algo_id) pairs used to decrement sticky-post credits.
    pub post_algo_pairs: Vec<(String, i64)>,
    /// Distinct feed_uris referenced (for the algorithm lookup up the stack).
    pub feed_uri: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("missing feed_uri")]
    MissingFeedUri,
    #[error("bad attribution `{0}`")]
    BadAttribution(String),
    #[error("bad limit")]
    BadLimit,
    #[error("index out of range")]
    IndexOutOfRange,
}

/// Parse a single attribution tag. Returns `Ok(None)` when the tag is null or
/// does not start with `s` (i.e. not attributable). Returns `Err` when it looks
/// attributable but the embedded integers fail to parse — matching Python's
/// `int(...)` raising and aborting the log.
pub fn parse_attribution(tag: Option<&str>) -> Result<Option<Attribution>, ParseError> {
    let e = match tag {
        Some(e) if e.starts_with('s') => e,
        _ => return Ok(None),
    };

    // attribution_id = int(e.split("s")[1].split("_")[0])
    let after_first_s = e
        .split('s')
        .nth(1)
        .ok_or_else(|| ParseError::BadAttribution(e.to_string()))?;
    let attribution_id: i64 = after_first_s
        .split('_')
        .next()
        .unwrap_or("")
        .parse()
        .map_err(|_| ParseError::BadAttribution(e.to_string()))?;

    let has_c = e.contains("_c");

    // campaign_id present iff `_c` substring.
    let campaign_id = if has_c {
        let seg = e
            .split("_c")
            .nth(1)
            .ok_or_else(|| ParseError::BadAttribution(e.to_string()))?;
        Some(
            seg.split('_')
                .next()
                .unwrap_or("")
                .parse()
                .map_err(|_| ParseError::BadAttribution(e.to_string()))?,
        )
    } else {
        None
    };

    // PARITY QUIRK: cpm gated on `_c`, but split on `_p`.
    let cpm_cents = if has_c {
        let seg = e
            .split("_p")
            .nth(1)
            .ok_or_else(|| ParseError::BadAttribution(e.to_string()))?;
        Some(
            seg.parse()
                .map_err(|_| ParseError::BadAttribution(e.to_string()))?,
        )
    } else {
        None
    };

    Ok(Some(Attribution {
        attribution_id,
        campaign_id,
        cpm_cents,
    }))
}

/// Decode the `iss` claim from a `Bearer <jwt>` header WITHOUT verifying the
/// signature (mirrors `jwt.decode(..., verify_signature=False)`). Any failure
/// yields `None`, matching the Python `try/except: user_did = None`.
pub fn user_did_from_auth(header: Option<&str>) -> Option<String> {
    let header = header.unwrap_or("Bearer ");
    let token = header.split("Bearer ").nth(1)?;
    let payload_b64 = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64.trim())
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    claims
        .get("iss")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Parse `created_at` (ISO-8601, trailing `Z` accepted) to a naive-UTC datetime,
/// falling back to "now" when absent — matching `datetime.utcnow()`.
pub fn parse_created_at(raw: Option<&str>, now: NaiveDateTime) -> NaiveDateTime {
    match raw {
        Some(s) if !s.is_empty() => {
            let normalized = s.replace('Z', "+00:00");
            DateTime::parse_from_rfc3339(&normalized)
                .map(|dt| dt.with_timezone(&Utc).naive_utc())
                .unwrap_or(now)
        }
        _ => now,
    }
}

/// Coerce `limit` the way `int(limit)` does in Python: accepts JSON numbers and
/// numeric strings; anything else is a parse error that aborts the log.
fn coerce_limit(v: &Option<serde_json::Value>) -> Result<i64, ParseError> {
    match v {
        Some(serde_json::Value::Number(n)) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .ok_or(ParseError::BadLimit),
        Some(serde_json::Value::String(s)) => s.trim().parse().map_err(|_| ParseError::BadLimit),
        _ => Err(ParseError::BadLimit),
    }
}

/// Expand one raw log into its rows. Returns `Err` if the log should be skipped
/// entirely (Python's per-log `try/except`).
pub fn expand_log(raw: &RawLog, now: NaiveDateTime) -> Result<ExpandedLog, ParseError> {
    let feed_uri = raw.feed_uri.clone().ok_or(ParseError::MissingFeedUri)?;
    let feed_uuid = raw
        .req_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let created_at = parse_created_at(raw.created_at.as_deref(), now);
    let user_did = user_did_from_auth(raw.authorization_header.as_deref());

    // feed_operator_did = feed_uri.split("/")[2]; slug = feed_uri.split("/")[-1]
    let segments: Vec<&str> = feed_uri.split('/').collect();
    let feed_operator_did = segments.get(2).copied().unwrap_or("").to_string();
    let slug = segments.last().copied().unwrap_or("").to_string();

    let post_count = raw.post_ids.len();

    // `limit` is only coerced when there are posts to render (Python casts it at
    // row-build time). With zero posts the cast never runs, so tolerate absence.
    let limit = if post_count > 0 {
        coerce_limit(&raw.limit)?
    } else {
        0
    };

    let mut out = ExpandedLog {
        feed_uri: feed_uri.clone(),
        ..Default::default()
    };

    // attribution_list defaults to [None; len(post_ids)] and is indexed by i.
    let empty_attr: Vec<Option<String>> = vec![None; post_count];
    let attribution_list = raw.attributions.as_ref().unwrap_or(&empty_attr);

    // ── Pass 1: one PostView per post_id (post_render_log) ──────────────────
    for (i, post_id) in raw.post_ids.iter().enumerate() {
        let tag = attribution_list.get(i).and_then(|o| o.as_deref());
        let attr = parse_attribution(tag)?;
        out.post_views.push(PostView {
            attribution_id: attr.map(|a| a.attribution_id),
            campaign_id: attr.and_then(|a| a.campaign_id),
            post_id: post_id.clone(),
            position: i,
            uuid: feed_uuid.clone(),
            algorithm_id: 0, // filled in by the caller once the algo is resolved
            user_did: user_did.clone().unwrap_or_else(|| "anonymous".to_string()),
            cpm_cents: attr.and_then(|a| a.cpm_cents),
            created_at,
            feed_operator_did: feed_operator_did.clone(),
            slug: slug.clone(),
            cursor: raw.cursor.clone(),
            limit,
            post_count,
        });
    }

    // ── Pass 2: attributable rows (sponsored_feed_impressions) ──────────────
    // Mirrors the SECOND loop in Python which iterates `attributions` and
    // indexes `post_ids[i]` — an out-of-range index aborts the whole log.
    for (i, tag) in attribution_list.iter().enumerate() {
        if let Some(attr) = parse_attribution(tag.as_deref())? {
            let post_id = raw.post_ids.get(i).ok_or(ParseError::IndexOutOfRange)?;
            out.attributable_rows.push(AttributableRow {
                attribution_id: attr.attribution_id,
                campaign_id: attr.campaign_id,
                post_id: post_id.clone(),
                uuid: feed_uuid.clone(),
                algorithm_id: 0, // filled in by the caller
                user_did: user_did.clone(),
                cpm_cents: attr.cpm_cents,
                created_at,
            });
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> NaiveDateTime {
        NaiveDateTime::parse_from_str("2026-08-06 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
    }

    #[test]
    fn attribution_full() {
        let a = parse_attribution(Some("s123_c45_p678")).unwrap().unwrap();
        assert_eq!(a.attribution_id, 123);
        assert_eq!(a.campaign_id, Some(45));
        assert_eq!(a.cpm_cents, Some(678));
    }

    #[test]
    fn attribution_null_and_non_s() {
        assert_eq!(parse_attribution(None).unwrap(), None);
        assert_eq!(parse_attribution(Some("nope")).unwrap(), None);
    }

    #[test]
    fn attribution_quirk_cpm_gated_on_c() {
        // `_p` present but no `_c`: cpm stays None because the gate is `_c`.
        let a = parse_attribution(Some("s7_p900")).unwrap().unwrap();
        assert_eq!(a.attribution_id, 7);
        assert_eq!(a.campaign_id, None);
        assert_eq!(a.cpm_cents, None);
    }

    #[test]
    fn jwt_iss_extracted_unverified() {
        // header.payload.signature where payload = {"iss":"did:plc:abc"}
        let payload =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"iss":"did:plc:abc"}"#);
        let token = format!("aaa.{}.bbb", payload);
        let did = user_did_from_auth(Some(&format!("Bearer {}", token)));
        assert_eq!(did.as_deref(), Some("did:plc:abc"));
    }

    #[test]
    fn missing_auth_is_none() {
        assert_eq!(user_did_from_auth(None), None);
        assert_eq!(user_did_from_auth(Some("Bearer ")), None);
    }

    #[test]
    fn created_at_z_suffix() {
        let dt = parse_created_at(Some("2026-08-06T12:00:00Z"), now());
        assert_eq!(
            dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            "2026-08-06 12:00:00"
        );
    }

    #[test]
    fn anonymous_user_and_expansion() {
        let raw = RawLog {
            req_id: Some("11111111-1111-1111-1111-111111111111".to_string()),
            feed_uri: Some("at://did:plc:op/app.bsky.feed.generator/myfeed".to_string()),
            post_ids: vec!["at://did:plc:a/app.bsky.feed.post/1".to_string()],
            attributions: Some(vec![Some("s1_c2_p3".to_string())]),
            cursor: None,
            limit: Some(serde_json::json!(20)),
            created_at: Some("2026-08-06T12:00:00Z".to_string()),
            authorization_header: None,
        };
        let ex = expand_log(&raw, now()).unwrap();
        assert_eq!(ex.post_views.len(), 1);
        assert_eq!(ex.post_views[0].user_did, "anonymous");
        assert_eq!(ex.post_views[0].feed_operator_did, "did:plc:op");
        assert_eq!(ex.post_views[0].slug, "myfeed");
        assert_eq!(ex.post_views[0].limit, 20);
        assert_eq!(ex.attributable_rows.len(), 1);
        assert_eq!(ex.attributable_rows[0].attribution_id, 1);
    }
}
