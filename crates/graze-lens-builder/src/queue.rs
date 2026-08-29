//! Build-request queue: a Redis stream with a consumer group.
//!
//! Shape follows the house convention — `XADD ... MAXLEN ~` with a single
//! `data` field holding JSON (`feed-processor/app/job_helpers.py:127`), read
//! through `XREADGROUP` so several replicas can share the stream without
//! duplicating work. feeder-rs is the producer (`feeder-rs/src/lens.rs`).

use deadpool_redis::Pool;
use redis::streams::StreamReadReply;
use redis::{RedisResult, Value};
use serde::Deserialize;
use tracing::warn;

pub const STREAM_KEY: &str = "queue:lens";

/// One build request, as written by the serve path.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BuildRequest {
    pub viewer_did: String,
    pub facet: String,
}

/// A request together with the stream id needed to acknowledge it.
#[derive(Debug, Clone)]
pub struct Delivery {
    pub id: String,
    pub request: BuildRequest,
}

pub struct Queue {
    redis: Pool,
    group: String,
    consumer: String,
}

impl Queue {
    pub fn new(redis: Pool, group: String, consumer: String) -> Self {
        Self {
            redis,
            group,
            consumer,
        }
    }

    /// Create the consumer group, tolerating the common case where it exists.
    ///
    /// `MKSTREAM` matters on a cold system: the producer may not have pushed
    /// anything yet, and without it the group creation fails on a missing key
    /// and the worker crash-loops until the first request arrives.
    pub async fn ensure_group(&self) -> anyhow::Result<()> {
        let mut conn = self.redis.get().await?;
        let result: RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(STREAM_KEY)
            .arg(&self.group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Claim up to `count` new requests, blocking up to `block_ms`.
    pub async fn read(&self, count: usize, block_ms: u64) -> anyhow::Result<Vec<Delivery>> {
        let mut conn = self.redis.get().await?;

        // `>` means "messages never delivered to this group". Entries already
        // delivered but unacknowledged stay in the pending list; nothing here
        // reclaims them, which is deliberate — a build that died mid-flight is
        // re-requested by the serve path on the viewer's next request, and that
        // is cheaper than reasoning about ownership transfer.
        let reply: StreamReadReply = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.group)
            .arg(&self.consumer)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(block_ms)
            .arg("STREAMS")
            .arg(STREAM_KEY)
            .arg(">")
            .query_async(&mut conn)
            .await
            .map_err(anyhow::Error::from)?;

        let mut out = Vec::new();
        for key in reply.keys {
            for entry in key.ids {
                match parse_entry(&entry.map) {
                    Some(request) => out.push(Delivery {
                        id: entry.id,
                        request,
                    }),
                    None => {
                        // Unparseable entries are acknowledged rather than
                        // retried: they will never parse, and leaving them
                        // pending grows the PEL forever.
                        warn!(id = %entry.id, "dropping unparseable build request");
                        let _ = self.ack(&entry.id).await;
                    }
                }
            }
        }
        Ok(out)
    }

    pub async fn ack(&self, id: &str) -> anyhow::Result<()> {
        let mut conn = self.redis.get().await?;
        let _: i64 = redis::cmd("XACK")
            .arg(STREAM_KEY)
            .arg(&self.group)
            .arg(id)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }
}

/// Pull the `data` field out of a stream entry and parse it.
fn parse_entry(map: &std::collections::HashMap<String, Value>) -> Option<BuildRequest> {
    let text = match map.get("data")? {
        Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
        Value::SimpleString(s) => s.clone(),
        _ => return None,
    };
    serde_json::from_str(&text).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn entry(field: &str, json: &str) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert(
            field.to_string(),
            Value::BulkString(json.as_bytes().to_vec()),
        );
        m
    }

    /// The exact payload feeder-rs writes must parse here. This is the contract
    /// between the two repos.
    #[test]
    fn parses_the_producers_payload() {
        let map = entry("data", r#"{"viewer_did":"did:plc:abc","facet":"follows"}"#);
        let parsed = parse_entry(&map).expect("must parse");
        assert_eq!(parsed.viewer_did, "did:plc:abc");
        assert_eq!(parsed.facet, "follows");
    }

    #[test]
    fn rejects_entries_without_a_data_field() {
        let map = entry(
            "payload",
            r#"{"viewer_did":"did:plc:abc","facet":"follows"}"#,
        );
        assert!(parse_entry(&map).is_none());
    }

    #[test]
    fn rejects_malformed_json() {
        let map = entry("data", "{not json");
        assert!(parse_entry(&map).is_none());
    }
}
