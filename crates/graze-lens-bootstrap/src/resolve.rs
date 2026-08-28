//! DID → PDS endpoint resolution.
//!
//! Follow records live in the account's own repo, so a backfill has to find the
//! PDS hosting it. `did:plc` resolves through the PLC directory; `did:web`
//! resolves from the domain itself. Results are cached for the run — a backfill
//! of many DIDs hits the same handful of PDS hosts.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::debug;

const PLC_DIRECTORY: &str = "https://plc.directory";
const ATPROTO_PDS_SERVICE: &str = "#atproto_pds";

#[derive(Deserialize)]
struct DidDocument {
    service: Option<Vec<Service>>,
}

#[derive(Deserialize)]
struct Service {
    id: String,
    #[serde(rename = "serviceEndpoint")]
    service_endpoint: String,
}

#[derive(Clone)]
pub struct Resolver {
    http: reqwest::Client,
    cache: Arc<Mutex<HashMap<String, String>>>,
    plc_directory: String,
}

impl Resolver {
    pub fn new(http: reqwest::Client, plc_directory: Option<String>) -> Self {
        Self {
            http,
            cache: Arc::new(Mutex::new(HashMap::new())),
            plc_directory: plc_directory.unwrap_or_else(|| PLC_DIRECTORY.to_string()),
        }
    }

    /// The PDS base URL hosting `did`'s repo.
    pub async fn pds_for(&self, did: &str) -> anyhow::Result<String> {
        if let Some(hit) = self.cache.lock().await.get(did) {
            return Ok(hit.clone());
        }

        let doc_url = self.did_document_url(did)?;
        let doc: DidDocument = self
            .http
            .get(&doc_url)
            .timeout(Duration::from_secs(15))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let endpoint = doc
            .service
            .unwrap_or_default()
            .into_iter()
            .find(|s| s.id == ATPROTO_PDS_SERVICE)
            .map(|s| s.service_endpoint)
            .ok_or_else(|| anyhow::anyhow!("{did} has no {ATPROTO_PDS_SERVICE} service"))?;

        let endpoint = endpoint.trim_end_matches('/').to_string();
        debug!(did, endpoint, "resolved pds");
        self.cache
            .lock()
            .await
            .insert(did.to_string(), endpoint.clone());
        Ok(endpoint)
    }

    fn did_document_url(&self, did: &str) -> anyhow::Result<String> {
        if let Some(rest) = did.strip_prefix("did:web:") {
            // did:web encodes the host (and optional path) percent-style, with
            // `:` as the path separator.
            let host_and_path = rest.replace(':', "/");
            return Ok(format!("https://{host_and_path}/.well-known/did.json"));
        }
        if did.starts_with("did:plc:") {
            return Ok(format!("{}/{}", self.plc_directory, did));
        }
        anyhow::bail!("unsupported DID method: {did}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolver() -> Resolver {
        Resolver::new(reqwest::Client::new(), None)
    }

    #[test]
    fn plc_dids_resolve_through_the_directory() {
        let url = resolver()
            .did_document_url("did:plc:ppe4rzpiatethnqkvxf32xzj")
            .unwrap();
        assert_eq!(
            url,
            "https://plc.directory/did:plc:ppe4rzpiatethnqkvxf32xzj"
        );
    }

    #[test]
    fn web_dids_resolve_from_their_own_domain() {
        let url = resolver().did_document_url("did:web:example.com").unwrap();
        assert_eq!(url, "https://example.com/.well-known/did.json");
    }

    /// `did:web` uses `:` where a URL uses `/`; getting this wrong produces a
    /// request to a host that does not exist rather than a visible error.
    #[test]
    fn web_dids_with_paths_use_slash_separators() {
        let url = resolver()
            .did_document_url("did:web:example.com:user:alice")
            .unwrap();
        assert_eq!(url, "https://example.com/user/alice/.well-known/did.json");
    }

    #[test]
    fn unknown_methods_are_refused_not_guessed() {
        assert!(resolver().did_document_url("did:key:z6Mk").is_err());
    }
}
