//! DID exclusion list parsing for privacy opt-outs (`EXCLUSION_LIST` env).

use std::collections::HashSet;
use std::sync::Arc;

/// Extract author DID from an AT-URI (`at://did:plc:xxx/app.bsky.feed.post/rkey`).
pub fn author_did_from_at_uri(uri: &str) -> Option<&str> {
    if !uri.starts_with("at://") {
        return None;
    }
    let path = &uri[5..];
    let end = path.find('/')?;
    let did = &path[..end];
    if did.starts_with("did:") {
        Some(did)
    } else {
        None
    }
}

/// Parse `EXCLUSION_LIST` content: comma- and newline-separated DIDs, trimmed, empties dropped.
pub fn parse_exclusion_list(raw: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    for part in raw.split([',', '\n', '\r']) {
        let did = part.trim();
        if !did.is_empty() {
            out.insert(did.to_string());
        }
    }
    out
}

/// Load exclusion set from optional env value (caller passes `std::env::var("EXCLUSION_LIST").ok()`).
pub fn exclusion_set_from_env_opt(raw: Option<String>) -> Arc<HashSet<String>> {
    match raw {
        Some(s) if !s.trim().is_empty() => Arc::new(parse_exclusion_list(&s)),
        _ => Arc::new(HashSet::new()),
    }
}

#[inline]
pub fn is_excluded_did(did: &str, excluded: &HashSet<String>) -> bool {
    excluded.contains(did)
}

/// True if the post URI's author is in the exclusion set.
pub fn is_excluded_post_uri(uri: &str, excluded: &HashSet<String>) -> bool {
    author_did_from_at_uri(uri).is_some_and(|author| is_excluded_did(author, excluded))
}

/// Whether a like event from Jetstream should update the Redis graph (liker and post author must not be excluded).
pub fn should_process_like_event(
    liker_did: &str,
    post_uri: &str,
    excluded: &HashSet<String>,
) -> bool {
    if excluded.is_empty() {
        return true;
    }
    if is_excluded_did(liker_did, excluded) {
        return false;
    }
    if is_excluded_post_uri(post_uri, excluded) {
        return false;
    }
    true
}

/// Whether an interaction may be written to analytics (viewer and post author must not be excluded).
pub fn should_log_interaction(
    viewer_did: &str,
    interaction_item: &str,
    excluded: &HashSet<String>,
) -> bool {
    if excluded.is_empty() {
        return true;
    }
    if is_excluded_did(viewer_did, excluded) {
        return false;
    }
    if is_excluded_post_uri(interaction_item, excluded) {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn author_did_from_at_uri_valid() {
        assert_eq!(
            author_did_from_at_uri("at://did:plc:abc/app.bsky.feed.post/3xyz"),
            Some("did:plc:abc")
        );
    }

    #[test]
    fn author_did_from_at_uri_non_did_rejected() {
        assert_eq!(author_did_from_at_uri("at://handle.example/post/1"), None);
    }

    #[test]
    fn author_did_from_at_uri_not_at_uri() {
        assert_eq!(author_did_from_at_uri("https://x"), None);
    }

    #[test]
    fn parse_exclusion_list_commas_and_whitespace() {
        let s = " did:plc:a , did:plc:b  ";
        let set = parse_exclusion_list(s);
        assert_eq!(set.len(), 2);
        assert!(set.contains("did:plc:a"));
        assert!(set.contains("did:plc:b"));
    }

    #[test]
    fn parse_exclusion_list_newlines() {
        let s = "did:plc:x\n\r\ndid:plc:y,";
        let set = parse_exclusion_list(s);
        assert_eq!(set.len(), 2);
        assert!(set.contains("did:plc:x"));
        assert!(set.contains("did:plc:y"));
    }

    #[test]
    fn parse_exclusion_list_empty_tokens_ignored() {
        assert!(parse_exclusion_list(",  ,\n").is_empty());
    }

    #[test]
    fn excluded_post_uri_detects_author() {
        let mut set = HashSet::new();
        set.insert("did:plc:ex".to_string());
        assert!(super::is_excluded_post_uri(
            "at://did:plc:ex/app.bsky.feed.post/3a",
            &set
        ));
        assert!(!super::is_excluded_post_uri(
            "at://did:plc:ok/app.bsky.feed.post/3a",
            &set
        ));
    }

    #[test]
    fn should_log_interaction_viewer_excluded() {
        let mut set = HashSet::new();
        set.insert("did:plc:viewer".to_string());
        assert!(!should_log_interaction(
            "did:plc:viewer",
            "at://did:plc:other/app.bsky.feed.post/1",
            &set
        ));
    }

    #[test]
    fn should_log_interaction_post_author_excluded() {
        let mut set = HashSet::new();
        set.insert("did:plc:author".to_string());
        assert!(!should_log_interaction(
            "did:plc:viewer",
            "at://did:plc:author/app.bsky.feed.post/1",
            &set
        ));
    }

    #[test]
    fn should_log_interaction_empty_set_accepts() {
        let set = HashSet::new();
        assert!(should_log_interaction(
            "did:plc:any",
            "at://did:plc:x/app.bsky.feed.post/1",
            &set
        ));
    }

    #[test]
    fn should_process_like_event_liker_excluded() {
        let mut set = HashSet::new();
        set.insert("did:plc:liker".to_string());
        assert!(!should_process_like_event(
            "did:plc:liker",
            "at://did:plc:author/app.bsky.feed.post/1",
            &set
        ));
    }

    #[test]
    fn should_process_like_event_post_author_excluded() {
        let mut set = HashSet::new();
        set.insert("did:plc:author".to_string());
        assert!(!should_process_like_event(
            "did:plc:liker",
            "at://did:plc:author/app.bsky.feed.post/1",
            &set
        ));
    }
}
