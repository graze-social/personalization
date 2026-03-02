//! Serves in-repo documentation at /docs so the root payload link is valid.

use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};

/// Markdown content for each doc (embedded at compile time from repo root docs/).
const FEED_LIFECYCLE: &str = include_str!("../../../../docs/FEED_LIFECYCLE.md");
const ARCHITECTURE: &str = include_str!("../../../../docs/ARCHITECTURE.md");
const CONFIGURATION: &str = include_str!("../../../../docs/CONFIGURATION.md");
const ADMIN_API: &str = include_str!("../../../../docs/ADMIN_API.md");

/// GET /docs — HTML index linking to the three doc pages.
pub async fn docs_index() -> Response {
    let html = r#"<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Graze Personalization Service — Docs</title></head>
<body>
<h1>Graze Personalization Service</h1>
<p>Documentation:</p>
<ul>
<li><a href="/docs/feed-lifecycle">Feed lifecycle</a> — Add/remove feeds, candidate pool (ClickHouse vs manual), special posts, turn off</li>
<li><a href="/docs/architecture">Architecture</a> — Data flow, ClickHouse module, backends</li>
<li><a href="/docs/configuration">Configuration</a> — Environment variables, no-ClickHouse setup</li>
<li><a href="/docs/admin-api">Admin API</a> — Admin endpoints (candidates, special posts, Thompson, audit)</li>
</ul>
<p><a href="/">Service info</a> · <a href="/internal/ready">Health</a></p>
</body>
</html>"#;
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
        .into_response()
}

/// GET /docs/:name — Serve one of architecture, configuration, admin-api as markdown.
pub async fn docs_page(Path(name): Path<String>) -> Response {
    let (content_type, body) = match name.as_str() {
        "feed-lifecycle" => ("text/markdown; charset=utf-8", FEED_LIFECYCLE),
        "architecture" => ("text/markdown; charset=utf-8", ARCHITECTURE),
        "configuration" => ("text/markdown; charset=utf-8", CONFIGURATION),
        "admin-api" => ("text/markdown; charset=utf-8", ADMIN_API),
        _ => {
            return (
                StatusCode::NOT_FOUND,
                "No such doc. Use: feed-lifecycle, architecture, configuration, admin-api",
            )
                .into_response();
        }
    };
    (StatusCode::OK, [(header::CONTENT_TYPE, content_type)], body).into_response()
}
