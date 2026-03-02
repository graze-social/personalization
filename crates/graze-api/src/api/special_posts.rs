//! Special posts injection logic for feed generation.
//!
//! Handles injection of pinned, rotating (sticky), and sponsored posts
//! into the feed according to saturation rules.

use std::collections::HashSet;

use rand::seq::SliceRandom;
use rand::thread_rng;

use crate::api::cursor::FeedCursor;
use crate::api::fallback::BlendedSource;
use graze_common::services::special_posts::{SpecialPost, SpecialPostsResponse, SponsoredPost};

/// Per-item provenance after injection (base vs special).
#[derive(Debug, Clone)]
pub enum ItemProvenance {
    Base(BlendedSource),
    Pinned { attribution: String },
    Rotating { attribution: String },
    Sponsored { attribution: String },
}

/// Inject special posts into the feed according to saturation rules.
///
/// CRITICAL: This function must never inject a post that has already been shown
/// (tracked in feed_cursor). Duplicate posts will crash the Bluesky client.
///
/// Rules:
/// - Pinned: Only on first page (offset=0), placed at top
/// - Rotating (sticky): Up to 10% of feed, randomly interspersed
/// - Sponsored: Up to (page_size / 5) * (saturation_rate / 100) posts
/// - Total interspersed (rotating + sponsored): Max 20% of feed
///
/// # Arguments
/// * `base_posts` - (URI, base source) to inject into
/// * `special_posts` - The special posts response from the API
/// * `feed_cursor` - Current cursor with already-shown post tracking
/// * `limit` - The page size limit
///
/// # Returns
/// Tuple of (final (URI, provenance), updated_cursor)
pub fn inject_special_posts(
    base_posts: Vec<(String, BlendedSource)>,
    special_posts: &SpecialPostsResponse,
    feed_cursor: &FeedCursor,
    _limit: usize,
) -> (Vec<(String, ItemProvenance)>, FeedCursor) {
    // Make a copy of the cursor to update
    let mut new_cursor = FeedCursor {
        offset: feed_cursor.offset,
        last_score: feed_cursor.last_score,
        last_post_id: feed_cursor.last_post_id,
        fallback_only: feed_cursor.fallback_only,
        fallback_offset: feed_cursor.fallback_offset,
        shown_pinned: feed_cursor.shown_pinned.clone(),
        shown_rotating: feed_cursor.shown_rotating.clone(),
        shown_sponsored: feed_cursor.shown_sponsored.clone(),
        shown_fallback: feed_cursor.shown_fallback.clone(),
        is_personalization_holdout: feed_cursor.is_personalization_holdout,
    };

    let mut existing_uris: HashSet<String> = base_posts.iter().map(|(u, _)| u.clone()).collect();

    // ═══════════════════════════════════════════════════════════════════
    // STEP 1: Handle pinned posts (first page only)
    // ═══════════════════════════════════════════════════════════════════
    let mut pinned_to_add: Vec<(String, ItemProvenance)> = Vec::new();

    if feed_cursor.is_first_page() {
        for post in &special_posts.pinned {
            let can_show = !new_cursor.shown_pinned.contains(&post.attribution)
                && !existing_uris.contains(&post.post);
            if can_show {
                pinned_to_add.push((
                    post.post.clone(),
                    ItemProvenance::Pinned {
                        attribution: post.attribution.clone(),
                    },
                ));
                new_cursor.shown_pinned.insert(post.attribution.clone());
                existing_uris.insert(post.post.clone());
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // STEP 2: Calculate interspersed post slots
    // ═══════════════════════════════════════════════════════════════════
    // Total interspersed cap: 20% of feed
    let page_size = base_posts.len();
    let max_interspersed = ((page_size as f64) * 0.20).max(1.0) as usize;

    // Rotating posts: up to 10% of feed
    let max_rotating = ((page_size as f64) * 0.10).max(1.0) as usize;

    // Sponsored posts: (page_size / 5) * (saturation_rate / 100)
    let available_sponsored_slots = page_size / 5;
    let saturation_rate = special_posts.sponsorship_saturation_rate as f64 / 100.0;
    let max_sponsored = ((available_sponsored_slots as f64) * saturation_rate + 0.5) as usize;

    // ═══════════════════════════════════════════════════════════════════
    // STEP 3: Select rotating posts to inject
    // ═══════════════════════════════════════════════════════════════════
    let available_rotating: Vec<&SpecialPost> = special_posts
        .sticky
        .iter()
        .filter(|p| {
            !new_cursor.shown_rotating.contains(&p.attribution) && !existing_uris.contains(&p.post)
        })
        .collect();

    // Shuffle for randomness and take up to max_rotating
    let mut rotating_indices: Vec<usize> = (0..available_rotating.len()).collect();
    rotating_indices.shuffle(&mut thread_rng());
    rotating_indices.truncate(max_rotating);

    let rotating_to_add: Vec<&SpecialPost> = rotating_indices
        .iter()
        .map(|&i| available_rotating[i])
        .collect();

    // Track selected rotating posts
    for post in &rotating_to_add {
        new_cursor.shown_rotating.insert(post.attribution.clone());
        existing_uris.insert(post.post.clone());
    }

    // ═══════════════════════════════════════════════════════════════════
    // STEP 4: Select sponsored posts to inject
    // ═══════════════════════════════════════════════════════════════════
    let available_sponsored: Vec<&SponsoredPost> = special_posts
        .sponsored
        .iter()
        .filter(|p| {
            !new_cursor.shown_sponsored.contains(&p.attribution) && !existing_uris.contains(&p.post)
        })
        .collect();

    // Sponsored posts are already ordered by CPM (highest first), keep that order
    // but respect the total interspersed cap
    let remaining_interspersed = max_interspersed.saturating_sub(rotating_to_add.len());
    let sponsored_limit = max_sponsored.min(remaining_interspersed);
    let sponsored_to_add: Vec<&SponsoredPost> = available_sponsored
        .into_iter()
        .take(sponsored_limit)
        .collect();

    // Track selected sponsored posts
    for post in &sponsored_to_add {
        new_cursor.shown_sponsored.insert(post.attribution.clone());
        existing_uris.insert(post.post.clone());
    }

    // ═══════════════════════════════════════════════════════════════════
    // STEP 5: Build final post list with interspersed posts
    // ═══════════════════════════════════════════════════════════════════
    let mut posts_to_intersperse: Vec<(String, ItemProvenance)> = rotating_to_add
        .iter()
        .map(|p| {
            (
                p.post.clone(),
                ItemProvenance::Rotating {
                    attribution: p.attribution.clone(),
                },
            )
        })
        .chain(sponsored_to_add.iter().map(|p| {
            (
                p.post.clone(),
                ItemProvenance::Sponsored {
                    attribution: p.attribution.clone(),
                },
            )
        }))
        .collect();
    posts_to_intersperse.shuffle(&mut thread_rng());

    let mut final_posts: Vec<(String, ItemProvenance)> = pinned_to_add;

    if posts_to_intersperse.is_empty() {
        final_posts.extend(
            base_posts
                .into_iter()
                .map(|(u, s)| (u, ItemProvenance::Base(s))),
        );
    } else {
        let num_base = base_posts.len();
        let num_inject = posts_to_intersperse.len();

        if num_base == 0 {
            final_posts.extend(posts_to_intersperse);
        } else {
            let step = (num_base / (num_inject + 1)).max(2);
            let mut positions: Vec<usize> = Vec::with_capacity(num_inject);
            for i in 0..num_inject {
                let base_pos = (i + 1) * step;
                let jitter = if step > 2 {
                    (rand::random::<i32>() % 3) - 1
                } else {
                    0
                };
                let pos = ((base_pos as i32 + jitter).max(1) as usize).min(num_base);
                positions.push(pos);
            }
            positions.sort_unstable();
            positions.dedup();

            let mut inject_idx = 0;
            for (i, (uri, source)) in base_posts.into_iter().enumerate() {
                while inject_idx < positions.len() && inject_idx < posts_to_intersperse.len() {
                    if positions[inject_idx] <= i {
                        final_posts.push(posts_to_intersperse[inject_idx].clone());
                        inject_idx += 1;
                    } else {
                        break;
                    }
                }
                final_posts.push((uri, ItemProvenance::Base(source)));
            }
            while inject_idx < posts_to_intersperse.len() {
                final_posts.push(posts_to_intersperse[inject_idx].clone());
                inject_idx += 1;
            }
        }
    }

    (final_posts, new_cursor)
}

/// Count the number of injected posts of each type.
pub fn count_injected(before: &FeedCursor, after: &FeedCursor) -> (usize, usize, usize) {
    let pinned = after.shown_pinned.len() - before.shown_pinned.len();
    let rotating = after.shown_rotating.len() - before.shown_rotating.len();
    let sponsored = after.shown_sponsored.len() - before.shown_sponsored.len();
    (pinned, rotating, sponsored)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_special_post(attribution: &str, post: &str) -> SpecialPost {
        SpecialPost {
            attribution: attribution.to_string(),
            post: post.to_string(),
        }
    }

    fn make_sponsored_post(attribution: &str, post: &str, cpm: i32) -> SponsoredPost {
        SponsoredPost {
            attribution: attribution.to_string(),
            post: post.to_string(),
            cpm_cents: cpm,
        }
    }

    #[test]
    fn test_inject_pinned_first_page_only() {
        let base_posts: Vec<(String, BlendedSource)> = (0..10)
            .map(|i| {
                (
                    format!("post{}", i),
                    BlendedSource::PostLevelPersonalization,
                )
            })
            .collect();
        let special_posts = SpecialPostsResponse {
            algorithm_id: 1,
            sponsorship_saturation_rate: 20,
            pinned: vec![make_special_post("pin1", "at://pinned/post/1")],
            sticky: vec![],
            sponsored: vec![],
        };

        let cursor = FeedCursor::new();
        let (posts, new_cursor) =
            inject_special_posts(base_posts.clone(), &special_posts, &cursor, 10);

        assert_eq!(posts[0].0, "at://pinned/post/1");
        assert!(new_cursor.shown_pinned.contains("pin1"));

        let cursor2 = FeedCursor {
            offset: 10,
            ..Default::default()
        };
        let (posts2, new_cursor2) = inject_special_posts(base_posts, &special_posts, &cursor2, 10);
        assert_eq!(posts2[0].0, "post0");
        assert!(!new_cursor2.shown_pinned.contains("pin1"));
    }

    #[test]
    fn test_no_duplicate_injection() {
        let base_posts: Vec<(String, BlendedSource)> = (0..10)
            .map(|i| {
                (
                    format!("post{}", i),
                    BlendedSource::PostLevelPersonalization,
                )
            })
            .collect();
        let special_posts = SpecialPostsResponse {
            algorithm_id: 1,
            sponsorship_saturation_rate: 100,
            pinned: vec![],
            sticky: vec![make_special_post("rot1", "at://rotating/post/1")],
            sponsored: vec![make_sponsored_post("spon1", "at://sponsored/post/1", 100)],
        };

        let cursor = FeedCursor::new();
        let (_, cursor1) = inject_special_posts(base_posts.clone(), &special_posts, &cursor, 10);
        assert!(cursor1.shown_rotating.contains("rot1"));
        assert!(cursor1.shown_sponsored.contains("spon1"));

        let (posts2, _cursor2) = inject_special_posts(base_posts, &special_posts, &cursor1, 10);

        let rotating_count = posts2
            .iter()
            .filter(|(u, _)| u == "at://rotating/post/1")
            .count();
        let sponsored_count = posts2
            .iter()
            .filter(|(u, _)| u == "at://sponsored/post/1")
            .count();
        assert_eq!(rotating_count, 0);
        assert_eq!(sponsored_count, 0);
    }

    #[test]
    fn test_empty_base_posts() {
        let base_posts: Vec<(String, BlendedSource)> = vec![];
        let special_posts = SpecialPostsResponse {
            algorithm_id: 1,
            sponsorship_saturation_rate: 20,
            pinned: vec![make_special_post("pin1", "at://pinned/1")],
            sticky: vec![make_special_post("rot1", "at://rotating/1")],
            sponsored: vec![],
        };

        let cursor = FeedCursor::new();
        let (posts, _) = inject_special_posts(base_posts, &special_posts, &cursor, 10);

        assert!(posts.iter().any(|(u, _)| u == "at://pinned/1"));
    }

    #[test]
    fn test_max_interspersed_cap() {
        let base_posts: Vec<(String, BlendedSource)> = (0..10)
            .map(|i| {
                (
                    format!("post{}", i),
                    BlendedSource::PostLevelPersonalization,
                )
            })
            .collect();
        let special_posts = SpecialPostsResponse {
            algorithm_id: 1,
            sponsorship_saturation_rate: 100,
            pinned: vec![],
            sticky: (0..10)
                .map(|i| make_special_post(&format!("rot{}", i), &format!("at://rot/{}", i)))
                .collect(),
            sponsored: (0..10)
                .map(|i| {
                    make_sponsored_post(&format!("spon{}", i), &format!("at://spon/{}", i), 100)
                })
                .collect(),
        };

        let cursor = FeedCursor::new();
        let (_, new_cursor) = inject_special_posts(base_posts, &special_posts, &cursor, 10);

        let total_interspersed = new_cursor.shown_rotating.len() + new_cursor.shown_sponsored.len();
        assert!(total_interspersed <= 2);
    }

    #[test]
    fn test_count_injected() {
        let before = FeedCursor::new();
        let mut after = FeedCursor::new();
        after.shown_pinned.insert("p1".to_string());
        after.shown_pinned.insert("p2".to_string());
        after.shown_rotating.insert("r1".to_string());
        after.shown_sponsored.insert("s1".to_string());
        after.shown_sponsored.insert("s2".to_string());
        after.shown_sponsored.insert("s3".to_string());

        let (pinned, rotating, sponsored) = count_injected(&before, &after);
        assert_eq!(pinned, 2);
        assert_eq!(rotating, 1);
        assert_eq!(sponsored, 3);
    }
}
