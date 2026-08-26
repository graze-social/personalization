# How Graze Personalization Works

*A plain-language walkthrough of the algorithm powering personalized feeds on Bluesky.*

---

## Where the idea came from

Bluesky is an open social network built on the AT Protocol, which means anyone can write a feed algorithm and publish it as a "feed generator" that any user can subscribe to. One of the most interesting community-built algorithms came from [@spacecowboy17.bsky.social](https://bsky.app/profile/spacecowboy17.bsky.social), who published an algorithm called **LinkLonk**.

The insight behind LinkLonk is elegant and social-graph-native: instead of trying to infer what you might like from content features alone — keywords, embeddings, topic tags — it asks a simpler question: *who liked the same things you liked, and what else did they like?*

This is a **three-step random walk on the like graph:**

1. Take your recent likes.
2. For each post you liked, find other users who also liked it — call them your *co-likers*.
3. See what posts those co-likers have been liking lately, and surface those to you.

That's the whole thing at its core. No neural networks, no content classifiers, no opaque embeddings. Just: people who have agreed with your taste before are probably good taste signals for what you'd enjoy next.

---

## How we took that idea and inverted it

When we set out to build Graze, we started with the same intuition as LinkLonk, but we ran into a practical wall almost immediately: the original design computes the entire three-step walk *at query time*, inside a Redis Lua script. That works when you have one feed serving a small community. But when you have many feeds each with thousands of users hitting them simultaneously, blocking Redis's single execution thread with heavy Lua computation becomes a serious bottleneck.

Our first change was to **pull the scoring out of Redis and into Rust.** Instead of asking Redis to do the math, we use Redis purely as a fast key-value store for raw data — who liked what and when — and do all the scoring work in our Rust service. This reduces Redis CPU by an order of magnitude on hot feeds and lets us scale the compute side independently.

The second change was more fundamental: we **inverted the direction of the walk.**

In the original formulation, you start from the user and walk *outward* to candidate posts: user → liked posts → co-likers → their liked posts. In our inverted approach, we precompute the expensive middle step. We compute each user's co-liker weights ahead of time and cache them in Redis. Then at scoring time, we iterate over a curated **candidate pool** and ask: "does this post have likers who are in this user's co-liker set?"

This inversion means that adding more candidate posts doesn't require re-traversing the user's entire like history — we only need to look up who liked each candidate and intersect that with the pre-cached co-liker map. The result is a scoring pipeline that runs cleanly in linear time over the candidate set.

---

## The candidate pool: making it steerable

One of the key design decisions was to make the candidate pool **externally configurable**. Every feed has its own pool of eligible posts — stored as a Redis set keyed to the feed's algorithm ID. Those posts can come from ClickHouse (where an upstream data pipeline writes posts matching whatever content rules you care about), or they can be managed directly via an admin API.

This means you can aim the algorithm at whatever content universe you want. A feed for art posts only serves personalization over art posts. A feed for a specific community only considers posts from that community. The graph walk is the same; you're just controlling which posts can even appear in the output.

The candidate pool is kept fresh by a background sync worker that monitors which feeds are actively being used and periodically pulls updated post sets from ClickHouse. The first time a user hits a feed that has no candidate pool yet, the service queues an immediate sync so the pool gets populated before their next scroll.

---

## The scoring formula

Once we have a user's co-liker weights and a candidate pool, scoring a post works like this:

For each post in the candidate pool, we look at everyone who liked it within the last seven days. We intersect that set with the user's co-likers. For each co-liker who liked the post, we add their co-liker weight to the post's score, multiplied by a **recency factor** that decays with a 24-hour half-life. A like from eight hours ago contributes much more than a like from five days ago.

On top of that base score, two corrections are applied:

- **Popularity penalty.** Very viral posts — ones with thousands of likers — get penalized. The intuition is that if everyone is liking something, the signal that *your* co-likers liked it is weaker. We apply an exponent to the raw liker count to discount posts that are mass-appeal rather than taste-matched.

- **Path diversity boost.** If a post was liked by many *different* co-likers — rather than a single super-active user who liked everything — that's a stronger taste signal. We apply a mild exponent to the number of distinct co-liker paths to reward posts with broad agreement across the taste graph.

Posts also need at least some minimum number of overlapping co-likers to appear in your feed at all, as a guard against any single user's behavior contaminating your personalization.

---

## Hard limits: what data we keep and how long

The system has deliberate hard limits on how much history it retains. Likes are stored in date-partitioned Redis sorted sets with a **7-day retention window**. When computing your co-likers, we look at your most recent 500 likes (configurable) from within that 7-day window. We find up to 500 co-likers per liked post and cap the total co-liker set at 10,000 users.

These aren't arbitrary numbers — they reflect a tradeoff between graph richness and latency. A 7-day window keeps the graph fresh and means your co-likers reflect your current interests, not posts you liked two months ago that you've since forgotten about. The per-user and per-post caps mean computation stays bounded and predictable regardless of how active a user is.

When scoring, we check up to 500 candidate posts per request by default (again configurable per preset). Results are cached in Redis for five minutes, so back-to-back scrolls don't recompute from scratch every time.

---

## Cold start and the tiered fallback system

The biggest practical challenge with collaborative filtering is the cold start problem: a brand new user has no likes, so there's nothing to walk. A user with only a handful of likes has a very sparse graph.

We handle this with a **tiered fallback system** based on how many likes a user has accumulated:

- **Cold users (0–5 likes):** The feed is filled primarily with fallback content. There's not enough signal to personalize meaningfully, so we show the best of what the feed has to offer universally.

- **Warm users (6–20 likes):** A partial mix. Some personalization starts to come through, but fallback content takes up a larger portion than it would for an established user.

- **Active users (21+ likes):** Full personalization kicks in. The default configuration targets around 80% personalized content with 20% from fallback tranches.

The fallback content itself is not just one flat list. It's a blend of three distinct tranches:

- **Popular:** Posts with high total engagement, scored with a slow decay (7-day half-life). These are the feed's greatest hits.
- **Velocity:** Posts gaining engagement *rapidly right now* — measured by rate rather than total count. A post with 200 likes in two hours ranks above a post with 500 likes accumulated over a week.
- **Discovery:** Cold posts from authors who have demonstrated they produce quality content — newer posts that haven't had time to accumulate likes but are from trusted creators. This deliberately surfaces fresher material.

These three tranches are fetched concurrently and interleaved in a round-robin fashion, then staggered into the personalized posts based on a configurable blend factor. If there isn't enough personalization to fill the feed, an intermediate layer called **author affinity** kicks in first: instead of using exact post co-likers, it matches on authors you've liked, creating a coarser but denser graph that fills gaps before falling all the way back to the universal tranches.

---

## The Thompson Sampling engine

Once you have a working algorithm, the next question is: which configuration of the algorithm is *best*? The LinkLonk parameters aren't magic numbers — how many user likes to consider, how many co-likers per post, how deep to search, how aggressively to penalize popularity — all of these affect the quality of the output differently on different feeds.

We use **Thompson Sampling** to tune these parameters automatically. Thompson Sampling is a Bayesian approach to the classic "multi-armed bandit" problem: you have several choices (different parameter values), and you want to figure out which choice produces the best outcomes without wasting too much time on bad choices.

Each parameter has its own independent bandit. For a parameter like `max_user_likes`, the options might be `[100, 200, 300, 500, 750]`. Each option starts with equal probability. When we observe that a particular combination of settings produced a good result — meaning at least 60% of the feed was personalized, the candidate set was rich enough that scoring had real signal to work with, and the response came back in under 500ms — we update the probability distribution for those options upward. Bad outcomes shift probabilities down.

The key wrinkle is a **10% holdout group.** On one out of ten requests, instead of using the Thompson-sampled parameters, we use the configured defaults. This control group lets us compare the adaptive system against the baseline and measure whether the learning is actually helping. We also have a small 5% exploration rate on top of Thompson — occasionally picking parameter values at random to make sure we don't get stuck in local optima.

This means the system is continuously self-tuning in production. A feed that serves a community with different engagement patterns than the defaults will gradually adapt to what works for that specific audience, without any manual intervention.

---

## What it looks like end to end

When a user opens a Graze-powered feed, here's roughly what happens:

1. The service looks up the user's cached personalization result. If it's fresh (less than five minutes old), the cached ranking is returned immediately.
2. If the cache is stale or empty, the co-liker map is computed or retrieved from its own cache (which lasts up to six hours, invalidated immediately if you like new posts).
3. The Rust scorer walks the candidate pool, intersects each post's likers against the co-liker map, applies recency decay, popularity penalty, and path diversity boost, and produces a scored ranking.
4. If the ranking is thin, author-affinity supplementation fires to fill gaps before fallback tranches are consulted.
5. The final blended list is assembled, duplicate-deduplicated across sources, and returned to the client — along with a cryptographic proof in the feed context that lets you verify exactly which parameters produced that particular result, what fraction came from personalization versus fallback, and how the scoring was done.
6. The outcome is recorded by the Thompson Sampling engine, which updates the parameter distributions for the next request.

The whole thing typically completes in well under 200 milliseconds, and the result is something that feels like the feed actually knows you — not because it's been trained on your profile, but because it's been watching what you and people who share your taste have agreed on over the last seven days.

---

*Graze is built on the AT Protocol and is open to any Bluesky feed operator. The core algorithm is written in Rust with Axum and deadpool-redis.*
