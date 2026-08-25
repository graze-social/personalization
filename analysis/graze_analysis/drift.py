"""Density-drift guard for the per-feed personalization gate.

`MIN_POOL_SCOREABLE_SHARE` skips personalization for feeds whose candidate pool carries too little like
signal to rank. That is worth ~45% of the engine's scoring work for ~2% of its output, but it introduces
a failure mode this project has hit repeatedly in other guises: **a feed drifts under the threshold and
silently stops being personalized.** Nothing errors, nothing alerts, the feed just quietly serves
fallback forever.

Measured margins when the gate was set to 0.04:

- feed 5395 at 3.02% density — escapes gating if its density rises ~32%
- feed 2304 at 6.40% density — the lowest-density *productive* feed, starts being gated if it falls ~37%

So the corridor is narrow on both sides, and pool composition turns over continuously.

This guard reports two distinct things, and the distinction matters:

- **AT RISK** — a feed that is currently productive whose density sits within `warn_factor` of the gate.
  This is a *prediction*, and predictions in this project have a poor record.
- **NEWLY GATED** — a feed that produced personalized impressions in an earlier window, produces none
  now, and whose density is below the gate. This is a *detection*: the failure has already happened.

Both are printed, but the second is the one to act on.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class FeedDensity:
    algo: int
    post_count: int
    scoreable: int

    @property
    def share(self) -> float:
        return self.scoreable / self.post_count if self.post_count else 0.0


@dataclass(frozen=True)
class Finding:
    algo: int
    kind: str          # "at_risk" | "newly_gated"
    share: float
    gate: float
    impressions_now: int
    impressions_before: int
    detail: str


def classify(
    densities: dict[int, FeedDensity],
    impressions_now: dict[int, int],
    impressions_before: dict[int, int],
    gate: float,
    warn_factor: float = 1.5,
    min_impressions: int = 50,
) -> list[Finding]:
    """Compare published densities against the live gate and recent personalized output.

    A feed with no history is not evidence of anything, so `min_impressions` guards against calling a
    quiet feed a regression — the same small-sample trap that produced two wrong readings earlier in
    this project.
    """
    findings: list[Finding] = []
    for algo, d in sorted(densities.items()):
        now = impressions_now.get(algo, 0)
        before = impressions_before.get(algo, 0)

        # Detection first: it produced personalized items, now produces none, and is under the gate.
        if before >= min_impressions and now == 0 and d.share < gate:
            findings.append(Finding(
                algo=algo, kind="newly_gated", share=d.share, gate=gate,
                impressions_now=now, impressions_before=before,
                detail=(f"produced {before} personalized impressions in the previous window and 0 now, "
                        f"with density {d.share:.2%} below the {gate:.2%} gate — this feed has stopped "
                        f"being personalized"),
            ))
            continue

        # Prediction: still productive, but close enough to the gate that ordinary drift would cross it.
        if now >= min_impressions and gate <= d.share < gate * warn_factor:
            findings.append(Finding(
                algo=algo, kind="at_risk", share=d.share, gate=gate,
                impressions_now=now, impressions_before=before,
                detail=(f"productive ({now} personalized impressions) but density {d.share:.2%} is within "
                        f"{warn_factor:g}x of the {gate:.2%} gate — a {100*(1 - gate/d.share):.0f}% fall "
                        f"would silence it"),
            ))
    return findings


def render(findings: list[Finding], gate: float, n_feeds: int) -> str:
    if not findings:
        return (f"=== density drift guard — OK ===\n"
                f"  {n_feeds} feeds checked against gate {gate:.2%}; none newly gated, none within range")
    lines = [f"=== density drift guard — {len(findings)} FINDING(S) ==="]
    for f in sorted(findings, key=lambda x: (x.kind != "newly_gated", x.share)):
        lines.append(f"  [{f.kind.upper()}] algo {f.algo}: {f.detail}")
    return "\n".join(lines)


def _densities_from_redis(threshold: int) -> dict[int, FeedDensity]:  # pragma: no cover - needs a server
    import redis

    r = redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    out: dict[int, FeedDensity] = {}
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="am:*", count=500)
        for k in keys:
            rest = k[3:]
            if not rest.isdigit():
                continue
            m = r.hgetall(k)
            field = f"scoreable_{threshold}"
            if field not in m:
                # Sync has not published for this pool yet. Absence is not drift, so skip rather than
                # treating it as zero density — that would flag every pool on a fresh deploy.
                continue
            try:
                pc = int(m.get("post_count", 0) or 0)
                sc = int(m[field])
            except ValueError:
                continue
            if pc > 0:
                out[int(rest)] = FeedDensity(algo=int(rest), post_count=pc, scoreable=sc)
        if cursor == 0:
            return out


def _impressions(hours_back: int, hours_span: int) -> dict[int, int]:  # pragma: no cover - needs a server
    from .data import ClickHouseReader

    d = "tryBase64Decode(interaction_feed_context)"
    sql = f"""
    SELECT JSONExtractInt({d}, 'algo_id') AS algo, count() AS n
    FROM default.feed_interactions
    WHERE occurred >= now() - INTERVAL {hours_back + hours_span} HOUR
      AND occurred <  now() - INTERVAL {hours_back} HOUR
      AND interaction_feed_context != ''
      AND JSONExtractString({d}, 'source') = 'personalized'
      AND interaction_event = 'app.bsky.feed.defs#interactionSeen'
    GROUP BY algo
    """
    return {int(a): int(n) for a, n in ClickHouseReader().query(sql) if a}


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - thin CLI
    gate = float(os.environ.get("MIN_POOL_SCOREABLE_SHARE", "0.0"))
    if gate <= 0.0:
        print("=== density drift guard — SKIPPED ===\n  gate disabled (MIN_POOL_SCOREABLE_SHARE=0)")
        return 0
    threshold = int(os.environ.get("INVERTED_MIN_POST_LIKES", "10"))
    warn_factor = float(os.environ.get("DRIFT_WARN_FACTOR", "1.5"))
    span = int(os.environ.get("DRIFT_WINDOW_HOURS", "6"))

    densities = _densities_from_redis(threshold)
    now = _impressions(0, span)
    before = _impressions(span, span)
    findings = classify(densities, now, before, gate, warn_factor)
    print(render(findings, gate, len(densities)))
    # Exit non-zero only for detections, so the CronJob surfaces a real regression as a failed job while
    # a mere warning does not page anyone.
    return 1 if any(f.kind == "newly_gated" for f in findings) else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
