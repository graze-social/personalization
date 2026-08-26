#!/usr/bin/env python3
"""Prune Redis keys to free memory for personalization layer.

Safe tiers (see --help). Default is dry-run.

Date-partitioned like graph keys older than RETENTION_DAYS are removed;
legacy ul:/pl:/authl: day-tranche and undated keys are removed (no longer read).
Caches and queues are optional tiers.

Usage:
  python scripts/redis_prune_retention.py --dry-run
  python scripts/redis_prune_retention.py --execute --tier caches --tier legacy --tier old-dates
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import redis

RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "6"))
SCAN_COUNT = int(os.environ.get("SCAN_COUNT", "500"))
UNLINK_BATCH = int(os.environ.get("UNLINK_BATCH", "500"))

DATE_SUFFIX = re.compile(r"^(\d{8})$")
DAY_SUFFIX = re.compile(r"^d[0-7]$")

# Prefixes where the last :segment may be YYYYMMDD
DATE_PREFIXES = ("ul:", "pl:", "authl:")

# Entire key patterns safe to delete (rebuilt or ephemeral)
CACHE_PREFIXES = (
    "fsc:",
    "ll:",
    "colikes:",
    "colikes:meta:",
    "colikes:ts:",
    "acolikes:",
    "acolikes:meta:",
    "seen:",
)

CACHE_EXACT = (
    # co-liker / author-affinity meta keys use prefixes above
)

QUEUE_EXACT = ("log_tasks", "feed_requests", "pending:syncs", "queue:sync")

LEGACY_SCAN = (
    "ul:*:d*",
    "pl:*:d*",
    "authl:*:d*",
)


def parse_date(s: str) -> datetime | None:
    try:
        return datetime.strptime(s, "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def cutoff_date() -> datetime:
    # Keep today and previous (RETENTION_DAYS - 1) full days = RETENTION_DAYS dates
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return today - timedelta(days=RETENTION_DAYS)


def classify_date_key(key: str) -> str | None:
    """Return 'keep', 'delete', or None if not a date-partitioned like key."""
    for prefix in DATE_PREFIXES:
        if not key.startswith(prefix):
            continue
        parts = key.split(":")
        if len(parts) < 3:
            return "delete"  # undated legacy ul:hash
        suffix = parts[-1]
        if DAY_SUFFIX.match(suffix):
            return "delete"
        m = DATE_SUFFIX.match(suffix)
        if m:
            d = parse_date(m.group(1))
            if d is None:
                return None
            if d < cutoff_date():
                return "delete"
            return "keep"
        return None
    return None


def connect() -> redis.Redis:
    url = os.environ.get("REDIS_URL")
    if not url:
        raise SystemExit("REDIS_URL not set")
    u = urlparse(url)
    return redis.Redis(
        host=u.hostname,
        port=u.port,
        password=u.password,
        username=u.username or None,
        ssl=url.startswith("rediss"),
        ssl_cert_reqs=None,
        socket_timeout=30,
        socket_connect_timeout=15,
    )


def scan_keys(r: redis.Redis, pattern: str):
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=SCAN_COUNT)
        for k in keys:
            yield k.decode() if isinstance(k, bytes) else k
        if cursor == 0:
            break


def unlink_batches(r: redis.Redis, keys: list[str], execute: bool) -> int:
    if not keys:
        return 0
    if not execute:
        return len(keys)
    deleted = 0
    for i in range(0, len(keys), UNLINK_BATCH):
        batch = keys[i : i + UNLINK_BATCH]
        try:
            deleted += r.unlink(*batch)
        except redis.ResponseError:
            deleted += r.delete(*batch)
        time.sleep(0.02)
    return deleted


def scan_and_prune(
    r: redis.Redis, pattern: str, execute: bool, batch: list[str] | None = None
) -> int:
    """Scan keys matching pattern; count or UNLINK in batches (low memory)."""
    if batch is None:
        batch = []
    total = 0
    for k in scan_keys(r, pattern):
        total += 1
        if not execute:
            continue
        batch.append(k)
        if len(batch) >= UNLINK_BATCH:
            unlink_batches(r, batch, True)
            batch.clear()
            if total % 50000 == 0:
                m = r.info("memory")
                print(
                    f"progress pattern={pattern} keys_seen={total} "
                    f"used={m.get('used_memory_human')}",
                    flush=True,
                )
    if execute and batch:
        unlink_batches(r, batch, True)
        batch.clear()
    return total


def scan_and_prune_classified(
    r: redis.Redis,
    pattern: str,
    execute: bool,
    batch: list[str],
    should_delete,
) -> int:
    """One keyspace pass: only UNLINK keys where should_delete(key) is true."""
    seen = matched = 0
    for k in scan_keys(r, pattern):
        seen += 1
        if not should_delete(k):
            continue
        matched += 1
        if not execute:
            continue
        batch.append(k)
        if len(batch) >= UNLINK_BATCH:
            unlink_batches(r, batch, True)
            batch.clear()
        if matched % 50000 == 0:
            m = r.info("memory")
            print(
                f"progress pattern={pattern} scanned={seen} matched={matched} "
                f"used={m.get('used_memory_human')}",
                flush=True,
            )
    if execute and batch:
        unlink_batches(r, batch, True)
        batch.clear()
    print(
        f"finished pattern={pattern} scanned={seen} matched={matched}",
        flush=True,
    )
    return matched if execute else seen


def main() -> int:
    parser = argparse.ArgumentParser(description="Prune personalization Redis keys")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually UNLINK keys (default: dry-run report only)",
    )
    parser.add_argument(
        "--tier",
        action="append",
        choices=["old-dates", "legacy", "caches", "queues"],
        help="Tiers to process (default: all)",
    )
    args = parser.parse_args()
    tiers = set(args.tier or ["old-dates", "legacy", "caches", "queues"])
    execute = args.execute
    dry = not execute

    r = connect()
    info = r.info("memory")
    print(f"mode={'DRY-RUN' if dry else 'EXECUTE'} retention_days={RETENTION_DAYS}", flush=True)
    print(f"cutoff_delete_before={cutoff_date().strftime('%Y-%m-%d')} (UTC)", flush=True)
    print(
        f"used_memory_human={info.get('used_memory_human')} "
        f"maxmemory={info.get('maxmemory_human', info.get('maxmemory'))}",
        flush=True,
    )
    print(f"tiers={sorted(tiers)}", flush=True)

    stats: dict[str, int] = {}
    batch: list[str] = []

    def flush_batch():
        if batch and execute:
            unlink_batches(r, batch, True)
            batch.clear()

    if "queues" in tiers:
        n = 0
        for q in QUEUE_EXACT:
            if r.exists(q):
                n += 1
                if execute:
                    r.delete(q)
                    print(f"deleted queue key {q}", flush=True)
        stats["queues"] = n
        m = r.info("memory")
        print(f"after queues used={m.get('used_memory_human')}", flush=True)

    if "caches" in tiers:
        n = 0
        for p in CACHE_PREFIXES:
            print(f"scanning {p}*", flush=True)
            n += scan_and_prune(r, f"{p}*", execute, batch)
        flush_batch()
        stats["caches"] = n
        m = r.info("memory")
        print(f"after caches used={m.get('used_memory_human')} count={n}", flush=True)

    if "legacy" in tiers:
        n = 0
        for pattern in LEGACY_SCAN:
            n += scan_and_prune(r, pattern, execute, batch)
        flush_batch()
        stats["legacy"] = n

    if "old-dates" in tiers:
        cutoff = cutoff_date()
        print(
            f"old-dates single-pass scan, delete before {cutoff.strftime('%Y%m%d')} (UTC)",
            flush=True,
        )

        def old_date_delete(key: str) -> bool:
            return classify_date_key(key) == "delete"

        n = 0
        for prefix in DATE_PREFIXES:
            print(f"scanning {prefix}* (classify by suffix)", flush=True)
            n += scan_and_prune_classified(
                r, f"{prefix}*", execute, batch, old_date_delete
            )
        stats["old-dates"] = n
        m = r.info("memory")
        print(f"after old-dates used={m.get('used_memory_human')} matched={n}", flush=True)

    print("categories:", stats, flush=True)
    if dry:
        print(f"keys_matched={sum(stats.values())} (dry-run, nothing deleted)", flush=True)
        return 0

    info2 = r.info("memory")
    print(
        f"done keys_processed={sum(stats.values())} "
        f"used_memory_human_now={info2.get('used_memory_human')}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
