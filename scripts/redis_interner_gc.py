#!/usr/bin/env python3
"""Orphan GC for URI interner tables (legacy global + date-sharded).

Deletes uri2id/id2uri entries whose post IDs are not referenced by live Redis
data (ap, ul/pl retention window, caches, fallbacks, seen).

Date-sharded keys also get EXPIRE from TTL; this job cleans stale fields and
legacy global hashes that predate sharding.

Usage:
  python scripts/redis_interner_gc.py --dry-run
  python scripts/redis_interner_gc.py --execute
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import redis

RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "6"))
SCAN_COUNT = int(os.environ.get("SCAN_COUNT", "500"))
HDEL_BATCH = int(os.environ.get("HDEL_BATCH", "500"))
PRUNE_SHARD_LOOKBACK_DAYS = int(os.environ.get("PRUNE_SHARD_LOOKBACK_DAYS", "400"))

LEGACY_URI2ID = "uri2id"
LEGACY_ID2URI = "id2uri"


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
        socket_timeout=60,
    )


def retention_dates(days: int) -> list[str]:
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return [(today - timedelta(days=i)).strftime("%Y%m%d") for i in range(days)]


def scan_keys(r: redis.Redis, pattern: str):
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=SCAN_COUNT)
        for k in keys:
            yield k.decode() if isinstance(k, bytes) else k
        if cursor == 0:
            break


def collect_live_post_ids(r: redis.Redis, dates: list[str]) -> set[str]:
    live: set[str] = set()

    print("collecting live post IDs from ap:* ...", flush=True)
    for key in scan_keys(r, "ap:*"):
        if ":temp" in key:
            continue
        for m in r.sscan_iter(key, count=SCAN_COUNT):
            mid = m.decode() if isinstance(m, bytes) else m
            live.add(mid)

    print("collecting from apc:* ...", flush=True)
    for key in scan_keys(r, "apc:*"):
        if ":temp" in key:
            continue
        cursor = 0
        while True:
            cursor, data = r.hscan(key, cursor=cursor, count=SCAN_COUNT)
            for field in data:
                fid = field.decode() if isinstance(field, bytes) else field
                live.add(fid)
            if cursor == 0:
                break

    for prefix in ("ul:", "pl:", "authl:"):
        for date in dates:
            pattern = f"{prefix}*:{date}"
            n = 0
            for key in scan_keys(r, pattern):
                for m in r.zscan_iter(key, count=SCAN_COUNT):
                    if prefix == "pl:":
                        continue
                    mid = m[0].decode() if isinstance(m[0], bytes) else m[0]
                    live.add(mid)
                n += 1
            print(f"  {prefix}*{date} keys_scanned={n}", flush=True)

    for pattern in ("ll:*", "fsc:*", "seen:*"):
        print(f"collecting from {pattern} ...", flush=True)
        for key in scan_keys(r, pattern):
            t = r.type(key)
            ts = t.decode() if isinstance(t, bytes) else t
            if ts == "zset":
                for m in r.zscan_iter(key, count=SCAN_COUNT):
                    mid = m[0].decode() if isinstance(m[0], bytes) else m[0]
                    live.add(mid)
            elif ts == "list":
                for m in r.lrange(key, 0, -1):
                    mid = m.decode() if isinstance(m, bytes) else m
                    live.add(mid)

    for prefix in ("trending:", "popular:", "velocity:", "discovery:"):
        print(f"collecting from {prefix}* ...", flush=True)
        for key in scan_keys(r, f"{prefix}*"):
            if ":meta" in key:
                continue
            for m in r.zscan_iter(key, count=SCAN_COUNT):
                mid = m[0].decode() if isinstance(m[0], bytes) else m[0]
                live.add(mid)

    print(f"live post IDs collected: {len(live)}", flush=True)
    return live


def gc_hash_orphans(
    r: redis.Redis,
    uri2id_key: str,
    id2uri_key: str,
    live: set[str],
    execute: bool,
) -> tuple[int, int]:
    """Remove hash fields not in live. Returns (scanned, deleted)."""
    scanned = deleted = 0
    uri_del_batch: list[str] = []
    id_del_batch: list[str] = []

    cursor = 0
    while True:
        cursor, items = r.hscan(id2uri_key, cursor=cursor, count=SCAN_COUNT)
        for post_id, uri in items.items():
            scanned += 1
            pid = post_id.decode() if isinstance(post_id, bytes) else post_id
            if pid in live:
                continue
            u = uri.decode() if isinstance(uri, bytes) else uri
            id_del_batch.append(pid)
            uri_del_batch.append(u)
            if len(id_del_batch) >= HDEL_BATCH:
                if execute:
                    r.hdel(id2uri_key, *id_del_batch)
                    r.hdel(uri2id_key, *uri_del_batch)
                deleted += len(id_del_batch)
                id_del_batch.clear()
                uri_del_batch.clear()
                if deleted and deleted % 50000 == 0:
                    m = r.info("memory")
                    print(
                        f"  progress key={id2uri_key} deleted={deleted} "
                        f"used={m.get('used_memory_human')}",
                        flush=True,
                    )
        if cursor == 0:
            break

    if id_del_batch:
        if execute:
            r.hdel(id2uri_key, *id_del_batch)
            r.hdel(uri2id_key, *uri_del_batch)
        deleted += len(id_del_batch)

    return scanned, deleted


def shard_dates_to_gc(dates_retention: list[str]) -> list[str]:
    """Shard dates to scan: retention window + older shards for orphan cleanup."""
    cutoff = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = cutoff - timedelta(days=PRUNE_SHARD_LOOKBACK_DAYS)
    out: set[str] = set(dates_retention)
    d = start
    while d < cutoff:
        out.add(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return sorted(out)


def main() -> int:
    parser = argparse.ArgumentParser(description="Orphan GC for URI interner")
    parser.add_argument("--execute", action="store_true", help="Apply HDEL (default dry-run)")
    args = parser.parse_args()
    execute = args.execute

    r = connect()
    info = r.info("memory")
    print(f"mode={'EXECUTE' if execute else 'DRY-RUN'} retention_days={RETENTION_DAYS}", flush=True)
    print(
        f"used_memory={info.get('used_memory_human')} maxmemory={info.get('maxmemory_human')}",
        flush=True,
    )

    dates = retention_dates(RETENTION_DAYS)
    live = collect_live_post_ids(r, dates)

    total_scanned = 0
    total_deleted = 0

    if r.exists(LEGACY_ID2URI):
        print("GC legacy global id2uri/uri2id ...", flush=True)
        s, d = gc_hash_orphans(r, LEGACY_URI2ID, LEGACY_ID2URI, live, execute)
        print(f"  legacy scanned={s} orphans={d}", flush=True)
        total_scanned += s
        total_deleted += d

    for date in shard_dates_to_gc(dates):
        id2uri = f"id2uri:{date}"
        uri2id = f"uri2id:{date}"
        if not r.exists(id2uri):
            continue
        print(f"GC shard {date} ...", flush=True)
        s, d = gc_hash_orphans(r, uri2id, id2uri, live, execute)
        print(f"  shard {date} scanned={s} orphans={d}", flush=True)
        total_scanned += s
        total_deleted += d
        time.sleep(0.01)

    info2 = r.info("memory")
    print(
        f"done scanned={total_scanned} deleted={total_deleted} "
        f"used_memory_now={info2.get('used_memory_human')}",
        flush=True,
    )
    if not execute:
        print("Re-run with --execute to HDEL orphans.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
