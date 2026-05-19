#!/usr/bin/env python3
import os
from urllib.parse import urlparse

import redis

u = urlparse(os.environ["REDIS_URL"])
r = redis.Redis(
    host=u.hostname,
    port=u.port,
    password=u.password,
    username=u.username or None,
    ssl=u.scheme == "rediss",
    ssl_cert_reqs=None,
    socket_timeout=60,
)
m = r.info("memory")
print("used", m.get("used_memory_human"), "max", m.get("maxmemory_human"))
print("mem_fragmentation", m.get("mem_fragmentation_ratio"))
for k in [
    "log_tasks",
    "feed_requests",
    "pending:syncs",
    "queue:sync",
    "uri2id",
    "id2uri",
    "supported_feeds",
]:
    t = r.type(k)
    ts = t.decode() if isinstance(t, bytes) else t
    if ts == "none":
        print(k, "missing")
    elif ts == "list":
        print(k, "list", r.llen(k))
    elif ts == "hash":
        print(k, "hash", r.hlen(k))
    else:
        print(k, ts)

shard_fields = 0
shard_keys = 0
for key in r.scan_iter(match="uri2id:*", count=500):
    shard_keys += 1
    shard_fields += r.hlen(key)
print("uri2id:* shards", shard_keys, "fields", shard_fields)
