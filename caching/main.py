"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                         CACHING (REDIS) SUITE                                ║
║                    All Concepts — Main Runner                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WHAT IS CACHING?                                                            ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Caching stores the result of an EXPENSIVE OPERATION in a FAST STORAGE      ║
║  layer so that future requests get the result INSTANTLY without repeating    ║
║  the expensive operation (DB query, API call, computation).                  ║
║                                                                              ║
║  REDIS: Remote Dictionary Server                                             ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Redis is an in-memory data structure store that can be used as:            ║
║    • Cache (with TTL)                                                        ║
║    • Message broker (Pub/Sub, Streams)                                       ║
║    • Rate limiter (INCR + EXPIRE)                                            ║
║    • Distributed lock (SETNX + EXPIRE)                                      ║
║    • Leaderboard (Sorted Set)                                                ║
║    • Session store                                                           ║
║                                                                              ║
║  KEY METRICS:                                                                ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Hit Ratio = Cache Hits / (Cache Hits + Cache Misses)                       ║
║  Aim for > 90% hit ratio in production                                       ║
║                                                                              ║
║  MODULES:                                                                    ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  eviction_policies.py   → LRU, LFU, FIFO, TTL eviction algorithms          ║
║  cache_patterns.py      → Cache-Aside, Read/Write-Through, Write-Behind     ║
║  redis_data_structures.py → String, Hash, List, Set, Sorted Set             ║
║  cache_stampede.py      → Mutex, Probabilistic, Background Refresh          ║
║                                                                              ║
║  RUN INDIVIDUAL MODULES:                                                     ║
║    python eviction_policies.py                                               ║
║    python cache_patterns.py                                                  ║
║    python redis_data_structures.py                                           ║
║    python cache_stampede.py                                                  ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from eviction_policies import LRUCache, LFUCache, FIFOCache, TTLCache
from cache_patterns import (
    SimpleCache, SimulatedDatabase,
    CacheAsidePattern, ReadThroughPattern, WriteThroughPattern
)
from redis_data_structures import (
    RedisString, RedisHash, RedisList, RedisSet, RedisZSet
)


def section(title: str):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print(f"{'═' * 60}")


def subsection(title: str):
    print(f"\n  ─── {title} ───")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 1: EVICTION POLICIES
# ─────────────────────────────────────────────────────────────────────────────
def demo_eviction_policies():
    section("MODULE 1: EVICTION POLICIES")
    print("  Concept: When cache is full, WHICH item gets removed?")

    subsection("LRU (Least Recently Used) — capacity=3")
    lru = LRUCache(3)
    for k, v in [("a", 1), ("b", 2), ("c", 3)]:
        lru.put(k, v)
    lru.get("a")                     # Access 'a' → now MRU
    evicted = lru.put("d", 4)        # 'b' should be evicted (LRU)
    print(f"  After PUT(d): evicted={evicted} (b was least recently used)")
    print(f"  {lru}")
    print(f"  Stats: {lru.stats()}")

    subsection("LFU (Least Frequently Used) — capacity=3")
    lfu = LFUCache(3)
    for k, v in [("x", 10), ("y", 20), ("z", 30)]:
        lfu.put(k, v)
    for _ in range(5): lfu.get("x")  # x accessed 5 times
    for _ in range(2): lfu.get("y")  # y accessed 2 times
    # z accessed 0 times (1 at insert = min_freq=1)
    evicted = lfu.put("w", 40)
    print(f"  After PUT(w): evicted={evicted} (z had lowest frequency)")
    print(f"  Frequencies: {lfu.get_frequencies()}")

    subsection("TTL Cache — 2s default TTL")
    ttl = TTLCache(capacity=5, default_ttl=2.0)
    ttl.put("otp:123", "456789", ttl=1.0)
    ttl.put("session:1", {"user": "Alice"}, ttl=5.0)
    print(f"  GET otp:123 → {ttl.get('otp:123')}")
    print(f"  Sleeping 1.2s (otp should expire)...")
    time.sleep(1.2)
    print(f"  GET otp:123 → {'HIT' if ttl.get('otp:123') else 'MISS (expired)'}")
    print(f"  GET session:1 → {'HIT' if ttl.get('session:1') else 'MISS'}")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 2: CACHE PATTERNS
# ─────────────────────────────────────────────────────────────────────────────
def demo_cache_patterns():
    section("MODULE 2: CACHE PATTERNS")
    print("  Concept: How do cache and database stay in sync?")

    DB_MS = 50   # 50ms simulated DB latency

    subsection("Cache-Aside (most common)")
    cache = SimpleCache()
    db = SimulatedDatabase(latency_ms=DB_MS)
    ca = CacheAsidePattern(cache, db)

    t0 = time.time()
    ca.read("user:1")   # Miss → DB
    miss_ms = (time.time() - t0) * 1000

    t0 = time.time()
    ca.read("user:1")   # Hit → cache
    hit_ms = (time.time() - t0) * 1000

    print(f"  First request (miss): {miss_ms:.0f}ms | Second (hit): {hit_ms:.1f}ms")
    print(f"  Speed improvement: {miss_ms/max(hit_ms,0.01):.0f}x faster")
    print(f"  DB Stats: {db.stats()}")

    subsection("Write-Through (strong consistency)")
    cache3 = SimpleCache()
    db3 = SimulatedDatabase(latency_ms=DB_MS)
    wt = WriteThroughPattern(cache3, db3)
    wt.write("user:2", {"id": 2, "name": "Bob Admin", "role": "admin"})
    val = wt.read("user:2")
    print(f"  After write-through: {val}")
    print(f"  Cache and DB always in sync (strong consistency)")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 3: REDIS DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────
def demo_data_structures():
    section("MODULE 3: REDIS DATA STRUCTURES")
    print("  Concept: Redis is NOT just key-value. Each structure is purpose-built.")

    subsection("STRING — Distributed Counter + Lock")
    s = RedisString()
    s.set("api_calls:user42", 0)
    for _ in range(5):
        s.incr("api_calls:user42")
    print(f"  INCR 5x → {s.get('api_calls:user42')}")
    print(f"  SETNX lock:resource → {s.set('lock:resource', 'worker1', nx=True)}")
    print(f"  SETNX lock:resource → {s.set('lock:resource', 'worker2', nx=True)} (blocked!)")

    subsection("HASH — User Profile (vs JSON string)")
    h = RedisHash()
    h.hmset("user:42", {"name": "Alice", "email": "alice@ex.com", "score": 100})
    print(f"  HGETALL → {h.hgetall('user:42')}")
    h.hincrby("user:42", "score", 50)
    print(f"  HINCRBY score +50 → {h.hget('user:42', 'score')} (only field updated, not whole JSON)")

    subsection("LIST — Message Queue (RPUSH + LPOP)")
    lst = RedisList()
    for job in ["email_job_1", "email_job_2", "sms_job_1"]:
        lst.rpush("queue:notifications", job)
    print(f"  Queue: {lst.lrange('queue:notifications', 0, -1)}")
    print(f"  LPOP (FIFO dequeue): {lst.lpop('queue:notifications')}")

    subsection("SET — Unique Visitors + Social Graph")
    st = RedisSet()
    # Unique visitors (duplicates auto-ignored)
    st.sadd("visitors:2026-08-12", "user_1", "user_2", "user_3", "user_1", "user_2")
    print(f"  Unique visitors: {st.scard('visitors:2026-08-12')} (5 adds, 3 unique)")
    # Mutual friends
    st.sadd("following:alice", "bob", "carol", "dave")
    st.sadd("following:bob", "carol", "eve", "alice")
    print(f"  Alice & Bob mutual follows: {st.sinter('following:alice', 'following:bob')}")

    subsection("SORTED SET — Leaderboard")
    z = RedisZSet()
    players = [("alice", 9500), ("bob", 8700), ("carol", 9800), ("dave", 7200)]
    for name, score in players:
        z.zadd("leaderboard", score, name)
    top3 = z.zrevrange("leaderboard", 0, 2, withscores=True)
    print(f"  Top 3: {top3}")
    z.zincrby("leaderboard", 200, "bob")
    print(f"  After bob +200: rank={z.zrevrank('leaderboard', 'bob')+1} (from bottom)")


# ─────────────────────────────────────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    section("CACHING CONCEPTS — FULL SUMMARY")
    print()
    print("  EVICTION POLICIES:")
    eviction = [
        ("LRU",  "Evict least recently accessed", "Sessions, API cache"),
        ("LFU",  "Evict least frequently used",   "Hot-spot / trending"),
        ("FIFO", "Evict oldest inserted",          "Streaming buffers"),
        ("TTL",  "Evict expired entries",          "OTPs, session tokens"),
    ]
    for r in eviction:
        print(f"    {r[0]:<6} → {r[1]:<33} | Best for: {r[2]}")

    print("\n  CACHE PATTERNS:")
    patterns = [
        ("Cache-Aside",    "App manages both",           "Eventual",  "Most web apps"),
        ("Read-Through",   "Cache fetches from DB",      "Eventual",  "ORM caching"),
        ("Write-Through",  "Write to both sync",         "Strong",    "Bank data"),
        ("Write-Behind",   "Write to cache, async DB",   "Eventual",  "Analytics, logs"),
    ]
    for r in patterns:
        print(f"    {r[0]:<16} {r[1]:<26} {r[2]:<10} | {r[3]}")

    print("\n  REDIS DATA STRUCTURES:")
    structs = [
        ("STRING", "SET/GET/INCR/SETNX",       "Counters, locks, tokens"),
        ("HASH",   "HSET/HGET/HGETALL",         "User objects, config"),
        ("LIST",   "RPUSH/LPOP/LRANGE",         "Queues, feeds"),
        ("SET",    "SADD/SINTER/SDIFF",         "Unique items, social graph"),
        ("ZSET",   "ZADD/ZREVRANGE/ZINCRBY",    "Leaderboards, rate limiting"),
    ]
    for r in structs:
        print(f"    {r[0]:<8} {r[1]:<26} | {r[2]}")

    print("\n  STAMPEDE PREVENTION:")
    strats = [
        ("Mutex Lock",    "One thread queries, rest wait",  "Guaranteed 1 DB hit"),
        ("Probabilistic", "XFetch early recompute",         "Lock-free, distributed"),
        ("BG Refresh",    "Serve stale + async refresh",    "Zero user latency"),
    ]
    for r in strats:
        print(f"    {r[0]:<16} {r[1]:<32} | {r[2]}")

    print(f"\n  Run individual files for in-depth demos:")
    for f in ["eviction_policies.py", "cache_patterns.py",
              "redis_data_structures.py", "cache_stampede.py"]:
        print(f"    python {f}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("       CACHING (REDIS) — All Concepts Demo")
    print("=" * 60)

    demo_eviction_policies()
    demo_cache_patterns()
    demo_data_structures()
    print_summary()
