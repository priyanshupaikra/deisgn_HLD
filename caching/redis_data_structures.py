"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       CACHING (REDIS) — MODULE 3                             ║
║                      REDIS DATA STRUCTURES                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
Redis is NOT just a key-value store. It provides RICH DATA STRUCTURES,
each optimized for specific use cases. Using the right structure is critical
for performance and correct semantics.

Redis Data Structures and their internal implementations:
─────────────────────────────────────────────────────────
  1. STRING  → Raw bytes, integers, floats         [Use: counters, tokens, flags]
  2. HASH    → Dict of field → value               [Use: user objects, config]
  3. LIST    → Doubly linked list of strings        [Use: queues, activity feeds]
  4. SET     → Unique unordered collection          [Use: tags, unique visitors]
  5. ZSET    → Sorted set (score + unique member)   [Use: leaderboards, rate limits]

WHY REDIS STRUCTURES MATTER:
─────────────────────────────
  WRONG: Store user as JSON string → must parse entire JSON to change one field
  RIGHT: Store user as HASH        → update single field with HSET in O(1)

  WRONG: Use STRING for a leaderboard → must read ALL, sort in app code
  RIGHT: Use ZSET                     → Redis maintains sorted order natively

REDIS INTERNAL ENCODINGS (for memory efficiency):
───────────────────────────────────────────────────
  HASH  → ziplist (< 128 fields, < 64 bytes each) OR hashtable
  LIST  → listpack OR quicklist (doubly-linked list of ziplists)
  SET   → intset (all integers) OR listpack OR hashtable
  ZSET  → listpack (small) OR skiplist + hashtable (large)

  Redis automatically switches encoding based on size thresholds.
  This is why Redis can store millions of small objects very efficiently.

"""

import time
import heapq
from typing import Any, Optional
from collections import defaultdict


# ─────────────────────────────────────────────────────────────────────────────
# STRUCTURE 1: STRING
# ─────────────────────────────────────────────────────────────────────────────
class RedisString:
    """
    Redis STRING — the most basic data structure.

    Can store:
      - Text strings (JSON, serialized objects)
      - Integers (supports atomic INCR/DECR without race conditions)
      - Floats (INCRBYFLOAT)
      - Binary data (images, serialized protobuf, etc.)

    KEY COMMANDS:
      SET key value [EX seconds]   → Set with optional TTL
      GET key                      → Get value
      INCR key                     → Atomic increment by 1
      INCRBY key amount            → Atomic increment by N
      SETNX key value              → Set only if Not eXists (distributed lock!)
      GETSET key value             → Atomic get old + set new
      APPEND key value             → Append to existing string
      STRLEN key                   → Length of string value

    USE CASES:
      - Session tokens: SET session:abc123 {user_data} EX 1800
      - Rate limit counters: INCR api_calls:user:42
      - Distributed locks: SETNX lock:resource "client_id" EX 30
      - Feature flags: SET feature:dark_mode "enabled"
      - Cached HTML fragments: SET page:home "<html>...</html>" EX 300
    """

    def __init__(self):
        self._store: dict[str, Any] = {}
        self._expires: dict[str, float] = {}  # key → expiry timestamp

    def _is_expired(self, key: str) -> bool:
        return key in self._expires and time.time() > self._expires[key]

    def _check(self, key: str) -> bool:
        """Returns False if key exists and is not expired."""
        if self._is_expired(key):
            del self._store[key]
            del self._expires[key]
            return False
        return key in self._store

    def set(self, key: str, value: Any, ex: float = None, nx: bool = False) -> bool:
        """
        SET key value [EX seconds] [NX]
        :param ex: Expiry in seconds
        :param nx: Only set if key does NOT exist (SETNX semantics)
        :return:   True if set, False if NX condition failed
        """
        if nx and self._check(key):
            return False  # Key exists, NX refuses to overwrite
        self._store[key] = value
        if ex:
            self._expires[key] = time.time() + ex
        elif key in self._expires:
            del self._expires[key]  # Clear any previous TTL
        return True

    def get(self, key: str) -> Optional[Any]:
        """GET key → Returns value or None."""
        if not self._check(key):
            return None
        return self._store.get(key)

    def incr(self, key: str, by: int = 1) -> int:
        """
        INCR/INCRBY key [amount]
        Atomically increments an integer value.
        Creates key with value 0 then increments if key doesn't exist.
        """
        current = self._store.get(key, 0)
        if not isinstance(current, (int, float)):
            raise ValueError(f"Value at '{key}' is not an integer")
        new_val = int(current) + by
        self._store[key] = new_val
        return new_val

    def ttl(self, key: str) -> float:
        """TTL key → Remaining seconds. -1=no TTL, -2=doesn't exist."""
        if not self._check(key):
            return -2
        if key not in self._expires:
            return -1
        return max(0, round(self._expires[key] - time.time(), 2))

    def exists(self, key: str) -> bool:
        return self._check(key)

    def delete(self, *keys) -> int:
        """DEL key [key ...] → Returns count of deleted keys."""
        count = 0
        for key in keys:
            if self._check(key):
                del self._store[key]
                self._expires.pop(key, None)
                count += 1
        return count


# ─────────────────────────────────────────────────────────────────────────────
# STRUCTURE 2: HASH
# ─────────────────────────────────────────────────────────────────────────────
class RedisHash:
    """
    Redis HASH — a dictionary of field → value pairs under a single key.

    Think of it as a Redis key that holds its OWN mini key-value store.

    KEY COMMANDS:
      HSET key field value         → Set one field
      HMSET key f1 v1 f2 v2 ...   → Set multiple fields at once
      HGET key field               → Get one field's value
      HMGET key f1 f2 ...          → Get multiple fields
      HGETALL key                  → Get ALL field-value pairs
      HDEL key field [field ...]   → Delete field(s)
      HINCRBY key field amount     → Atomic increment of numeric field
      HEXISTS key field            → Check if field exists
      HKEYS key                    → Get all field names
      HVALS key                    → Get all values
      HLEN key                     → Number of fields

    USE CASES:
      - User profile: HSET user:42 name "Alice" email "a@b.com" age 30
      - Shopping cart: HSET cart:user42 product:1 2 product:3 5
        (field=product_id, value=quantity)
      - Configuration: HSET config:app max_connections 100 debug false
      - Session data: HSET session:abc user_id 42 last_seen "2026-08-12"

    WHY HASH OVER STRING (JSON)?
      JSON string: GET/SET entire blob → O(n) parse to read ONE field
      HASH: HGET user:42 name         → O(1) field access, no parse needed
      HASH: HSET user:42 name "Bob"   → Update ONE field, rest unchanged
    """

    def __init__(self):
        self._store: dict[str, dict[str, Any]] = defaultdict(dict)

    def hset(self, key: str, field: str, value: Any) -> int:
        """HSET key field value → 1 if new field, 0 if updated."""
        is_new = field not in self._store[key]
        self._store[key][field] = value
        return 1 if is_new else 0

    def hmset(self, key: str, mapping: dict) -> None:
        """HMSET key field1 value1 field2 value2 ..."""
        self._store[key].update(mapping)

    def hget(self, key: str, field: str) -> Optional[Any]:
        """HGET key field → value or None."""
        return self._store.get(key, {}).get(field)

    def hmget(self, key: str, *fields) -> list:
        """HMGET key f1 f2 ... → [v1, v2, ...] (None for missing fields)."""
        h = self._store.get(key, {})
        return [h.get(f) for f in fields]

    def hgetall(self, key: str) -> dict:
        """HGETALL key → {field: value, ...}"""
        return dict(self._store.get(key, {}))

    def hdel(self, key: str, *fields) -> int:
        """HDEL key field [field...] → count of deleted fields."""
        count = 0
        for f in fields:
            if f in self._store.get(key, {}):
                del self._store[key][f]
                count += 1
        return count

    def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        """HINCRBY key field amount → atomically increment numeric field."""
        current = self._store[key].get(field, 0)
        new_val = int(current) + amount
        self._store[key][field] = new_val
        return new_val

    def hlen(self, key: str) -> int:
        """HLEN key → number of fields in the hash."""
        return len(self._store.get(key, {}))

    def hexists(self, key: str, field: str) -> bool:
        """HEXISTS key field → True/False."""
        return field in self._store.get(key, {})


# ─────────────────────────────────────────────────────────────────────────────
# STRUCTURE 3: LIST
# ─────────────────────────────────────────────────────────────────────────────
class RedisList:
    """
    Redis LIST — a doubly-linked list of strings.

    O(1) push/pop from BOTH ends.
    O(n) access by index.

    KEY COMMANDS:
      LPUSH key val [val ...]  → Push to LEFT (head)
      RPUSH key val [val ...]  → Push to RIGHT (tail)
      LPOP key [count]         → Remove and return from LEFT
      RPOP key [count]         → Remove and return from RIGHT
      LRANGE key start stop    → Get elements by index range
      LLEN key                 → Length of list
      LINDEX key idx           → Get element by index
      LSET key idx val         → Set element by index
      LTRIM key start stop     → Keep only elements in range
      LINSERT key BEFORE/AFTER pivot val

    USE CASES:
      Stack (LIFO):  LPUSH + LPOP  (or RPUSH + RPOP)
      Queue (FIFO):  RPUSH + LPOP  (or LPUSH + RPOP)
      ─────────────────────────────────────────────────
      - Message queue:     RPUSH queue:emails "email_job_data"
                           LPOP queue:emails
      - Activity feed:     LPUSH feed:user42 "liked post 99"
                           LRANGE feed:user42 0 49  (latest 50 activities)
      - Recent searches:   LPUSH searches:user42 "redis tutorial"
                           LTRIM searches:user42 0 9  (keep only last 10)
    """

    def __init__(self):
        self._store: dict[str, list] = defaultdict(list)

    def lpush(self, key: str, *values) -> int:
        """LPUSH key val → Push to front. Returns new length."""
        for v in values:
            self._store[key].insert(0, v)
        return len(self._store[key])

    def rpush(self, key: str, *values) -> int:
        """RPUSH key val → Push to back. Returns new length."""
        for v in values:
            self._store[key].append(v)
        return len(self._store[key])

    def lpop(self, key: str, count: int = 1) -> Any:
        """LPOP key [count] → Pop from front."""
        lst = self._store.get(key, [])
        if not lst:
            return None
        if count == 1:
            return lst.pop(0)
        result = lst[:count]
        self._store[key] = lst[count:]
        return result

    def rpop(self, key: str, count: int = 1) -> Any:
        """RPOP key [count] → Pop from back."""
        lst = self._store.get(key, [])
        if not lst:
            return None
        if count == 1:
            return lst.pop()
        result = lst[-count:][::-1]
        self._store[key] = lst[:-count]
        return result

    def lrange(self, key: str, start: int, stop: int) -> list:
        """LRANGE key start stop → Slice of list (inclusive, supports -1 = last)."""
        lst = self._store.get(key, [])
        if stop == -1:
            return lst[start:]
        return lst[start:stop + 1]

    def llen(self, key: str) -> int:
        """LLEN key → List length."""
        return len(self._store.get(key, []))

    def ltrim(self, key: str, start: int, stop: int) -> None:
        """LTRIM key start stop → Keep only elements in range."""
        lst = self._store.get(key, [])
        self._store[key] = lst[start:stop + 1 if stop != -1 else None]


# ─────────────────────────────────────────────────────────────────────────────
# STRUCTURE 4: SET
# ─────────────────────────────────────────────────────────────────────────────
class RedisSet:
    """
    Redis SET — unordered collection of UNIQUE strings.

    KEY COMMANDS:
      SADD key member [member ...] → Add member(s)
      SREM key member [member ...] → Remove member(s)
      SISMEMBER key member         → Check membership
      SMEMBERS key                 → Get all members
      SCARD key                    → Count of members
      SUNION key [key ...]         → Union of multiple sets
      SINTER key [key ...]         → Intersection of multiple sets
      SDIFF key [key ...]          → Difference of sets
      SRANDMEMBER key [count]      → Random member(s)

    USE CASES:
      - Unique visitors: SADD visitors:2026-08-12 user_42 user_99 user_42
        → Only 2 unique entries (duplicates ignored)
      - User tags: SADD tags:user42 "python" "redis" "system-design"
      - Friend lists / follow sets:
          SADD following:alice bob carol
          SINTER following:alice following:bob  → mutual friends
      - Deduplication: track which emails were already sent
      - Online users: SADD online_users "user_42" (SREM on disconnect)
    """

    def __init__(self):
        self._store: dict[str, set] = defaultdict(set)

    def sadd(self, key: str, *members) -> int:
        """SADD key member [...] → number of NEW members added."""
        before = len(self._store[key])
        self._store[key].update(members)
        return len(self._store[key]) - before

    def srem(self, key: str, *members) -> int:
        """SREM key member [...] → number of members removed."""
        removed = 0
        for m in members:
            if m in self._store[key]:
                self._store[key].discard(m)
                removed += 1
        return removed

    def sismember(self, key: str, member: Any) -> bool:
        """SISMEMBER key member → True if member in set."""
        return member in self._store.get(key, set())

    def smembers(self, key: str) -> set:
        """SMEMBERS key → all members (as a Python set)."""
        return set(self._store.get(key, set()))

    def scard(self, key: str) -> int:
        """SCARD key → cardinality (count)."""
        return len(self._store.get(key, set()))

    def sunion(self, *keys) -> set:
        """SUNION key1 key2 ... → union of all sets."""
        result = set()
        for k in keys:
            result |= self._store.get(k, set())
        return result

    def sinter(self, *keys) -> set:
        """SINTER key1 key2 ... → intersection (common members)."""
        if not keys:
            return set()
        result = set(self._store.get(keys[0], set()))
        for k in keys[1:]:
            result &= self._store.get(k, set())
        return result

    def sdiff(self, *keys) -> set:
        """SDIFF key1 key2 ... → members in key1 NOT in other keys."""
        if not keys:
            return set()
        result = set(self._store.get(keys[0], set()))
        for k in keys[1:]:
            result -= self._store.get(k, set())
        return result


# ─────────────────────────────────────────────────────────────────────────────
# STRUCTURE 5: SORTED SET (ZSET)
# ─────────────────────────────────────────────────────────────────────────────
class RedisZSet:
    """
    Redis SORTED SET (ZSET) — unique members, each with a floating-point SCORE.
    Members are always ordered by score (low to high).

    Internal implementation: SKIPLIST + HASHTABLE
      - O(log N) ZADD / ZREM / ZINCRBY
      - O(log N + M) range queries (M = elements returned)
      - O(1) ZSCORE lookup (via hashtable)

    KEY COMMANDS:
      ZADD key score member [...]  → Add member with score
      ZREM key member [member ...] → Remove member(s)
      ZSCORE key member            → Get member's score
      ZINCRBY key amount member    → Increment score atomically
      ZRANK key member             → 0-based rank (low-score first)
      ZREVRANK key member          → Rank from high score
      ZRANGE key start stop [WITHSCORES]      → By rank, ascending
      ZREVRANGE key start stop [WITHSCORES]   → By rank, descending
      ZRANGEBYSCORE key min max    → By score range

    USE CASES:
      - Leaderboard: ZADD leaderboard 9500 "alice" 8700 "bob"
                     ZREVRANGE leaderboard 0 9 WITHSCORES  → top 10
      - Rate limiting (sliding window): ZADD requests:user42 <timestamp> <uuid>
                                        ZREMRANGEBYSCORE requests:user42 0 <old_ts>
                                        ZCARD requests:user42  → count in window
      - Priority queue: ZADD jobs 1 "low_priority_job" 10 "critical_job"
      - Trending topics: ZINCRBY trending 1 "python"  (every mention = +1 score)
                         ZREVRANGE trending 0 9         (top 10 trending)
      - Delayed task scheduling: ZADD scheduled_jobs <execute_at_timestamp> job_id
    """

    def __init__(self):
        # {key → {member → score}} for O(1) score lookup
        self._scores: dict[str, dict[str, float]] = defaultdict(dict)

    def zadd(self, key: str, score: float, member: str) -> int:
        """ZADD key score member → 1 if new member, 0 if score updated."""
        is_new = member not in self._scores[key]
        self._scores[key][member] = score
        return 1 if is_new else 0

    def zrem(self, key: str, *members) -> int:
        """ZREM key member [...] → number of removed members."""
        removed = 0
        for m in members:
            if m in self._scores.get(key, {}):
                del self._scores[key][m]
                removed += 1
        return removed

    def zscore(self, key: str, member: str) -> Optional[float]:
        """ZSCORE key member → score or None."""
        return self._scores.get(key, {}).get(member)

    def zincrby(self, key: str, amount: float, member: str) -> float:
        """ZINCRBY key amount member → new score after increment."""
        current = self._scores[key].get(member, 0.0)
        new_score = current + amount
        self._scores[key][member] = new_score
        return new_score

    def _sorted_members(self, key: str) -> list:
        """Return [(score, member)] sorted by score ascending."""
        members = self._scores.get(key, {})
        return sorted(members.items(), key=lambda x: x[1])  # sort by score

    def zrank(self, key: str, member: str) -> Optional[int]:
        """ZRANK key member → 0-based rank (lowest score = rank 0)."""
        sorted_members = [m for m, s in self._sorted_members(key)]
        try:
            return sorted_members.index(member)
        except ValueError:
            return None

    def zrevrank(self, key: str, member: str) -> Optional[int]:
        """ZREVRANK key member → rank from highest score."""
        sorted_members = [m for m, s in self._sorted_members(key)][::-1]
        try:
            return sorted_members.index(member)
        except ValueError:
            return None

    def zrange(self, key: str, start: int, stop: int,
               withscores: bool = False) -> list:
        """ZRANGE key start stop [WITHSCORES] → ascending by score."""
        sorted_list = self._sorted_members(key)
        if stop == -1:
            sliced = sorted_list[start:]
        else:
            sliced = sorted_list[start:stop + 1]
        if withscores:
            return [(m, s) for m, s in sliced]
        return [m for m, s in sliced]

    def zrevrange(self, key: str, start: int, stop: int,
                  withscores: bool = False) -> list:
        """ZREVRANGE key start stop [WITHSCORES] → descending by score."""
        sorted_list = self._sorted_members(key)[::-1]
        if stop == -1:
            sliced = sorted_list[start:]
        else:
            sliced = sorted_list[start:stop + 1]
        if withscores:
            return [(m, s) for m, s in sliced]
        return [m for m, s in sliced]

    def zcard(self, key: str) -> int:
        """ZCARD key → number of members."""
        return len(self._scores.get(key, {}))

    def zrangebyscore(self, key: str, min_score: float, max_score: float,
                      withscores: bool = False) -> list:
        """ZRANGEBYSCORE key min max → members with scores in [min, max]."""
        result = [(m, s) for m, s in self._sorted_members(key)
                  if min_score <= s <= max_score]
        if withscores:
            return result
        return [m for m, s in result]

    def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        """Remove members with scores in [min, max]. Returns count removed."""
        to_remove = [m for m, s in self._sorted_members(key)
                     if min_score <= s <= max_score]
        for m in to_remove:
            del self._scores[key][m]
        return len(to_remove)


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 65)
    print("   REDIS DATA STRUCTURES — Demo")
    print("=" * 65)

    # ── STRING ────────────────────────────────────────────────────────────
    print("\n  ─── STRING ───")
    s = RedisString()
    s.set("counter", 0)
    print(f"  SET counter 0")
    print(f"  INCR counter → {s.incr('counter')}")
    print(f"  INCRBY counter 5 → {s.incr('counter', 5)}")
    s.set("session:abc", "user_42_data", ex=2)
    print(f"  SET session:abc ... EX 2  →  TTL={s.ttl('session:abc')}s")
    print(f"  SETNX lock:res 'worker1' → {s.set('lock:res', 'worker1', nx=True)}")
    print(f"  SETNX lock:res 'worker2' → {s.set('lock:res', 'worker2', nx=True)} (refused!)")

    # ── HASH ──────────────────────────────────────────────────────────────
    print("\n  ─── HASH ───")
    h = RedisHash()
    h.hmset("user:42", {"name": "Alice", "email": "alice@ex.com", "age": 30, "role": "admin"})
    print(f"  HMSET user:42 ...  →  HGETALL → {h.hgetall('user:42')}")
    print(f"  HGET user:42 name  → '{h.hget('user:42', 'name')}'")
    h.hset("user:42", "age", 31)
    print(f"  HSET user:42 age 31  → age now = {h.hget('user:42', 'age')}")
    h.hincrby("user:42", "login_count", 1)
    print(f"  HINCRBY user:42 login_count 1 → {h.hget('user:42', 'login_count')}")
    print(f"  HLEN user:42 → {h.hlen('user:42')} fields")

    # ── LIST ──────────────────────────────────────────────────────────────
    print("\n  ─── LIST (Queue: RPUSH + LPOP) ───")
    lst = RedisList()
    for job in ["job_A", "job_B", "job_C"]:
        ln = lst.rpush("queue:emails", job)
        print(f"  RPUSH queue:emails {job} → length={ln}")
    print(f"  LRANGE queue:emails 0 -1 → {lst.lrange('queue:emails', 0, -1)}")
    print(f"  LPOP queue:emails → '{lst.lpop('queue:emails')}' (FIFO dequeue)")

    print("\n  ─── LIST (Activity Feed: LPUSH + LTRIM) ───")
    feed = RedisList()
    for event in ["liked post 1", "commented on 2", "shared post 3", "liked post 4"]:
        feed.lpush("feed:alice", event)
    feed.ltrim("feed:alice", 0, 2)  # Keep only last 3
    print(f"  Latest 3 activities: {feed.lrange('feed:alice', 0, -1)}")

    # ── SET ───────────────────────────────────────────────────────────────
    print("\n  ─── SET ───")
    st = RedisSet()
    st.sadd("visitors:today", "user_1", "user_2", "user_3", "user_1")  # Dup ignored
    print(f"  SADD visitors:today (with dup) → SCARD={st.scard('visitors:today')} unique")
    st.sadd("following:alice", "bob", "carol", "dave")
    st.sadd("following:bob",   "carol", "dave", "eve")
    mutual = st.sinter("following:alice", "following:bob")
    print(f"  SINTER following:alice following:bob → {mutual} (mutual)")
    only_alice = st.sdiff("following:alice", "following:bob")
    print(f"  SDIFF following:alice following:bob → {only_alice} (only alice follows)")

    # ── SORTED SET ────────────────────────────────────────────────────────
    print("\n  ─── SORTED SET — Leaderboard ───")
    z = RedisZSet()
    scores = [("alice", 9500), ("bob", 8700), ("carol", 9200), ("dave", 7800), ("eve", 9800)]
    for name, score in scores:
        z.zadd("leaderboard", score, name)
    top3 = z.zrevrange("leaderboard", 0, 2, withscores=True)
    print(f"  Top 3: {top3}")
    z.zincrby("leaderboard", 500, "bob")
    print(f"  ZINCRBY leaderboard 500 bob → new score={z.zscore('leaderboard', 'bob')}")
    print(f"  ZREVRANK leaderboard bob → #{z.zrevrank('leaderboard', 'bob') + 1}")

    print("\n  ─── SORTED SET — Sliding Window Rate Limiting ───")
    # Use ZSET timestamps as a sliding window rate limiter
    now = time.time()
    window = 10  # 10-second window
    for i in range(5):
        z.zadd("requests:user42", now - i, f"req_{i}")  # Simulate past requests
    z.zadd("requests:user42", now, "req_new")  # New request

    # Remove old entries outside the window
    z.zremrangebyscore("requests:user42", 0, now - window)
    count_in_window = z.zcard("requests:user42")
    print(f"  Requests in last {window}s: {count_in_window}/10 limit")

    print("\n" + "=" * 65)
    print("  REDIS DATA STRUCTURES SUMMARY")
    print("=" * 65)
    rows = [
        ("Structure", "Commands",                    "Key Use Case"),
        ("─"*10,      "─"*28,                        "─"*26),
        ("STRING",    "SET/GET/INCR/SETNX",           "Counters, tokens, locks"),
        ("HASH",      "HSET/HGET/HGETALL/HINCRBY",    "User objects, config"),
        ("LIST",      "RPUSH/LPOP/LRANGE/LTRIM",      "Queues, activity feeds"),
        ("SET",       "SADD/SMEMBERS/SINTER/SDIFF",   "Tags, unique visitors"),
        ("ZSET",      "ZADD/ZREVRANGE/ZINCRBY/ZRANK", "Leaderboards, rate limits"),
    ]
    for r in rows:
        print(f"  {r[0]:<11} {r[1]:<30} {r[2]}")
    print("=" * 65)
