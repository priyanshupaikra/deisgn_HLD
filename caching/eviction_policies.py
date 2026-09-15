"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       CACHING (REDIS) — MODULE 1                             ║
║                          EVICTION POLICIES                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
A cache has LIMITED memory. When it's full and a new item must be added,
an existing item must be EVICTED (removed). The EVICTION POLICY decides
WHICH item to remove.

Choosing the right policy is critical:
  - Wrong policy → poor cache-hit ratio → defeats the purpose of caching
  - Right policy  → high cache-hit ratio → fast responses, low DB load

EVICTION POLICIES COVERED:
───────────────────────────
  1. LRU  — Least Recently Used      (most common in Redis, Memcached)
  2. LFU  — Least Frequently Used    (Redis 4.0+, optimal for skewed access)
  3. FIFO — First In, First Out      (simple but ignores access patterns)
  4. TTL  — Time To Live             (time-based expiry, not count-based)

REDIS EVICTION MODES (redis.conf: maxmemory-policy):
──────────────────────────────────────────────────────
  noeviction       → Return error when memory is full (default — dangerous!)
  allkeys-lru      → Evict LRU key from ALL keys
  volatile-lru     → Evict LRU key only from keys WITH expiry set
  allkeys-lfu      → Evict LFU key from ALL keys (Redis 4.0+)
  volatile-lfu     → Evict LFU key only from keys WITH expiry set
  allkeys-random   → Evict random key from ALL keys
  volatile-random  → Evict random key from keys WITH expiry set
  volatile-ttl     → Evict key with NEAREST expiry time

REAL-WORLD POLICY GUIDE:
─────────────────────────
  LRU  → General-purpose caching (user sessions, API responses)
  LFU  → Skewed access patterns (some items are "hot" always, e.g., trending)
  FIFO → Simple queues, event log buffers
  TTL  → Session tokens, OTPs, temporary data

"""

import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from typing import Optional, Any


# ─────────────────────────────────────────────────────────────────────────────
# POLICY 1: LRU — Least Recently Used
# ─────────────────────────────────────────────────────────────────────────────
class LRUCache:
    """
    Least Recently Used (LRU) Cache.

    CONCEPT:
      Evict the key that has NOT BEEN ACCESSED for the LONGEST time.
      Intuition: if something hasn't been used in a while, it likely
      won't be needed soon — discard it to make room for new data.

    DATA STRUCTURE:
      Uses an OrderedDict (Python's built-in) which maintains insertion
      order. We move a key to the END on every access (most-recently-used).
      The FRONT (beginning) always holds the least-recently-used item.

      On eviction: remove from the front.
      On access/insert: move/add to the back.

    COMPLEXITY:
      GET: O(1) — dict lookup
      PUT: O(1) — dict insert + move to end

    USED BY:
      - Redis: allkeys-lru / volatile-lru
      - Memcached: default policy
      - CPU L1/L2/L3 cache replacement
      - Browser cache (tab memory management)
    """

    def __init__(self, capacity: int):
        """
        :param capacity: Maximum number of key-value pairs to hold
        """
        self.capacity = capacity
        # OrderedDict preserves insertion order and allows O(1) move_to_end
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self.hits = 0    # Number of cache hits
        self.misses = 0  # Number of cache misses

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value. On hit, mark it as most-recently-used.

        :return: Value if found, None if cache miss
        """
        if key not in self._cache:
            self.misses += 1
            return None  # CACHE MISS

        # CACHE HIT: move to end (mark as most recently used)
        self._cache.move_to_end(key)
        self.hits += 1
        return self._cache[key]

    def put(self, key: str, value: Any) -> Optional[str]:
        """
        Insert or update a key-value pair.

        If key exists: update value + mark as most-recently-used.
        If cache is full: evict the LEAST recently used item first.

        :return: The evicted key name (for observability), or None
        """
        evicted = None
        if key in self._cache:
            # Update existing: move to end (most-recently-used position)
            self._cache.move_to_end(key)
        elif len(self._cache) >= self.capacity:
            # Cache full: evict from FRONT (least-recently-used)
            evicted, _ = self._cache.popitem(last=False)

        self._cache[key] = value
        return evicted

    def __repr__(self):
        order = list(self._cache.keys())
        return (f"LRUCache(capacity={self.capacity}, size={len(self._cache)}) "
                f"[LRU→MRU: {order}]")

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_ratio": f"{self.hits/total*100:.1f}%" if total else "N/A",
        }


# ─────────────────────────────────────────────────────────────────────────────
# POLICY 2: LFU — Least Frequently Used
# ─────────────────────────────────────────────────────────────────────────────
class LFUCache:
    """
    Least Frequently Used (LFU) Cache.

    CONCEPT:
      Evict the key with the LOWEST ACCESS COUNT.
      Intuition: items accessed many times are "hot" and should stay in cache.
      Items rarely accessed should be evicted first.

      TIE-BREAKING: if multiple keys have the same frequency, evict the
      LEAST RECENTLY USED among them (LRU within the same frequency bucket).

    DATA STRUCTURE (O(1) implementation by Fan, Cao, Ng & Aggarwal, 2010):
      - key_count: {key → access_frequency}
      - freq_keys: {frequency → OrderedDict of keys} (ordered for LRU tie-breaking)
      - min_freq:  tracks current minimum frequency (for O(1) eviction)

    COMPLEXITY:
      GET: O(1)
      PUT: O(1)

    USED BY:
      - Redis 4.0+: allkeys-lfu / volatile-lfu (maxmemory-policy)
      - Web proxies (Squid, Varnish)
      - CPU cache replacement (optimal for hot-spot workloads)

    LFU vs LRU:
      LRU suffers from "cache pollution" — one-time sequential scans evict
      popular items. LFU is immune to this but has "frequency bias" —
      old popular items accumulate high counts and resist eviction even
      when no longer relevant.
    """

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.key_val: dict[str, Any] = {}           # key → value
        self.key_count: dict[str, int] = {}          # key → access_count
        self.freq_keys: dict[int, OrderedDict] = defaultdict(OrderedDict)
        self.min_freq: int = 0
        self.hits = 0
        self.misses = 0

    def _increment_freq(self, key: str) -> None:
        """Increment access frequency for a key and update freq_keys buckets."""
        freq = self.key_count[key]

        # Remove from current frequency bucket
        del self.freq_keys[freq][key]
        # If this was the minimum frequency bucket and it's now empty, increase min_freq
        if not self.freq_keys[freq] and freq == self.min_freq:
            self.min_freq += 1

        # Add to next frequency bucket
        new_freq = freq + 1
        self.key_count[key] = new_freq
        self.freq_keys[new_freq][key] = True  # OrderedDict maintains insertion order

    def get(self, key: str) -> Optional[Any]:
        if key not in self.key_val:
            self.misses += 1
            return None  # CACHE MISS

        self._increment_freq(key)
        self.hits += 1
        return self.key_val[key]

    def put(self, key: str, value: Any) -> Optional[str]:
        """Insert or update. Evicts least-frequently-used key if at capacity."""
        if self.capacity <= 0:
            return None

        evicted = None
        if key in self.key_val:
            # Update: just change value, then increment freq
            self.key_val[key] = value
            self._increment_freq(key)
        else:
            if len(self.key_val) >= self.capacity:
                # EVICT: remove LRU item from the minimum-frequency bucket
                # first() of OrderedDict = least-recently-used at this frequency
                evicted_key, _ = self.freq_keys[self.min_freq].popitem(last=False)
                del self.key_val[evicted_key]
                del self.key_count[evicted_key]
                evicted = evicted_key

            # Insert new key with frequency = 1
            self.key_val[key] = value
            self.key_count[key] = 1
            self.freq_keys[1][key] = True
            self.min_freq = 1  # New item always starts at frequency 1

        return evicted

    def get_frequencies(self) -> dict:
        """Return current access frequencies for all keys (for debugging)."""
        return {k: self.key_count[k] for k in self.key_val}

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_ratio": f"{self.hits/total*100:.1f}%" if total else "N/A",
            "frequencies": self.get_frequencies(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# POLICY 3: FIFO — First In, First Out
# ─────────────────────────────────────────────────────────────────────────────
class FIFOCache:
    """
    First In, First Out (FIFO) Cache.

    CONCEPT:
      Evict the OLDEST item — the one that was inserted first.
      Does NOT track access frequency or recency — purely insertion order.

    DATA STRUCTURE:
      OrderedDict to maintain insertion order.
      Unlike LRU, we do NOT move items on access — order = insertion order.

    COMPLEXITY:
      GET: O(1)
      PUT: O(1)

    WHEN TO USE:
      - Simple streaming/pipeline buffers
      - Log rotation (oldest logs evicted first)
      - NOT great for general caching (ignores access patterns)
      - Results in worst performance for many access patterns
    """

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        if key not in self._cache:
            self.misses += 1
            return None
        self.hits += 1
        return self._cache[key]  # No reordering on access!

    def put(self, key: str, value: Any) -> Optional[str]:
        evicted = None
        if key in self._cache:
            self._cache[key] = value  # Update value (don't change order)
        else:
            if len(self._cache) >= self.capacity:
                # Evict the FIRST inserted item
                evicted, _ = self._cache.popitem(last=False)
            self._cache[key] = value
        return evicted

    def stats(self) -> dict:
        total = self.hits + self.misses
        order = list(self._cache.keys())
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_ratio": f"{self.hits/total*100:.1f}%" if total else "N/A",
            "insertion_order": order,
        }


# ─────────────────────────────────────────────────────────────────────────────
# POLICY 4: TTL — Time To Live
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class TTLEntry:
    """A cache entry with an expiry timestamp."""
    value: Any
    expires_at: float   # Unix timestamp when this entry expires


class TTLCache:
    """
    TTL (Time To Live) Cache — time-based expiry.

    CONCEPT:
      Every cached item has an expiry timestamp.
      Items that have EXPIRED are considered cache misses.
      Expired items are lazily removed on access (lazy eviction)
      and periodically purged in a background sweep.

    This is the PRIMARY mechanism used in Redis for key expiry:
      EXPIRE key seconds     → Set a TTL on a key
      TTL key                → Get remaining seconds
      PERSIST key            → Remove TTL (make permanent)
      SETEX key seconds val  → Set with TTL in one command

    TWO EVICTION STRATEGIES:
      1. LAZY (passive): Check expiry on GET — if expired, delete + return miss
      2. ACTIVE (periodic): Background sweeper removes expired keys periodically
         (Redis does this every 100ms, sampling 20 random volatile keys)

    USED FOR:
      - Session tokens (expire after 30 minutes of inactivity)
      - OTPs / verification codes (expire in 5 minutes)
      - API response caching (stale after 60 seconds)
      - Distributed locks (auto-release after timeout)
      - Rate limit windows (reset every 60 seconds)

    REDIS TTL COMMANDS:
      SET user:42 "{...}" EX 3600    → Expire in 3600 seconds
      SETEX user:42 3600 "{...}"     → Same (atomic)
      EXPIRE key 3600                → Set TTL on existing key
      PEXPIRE key 3600000            → Set TTL in milliseconds
      TTL key                        → Get remaining seconds (-1=no TTL, -2=gone)
    """

    def __init__(self, capacity: int, default_ttl: float = 60.0):
        """
        :param capacity:    Maximum number of entries
        :param default_ttl: Default TTL in seconds (used when none specified)
        """
        self.capacity = capacity
        self.default_ttl = default_ttl
        self._cache: dict[str, TTLEntry] = {}
        self.hits = 0
        self.misses = 0

    def _is_expired(self, key: str) -> bool:
        """Check if a key has passed its expiry timestamp."""
        entry = self._cache.get(key)
        if entry is None:
            return True
        return time.time() > entry.expires_at

    def get(self, key: str) -> Optional[Any]:
        """
        LAZY expiry: check TTL on every GET.
        If expired → delete the entry → return miss.
        """
        if key not in self._cache:
            self.misses += 1
            return None

        if self._is_expired(key):
            # LAZY EVICTION: remove expired entry on access
            del self._cache[key]
            self.misses += 1
            return None

        self.hits += 1
        return self._cache[key].value

    def put(self, key: str, value: Any, ttl: float = None) -> None:
        """
        Set a value with an optional TTL.

        :param key:   Cache key
        :param value: Value to store
        :param ttl:   Time-to-live in seconds (uses default_ttl if None)
        """
        if len(self._cache) >= self.capacity and key not in self._cache:
            self._evict_one()

        expire_at = time.time() + (ttl if ttl is not None else self.default_ttl)
        self._cache[key] = TTLEntry(value=value, expires_at=expire_at)

    def _evict_one(self) -> None:
        """Evict the entry that will expire the soonest (volatile-ttl strategy)."""
        if not self._cache:
            return
        soonest_key = min(self._cache, key=lambda k: self._cache[k].expires_at)
        del self._cache[soonest_key]

    def sweep(self) -> int:
        """
        ACTIVE EVICTION: Remove all expired keys.
        In Redis this runs every 100ms on a sample of keys.

        :return: Number of keys evicted
        """
        now = time.time()
        expired_keys = [k for k, v in self._cache.items() if now > v.expires_at]
        for k in expired_keys:
            del self._cache[k]
        return len(expired_keys)

    def ttl(self, key: str) -> float:
        """Get remaining TTL in seconds. Returns -1 if no TTL, -2 if missing."""
        if key not in self._cache:
            return -2  # Key doesn't exist
        entry = self._cache[key]
        remaining = entry.expires_at - time.time()
        return max(0, round(remaining, 2))

    def stats(self) -> dict:
        total = self.hits + self.misses
        # Purge expired first for accurate count
        self.sweep()
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_ratio": f"{self.hits/total*100:.1f}%" if total else "N/A",
            "active_keys": len(self._cache),
        }


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 65)
    print("   CACHE EVICTION POLICIES — Demo")
    print("=" * 65)

    # ── LRU ───────────────────────────────────────────────────────────────
    print("\n  ─── LRU Cache (capacity=3) ───")
    lru = LRUCache(3)
    ops = [
        ("PUT", "a", 1), ("PUT", "b", 2), ("PUT", "c", 3),
        ("GET", "a"),                            # Access 'a' → moves to MRU
        ("PUT", "d", 4),                         # 'b' evicted (LRU = b)
        ("GET", "b"),                            # Miss! b was evicted
        ("GET", "a"), ("GET", "c"), ("GET", "d"),
    ]
    for op in ops:
        if op[0] == "PUT":
            evicted = lru.put(op[1], op[2])
            ev_msg = f"  EVICTED={evicted}" if evicted else ""
            print(f"    PUT({op[1]}={op[2]}){ev_msg}")
        else:
            val = lru.get(op[1])
            print(f"    GET({op[1]}) → {'HIT:'+str(val) if val else 'MISS'}")
    print(f"    {lru}  Stats={lru.stats()}")

    # ── LFU ───────────────────────────────────────────────────────────────
    print("\n  ─── LFU Cache (capacity=3) ───")
    lfu = LFUCache(3)
    for key, val in [("a", 1), ("b", 2), ("c", 3)]:
        lfu.put(key, val)

    # Access 'a' 3x and 'b' 2x — 'c' has lowest freq (1)
    for _ in range(3): lfu.get("a")
    for _ in range(2): lfu.get("b")
    print(f"    Frequencies before eviction: {lfu.get_frequencies()}")

    evicted = lfu.put("d", 4)   # 'c' should be evicted (freq=1, lowest)
    print(f"    PUT(d=4) → EVICTED={evicted} (lowest frequency)")
    print(f"    Frequencies after: {lfu.get_frequencies()}")

    # ── FIFO ──────────────────────────────────────────────────────────────
    print("\n  ─── FIFO Cache (capacity=3) ───")
    fifo = FIFOCache(3)
    for key, val in [("a", 1), ("b", 2), ("c", 3)]:
        fifo.put(key, val)
    fifo.get("a")   # Access 'a' — but FIFO ignores recency!
    evicted = fifo.put("d", 4)  # 'a' evicted (FIRST inserted), not LRU
    print(f"    PUT(d=4) → EVICTED={evicted} (first inserted, even though just accessed)")
    print(f"    Stats: {fifo.stats()}")

    # ── TTL ───────────────────────────────────────────────────────────────
    print("\n  ─── TTL Cache (capacity=5, default_ttl=2s) ───")
    ttl_cache = TTLCache(5, default_ttl=2.0)
    ttl_cache.put("session:1", {"user": "Alice"}, ttl=2.0)
    ttl_cache.put("otp:123",   "456789",          ttl=1.0)  # Shorter TTL
    ttl_cache.put("product:1", {"name": "Widget"}, ttl=60.0)

    print(f"    GET session:1 → {ttl_cache.get('session:1')}")
    print(f"    TTL session:1 → {ttl_cache.ttl('session:1')}s remaining")
    print(f"    TTL otp:123   → {ttl_cache.ttl('otp:123')}s remaining")

    print(f"    Sleeping 1.5s (otp:123 should expire)...")
    time.sleep(1.5)
    print(f"    GET otp:123   → {'HIT' if ttl_cache.get('otp:123') else 'MISS (expired)'}")
    print(f"    GET session:1 → {'HIT' if ttl_cache.get('session:1') else 'MISS (expired)'}")

    print(f"    Sleeping 1s more (session:1 also expires)...")
    time.sleep(1.0)
    print(f"    GET session:1 → {'HIT' if ttl_cache.get('session:1') else 'MISS (expired)'}")
    print(f"    GET product:1 → {'HIT' if ttl_cache.get('product:1') else 'MISS'} (60s TTL, still alive)")
    swept = ttl_cache.sweep()
    print(f"    Sweep removed {swept} expired key(s)")
    print(f"    Stats: {ttl_cache.stats()}")

    print("\n" + "=" * 65)
    print("  EVICTION POLICY COMPARISON")
    print("  ─────────────────────────────────────────────────────────")
    rows = [
        ("Policy", "Evicts",              "Access-Aware?", "Best For"),
        ("─"*6,    "─"*22,               "─"*13,          "─"*28),
        ("LRU",    "Least recently used", "Yes",           "General-purpose caching"),
        ("LFU",    "Least freq. used",    "Yes",           "Hot-spot / skewed access"),
        ("FIFO",   "Oldest inserted",     "No",            "Streaming buffers"),
        ("TTL",    "Expired entries",     "Time-based",    "Sessions, OTPs, API cache"),
    ]
    for r in rows:
        print(f"  {r[0]:<8} {r[1]:<24} {r[2]:<15} {r[3]}")
    print("=" * 65)
