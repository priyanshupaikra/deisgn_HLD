"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       CACHING (REDIS) — MODULE 4                             ║
║                         CACHE STAMPEDE PREVENTION                            ║
║                     (Thundering Herd / Dog-Pile Problem)                     ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT — THE PROBLEM:
-----------------------
A Cache Stampede (also called Thundering Herd or Dog-Pile Effect) occurs when:

  1. A popular cache entry EXPIRES (TTL runs out)
  2. MANY concurrent requests all get a cache MISS at the same time
  3. ALL of them rush to the database to recompute/fetch the value
  4. Database gets overwhelmed → slow responses → further delays → more stampede

VISUAL:
───────
  ┌─────────────────────────────────────────────────────────────┐
  │  Cache key "trending_posts" expires at t=60s                │
  │                                                             │
  │  t=60.001s: 1000 requests arrive simultaneously            │
  │    All get CACHE MISS                                       │
  │    All 1000 query the database                             │
  │    DB receives 1000 concurrent queries → CRASH / timeout   │
  │                                                             │
  │  Result: service outage from a cache expiry event           │
  └─────────────────────────────────────────────────────────────┘

WHEN IS IT A PROBLEM?
──────────────────────
  - High-traffic systems (millions of users)
  - Short TTL on popular keys (trending, homepage)
  - Expensive cache misses (slow DB queries, complex computations)
  - Microservices where one slow dependency cascades

THREE PREVENTION STRATEGIES:
─────────────────────────────
  1. MUTEX LOCKING       — Only ONE request computes; others wait for it
  2. EARLY RECOMPUTATION — Probabilistically refresh BEFORE expiry
  3. BACKGROUND REFRESH  — A background worker pre-refreshes popular keys

"""

import time
import threading
import random
import math
from typing import Any, Optional, Callable
from dataclasses import dataclass, field


# ─────────────────────────────────────────────────────────────────────────────
# Shared infrastructure
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CacheEntry:
    """A cache entry with metadata for stampede prevention."""
    value: Any
    computed_at: float     # When this value was fetched/computed
    expires_at: float      # When it expires (for standard TTL)
    compute_time_ms: float = 0  # How long it took to compute (for Probabilistic)


class StampedeMetrics:
    """Tracks how many times the DB was hit vs. cache hits."""
    def __init__(self):
        self.cache_hits = 0
        self.db_hits = 0
        self.stampede_prevented = 0

    def __repr__(self):
        return (f"DB hits={self.db_hits}, Cache hits={self.cache_hits}, "
                f"Prevented={self.stampede_prevented}")


def simulate_db_query(key: str, delay: float = 0.2) -> Any:
    """Simulates a slow, expensive database query."""
    time.sleep(delay)  # 200ms DB query
    return {"key": key, "data": f"value_for_{key}", "timestamp": time.time()}


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY 1: MUTEX LOCKING (also called "Cache Lock" or "Read-Through Lock")
# ─────────────────────────────────────────────────────────────────────────────
class MutexCacheStampedePrevention:
    """
    Mutex Locking Strategy.

    ON CACHE MISS:
      1. Only the FIRST thread gets a lock (mutex/semaphore)
      2. First thread queries DB and populates cache
      3. All OTHER threads WAIT for the lock to be released
      4. Waiting threads read the freshly-populated cache

    DIAGRAM:
      Thread 1 → MISS → acquires LOCK → queries DB → SET cache → releases LOCK
      Thread 2 → MISS → waits for LOCK ─────────────────────────▶ reads cache
      Thread 3 → MISS → waits for LOCK ─────────────────────────▶ reads cache
      Thread 999 → MISS → waits for LOCK ────────────────────────▶ reads cache

    ADVANTAGES:
      ✅ DB only gets ONE query per cache miss (guaranteed)
      ✅ Simple to understand

    DISADVANTAGES:
      ❌ All other threads are BLOCKED while first thread queries DB
         (adds latency to all concurrent requests)
      ❌ Deadlock risk if lock holder crashes (use lock expiry!)
      ❌ Not suitable for distributed systems without distributed lock (Redis SETNX)

    REDIS IMPLEMENTATION:
      # Distributed lock with SETNX + EXPIRE
      locked = redis.setnx(f"lock:{key}", "1")
      if locked:
          redis.expire(f"lock:{key}", 10)  # Auto-release after 10s
          value = db.query(key)
          redis.set(key, value, ex=ttl)
          redis.delete(f"lock:{key}")
      else:
          # Wait and retry
          while not redis.exists(key):
              time.sleep(0.01)
          value = redis.get(key)
    """

    def __init__(self, ttl: float = 5.0, db_delay: float = 0.2):
        self._cache: dict[str, CacheEntry] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._locks_meta_lock = threading.Lock()  # Protects the _locks dict
        self.ttl = ttl
        self.db_delay = db_delay
        self.metrics = StampedeMetrics()

    def _get_lock(self, key: str) -> threading.Lock:
        """Get or create a per-key lock."""
        with self._locks_meta_lock:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]

    def get(self, key: str, thread_id: int = 0) -> Any:
        """
        Get value from cache with mutex locking for stampede prevention.

        Flow:
          1. Check cache — HIT: return (no lock needed)
          2. MISS: acquire per-key lock
          3. Re-check cache (another thread may have just populated it)
          4. If still miss: query DB, populate cache, release lock
          5. Other waiting threads will find cache populated after lock releases
        """
        # Step 1: Optimistic cache check (no lock needed)
        entry = self._cache.get(key)
        if entry and time.time() < entry.expires_at:
            self.metrics.cache_hits += 1
            return entry.value

        # Step 2: Cache miss — acquire per-key mutex
        lock = self._get_lock(key)
        print(f"    [Thread-{thread_id}] CACHE MISS: {key} — acquiring lock...")

        with lock:
            # Step 3: DOUBLE-CHECK after acquiring lock
            # (another thread may have computed while we were waiting)
            entry = self._cache.get(key)
            if entry and time.time() < entry.expires_at:
                self.metrics.cache_hits += 1
                self.metrics.stampede_prevented += 1
                print(f"    [Thread-{thread_id}] Lock acquired — cache already populated (no DB hit)")
                return entry.value

            # Step 4: Still a miss — this thread queries the DB
            print(f"    [Thread-{thread_id}] Querying DB (others waiting for lock)...")
            value = simulate_db_query(key, delay=self.db_delay)
            self.metrics.db_hits += 1

            # Populate cache for all waiting threads
            self._cache[key] = CacheEntry(
                value=value,
                computed_at=time.time(),
                expires_at=time.time() + self.ttl,
            )
            print(f"    [Thread-{thread_id}] DB result cached. Lock released.")
            return value


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY 2: PROBABILISTIC EARLY RECOMPUTATION (XFetch Algorithm)
# ─────────────────────────────────────────────────────────────────────────────
class ProbabilisticEarlyRecompute:
    """
    Probabilistic Early Recomputation (XFetch Algorithm).

    Paper: "Optimal Probabilistic Cache Stampede Prevention" (Vattani et al., 2015)

    KEY IDEA:
      DON'T wait for the cache to expire.
      BEFORE expiry, each request calculates a probability of early recomputation.
      The probability INCREASES as the key gets closer to expiry.

    FORMULA:
      P(recompute) = 1  if  current_time >= expires_at - beta * delta * log(rand())

      Where:
        delta   = how long the DB query takes (compute time in seconds)
        beta    = tuning parameter (default=1.0; higher = recompute earlier)
        rand()  = random number in (0, 1)
        expires_at = when the cached value expires

    INTUITION:
      - When TTL is large: probability is very low → mostly cache hits
      - As TTL approaches 0: probability grows → more recomputations
      - Expensive queries (high delta) trigger recomputation EARLIER
      - Result: cache is usually refreshed BEFORE expiry by natural traffic,
        so when it finally expires, it's already been refreshed

    ADVANTAGES:
      ✅ No locking needed (lock-free, naturally distributed)
      ✅ Works across multiple servers without coordination
      ✅ Expensive queries are proactively refreshed earlier

    DISADVANTAGES:
      ❌ Some requests will take longer (they do the recomputation)
      ❌ Slightly complex to tune the beta parameter
      ❌ Can result in multiple early recomputations if traffic is very high

    REDIS IMPLEMENTATION:
      Store: SET key:value <data> EX <ttl>
             SET key:delta <compute_time_ms>
             SET key:expires_at <timestamp>
      On GET: if XFetch formula triggers → recompute before expiry
    """

    def __init__(self, ttl: float = 5.0, beta: float = 1.0, db_delay: float = 0.2):
        """
        :param ttl:   Default time-to-live for cache entries (seconds)
        :param beta:  Tuning parameter — higher = earlier recomputation
        :param db_delay: Simulated DB query time in seconds
        """
        self._cache: dict[str, CacheEntry] = {}
        self.ttl = ttl
        self.beta = beta
        self.db_delay = db_delay
        self.metrics = StampedeMetrics()
        self._cache_lock = threading.Lock()  # Protects cache writes

    def _should_recompute(self, entry: CacheEntry) -> bool:
        """
        XFetch decision function:
        Returns True if this request should proactively recompute.

        Formula: now >= expires_at - beta * delta * log(rand())
        """
        now = time.time()
        delta = entry.compute_time_ms / 1000.0   # Convert ms → seconds

        # Random number from (0,1) exclusive
        rnd = random.uniform(1e-10, 1.0)

        # XFetch threshold: the more expensive the query, the earlier we recompute
        threshold = entry.expires_at - self.beta * delta * math.log(rnd)

        return now >= threshold

    def get(self, key: str, thread_id: int = 0) -> Any:
        """
        Get value with probabilistic early recomputation.

        Flow:
          1. Check if entry exists and is not expired
          2. If exists: run XFetch formula — if True, proactively recompute
          3. If expired or doesn't exist: always recompute
        """
        entry = self._cache.get(key)

        # Definitely expired or missing → must recompute
        if not entry or time.time() >= entry.expires_at:
            return self._recompute(key, thread_id, reason="expired/missing")

        # XFetch: probabilistically recompute before expiry
        if self._should_recompute(entry):
            print(f"    [Thread-{thread_id}] XFetch triggered early recompute for '{key}' "
                  f"(TTL remaining: {entry.expires_at - time.time():.2f}s)")
            return self._recompute(key, thread_id, reason="xfetch-early")

        # Normal cache hit
        self.metrics.cache_hits += 1
        print(f"    [Thread-{thread_id}] CACHE HIT: {key}")
        return entry.value

    def _recompute(self, key: str, thread_id: int, reason: str) -> Any:
        """Execute DB query and update cache."""
        print(f"    [Thread-{thread_id}] Recomputing ({reason}): {key}...")
        start = time.time()
        value = simulate_db_query(key, delay=self.db_delay)
        compute_time = (time.time() - start) * 1000  # ms
        self.metrics.db_hits += 1

        with self._cache_lock:
            self._cache[key] = CacheEntry(
                value=value,
                computed_at=time.time(),
                expires_at=time.time() + self.ttl,
                compute_time_ms=compute_time,
            )
        return value


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY 3: BACKGROUND REFRESH (Stale-While-Revalidate)
# ─────────────────────────────────────────────────────────────────────────────
class BackgroundRefreshCache:
    """
    Background Refresh / Stale-While-Revalidate Pattern.

    KEY IDEA:
      Use a TWO-LEVEL TTL:
        - soft_ttl: When to START a background refresh (serve stale data)
        - hard_ttl: When to FORCE a synchronous refresh (data too old)

      When a request arrives after soft_ttl:
        → Serve the STALE cached value immediately (low latency)
        → ALSO launch a background thread to refresh the cache

      When a request arrives after hard_ttl:
        → Must wait for fresh data (synchronous)

    USED IN:
      - HTTP Cache-Control: stale-while-revalidate=60
      - Cloudflare: stale-while-revalidate for CDN caching
      - Service Worker API (browser caching)
      - Next.js ISR (Incremental Static Regeneration)

    ADVANTAGES:
      ✅ Users ALWAYS get a fast response (stale data is still served)
      ✅ No blocking wait for DB queries
      ✅ DB load is smooth (refresh is background, not burst)

    DISADVANTAGES:
      ❌ Serves slightly stale data (acceptable for most use cases)
      ❌ Background threads must be managed carefully
    """

    def __init__(self, soft_ttl: float = 3.0, hard_ttl: float = 10.0, db_delay: float = 0.2):
        """
        :param soft_ttl: After this time → serve stale + trigger background refresh
        :param hard_ttl: After this time → force synchronous refresh (block)
        """
        self._cache: dict[str, CacheEntry] = {}
        self._refreshing: set = set()        # Keys currently being refreshed
        self._refresh_lock = threading.Lock()
        self.soft_ttl = soft_ttl
        self.hard_ttl = hard_ttl
        self.db_delay = db_delay
        self.metrics = StampedeMetrics()

    def get(self, key: str, thread_id: int = 0) -> Any:
        """
        Get with stale-while-revalidate semantics.

        Returns stale data immediately while scheduling a background refresh.
        Only blocks if data is EXTREMELY stale (past hard_ttl).
        """
        entry = self._cache.get(key)
        now = time.time()

        if entry is None:
            # First time — must do synchronous fetch
            print(f"    [Thread-{thread_id}] No cache entry — synchronous fetch")
            return self._fetch_sync(key, thread_id)

        age = now - entry.computed_at

        if age > self.hard_ttl:
            # Data is too stale — must block for fresh data
            print(f"    [Thread-{thread_id}] STALE (age={age:.1f}s > hard_ttl={self.hard_ttl}s) — blocking refresh")
            return self._fetch_sync(key, thread_id)

        if age > self.soft_ttl:
            # Slightly stale — serve current + trigger background refresh
            print(f"    [Thread-{thread_id}] STALE (age={age:.1f}s > soft_ttl={self.soft_ttl}s) "
                  f"— serving stale, triggering background refresh")
            self._trigger_background_refresh(key)
            self.metrics.cache_hits += 1
            return entry.value  # Return stale data immediately!

        # Fresh data — simple cache hit
        print(f"    [Thread-{thread_id}] CACHE HIT (age={age:.1f}s, fresh)")
        self.metrics.cache_hits += 1
        return entry.value

    def _fetch_sync(self, key: str, thread_id: int) -> Any:
        """Synchronous DB fetch (blocks the caller)."""
        value = simulate_db_query(key, delay=self.db_delay)
        self.metrics.db_hits += 1
        self._cache[key] = CacheEntry(
            value=value,
            computed_at=time.time(),
            expires_at=time.time() + self.hard_ttl,
        )
        return value

    def _trigger_background_refresh(self, key: str) -> None:
        """Launch a background thread to refresh the key — only one per key at a time."""
        with self._refresh_lock:
            if key in self._refreshing:
                return  # Already being refreshed
            self._refreshing.add(key)

        def refresh():
            print(f"    [BG Refresh] Starting background refresh for '{key}'...")
            value = simulate_db_query(key, delay=self.db_delay)
            self._cache[key] = CacheEntry(
                value=value,
                computed_at=time.time(),
                expires_at=time.time() + self.hard_ttl,
            )
            with self._refresh_lock:
                self._refreshing.discard(key)
            print(f"    [BG Refresh] Cache updated for '{key}'")
            self.metrics.db_hits += 1

        t = threading.Thread(target=refresh, daemon=True)
        t.start()


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
def run_concurrent(cache, key: str, num_threads: int, label: str):
    """Spawn multiple threads to simulate concurrent cache misses."""
    print(f"\n  [{label}] Spawning {num_threads} concurrent threads for key='{key}'\n")
    threads = []
    results = []

    def worker(tid):
        val = cache.get(key, thread_id=tid)
        results.append(val is not None)

    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)

    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = (time.time() - start) * 1000

    print(f"\n  [{label}] Completed: {sum(results)}/{num_threads} succeeded "
          f"in {elapsed:.0f}ms | DB hits={cache.metrics.db_hits} "
          f"(expected: 1 with protection)")


if __name__ == "__main__":
    print("=" * 65)
    print("   CACHE STAMPEDE PREVENTION — Demo")
    print("=" * 65)

    # ── Strategy 1: Mutex Locking ──────────────────────────────────────────
    print("\n\n  ══ STRATEGY 1: MUTEX LOCKING ══")
    print("  10 threads hit the same key simultaneously (cache cold)")
    mutex_cache = MutexCacheStampedePrevention(ttl=30.0, db_delay=0.2)
    run_concurrent(mutex_cache, "trending_posts", 5, "Mutex")
    print(f"\n  Metrics: {mutex_cache.metrics}")

    # ── Strategy 2: Probabilistic Early Recompute ─────────────────────────
    print("\n\n  ══ STRATEGY 2: PROBABILISTIC EARLY RECOMPUTE (XFetch) ══")
    prob_cache = ProbabilisticEarlyRecompute(ttl=5.0, beta=1.0, db_delay=0.2)

    print("\n  Phase 1: First request (cold miss)")
    prob_cache.get("homepage", thread_id=0)

    print("\n  Phase 2: Requests while cache is fresh (should all hit)")
    for i in range(3):
        prob_cache.get("homepage", thread_id=i+1)

    print(f"\n  Sleeping 4.5s (approaching expiry, XFetch may trigger early)")
    time.sleep(4.5)

    print("\n  Phase 3: Near expiry — XFetch probabilistic recompute")
    for i in range(3):
        prob_cache.get("homepage", thread_id=i+10)
    print(f"\n  Metrics: {prob_cache.metrics}")

    # ── Strategy 3: Background Refresh ────────────────────────────────────
    print("\n\n  ══ STRATEGY 3: BACKGROUND REFRESH (Stale-While-Revalidate) ══")
    bg_cache = BackgroundRefreshCache(soft_ttl=1.0, hard_ttl=5.0, db_delay=0.2)

    print("\n  Phase 1: Cold start (synchronous fetch)")
    bg_cache.get("product_list", thread_id=0)

    print("\n  Phase 2: Fresh data (cache hit)")
    bg_cache.get("product_list", thread_id=1)

    print(f"\n  Sleeping 1.5s (past soft_ttl={bg_cache.soft_ttl}s)...")
    time.sleep(1.5)

    print("\n  Phase 3: Stale data — serve immediately + background refresh")
    val = bg_cache.get("product_list", thread_id=2)
    print(f"  Got value: {val is not None} (served stale instantly)")
    time.sleep(0.5)  # Allow background refresh to complete

    print(f"\n  Metrics: {bg_cache.metrics}")

    print("\n" + "=" * 65)
    print("  STAMPEDE PREVENTION COMPARISON")
    print("=" * 65)
    rows = [
        ("Strategy",        "Approach",            "Latency Impact", "Complexity"),
        ("─"*16,            "─"*22,                "─"*15,           "─"*10),
        ("Mutex Lock",      "Block all but one",   "High (blocking)", "Low"),
        ("Probabilistic",   "XFetch formula",      "Low (no block)",  "Medium"),
        ("Background Ref.", "Serve stale + async", "None (stale)",    "Medium"),
    ]
    for r in rows:
        print(f"  {r[0]:<18} {r[1]:<24} {r[2]:<17} {r[3]}")
    print("=" * 65)
