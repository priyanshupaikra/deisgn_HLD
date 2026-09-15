"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       CACHING (REDIS) — MODULE 2                             ║
║                           CACHE PATTERNS                                     ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
Cache patterns define the STRATEGY for keeping the cache and database in sync.
Choosing the wrong pattern leads to:
  - Stale data served to users
  - Data loss on cache crash
  - Write amplification (unnecessary DB writes)
  - Cache and DB being permanently out of sync

FOUR CORE PATTERNS:
───────────────────
  1. Cache-Aside   (Lazy Loading)    — APP manages cache + DB
  2. Read-Through                    — CACHE manages DB reads
  3. Write-Through                   — CACHE writes to DB synchronously
  4. Write-Behind  (Write-Back)      — CACHE writes to DB asynchronously

VISUAL ARCHITECTURE:
────────────────────

  Cache-Aside:
    App ──GET──▶ Cache ──HIT──▶ App
                   │ MISS
                   ▼
    App ──GET──▶ Database ──▶ App ──SET──▶ Cache

  Read-Through:
    App ──GET──▶ Cache ──HIT──▶ App
                   │ MISS
                   ▼
                 Cache ──GET──▶ Database ──▶ Cache ──▶ App

  Write-Through:
    App ──SET──▶ Cache ──SET──▶ Database (synchronous, both updated)

  Write-Behind:
    App ──SET──▶ Cache ──▶ App (returns immediately)
               Queue → Database (asynchronous, eventual consistency)

"""

import time
import threading
from queue import Queue
from typing import Any, Optional, Callable


# ─────────────────────────────────────────────────────────────────────────────
# Shared infrastructure
# ─────────────────────────────────────────────────────────────────────────────

class SimpleCache:
    """Minimal in-memory cache (simulates Redis)."""

    def __init__(self):
        self._store: dict[str, Any] = {}

    def get(self, key: str) -> Optional[Any]:
        return self._store.get(key)

    def set(self, key: str, value: Any, ttl: float = None) -> None:
        self._store[key] = value

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def exists(self, key: str) -> bool:
        return key in self._store

    def keys(self) -> list:
        return list(self._store.keys())

    def __repr__(self):
        return f"Cache({dict(list(self._store.items())[:5])})"


class SimulatedDatabase:
    """
    Simulates a slow relational database (PostgreSQL / MySQL).
    Adds artificial latency to illustrate the benefit of caching.
    """

    def __init__(self, latency_ms: float = 100.0):
        self._data: dict[str, Any] = {
            "user:1": {"id": 1, "name": "Alice", "email": "alice@ex.com", "role": "admin"},
            "user:2": {"id": 2, "name": "Bob",   "email": "bob@ex.com",   "role": "user"},
            "user:3": {"id": 3, "name": "Carol",  "email": "carol@ex.com", "role": "user"},
            "product:1": {"id": 1, "name": "Widget", "price": 9.99,  "stock": 100},
            "product:2": {"id": 2, "name": "Gadget", "price": 49.99, "stock": 25},
        }
        self.latency_ms = latency_ms
        self.read_count = 0     # Track how many times DB was hit
        self.write_count = 0

    def find(self, key: str) -> Optional[Any]:
        """Simulate slow DB read."""
        time.sleep(self.latency_ms / 1000)  # Simulate network + query latency
        self.read_count += 1
        return self._data.get(key)

    def save(self, key: str, value: Any) -> None:
        """Simulate slow DB write."""
        time.sleep(self.latency_ms / 1000)
        self.write_count += 1
        self._data[key] = value

    def stats(self) -> dict:
        return {"db_reads": self.read_count, "db_writes": self.write_count}


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 1: CACHE-ASIDE (Lazy Loading)
# ─────────────────────────────────────────────────────────────────────────────
class CacheAsidePattern:
    """
    Cache-Aside Pattern (also called Lazy Loading).

    WHO IS RESPONSIBLE: The APPLICATION code manages both cache and DB.

    READ FLOW:
      1. App checks the cache first
      2. On CACHE HIT → return cached value (fast path)
      3. On CACHE MISS → app reads from DB → app writes result to cache → return

    WRITE FLOW (common approach):
      - Option A: INVALIDATE the cache entry (delete it) → next read will re-populate
      - Option B: UPDATE the cache entry directly

    ADVANTAGES:
      ✅ Only requested data is cached (no wasted cache space on unused data)
      ✅ Cache can fail without breaking reads (fallback to DB)
      ✅ Most popular pattern — used by most web applications

    DISADVANTAGES:
      ❌ First request is always slow (cache miss + DB read + cache write)
      ❌ "Cache stampede" possible if many requests miss simultaneously
      ❌ Stale data possible between invalidation and re-population
      ❌ App code is more complex (must manage both cache and DB)

    USED BY:
      - Virtually every web application (Django, Rails, Spring Boot)
      - Default pattern in most Redis tutorials
      - Social media feeds (user timeline caching)
    """

    def __init__(self, cache: SimpleCache, db: SimulatedDatabase, default_ttl: float = 30.0):
        self.cache = cache
        self.db = db
        self.default_ttl = default_ttl

    def read(self, key: str) -> Optional[Any]:
        """
        Cache-Aside READ:
          1. Try cache → HIT: return immediately
          2. MISS: query DB → populate cache → return
        """
        # Step 1: Check cache
        value = self.cache.get(key)
        if value is not None:
            print(f"    [Cache-Aside] GET {key} → CACHE HIT")
            return value  # Fast path!

        # Step 2: Cache miss → query database (slow path)
        print(f"    [Cache-Aside] GET {key} → CACHE MISS → querying DB...")
        value = self.db.find(key)

        # Step 3: Populate cache for next time
        if value is not None:
            self.cache.set(key, value, ttl=self.default_ttl)
            print(f"    [Cache-Aside] Populated cache: {key}")

        return value

    def write(self, key: str, value: Any) -> None:
        """
        Cache-Aside WRITE (Cache Invalidation):
          Write to DB, then DELETE from cache.
          Next read will re-populate from DB with fresh data.

          Why delete instead of update?
          Updating cache with new value can lead to race conditions
          (two concurrent writes could result in stale data in cache).
          DELETE is safer — forces a fresh DB read on next access.
        """
        # Write to DB
        print(f"    [Cache-Aside] WRITE {key} → writing to DB")
        self.db.save(key, value)

        # Invalidate cache entry (don't update — delete to avoid stale data)
        print(f"    [Cache-Aside] Invalidating cache: {key}")
        self.cache.delete(key)


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 2: READ-THROUGH
# ─────────────────────────────────────────────────────────────────────────────
class ReadThroughPattern:
    """
    Read-Through Pattern.

    WHO IS RESPONSIBLE: The CACHE manages DB reads (via a loader function).

    READ FLOW:
      1. App asks cache for a key
      2. If HIT → cache returns it
      3. If MISS → cache automatically calls the DB loader function
                → stores the result → returns it to app

    KEY DIFFERENCE from Cache-Aside:
      In Cache-Aside: APP fetches from DB and stores in cache (2 steps in app code)
      In Read-Through: CACHE fetches from DB automatically (app just calls cache.get)

    ADVANTAGES:
      ✅ Simpler app code — app only talks to cache, never directly to DB
      ✅ Cache is always populated for requested keys
      ✅ Good for read-heavy workloads

    DISADVANTAGES:
      ❌ Cold start problem: first request for every key is slow
      ❌ Cache must know how to fetch from DB (coupling)
      ❌ Stale data unless TTL is configured carefully

    USED BY:
      - Redis with client-side read-through libraries
      - JPA/Hibernate second-level cache (EhCache, Infinispan)
      - AWS ElastiCache lazy-loading with TTL
    """

    def __init__(self, cache: SimpleCache, db: SimulatedDatabase,
                 loader: Callable[[str], Any], default_ttl: float = 30.0):
        """
        :param loader: Function that loads data from DB given a key.
                       This is what the cache calls on a miss.
        """
        self.cache = cache
        self.db = db
        self.loader = loader          # The DB fetcher function (injected)
        self.default_ttl = default_ttl

    def get(self, key: str) -> Optional[Any]:
        """
        Read-Through GET:
          The cache is the SINGLE interface — if miss, cache loads from DB.
        """
        value = self.cache.get(key)
        if value is not None:
            print(f"    [Read-Through] GET {key} → CACHE HIT")
            return value

        # Cache miss: automatically load from DB
        print(f"    [Read-Through] GET {key} → CACHE MISS → auto-loading from DB...")
        value = self.loader(key)   # Cache calls the loader function

        if value is not None:
            self.cache.set(key, value, ttl=self.default_ttl)
            print(f"    [Read-Through] Auto-populated cache: {key}")

        return value


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 3: WRITE-THROUGH
# ─────────────────────────────────────────────────────────────────────────────
class WriteThroughPattern:
    """
    Write-Through Pattern.

    WHO IS RESPONSIBLE: Every WRITE goes to cache AND DB simultaneously.

    WRITE FLOW:
      1. App writes to CACHE
      2. Cache SYNCHRONOUSLY writes to DB (before returning to app)
      3. App gets confirmation only after BOTH cache and DB are updated

    ADVANTAGES:
      ✅ Cache is ALWAYS consistent with DB (no stale data)
      ✅ No cache invalidation needed — cache is always up to date
      ✅ Strong consistency guarantee

    DISADVANTAGES:
      ❌ Write latency = cache write time + DB write time (slower writes)
      ❌ Cache may store data that's never read ("write pollution")
         Solution: combine with TTL to auto-expire unused entries
      ❌ DB must be available for every write (no write fault tolerance)

    USED BY:
      - Distributed databases (Google Spanner-style caching)
      - CPU cache ↔ RAM synchronization
      - Bank transaction systems (consistency is critical)
      - Redis as a write-through cache layer in front of MySQL
    """

    def __init__(self, cache: SimpleCache, db: SimulatedDatabase, default_ttl: float = 60.0):
        self.cache = cache
        self.db = db
        self.default_ttl = default_ttl

    def write(self, key: str, value: Any) -> None:
        """
        Write-Through WRITE:
          Updates BOTH cache and DB in a single operation.
          Returns only after BOTH are confirmed written.
        """
        print(f"    [Write-Through] SET {key} → writing to CACHE + DB simultaneously")

        # Write to cache FIRST (fast)
        self.cache.set(key, value, ttl=self.default_ttl)

        # SYNCHRONOUSLY write to DB (slow — blocks until DB confirms)
        self.db.save(key, value)

        print(f"    [Write-Through] SET {key} → COMMITTED to both cache and DB")

    def read(self, key: str) -> Optional[Any]:
        """Read-side is straightforward: cache is always up-to-date."""
        value = self.cache.get(key)
        if value is not None:
            print(f"    [Write-Through] GET {key} → CACHE HIT")
            return value

        # Cache miss is rare in write-through (only on cold start)
        print(f"    [Write-Through] GET {key} → CACHE MISS → DB read")
        value = self.db.find(key)
        if value:
            self.cache.set(key, value, ttl=self.default_ttl)
        return value


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 4: WRITE-BEHIND (Write-Back)
# ─────────────────────────────────────────────────────────────────────────────
class WriteBehindPattern:
    """
    Write-Behind (Write-Back) Pattern.

    WHO IS RESPONSIBLE: Writes go to cache IMMEDIATELY, DB is updated LATER
    by an ASYNCHRONOUS background worker.

    WRITE FLOW:
      1. App writes to CACHE (fast — returns immediately)
      2. Write is queued in a write buffer (async queue)
      3. Background flusher dequeues and writes to DB (eventual consistency)

    ADVANTAGES:
      ✅ Lowest write latency — app doesn't wait for DB
      ✅ DB writes can be BATCHED → fewer round trips → higher throughput
      ✅ Can absorb write spikes (queue acts as a buffer)
      ✅ DB write failures don't block the application

    DISADVANTAGES:
      ❌ Data loss risk: if cache crashes before flushing, writes are lost
      ❌ Eventual consistency only — not suitable for financial data
      ❌ Complex implementation (need background worker, retry logic)
      ❌ Hard to reason about data consistency

    USED BY:
      - CPU L1 cache writing back to RAM (hardware implementation)
      - Redis AOF (Append-Only File) with everysec fsync setting
      - Write-behind in Hibernate/JPA ORM
      - Analytics event collection (Kafka sink connectors)
      - MongoDB write concern "w:0" (fire-and-forget)
    """

    def __init__(self, cache: SimpleCache, db: SimulatedDatabase,
                 flush_interval: float = 1.0, batch_size: int = 5):
        """
        :param flush_interval: Seconds between background flush cycles
        :param batch_size:     Max writes per flush batch
        """
        self.cache = cache
        self.db = db
        self.flush_interval = flush_interval
        self.batch_size = batch_size

        # Async write buffer: stores pending (key, value) pairs
        self.write_queue: Queue = Queue()
        self.pending_count = 0

        # Background flusher thread
        self._stop = threading.Event()
        self._flusher = threading.Thread(target=self._flush_worker, daemon=True)
        self._flusher.start()

    def write(self, key: str, value: Any) -> None:
        """
        Write-Behind WRITE (fire-and-forget):
          1. Update cache IMMEDIATELY (fast return to caller)
          2. Enqueue the write for async DB flush
        """
        # Immediately update cache — no DB wait
        self.cache.set(key, value)
        self.pending_count += 1
        print(f"    [Write-Behind] SET {key} → cached immediately "
              f"(DB flush pending, queue_size={self.write_queue.qsize()+1})")

        # Enqueue for background DB write
        self.write_queue.put((key, value))

    def read(self, key: str) -> Optional[Any]:
        """Read from cache — may have data not yet in DB (dirty read)."""
        value = self.cache.get(key)
        if value is not None:
            print(f"    [Write-Behind] GET {key} → CACHE HIT (may be ahead of DB)")
            return value
        # Fallback to DB
        value = self.db.find(key)
        if value:
            self.cache.set(key, value)
        return value

    def _flush_worker(self) -> None:
        """
        Background flusher thread.
        Periodically drains the write queue and commits to DB in batches.
        """
        while not self._stop.is_set():
            time.sleep(self.flush_interval)
            batch = []
            while not self.write_queue.empty() and len(batch) < self.batch_size:
                batch.append(self.write_queue.get())

            if batch:
                print(f"    [Write-Behind] FLUSH: committing {len(batch)} write(s) to DB...")
                for key, value in batch:
                    self.db.save(key, value)
                print(f"    [Write-Behind] FLUSH complete. DB writes={self.db.stats()['db_writes']}")

    def flush_now(self) -> None:
        """Force an immediate flush of all pending writes (for shutdown/testing)."""
        while not self.write_queue.empty():
            key, value = self.write_queue.get()
            self.db.save(key, value)
        print(f"    [Write-Behind] Manual flush complete. DB={self.db.stats()}")

    def stop(self) -> None:
        """Graceful shutdown: flush remaining writes, stop background thread."""
        self._stop.set()
        self.flush_now()


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
def timer(label: str):
    """Context-manager-style helper to measure latency."""
    class Timer:
        def __enter__(self):
            self.start = time.time()
            return self
        def __exit__(self, *args):
            ms = (time.time() - self.start) * 1000
            print(f"    ⏱  {label}: {ms:.1f}ms")
    return Timer()


if __name__ == "__main__":
    print("=" * 65)
    print("   CACHE PATTERNS — Demo (DB latency = 100ms)")
    print("=" * 65)

    DB_LATENCY = 0.1   # 100ms simulated DB latency

    # ── CACHE-ASIDE ───────────────────────────────────────────────────────
    print("\n  ═══ PATTERN 1: CACHE-ASIDE ═══")
    cache = SimpleCache()
    db    = SimulatedDatabase(latency_ms=DB_LATENCY * 1000)
    ca    = CacheAsidePattern(cache, db, default_ttl=30.0)

    with timer("Miss (DB read)"):    result = ca.read("user:1")
    print(f"    Result: {result}")
    with timer("Hit (cache)"):       result = ca.read("user:1")
    print(f"    Result: {result}")
    ca.write("user:1", {"id": 1, "name": "Alice Updated", "role": "superadmin"})
    with timer("After invalidation"): result = ca.read("user:1")
    print(f"    Result after write: {result}")
    print(f"    DB Stats: {db.stats()}")

    # ── READ-THROUGH ──────────────────────────────────────────────────────
    print("\n  ═══ PATTERN 2: READ-THROUGH ═══")
    cache2 = SimpleCache()
    db2    = SimulatedDatabase(latency_ms=DB_LATENCY * 1000)
    rt     = ReadThroughPattern(cache2, db2, loader=db2.find, default_ttl=30.0)

    with timer("Miss (auto-loaded)"): result = rt.get("product:1")
    print(f"    Result: {result}")
    with timer("Hit (cache)"):        result = rt.get("product:1")
    print(f"    Result: {result}")
    print(f"    DB Stats: {db2.stats()}")

    # ── WRITE-THROUGH ─────────────────────────────────────────────────────
    print("\n  ═══ PATTERN 3: WRITE-THROUGH ═══")
    cache3 = SimpleCache()
    db3    = SimulatedDatabase(latency_ms=DB_LATENCY * 1000)
    wt     = WriteThroughPattern(cache3, db3, default_ttl=60.0)

    with timer("Write (cache + DB)"):
        wt.write("user:2", {"id": 2, "name": "Bob Updated", "role": "admin"})
    with timer("Read (cache hit)"):
        result = wt.read("user:2")
    print(f"    Result: {result}")
    print(f"    DB Stats: {db3.stats()}")

    # ── WRITE-BEHIND ──────────────────────────────────────────────────────
    print("\n  ═══ PATTERN 4: WRITE-BEHIND ═══")
    cache4 = SimpleCache()
    db4    = SimulatedDatabase(latency_ms=DB_LATENCY * 1000)
    wb     = WriteBehindPattern(cache4, db4, flush_interval=1.0, batch_size=10)

    with timer("Write #1 (returns immediately)"):
        wb.write("product:1", {"name": "Widget v2", "price": 12.99})
    with timer("Write #2 (returns immediately)"):
        wb.write("product:2", {"name": "Gadget Pro", "price": 59.99})
    with timer("Write #3 (returns immediately)"):
        wb.write("user:3", {"id": 3, "name": "Carol Edited"})

    print(f"    DB writes so far: {db4.stats()['db_writes']} (0 — async!)")
    print(f"    Waiting for background flush (1s)...")
    time.sleep(1.5)
    print(f"    DB writes after flush: {db4.stats()['db_writes']}")
    wb.stop()

    # ── Comparison ────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("  CACHE PATTERN COMPARISON")
    print("=" * 65)
    rows = [
        ("Pattern",       "Read Perf", "Write Perf", "Consistency", "Complexity"),
        ("─"*13,          "─"*10,      "─"*11,       "─"*13,        "─"*10),
        ("Cache-Aside",   "Fast*",     "Instant",    "Eventual",    "Medium"),
        ("Read-Through",  "Fast*",     "Instant",    "Eventual",    "Low"),
        ("Write-Through", "Fast",      "Slow",       "Strong",      "Medium"),
        ("Write-Behind",  "Fast",      "Fastest",    "Eventual",    "High"),
    ]
    for r in rows:
        print(f"  {r[0]:<15} {r[1]:<11} {r[2]:<12} {r[3]:<14} {r[4]}")
    print("  * Fast after first hit. First hit is slow (DB round-trip).")
    print("=" * 65)
