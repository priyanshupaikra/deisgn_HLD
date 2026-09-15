"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    RATE LIMITING — ALGORITHM 4                               ║
║                           LEAKY BUCKET                                       ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
Imagine a bucket with a HOLE at the bottom. Water (requests) pours in from the
top at any rate, but drains (leaks) out at a FIXED, CONSTANT rate through the
hole — regardless of how fast water is poured in.

  - If the bucket is NOT full → accept the request (add water to bucket)
  - If the bucket IS full → reject the request (overflow = dropped)
  - Regardless of input rate, output is always at a constant rate (leak rate)

This enforces a STRICTLY UNIFORM output rate. No bursts make it through.

HOW IT WORKS (Visual):
──────────────────────
  Bucket capacity = 5 (queue size)
  Leak rate       = 1 request/second

  Incoming:  ▲▲▲▲▲  (5 rapid requests)
             Bucket: [R1, R2, R3, R4, R5]  ← queued
  Incoming:  ▲       → Bucket full! → REJECTED (overflow)

  Processing: 1 request dequeued every 1 second (constant output):
    t=1s: Process R1
    t=2s: Process R2
    t=3s: Process R3
    ...

TOKEN BUCKET vs LEAKY BUCKET:
──────────────────────────────
  Token Bucket:  Allows BURSTS (full bucket = consume many tokens at once)
  Leaky Bucket:  NEVER bursts — output is always at a constant rate
                 (great for traffic shaping / network QoS)

TWO INTERPRETATIONS:
────────────────────
  1. As a QUEUE (classic network QoS):
     - Requests queue up inside the bucket
     - Processed at a fixed rate by a background worker
     - Used for traffic shaping in routers

  2. As a COUNTER (used for rate limiting APIs):
     - Track when the last "leak" happened
     - Calculate how much water has "leaked" since then
     - Allow request if current water level < capacity
     This implementation uses interpretation #2 (no background thread needed)

ADVANTAGES:
-----------
  ✅ Produces a perfectly UNIFORM output rate (no burst)
  ✅ Protects downstream services from sudden traffic spikes
  ✅ Great for real-time streaming, video, VoIP

DISADVANTAGES:
--------------
  ❌ Burst requests are LOST or delayed — not buffered indefinitely
  ❌ Bursty but legitimate traffic is penalized (e.g., batch jobs)
  ❌ Not ideal for interactive APIs where occasional bursts are expected

REAL-WORLD USE CASE:
--------------------
  - Network QoS / traffic shaping (NGINX, routers)
  - Video streaming services (controlling bitrate)
  - SMS/email throttling (exactly N messages per second)
  - NGINX's limit_req module uses a variant of Leaky Bucket

"""

import time
import threading
from collections import defaultdict


class LeakyBucketRateLimiter:
    """
    Leaky Bucket Rate Limiter (Counter-based implementation).

    Instead of an actual queue, we track the water level as a float
    and compute the "leaked" amount since the last check lazily.

    Analogy:
      - water_level = current number of "pending" requests in the bucket
      - capacity    = max bucket size (overflow = reject)
      - leak_rate   = requests processed (leaked) per second

    Attributes:
        capacity   : Max bucket size (requests queue depth)
        leak_rate  : Rate at which requests are processed (req/second)
        buckets    : Dict mapping {client_id: [water_level, last_check_time]}
        lock       : Thread lock for atomic operations
    """

    def __init__(self, capacity: int, leak_rate: float):
        """
        :param capacity:  Max requests the bucket can hold before overflowing
        :param leak_rate: Rate at which water leaks out (requests/second)
        """
        self.capacity = capacity
        self.leak_rate = leak_rate
        # [current_water_level, last_leak_timestamp]
        self.buckets = defaultdict(lambda: [0.0, time.time()])
        self.lock = threading.Lock()

    def _leak(self, client_id: str) -> None:
        """
        Simulate the leaking process (lazy drain).

        Water leaked = elapsed_seconds * leak_rate
        Water level cannot go below 0 (bucket can't have negative water).

        This is called before every incoming request to update the water level.
        No background thread needed — the math handles it virtually.
        """
        now = time.time()
        water_level, last_time = self.buckets[client_id]

        # How much time has passed?
        elapsed = now - last_time

        # How much water has leaked out in that time?
        leaked = elapsed * self.leak_rate

        # Reduce water level (can't go below 0)
        new_level = max(0.0, water_level - leaked)

        # Update bucket state
        self.buckets[client_id][0] = new_level
        self.buckets[client_id][1] = now

    def allow_request(self, client_id: str, request_weight: float = 1.0) -> bool:
        """
        Determine if the incoming request should be accepted.

        Steps:
          1. Lazy-drain the bucket (simulate leaking since last call)
          2. If (current water level + request_weight) <= capacity → ALLOW
             Add water (request) to bucket
          3. Else → REJECT (bucket overflow)

        :param client_id:      Unique identifier for the caller
        :param request_weight: How much "water" this request adds (default: 1)
                               Use >1 for heavier requests
        :return:               True if allowed, False if rate-limited
        """
        with self.lock:  # Thread-safe
            self._leak(client_id)  # Drain first
            water_level = self.buckets[client_id][0]

            if water_level + request_weight <= self.capacity:
                # There is space in the bucket → ALLOW
                self.buckets[client_id][0] += request_weight
                return True
            else:
                # Bucket is full → OVERFLOW → REJECT
                return False

    def get_status(self, client_id: str) -> dict:
        """Returns current bucket status for the client."""
        self._leak(client_id)
        water_level = self.buckets[client_id][0]
        return {
            "water_level": round(water_level, 2),
            "capacity": self.capacity,
            "available_space": round(self.capacity - water_level, 2),
            "leak_rate_per_sec": self.leak_rate,
            "overflow_risk": "HIGH" if water_level / self.capacity > 0.8 else "LOW",
        }


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("   LEAKY BUCKET — Rate Limiter Demo")
    print("=" * 60)
    print("Config: capacity=5 requests, leak_rate=1 request/sec\n")

    # Bucket can hold 5 requests; drains at 1 req/second
    limiter = LeakyBucketRateLimiter(capacity=5, leak_rate=1.0)
    client = "user_D"

    print("  Phase 1: Burst — fill the bucket rapidly")
    for i in range(1, 9):
        allowed = limiter.allow_request(client)
        status = limiter.get_status(client)
        result = "[ALLOWED]" if allowed else "[REJECTED - OVERFLOW]"
        print(f"  Request #{i:02d}: {result}  |  "
              f"Water Level={status['water_level']}/{status['capacity']}, "
              f"Space={status['available_space']}")

    print(f"\n  Sleeping 3 seconds (bucket drains at 1 req/sec)...")
    time.sleep(3)

    print(f"\n  Phase 2: After 3s (should have drained ~3 slots)")
    status = limiter.get_status(client)
    print(f"  Bucket status: Water Level={status['water_level']}, "
          f"Space Available={status['available_space']}\n")

    for i in range(9, 13):
        allowed = limiter.allow_request(client)
        status = limiter.get_status(client)
        result = "[ALLOWED]" if allowed else "[REJECTED - OVERFLOW]"
        print(f"  Request #{i:02d}: {result}  |  "
              f"Water Level={status['water_level']}/{status['capacity']}, "
              f"Risk={status['overflow_risk']}")

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY: Output rate is ALWAYS constant (leak_rate).")
    print("  Burst traffic either fills the bucket or overflows.")
    print("  Best for traffic shaping, not general-purpose API limiting.")
    print("=" * 60)
