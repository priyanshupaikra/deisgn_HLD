"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    RATE LIMITING — ALGORITHM 3                               ║
║                           TOKEN BUCKET                                       ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
Imagine a bucket that holds tokens. Tokens are added to the bucket at a
constant REFILL RATE. Each incoming request CONSUMES one token from the bucket.
If the bucket has tokens → request is ALLOWED.
If the bucket is EMPTY → request is REJECTED.

The bucket has a maximum capacity (burst capacity). Extra tokens beyond that
are simply discarded — they don't accumulate indefinitely.

HOW IT WORKS (Visual):
──────────────────────
  Bucket capacity = 5 tokens
  Refill rate     = 2 tokens/second

  t=0s:  Bucket [■■■■■]  (5 tokens full)
  t=0s:  Request arrives → consume 1 → [■■■■ ]  → ALLOW
  t=0s:  Request arrives → consume 1 → [■■■  ]  → ALLOW
  t=0s:  Request arrives → consume 1 → [■■   ]  → ALLOW
  t=0s:  Request arrives → consume 1 → [■    ]  → ALLOW
  t=0s:  Request arrives → consume 1 → [     ]  → ALLOW
  t=0s:  Request arrives → NO TOKEN  → [     ]  → REJECT
  t=1s:  Refill +2       → [■■   ]  (2 new tokens)
  t=1s:  Request arrives → consume 1 → [■    ]  → ALLOW

KEY PROPERTIES:
───────────────
  - BURST ALLOWED: Full bucket = can serve `capacity` requests instantly
    This is the biggest feature: burst traffic is naturally absorbed
  - SMOOTH LONG-TERM RATE: Over time, avg throughput = refill_rate
  - LAZY REFILL: We don't use a background thread; instead we calculate
    how many tokens should have accumulated since the last request
    (lazy/virtual refill based on elapsed time)

ADVANTAGES:
-----------
  ✅ Allows controlled bursting — good for APIs where occasional traffic spikes
     are acceptable but sustained overload is not
  ✅ Simple and memory-efficient (just store tokens + last_refill_time)
  ✅ Widely used in real systems (AWS API Gateway, Stripe, etc.)
  ✅ No background threads needed (lazy refill)

DISADVANTAGES:
--------------
  ❌ Does NOT guarantee uniform request spacing
     (a burst of tokens can be consumed all at once)
  ❌ Slightly more complex than Fixed Window

REAL-WORLD USE CASE:
--------------------
  - AWS API Gateway default throttling algorithm
  - Stripe API rate limiting
  - Network routers (QoS / traffic shaping)
  - Redis-based rate limiters

"""

import time
import threading
from collections import defaultdict


class TokenBucketRateLimiter:
    """
    Token Bucket Rate Limiter.

    Uses lazy/virtual token refill — no background thread needed.
    Tokens accumulate based on elapsed time since the last request.

    Attributes:
        capacity    : Maximum tokens the bucket can hold (also the burst size)
        refill_rate : Tokens added per second
        buckets     : Dict mapping {client_id: [current_tokens, last_refill_time]}
        lock        : Thread lock to ensure atomic token operations
    """

    def __init__(self, capacity: int, refill_rate: float):
        """
        :param capacity:    Max tokens in bucket (burst capacity)
        :param refill_rate: Tokens refilled per second (sustained rate)
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        # [tokens_available, last_refill_timestamp]
        self.buckets = defaultdict(lambda: [float(capacity), time.time()])
        self.lock = threading.Lock()

    def _refill(self, client_id: str) -> None:
        """
        Lazy refill: compute how many tokens should be added since last request.

        Tokens added = elapsed_seconds * refill_rate
        Tokens are capped at `capacity` (bucket can't overflow).

        This is called before every request check — no background thread needed.
        """
        now = time.time()
        tokens, last_time = self.buckets[client_id]

        # How much time has elapsed since the last refill?
        elapsed = now - last_time

        # How many tokens should have been added in that time?
        new_tokens = elapsed * self.refill_rate

        # Add new tokens, but never exceed the bucket capacity
        self.buckets[client_id][0] = min(self.capacity, tokens + new_tokens)
        self.buckets[client_id][1] = now  # Update last refill timestamp

    def allow_request(self, client_id: str, tokens_required: int = 1) -> bool:
        """
        Check if a request can be served.

        Steps:
          1. Lazy-refill the bucket based on elapsed time
          2. If available tokens >= required tokens → consume & ALLOW
          3. Else → REJECT (bucket empty)

        :param client_id:       Unique identifier for the caller
        :param tokens_required: Tokens needed for this request (default: 1)
                                Useful for weighted requests (e.g., heavy ops cost 5 tokens)
        :return:                True if allowed, False if rate-limited
        """
        with self.lock:  # Thread-safe operation
            self._refill(client_id)
            tokens = self.buckets[client_id][0]

            if tokens >= tokens_required:
                # Consume tokens and ALLOW
                self.buckets[client_id][0] -= tokens_required
                return True
            else:
                # Not enough tokens → REJECT
                return False

    def get_status(self, client_id: str) -> dict:
        """Returns current bucket status for the client."""
        self._refill(client_id)
        tokens = self.buckets[client_id][0]
        return {
            "tokens_available": round(tokens, 2),
            "bucket_capacity": self.capacity,
            "refill_rate_per_sec": self.refill_rate,
            "time_to_full_refill": round((self.capacity - tokens) / self.refill_rate, 2),
        }


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("   TOKEN BUCKET — Rate Limiter Demo")
    print("=" * 60)
    print("Config: capacity=5 tokens, refill_rate=1 token/sec\n")

    # Bucket holds max 5 tokens, refills at 1 token/second
    limiter = TokenBucketRateLimiter(capacity=5, refill_rate=1.0)
    client = "user_C"

    print("  Phase 1: Rapid burst (drain the bucket)")
    for i in range(1, 8):
        allowed = limiter.allow_request(client)
        status = limiter.get_status(client)
        result = "[ALLOWED]" if allowed else "[REJECTED]"
        print(f"  Request #{i:02d}: {result}  |  "
              f"Tokens={status['tokens_available']}/{status['bucket_capacity']}")

    print(f"\n  Sleeping 3 seconds (bucket refills at 1 token/sec)...")
    time.sleep(3)

    print(f"\n  Phase 2: After 3 seconds (should have ~3 tokens)")
    status = limiter.get_status(client)
    print(f"  Bucket status: Tokens={status['tokens_available']}\n")

    for i in range(8, 12):
        allowed = limiter.allow_request(client)
        status = limiter.get_status(client)
        result = "[ALLOWED]" if allowed else "[REJECTED]"
        print(f"  Request #{i:02d}: {result}  |  "
              f"Tokens={status['tokens_available']}/{status['bucket_capacity']}")

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY: Burst traffic is absorbed by full bucket.")
    print("  Long-term rate is always capped at refill_rate.")
    print("  Most widely used algorithm in production systems.")
    print("=" * 60)
