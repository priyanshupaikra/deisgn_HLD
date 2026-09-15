"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    RATE LIMITING — ALGORITHM 1                               ║
║                        FIXED WINDOW COUNTER                                  ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
Fixed Window Counter divides time into fixed-size windows (e.g., every 60 sec).
Each window maintains a counter that increments with every incoming request.
If the counter exceeds the allowed limit within the current window, the request
is REJECTED until the window resets.

HOW IT WORKS:
─────────────
  Timeline:   |--- Window 1 (0-60s) ---|--- Window 2 (60-120s) ---|
  Requests:         ▲ ▲ ▲ ▲ ▲                    ▲ ▲
  Counter:          1 2 3 4 5(LIMIT→REJECT)        1 2

  When time crosses the boundary, the counter resets back to 0.

ADVANTAGES:
-----------
  ✅ Very simple to implement
  ✅ Low memory footprint (only store count + window start time)
  ✅ Predictable — users know exactly when the window resets

DISADVANTAGES:
--------------
  ❌ "Boundary Burst" problem: A client can send 2x the allowed
     requests around a window boundary.
     Example (limit=5, window=60s):
       - Send 5 requests at t=59s  → All pass (Window 1 count: 5)
       - Send 5 requests at t=61s  → All pass (Window 2 count: 5)
       → 10 requests in ~2 seconds — doubles the effective rate!

REAL-WORLD USE CASE:
--------------------
  - Simple APIs where exact rate is not critical
  - Per-minute/per-hour API keys (e.g., GitHub public API: 60 req/hour)

"""

import time
from collections import defaultdict


class FixedWindowRateLimiter:
    """
    Fixed Window Counter Rate Limiter.

    Attributes:
        limit       : Max allowed requests per window
        window_size : Duration of each window in seconds
        clients     : Dict storing {client_id: [request_count, window_start_time]}
    """

    def __init__(self, limit: int, window_size: int):
        """
        :param limit:       Max requests allowed in each time window
        :param window_size: Size of the time window in seconds
        """
        self.limit = limit
        self.window_size = window_size
        # Each client gets its own counter + window start time
        self.clients = defaultdict(lambda: [0, None])

    def allow_request(self, client_id: str) -> bool:
        """
        Check if the request from `client_id` should be allowed.

        Steps:
          1. Get current timestamp
          2. If no window exists for client, start a new one
          3. If current time has crossed the window boundary → RESET counter
          4. If counter < limit → ALLOW and increment counter
          5. Else → REJECT

        :param client_id: Unique identifier for the client (e.g., IP address)
        :return:          True if allowed, False if rate-limited
        """
        now = time.time()
        count, window_start = self.clients[client_id]

        # Step 2 & 3: Initialize or reset the window if expired
        if window_start is None or (now - window_start) >= self.window_size:
            # New window starts now, reset counter to 0
            self.clients[client_id] = [0, now]
            count = 0

        # Step 4 & 5: Check the counter against the limit
        if count < self.limit:
            self.clients[client_id][0] += 1  # Increment counter
            return True  # ALLOW
        else:
            return False  # REJECT

    def get_status(self, client_id: str) -> dict:
        """Returns current window status for a client."""
        now = time.time()
        count, window_start = self.clients[client_id]
        if window_start is None:
            return {"count": 0, "remaining": self.limit, "reset_in": self.window_size}
        elapsed = now - window_start
        reset_in = max(0, self.window_size - elapsed)
        return {
            "count": count,
            "remaining": max(0, self.limit - count),
            "reset_in": round(reset_in, 2),
        }


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("   FIXED WINDOW COUNTER — Rate Limiter Demo")
    print("=" * 60)
    print("Config: limit=5 requests per 10-second window\n")

    # Allow 5 requests per 10 seconds
    limiter = FixedWindowRateLimiter(limit=5, window_size=10)
    client = "user_A"

    # Simulate 8 rapid requests
    for i in range(1, 9):
        allowed = limiter.allow_request(client)
        status = limiter.get_status(client)
        result = "[ALLOWED]" if allowed else "[REJECTED]"
        print(f"  Request #{i:02d}: {result}  |  "
              f"Count={status['count']}, Remaining={status['remaining']}, "
              f"Reset in {status['reset_in']}s")

    print(f"\n  Simulating window reset (sleeping 11s)...")
    time.sleep(11)

    print(f"\n  --- New Window Started ---")
    for i in range(9, 12):
        allowed = limiter.allow_request(client)
        status = limiter.get_status(client)
        result = "[ALLOWED]" if allowed else "[REJECTED]"
        print(f"  Request #{i:02d}: {result}  |  "
              f"Count={status['count']}, Remaining={status['remaining']}, "
              f"Reset in {status['reset_in']}s")

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY: Counter resets at every window boundary.")
    print("  Burst at boundary = major weakness of this algorithm.")
    print("=" * 60)
