"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    RATE LIMITING — ALGORITHM 2                               ║
║                       SLIDING WINDOW LOG                                     ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
The Sliding Window Log algorithm keeps a timestamp log (queue) of every
request made by a client. When a new request arrives, it:
  1. Removes all timestamps older than (now - window_size)  ← "slides" the window
  2. Counts the remaining timestamps in the log
  3. If count < limit → ALLOW (and add the new timestamp)
  4. Else → REJECT

Unlike Fixed Window, the window is always anchored to the CURRENT TIME,
not to a fixed clock boundary. This is why there is NO boundary burst problem.

HOW IT WORKS (Visual):
──────────────────────
  Window size = 60s, Limit = 3

  At t=100s: log = [60, 80, 90]   → 3 entries in [40, 100] → REJECT new req
  At t=105s: log = [80, 90]       → entry @60 is now >60s old, evicted
             → 2 entries in [45, 105] → ALLOW new req → log = [80, 90, 105]

  The window literally SLIDES forward with every request.

ADVANTAGES:
-----------
  ✅ No boundary burst problem — window always anchored to current time
  ✅ Very accurate — limits are enforced in any rolling time window
  ✅ Fair across all time frames

DISADVANTAGES:
--------------
  ❌ Higher memory usage — must store a timestamp for every request
     (can be huge for high-traffic clients: 1000 req/min = 1000 entries)
  ❌ Slightly more compute per request (evicting old entries)

REAL-WORLD USE CASE:
--------------------
  - When precision matters (financial APIs, billing systems)
  - Stripe, Shopify use sliding window variants for API rate limiting

"""

import time
from collections import defaultdict, deque


class SlidingWindowLogRateLimiter:
    """
    Sliding Window Log Rate Limiter.

    Stores a deque (double-ended queue) of request timestamps for each client.
    Old timestamps are evicted as the window slides forward in time.

    Attributes:
        limit       : Max requests allowed in any rolling window
        window_size : Duration of the rolling window in seconds
        logs        : Dict mapping {client_id: deque of timestamps}
    """

    def __init__(self, limit: int, window_size: int):
        """
        :param limit:       Max requests allowed in the rolling window
        :param window_size: Rolling window duration in seconds
        """
        self.limit = limit
        self.window_size = window_size
        # deque allows O(1) append from right and O(1) pop from left
        self.logs = defaultdict(deque)

    def allow_request(self, client_id: str) -> bool:
        """
        Determine if the request should be allowed.

        Algorithm:
          - Slide the window: remove timestamps older than (now - window_size)
          - Count remaining timestamps
          - If count < limit: add current timestamp and ALLOW
          - Else: REJECT

        Time Complexity: O(k) where k = requests evicted (amortized O(1))
        Space Complexity: O(n) where n = requests in current window

        :param client_id: Unique identifier for the caller
        :return:          True if allowed, False if rate-limited
        """
        now = time.time()
        window_start = now - self.window_size
        log = self.logs[client_id]

        # SLIDE THE WINDOW: Remove all timestamps outside the rolling window
        # deque front (left) holds oldest timestamps
        while log and log[0] <= window_start:
            log.popleft()  # O(1) removal from front

        # COUNT requests currently in the sliding window
        if len(log) < self.limit:
            # ALLOW: record this request's timestamp
            log.append(now)  # O(1) append to back
            return True
        else:
            # REJECT: limit exceeded in the rolling window
            return False

    def get_status(self, client_id: str) -> dict:
        """Returns how many requests are in the current rolling window."""
        now = time.time()
        window_start = now - self.window_size
        log = self.logs[client_id]
        # Count valid timestamps (without modifying the log)
        valid_count = sum(1 for ts in log if ts > window_start)
        # Time until oldest request ages out of the window
        oldest = log[0] if log else None
        retry_after = round(max(0, oldest + self.window_size - now), 2) if oldest else 0
        return {
            "requests_in_window": valid_count,
            "remaining": max(0, self.limit - valid_count),
            "retry_after_sec": retry_after,
        }


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("   SLIDING WINDOW LOG — Rate Limiter Demo")
    print("=" * 60)
    print("Config: limit=5 requests per 10-second rolling window\n")

    limiter = SlidingWindowLogRateLimiter(limit=5, window_size=10)
    client = "user_B"

    # Rapid fire 7 requests
    for i in range(1, 8):
        allowed = limiter.allow_request(client)
        status = limiter.get_status(client)
        result = "[ALLOWED]" if allowed else "[REJECTED]"
        print(f"  Request #{i:02d}: {result}  |  "
              f"In-Window={status['requests_in_window']}, "
              f"Remaining={status['remaining']}, "
              f"Retry after={status['retry_after_sec']}s")
        time.sleep(0.1)  # slight spacing

    print(f"\n  Sleeping 5 seconds (some old requests will age out)...")
    time.sleep(5)

    print(f"\n  --- Sending more requests after 5s ---")
    for i in range(8, 11):
        allowed = limiter.allow_request(client)
        status = limiter.get_status(client)
        result = "[ALLOWED]" if allowed else "[REJECTED]"
        print(f"  Request #{i:02d}: {result}  |  "
              f"In-Window={status['requests_in_window']}, "
              f"Remaining={status['remaining']}, "
              f"Retry after={status['retry_after_sec']}s")

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY: Window slides with every request.")
    print("  More accurate than Fixed Window — no burst at boundaries.")
    print("=" * 60)
