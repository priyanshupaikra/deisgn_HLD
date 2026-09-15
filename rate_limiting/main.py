"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                         RATE LIMITING SUITE                                  ║
║                    All 4 Algorithms — Main Runner                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WHAT IS RATE LIMITING?                                                      ║
║  ─────────────────────                                                       ║
║  Rate limiting controls how many requests a client can make to a service     ║
║  within a given time window. It protects against:                            ║
║    • DDoS (Distributed Denial of Service) attacks                            ║
║    • API abuse and scraping                                                  ║
║    • Server overload from misbehaving clients                                ║
║    • Accidental infinite-loop bugs in client code                            ║
║                                                                              ║
║  ALGORITHMS COVERED:                                                         ║
║  ───────────────────                                                         ║
║  1. Fixed Window Counter  — Simple counter per time window                   ║
║  2. Sliding Window Log    — Accurate rolling window with timestamp log       ║
║  3. Token Bucket          — Allows bursts; smooths long-term rate            ║
║  4. Leaky Bucket          — Enforces constant output rate; no burst          ║
║                                                                              ║
║  COMPARISON TABLE:                                                           ║
║  ────────────────────────────────────────────────────────────────────────    ║
║  Algorithm        | Burst OK? | Memory  | Accuracy | Complexity              ║
║  ─────────────────────────────────────────────────────────────────────────   ║
║  Fixed Window     |  Partial  |  Low    |  Low     | Very Simple             ║
║  Sliding Window   |  No       |  High   |  Highest | Medium                  ║
║  Token Bucket     |  Yes      |  Low    |  High    | Medium                  ║
║  Leaky Bucket     |  No       |  Low    |  High    | Medium                  ║
║                                                                              ║
║  HOW TO RUN INDIVIDUAL ALGORITHMS:                                           ║
║    python fixed_window.py                                                    ║
║    python sliding_window.py                                                  ║
║    python token_bucket.py                                                    ║
║    python leaky_bucket.py                                                    ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import time

# ─── Import all 4 rate limiting implementations ───────────────────────────────
from fixed_window import FixedWindowRateLimiter
from sliding_window import SlidingWindowLogRateLimiter
from token_bucket import TokenBucketRateLimiter
from leaky_bucket import LeakyBucketRateLimiter


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: Run a quick simulation for any limiter
# ─────────────────────────────────────────────────────────────────────────────
def run_simulation(name: str, limiter, client_id: str, num_requests: int, delay: float = 0):
    """
    Generic simulation runner.

    :param name:         Algorithm name for display
    :param limiter:      Any rate limiter with .allow_request(client_id) method
    :param client_id:    Client identifier string
    :param num_requests: Number of requests to simulate
    :param delay:        Delay in seconds between each request
    """
    print(f"\n{'─' * 55}")
    print(f"  [{name}] — Simulating {num_requests} requests")
    print(f"{'─' * 55}")

    allowed_count = 0
    rejected_count = 0

    for i in range(1, num_requests + 1):
        result = limiter.allow_request(client_id)
        if result:
            allowed_count += 1
            status = "[ALLOWED] "
        else:
            rejected_count += 1
            status = "[REJECTED]"
        print(f"    Request #{i:02d}: {status}")
        if delay > 0:
            time.sleep(delay)

    print(f"\n  Summary: {allowed_count} allowed, {rejected_count} rejected "
          f"(out of {num_requests} total)")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — Run all 4 algorithms back to back
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":

    print("\n" + "=" * 55)
    print("       RATE LIMITING — All 4 Algorithms Demo")
    print("=" * 55)
    print("""
  SYSTEM DESIGN CONTEXT:
  ─────────────────────
  When to use which algorithm:

  Fixed Window   → Simple use cases, dashboard analytics, cheap storage
  Sliding Window → High-accuracy requirements (billing, compliance)
  Token Bucket   → REST APIs allowing occasional bursts (most popular)
  Leaky Bucket   → Stream processing, network QoS, uniform throughput

  All examples below: Limit=5 requests per 10 seconds
    """)

    # ── 1. FIXED WINDOW ────────────────────────────────────────────────────
    # Simple and cheap — divides time into fixed windows, counts per window
    fixed = FixedWindowRateLimiter(limit=5, window_size=10)
    run_simulation(
        name="FIXED WINDOW",
        limiter=fixed,
        client_id="client_1",
        num_requests=8   # First 5 pass, next 3 are rejected
    )

    # ── 2. SLIDING WINDOW LOG ──────────────────────────────────────────────
    # Most accurate — no boundary burst, uses timestamp log per request
    sliding = SlidingWindowLogRateLimiter(limit=5, window_size=10)
    run_simulation(
        name="SLIDING WINDOW LOG",
        limiter=sliding,
        client_id="client_2",
        num_requests=8   # First 5 pass, next 3 rejected (rolling window)
    )

    # ── 3. TOKEN BUCKET ────────────────────────────────────────────────────
    # Allows bursting — consumes tokens from bucket, lazy refill over time
    token = TokenBucketRateLimiter(capacity=5, refill_rate=0.5)
    run_simulation(
        name="TOKEN BUCKET",
        limiter=token,
        client_id="client_3",
        num_requests=8   # First 5 pass (full bucket), rest rejected until refill
    )

    # ── 4. LEAKY BUCKET ────────────────────────────────────────────────────
    # No bursting — strictly uniform output rate, bucket overflows = reject
    leaky = LeakyBucketRateLimiter(capacity=5, leak_rate=0.5)
    run_simulation(
        name="LEAKY BUCKET",
        limiter=leaky,
        client_id="client_4",
        num_requests=8   # First 5 fill bucket, next 3 overflow = rejected
    )

    # ── FINAL COMPARISON ──────────────────────────────────────────────────
    print("\n" + "=" * 55)
    print("  ALGORITHM COMPARISON SUMMARY")
    print("=" * 55)
    rows = [
        ("Algorithm",       "Burst?",    "Memory", "Accuracy", "Best For"),
        ("─" * 16,          "─" * 7,     "─" * 8,  "─" * 9,   "─" * 24),
        ("Fixed Window",    "Partial",   "Low",    "Low",      "Simple APIs"),
        ("Sliding Window",  "No",        "High",   "Highest",  "Billing/Compliance"),
        ("Token Bucket",    "Yes",       "Low",    "High",     "REST APIs (most common)"),
        ("Leaky Bucket",    "No",        "Low",    "High",     "Traffic shaping/QoS"),
    ]
    for row in rows:
        print(f"  {row[0]:<18} {row[1]:<9} {row[2]:<10} {row[3]:<11} {row[4]}")

    print("\n" + "=" * 55)
    print("  Run individual files for detailed per-algorithm demos.")
    print("=" * 55 + "\n")
