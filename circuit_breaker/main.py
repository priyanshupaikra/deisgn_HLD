"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       CIRCUIT BREAKER SUITE                                  ║
║                     All Concepts — Main Runner                               ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  RESILIENCY IN DISTRIBUTED SYSTEMS:                                          ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  If a downstream service is struggling, hitting it with more requests will   ║
║  only make it worse (and tie up your own server's resources).                ║
║                                                                              ║
║  The Circuit Breaker pattern wraps network calls and 'trips' (stops sending  ║
║  requests) when failure thresholds are exceeded, allowing the downstream     ║
║  service time to recover.                                                    ║
║                                                                              ║
║  MODULES IN THIS DIRECTORY:                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  circuit_breaker.py → Implements Closed, Open, and Half-Open states.        ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import time
import circuit_breaker as cb


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


# ─────────────────────────────────────────────────────────────────────────────
def demo_circuit_breaker():
    section("MODULE 1: CIRCUIT BREAKER STATE MACHINE")
    
    payment_service = cb.PaymentService()
    breaker = cb.CircuitBreaker(failure_threshold=2, recovery_timeout=1.5)

    def safe_payment(amount: float):
        try:
            res = breaker.call(payment_service.process_payment, amount)
            print(f"    Success: {res}")
        except Exception as e:
            print(f"    Error: {str(e)}")

    print("\n  --- Phase 1: Normal Operation (CLOSED) ---")
    safe_payment(10.0)
    
    print("\n  --- Phase 2: Outage (Tripping to OPEN) ---")
    payment_service.is_down = True
    safe_payment(20.0) # Fail 1
    safe_payment(30.0) # Fail 2 -> Trips!
    
    print("\n  --- Phase 3: Fast Failing (OPEN) ---")
    safe_payment(40.0) 

    print("\n  --- Phase 4: Recovery Test (HALF-OPEN) ---")
    time.sleep(1.6)
    payment_service.is_down = False # Service healed!
    safe_payment(50.0) # Tests the waters, succeeds, resets to CLOSED
    safe_payment(60.0) # Normal


# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    section("CIRCUIT BREAKER — COMPLETE REFERENCE")
    print()
    
    print("  THE THREE STATES:")
    for row in [
        ("CLOSED",    "Normal operation. Requests pass through. Failures are counted."),
        ("OPEN",      "Service is failing. Requests 'fast-fail' immediately without"),
        ("",          "hitting the network. Prevents cascading failure."),
        ("HALF-OPEN", "Cooldown timer expired. Let a few test requests through. If"),
        ("",          "they succeed -> CLOSED. If they fail -> back to OPEN."),
    ]:
        print(f"    {row[0]:<10} {row[1]}")

    print("\n  POPULAR LIBRARIES:")
    for row in [
        ("Java",   "Resilience4j (replaced Netflix Hystrix)"),
        ("Python", "Pybreaker"),
        ("Go",     "Gobreaker"),
    ]:
        print(f"    {row[0]:<10} {row[1]}")

    print()
    print("  Run individual file:")
    print("    python circuit_breaker.py")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       CIRCUIT BREAKER — All Concepts")
    print("=" * 65)

    demo_circuit_breaker()
    print_summary()
