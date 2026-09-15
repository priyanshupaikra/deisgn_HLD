"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       API GATEWAY — COMPONENT 3                              ║
║                           CIRCUIT BREAKER                                    ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
The Circuit Breaker is a resilience pattern that prevents the API Gateway from
repeatedly calling a backend service that is FAILING or UNRESPONSIVE.

Named after electrical circuit breakers — when too much current flows (failures),
the breaker "trips" (opens) to protect the circuit (system).

Without a circuit breaker:
  - Each request to a failing service WAITS for a timeout (e.g., 30s)
  - Hundreds of requests pile up → thread pool exhaustion → gateway crashes
  - One failing microservice can cascade and bring down the ENTIRE system

With a circuit breaker:
  - After N consecutive failures, the breaker OPENS
  - Requests immediately get a fast-fail error (no waiting for timeout)
  - After a cooldown period, a PROBE request is allowed through (HALF-OPEN)
  - If probe succeeds → CLOSE the breaker (back to normal)
  - If probe fails → back to OPEN

THREE STATES:
─────────────

  ┌──────────┐  failure_threshold  ┌──────────┐
  │  CLOSED  │ ─────────────────→  │   OPEN   │
  │ (normal) │ ←───────────────── │  (trips) │
  └──────────┘   probe succeeds    └────┬─────┘
                                        │ cooldown expires
                                        ▼
                                  ┌──────────┐
                                  │HALF-OPEN │
                                  │ (probe)  │
                                  └──────────┘

STATE TRANSITIONS:
──────────────────
  CLOSED    → OPEN       : When consecutive failures >= failure_threshold
  OPEN      → HALF-OPEN  : After recovery_timeout seconds pass
  HALF-OPEN → CLOSED     : When the probe request SUCCEEDS
  HALF-OPEN → OPEN       : When the probe request FAILS

PARAMETERS TO TUNE:
───────────────────
  failure_threshold  : How many failures before tripping (e.g., 5)
  recovery_timeout   : Seconds to wait before probing again (e.g., 30s)
  success_threshold  : How many successes in HALF-OPEN to fully close (e.g., 2)

ADVANTAGES:
-----------
  ✅ Fail-fast: No wasted time waiting for timeouts
  ✅ Cascading failure prevention
  ✅ Auto-recovery: Probe allows detecting when service comes back
  ✅ Improves overall system resilience

REAL-WORLD EXAMPLES:
────────────────────
  - Netflix Hystrix (now in maintenance) — the original
  - Resilience4j (Java — most popular today)
  - Polly (C#/.NET)
  - AWS App Mesh, Istio service mesh — built-in circuit breaking
  - This pattern is used in virtually every production microservice system

"""

import time
import threading
import random
from enum import Enum
from dataclasses import dataclass, field
from router import Request, Response


class CircuitState(Enum):
    """
    The three states of a Circuit Breaker.

    CLOSED    : Normal operation — requests pass through
    OPEN      : Tripped — requests immediately fail-fast
    HALF_OPEN : Recovery probe — one test request is allowed through
    """
    CLOSED    = "CLOSED"
    OPEN      = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class CircuitBreakerStats:
    """Metrics tracked by the circuit breaker."""
    total_calls: int = 0
    total_failures: int = 0
    total_successes: int = 0
    total_short_circuits: int = 0    # Requests rejected while OPEN


class CircuitBreaker:
    """
    Circuit Breaker for a single backend service.

    Each microservice in the gateway gets its own circuit breaker instance.
    They operate independently — a failure in Order Service doesn't trip
    the User Service circuit.

    Attributes:
        service_name       : Name of the guarded backend service
        failure_threshold  : Consecutive failures needed to OPEN the circuit
        recovery_timeout   : Seconds to wait before moving to HALF_OPEN
        success_threshold  : Successes in HALF_OPEN needed to re-CLOSE
        state              : Current circuit state
        consecutive_failures: Running count of failures in CLOSED state
        half_open_successes : Running count of successes in HALF_OPEN state
        opened_at          : Timestamp when circuit last opened
    """

    def __init__(self, service_name: str,
                 failure_threshold: int = 3,
                 recovery_timeout: float = 10.0,
                 success_threshold: int = 1):
        """
        :param service_name:      Name of the microservice being protected
        :param failure_threshold: Consecutive failures needed to open circuit
        :param recovery_timeout:  Seconds before trying HALF_OPEN probe
        :param success_threshold: Successes in HALF_OPEN before closing
        """
        self.service_name = service_name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        # State machine
        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0
        self.half_open_successes = 0
        self.opened_at: float = 0

        # Stats
        self.stats = CircuitBreakerStats()
        self.lock = threading.Lock()

    def _transition_to(self, new_state: CircuitState) -> None:
        """Change state and print the transition for observability."""
        old = self.state.value
        self.state = new_state
        if new_state == CircuitState.OPEN:
            self.opened_at = time.time()
        print(f"  [CircuitBreaker:{self.service_name}] "
              f"{old} → {new_state.value}")

    def can_attempt(self) -> bool:
        """
        Determine if a request is allowed through based on current state.

        CLOSED    → Always allow
        OPEN      → Block UNLESS recovery_timeout has elapsed
                    (if elapsed → transition to HALF_OPEN and allow ONE probe)
        HALF_OPEN → Allow (the probe is passing through)
        """
        with self.lock:
            if self.state == CircuitState.CLOSED:
                return True  # Normal operation

            if self.state == CircuitState.OPEN:
                # Check if recovery window has elapsed
                elapsed = time.time() - self.opened_at
                if elapsed >= self.recovery_timeout:
                    # Time to probe — transition to HALF_OPEN
                    self._transition_to(CircuitState.HALF_OPEN)
                    self.half_open_successes = 0
                    return True  # Allow the probe request through
                else:
                    # Still in cooldown — reject immediately
                    self.stats.total_short_circuits += 1
                    return False

            # HALF_OPEN: allow the probe
            return True

    def record_success(self) -> None:
        """
        Call this when the backend call SUCCEEDS.

        CLOSED    → Reset consecutive failure counter
        HALF_OPEN → Increment success counter; close if threshold met
        """
        with self.lock:
            self.stats.total_successes += 1
            self.stats.total_calls += 1
            self.consecutive_failures = 0  # Reset on any success

            if self.state == CircuitState.HALF_OPEN:
                self.half_open_successes += 1
                if self.half_open_successes >= self.success_threshold:
                    self._transition_to(CircuitState.CLOSED)

    def record_failure(self) -> None:
        """
        Call this when the backend call FAILS.

        CLOSED    → Increment failure counter; open if threshold hit
        HALF_OPEN → Probe failed → reopen the circuit
        """
        with self.lock:
            self.stats.total_failures += 1
            self.stats.total_calls += 1

            if self.state == CircuitState.HALF_OPEN:
                # Probe failed — back to OPEN
                self._transition_to(CircuitState.OPEN)
                return

            if self.state == CircuitState.CLOSED:
                self.consecutive_failures += 1
                if self.consecutive_failures >= self.failure_threshold:
                    self._transition_to(CircuitState.OPEN)

    def call(self, func, request: Request) -> Response:
        """
        Execute a backend call through the circuit breaker guard.

        :param func:    The function to call (backend handler)
        :param request: The request to pass to the function
        :return:        Backend Response, or fast-fail Response if circuit OPEN
        """
        if not self.can_attempt():
            # Circuit is OPEN — fail fast without calling backend
            return Response(503, {
                "error": "Service Unavailable",
                "message": f"Circuit breaker OPEN for '{self.service_name}'. "
                           f"Service is experiencing failures. Retry later.",
                "retry_after": int(self.recovery_timeout - (time.time() - self.opened_at))
            })

        try:
            response = func(request)
            # Consider 5xx responses as failures
            if response.status_code >= 500:
                self.record_failure()
            else:
                self.record_success()
            return response
        except Exception as e:
            # Exception = failure (e.g., connection timeout)
            self.record_failure()
            return Response(502, {"error": "Bad Gateway", "message": str(e)})

    def get_status(self) -> dict:
        """Return the current circuit breaker status for monitoring."""
        return {
            "service": self.service_name,
            "state": self.state.value,
            "consecutive_failures": self.consecutive_failures,
            "stats": {
                "total_calls": self.stats.total_calls,
                "successes": self.stats.total_successes,
                "failures": self.stats.total_failures,
                "short_circuits": self.stats.total_short_circuits,
            }
        }


# ─────────────────────────────────────────────────────────────────────────────
# Circuit Breaker Registry (one per service)
# ─────────────────────────────────────────────────────────────────────────────

class CircuitBreakerRegistry:
    """Manages circuit breakers for all registered backend services."""

    def __init__(self, failure_threshold=3, recovery_timeout=10.0):
        self.breakers: dict[str, CircuitBreaker] = {}
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

    def get(self, service_name: str) -> CircuitBreaker:
        """Get or create a circuit breaker for the given service."""
        if service_name not in self.breakers:
            self.breakers[service_name] = CircuitBreaker(
                service_name,
                failure_threshold=self.failure_threshold,
                recovery_timeout=self.recovery_timeout,
            )
        return self.breakers[service_name]

    def print_all_status(self) -> None:
        """Print status of all circuit breakers."""
        print("\n  --- Circuit Breaker Status ---")
        for name, cb in self.breakers.items():
            status = cb.get_status()
            print(f"    {name:<20} State={status['state']:<10} "
                  f"Failures={status['consecutive_failures']} "
                  f"Short-circuits={status['stats']['short_circuits']}")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

# Simulate an unreliable backend service
_call_count = 0

def unreliable_order_service(req: Request) -> Response:
    """Simulates a backend that fails 60% of the time."""
    global _call_count
    _call_count += 1
    if _call_count <= 5 or random.random() < 0.6:
        # Return HTTP 500 (simulated backend failure)
        return Response(500, {"error": "Internal Server Error", "service": "order-service"})
    return Response(200, {"order_id": "ORD-999", "status": "created"})


if __name__ == "__main__":
    print("=" * 60)
    print("   CIRCUIT BREAKER — API Gateway Component Demo")
    print("=" * 60)
    print("  Config: failure_threshold=3, recovery_timeout=5s\n")

    cb = CircuitBreaker("order-service", failure_threshold=3, recovery_timeout=5.0)

    print("  Phase 1: Sending requests to a failing service\n")
    for i in range(1, 10):
        req = Request("POST", "/orders", body={"product_id": "p1"})
        resp = cb.call(unreliable_order_service, req)
        status = cb.get_status()
        print(f"    Request #{i:02d}: HTTP {resp.status_code} | "
              f"Circuit={status['state']:<10} | "
              f"Failures={status['consecutive_failures']}")

    print(f"\n  Phase 2: Waiting 6s for recovery window to pass...")
    time.sleep(6)

    # Reset the global counter so the service "recovers"
    _call_count = 999

    def recovering_service(req: Request) -> Response:
        return Response(200, {"order_id": "ORD-100", "status": "created"})

    print(f"\n  Phase 3: Service recovered — probe request (HALF-OPEN)\n")
    for i in range(1, 4):
        req = Request("POST", "/orders")
        resp = cb.call(recovering_service, req)
        status = cb.get_status()
        print(f"    Request #{i:02d}: HTTP {resp.status_code} | "
              f"Circuit={status['state']}")

    print()
    print("  Final status:", cb.get_status())

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY: Circuit Breaker prevents cascade failures.")
    print("  CLOSED → OPEN on repeated failures → HALF_OPEN probe")
    print("  → CLOSED on recovery. Fast-fail protects the gateway.")
    print("=" * 60)
