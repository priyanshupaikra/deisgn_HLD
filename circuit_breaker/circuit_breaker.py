"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       CIRCUIT BREAKER — MODULE 1                             ║
║                     STATE MACHINE (Closed, Open, Half-Open)                  ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS A CIRCUIT BREAKER?
──────────────────────────
In electrical engineering, if too much current flows, a circuit breaker trips
(opens) to stop the flow and prevent the house from burning down.

In distributed systems, if Service A calls Service B, and Service B is struggling
(timing out or returning 500s), Service A should STOP calling Service B.
If Service A keeps calling, it will exhaust its own threads waiting for timeouts,
causing a cascading failure where Service A also crashes.

THE THREE STATES:
─────────────────
  1. CLOSED (Normal state)
     - The circuit is closed, electricity (requests) flows normally.
     - We count failures. If failures > threshold, trip to OPEN.

  2. OPEN (Failing state)
     - The circuit is tripped. Requests fail IMMEDIATELY without hitting the network.
     - This gives the downstream service time to recover.
     - Starts a cooldown timer. When timer expires, transition to HALF-OPEN.

  3. HALF-OPEN (Testing state)
     - We let a *limited* number of requests through to test the waters.
     - If the test requests SUCCEED, the service healed! Go back to CLOSED.
     - If the test requests FAIL, it's still broken. Go back to OPEN.
"""

import time
import random
from enum import Enum


class CircuitState(Enum):
    CLOSED = "CLOSED"       # Normal operation
    OPEN = "OPEN"           # Failing, fast-fail requests
    HALF_OPEN = "HALF_OPEN" # Testing if it recovered


class CircuitBreaker:
    """
    Protects downstream services by wrapping calls in a state machine.
    """
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 2.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0

    def call(self, func, *args, **kwargs):
        """Wraps the function call in circuit breaker logic."""
        
        # 1. State: OPEN
        if self.state == CircuitState.OPEN:
            # Check if recovery timeout has passed
            if time.time() - self.last_failure_time > self.recovery_timeout:
                print("  [CircuitBreaker] Timeout passed. Transitioning to HALF-OPEN.")
                self.state = CircuitState.HALF_OPEN
            else:
                # Fast fail! Do not hit the network.
                raise Exception("CircuitBreaker is OPEN: Fast failing request.")

        # 2. State: HALF-OPEN or CLOSED
        try:
            # Actually call the downstream service
            result = func(*args, **kwargs)
            
            # If we get here, the call succeeded!
            if self.state == CircuitState.HALF_OPEN:
                print("  [CircuitBreaker] Test request succeeded! Transitioning to CLOSED.")
                self._reset()
            elif self.state == CircuitState.CLOSED:
                self.failure_count = 0 # Reset count on success
                
            return result

        except Exception as e:
            # The call failed (network timeout, 500 error, etc.)
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == CircuitState.HALF_OPEN:
                print("  [CircuitBreaker] Test request failed! Reverting to OPEN.")
                self.state = CircuitState.OPEN
                
            elif self.state == CircuitState.CLOSED:
                if self.failure_count >= self.failure_threshold:
                    print(f"  [CircuitBreaker] {self.failure_count} failures reached! Tripping to OPEN.")
                    self.state = CircuitState.OPEN
                    
            raise e

    def _reset(self):
        self.state = CircuitState.CLOSED
        self.failure_count = 0


# ─────────────────────────────────────────────────────────────────────────────
# SIMULATED DOWNSTREAM SERVICE
# ─────────────────────────────────────────────────────────────────────────────

class PaymentService:
    def __init__(self):
        self.is_down = False
        
    def process_payment(self, amount: float):
        if self.is_down:
            time.sleep(0.1) # Simulate network timeout
            raise ConnectionError("Payment Gateway Timeout")
        
        return f"Payment of ${amount} processed successfully."


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   CIRCUIT BREAKER — Demo")
    print("=" * 65)
    
    payment_service = PaymentService()
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1.5)

    def safe_payment(amount: float):
        try:
            res = cb.call(payment_service.process_payment, amount)
            print(f"    Success: {res}")
        except Exception as e:
            print(f"    Error: {str(e)}")

    # 1. Normal Operation
    print("\n  --- Phase 1: Normal Operation (State: CLOSED) ---")
    safe_payment(10.0)
    safe_payment(20.0)

    # 2. Service Goes Down
    print("\n  --- Phase 2: Downstream Outage (Tripping the Circuit) ---")
    payment_service.is_down = True
    safe_payment(30.0) # Failure 1
    safe_payment(40.0) # Failure 2 -> Trips to OPEN
    
    print("\n  --- Phase 3: Fast Failing (State: OPEN) ---")
    print("  (Notice we instantly fail without waiting for network timeouts)")
    safe_payment(50.0) 
    safe_payment(60.0) 

    # 3. Recovery Attempt
    print("\n  --- Phase 4: Recovery (State: HALF-OPEN) ---")
    print("  (Waiting 1.6 seconds for cooldown...)")
    time.sleep(1.6)
    
    # It tests the water, but the service is still down!
    safe_payment(70.0) 
    
    print("\n  --- Phase 5: Fast Failing Again (State: OPEN) ---")
    safe_payment(80.0)

    # 4. Service Recovers
    print("\n  --- Phase 6: Service Restored (State: HALF-OPEN -> CLOSED) ---")
    print("  (Waiting 1.6 seconds for cooldown...)")
    time.sleep(1.6)
    payment_service.is_down = False # FIX THE SERVICE
    
    # Test request succeeds, closes the circuit
    safe_payment(90.0)
    
    # Back to normal
    safe_payment(100.0)
