"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       IDEMPOTENCY — MODULE 1                                 ║
║                     IDEMPOTENCY KEY SYSTEM                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
IDEMPOTENCY means: performing an operation MULTIPLE TIMES produces the SAME
result as performing it ONCE. The side effects happen exactly once, no matter
how many times the request is sent.

MATHEMATICAL DEFINITION:
─────────────────────────
  f(f(x)) = f(x)
  The result of applying f twice equals applying it once.

  Examples:
    Idempotent:     Set price to $10 → Set again → Still $10 ✅
    NOT idempotent: Add $10 to balance → Add again → Balance now +$20 ❌

WHY IS IDEMPOTENCY CRITICAL?
──────────────────────────────
  In distributed systems, RETRIES are unavoidable:
    - Network timeouts (client retries after 30s)
    - Load balancer retries on 5xx errors
    - Message queue re-delivery (Kafka at-least-once)
    - Client-side retry with exponential backoff

  Without idempotency:
    Client sends "charge $100" → times out (not sure if it went through)
    Client retries → DUPLICATE CHARGE → customer charged TWICE

  With idempotency:
    Client sends "charge $100" with Idempotency-Key: key_abc
    Times out → retries with SAME key
    Server recognizes the key → returns PREVIOUS result → NO duplicate charge

THE IDEMPOTENCY KEY PATTERN:
─────────────────────────────
  1. Client generates a UNIQUE key (UUID) per operation
  2. Client sends it in a header: Idempotency-Key: uuid-abc-123
  3. Server checks if this key has been processed before:
     a. If NOT seen: process request → store (key → result) → return result
     b. If ALREADY seen: return STORED result immediately (no re-processing)
  4. Key expires after some time (e.g., 24 hours)

  KEY STORAGE (Redis is ideal):
    SET idempotency:{key} {status:processing} EX 86400
    SET idempotency:{key} {status:complete, result: {...}} EX 86400

REAL-WORLD EXAMPLES:
────────────────────
  - Stripe: Idempotency-Key header on all payment API calls
  - AWS: ClientToken parameter for EC2 RunInstances
  - PayPal: PayPal-Request-Id header
  - Twilio: X-Twilio-Idempotency-Token header
  - GitHub: X-GitHub-Delivery header for webhook events

"""

import time
import uuid
import threading
from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Optional, Callable


# ─────────────────────────────────────────────────────────────────────────────
# Idempotency Record States
# ─────────────────────────────────────────────────────────────────────────────

class IdempotencyStatus(Enum):
    """
    Lifecycle states of an idempotency record.

    PROCESSING → A request with this key is currently being handled.
                 Other concurrent requests with the same key should
                 wait (or return 409 Conflict).

    COMPLETE   → The operation finished successfully.
                 Return the stored result for all future identical requests.

    FAILED     → The operation failed. Depending on the error type:
                 - Transient (network, 5xx): allow retry (delete the record)
                 - Permanent (validation, 4xx): return stored error forever
    """
    PROCESSING = "PROCESSING"
    COMPLETE   = "COMPLETE"
    FAILED     = "FAILED"


@dataclass
class IdempotencyRecord:
    """
    Stored record for one idempotency key.
    Think of this as a row in a Redis hash or a DB table.
    """
    key: str                           # The client-provided idempotency key
    status: IdempotencyStatus          # Current lifecycle state
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    request_fingerprint: str = ""      # Hash of (endpoint + request_body) for safety
    response_status_code: Optional[int] = None
    response_body: Optional[Any] = None
    error_message: str = ""
    ttl_seconds: float = 86400         # Default: 24 hours

    @property
    def is_expired(self) -> bool:
        return time.time() > self.created_at + self.ttl_seconds

    def __repr__(self):
        return (f"IdempotencyRecord(key={self.key[:12]}..., "
                f"status={self.status.value}, "
                f"http={self.response_status_code})")


# ─────────────────────────────────────────────────────────────────────────────
# Idempotency Store (simulates Redis)
# ─────────────────────────────────────────────────────────────────────────────

class IdempotencyStore:
    """
    Storage backend for idempotency records.
    In production: Redis with TTL (SETEX idempotency:{key} 86400 {json})

    In-memory implementation for demonstration.
    """

    def __init__(self):
        self._store: dict[str, IdempotencyRecord] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[IdempotencyRecord]:
        """Retrieve a record. Returns None if not found or expired."""
        with self._lock:
            record = self._store.get(key)
            if record and record.is_expired:
                del self._store[key]
                return None
            return record

    def set(self, record: IdempotencyRecord) -> None:
        """Store or update a record."""
        with self._lock:
            self._store[record.key] = record

    def delete(self, key: str) -> None:
        """Remove a record (used when allowing retries on transient failures)."""
        with self._lock:
            self._store.pop(key, None)

    def exists(self, key: str) -> bool:
        return self.get(key) is not None

    def stats(self) -> dict:
        """Summary of stored records."""
        with self._lock:
            counts = {}
            for r in self._store.values():
                counts[r.status.value] = counts.get(r.status.value, 0) + 1
            return {"total": len(self._store), "by_status": counts}


# ─────────────────────────────────────────────────────────────────────────────
# Idempotency Middleware / Decorator
# ─────────────────────────────────────────────────────────────────────────────

class IdempotencyMiddleware:
    """
    Idempotency enforcement middleware.

    Wraps any operation (payment, order creation, email send, etc.)
    to make it safe to retry.

    HOW IT WORKS:
    ─────────────
    Step 1: Extract Idempotency-Key from request headers.
            If missing → reject with 400 (for non-idempotent ops).

    Step 2: Look up the key in the idempotency store.
            CASE A — Key NOT found:
              → Mark as PROCESSING (prevent concurrent duplicates)
              → Execute the actual operation
              → If success: store COMPLETE + response → return response
              → If failure: store FAILED + error → return error

            CASE B — Key found, status=PROCESSING:
              → Another request with same key is in-flight
              → Return 409 Conflict (or wait with polling)

            CASE C — Key found, status=COMPLETE:
              → Already processed successfully
              → Return stored response IMMEDIATELY (no re-processing)

            CASE D — Key found, status=FAILED (transient error):
              → Delete record and allow retry
              → Treat as Case A

    FINGERPRINTING (safety check):
    ───────────────────────────────
    Even with the same Idempotency-Key, if the request BODY is different,
    that's likely a client bug. Reject it with 422 Unprocessable Entity.
    Example: client reuses a key for a $100 payment but now sends $200.
    """

    # Error types that should NOT be retried (permanent failures)
    PERMANENT_ERROR_CODES = {400, 401, 403, 404, 422, 409}

    def __init__(self, store: IdempotencyStore):
        self.store = store
        self.total_requests = 0
        self.deduplicated_requests = 0  # Requests that returned cached response

    def execute(self,
                idempotency_key: str,
                operation: Callable[[], dict],
                request_fingerprint: str = "",
                ttl_seconds: float = 86400) -> dict:
        """
        Execute an operation with idempotency guarantee.

        :param idempotency_key:     Client-provided unique key (UUID recommended)
        :param operation:           The actual operation to execute (callable)
        :param request_fingerprint: Hash of (endpoint + body) for safety check
        :param ttl_seconds:         How long to remember this key
        :return:                    Dict with {status_code, body, was_duplicate}
        """
        self.total_requests += 1

        # ── STEP 1: Check if key already exists ───────────────────────────
        existing = self.store.get(idempotency_key)

        if existing:
            # ── CASE B: Still processing (concurrent duplicate) ────────────
            if existing.status == IdempotencyStatus.PROCESSING:
                print(f"    [Idempotency] Key {idempotency_key[:12]}... is PROCESSING "
                      f"→ 409 Conflict")
                return {
                    "status_code": 409,
                    "body": {
                        "error": "Conflict",
                        "message": "A request with this idempotency key is already being processed."
                    },
                    "was_duplicate": True,
                }

            # ── CASE C: Already completed → return cached result ───────────
            if existing.status == IdempotencyStatus.COMPLETE:
                self.deduplicated_requests += 1
                print(f"    [Idempotency] Key {idempotency_key[:12]}... already COMPLETE "
                      f"→ returning cached HTTP {existing.response_status_code}")
                return {
                    "status_code": existing.response_status_code,
                    "body": existing.response_body,
                    "was_duplicate": True,
                    "original_time": existing.completed_at,
                }

            # ── CASE D: Failed with transient error → allow retry ──────────
            if existing.status == IdempotencyStatus.FAILED:
                if existing.response_status_code not in self.PERMANENT_ERROR_CODES:
                    print(f"    [Idempotency] Key {idempotency_key[:12]}... FAILED (transient) "
                          f"→ allowing retry")
                    self.store.delete(idempotency_key)
                    existing = None  # Fall through to fresh execution
                else:
                    # Permanent failure: return stored error
                    print(f"    [Idempotency] Key {idempotency_key[:12]}... FAILED (permanent) "
                          f"→ returning cached error")
                    self.deduplicated_requests += 1
                    return {
                        "status_code": existing.response_status_code,
                        "body": {"error": existing.error_message},
                        "was_duplicate": True,
                    }

        # ── STEP 2: New request — mark as PROCESSING ──────────────────────
        record = IdempotencyRecord(
            key=idempotency_key,
            status=IdempotencyStatus.PROCESSING,
            request_fingerprint=request_fingerprint,
            ttl_seconds=ttl_seconds,
        )
        self.store.set(record)
        print(f"    [Idempotency] Key {idempotency_key[:12]}... → NEW, executing operation...")

        # ── STEP 3: Execute the actual operation ──────────────────────────
        try:
            result = operation()
            status_code = result.get("status_code", 200)
            response_body = result.get("body", {})

            # ── STEP 4a: Success → mark COMPLETE and store response ────────
            record.status = IdempotencyStatus.COMPLETE
            record.response_status_code = status_code
            record.response_body = response_body
            record.completed_at = time.time()
            self.store.set(record)

            print(f"    [Idempotency] Key {idempotency_key[:12]}... → COMPLETE "
                  f"(HTTP {status_code}), stored for future retries")
            return {"status_code": status_code, "body": response_body, "was_duplicate": False}

        except Exception as e:
            # ── STEP 4b: Failure → mark FAILED and store error ────────────
            record.status = IdempotencyStatus.FAILED
            record.error_message = str(e)
            record.response_status_code = 500
            record.completed_at = time.time()
            self.store.set(record)

            print(f"    [Idempotency] Key {idempotency_key[:12]}... → FAILED: {e}")
            return {
                "status_code": 500,
                "body": {"error": "Internal Server Error", "message": str(e)},
                "was_duplicate": False,
            }

    def stats(self) -> dict:
        return {
            "total_requests": self.total_requests,
            "deduplicated": self.deduplicated_requests,
            "dedup_ratio": f"{self.deduplicated_requests/max(self.total_requests,1)*100:.1f}%",
            "store_stats": self.store.stats(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

class PaymentService:
    """Simulates a payment service that processes charges."""

    def __init__(self):
        self.charges: list[dict] = []   # All actual charges processed

    def charge(self, user_id: str, amount: float, currency: str) -> dict:
        """Process a payment. This is the expensive side-effecting operation."""
        # Simulate processing time
        time.sleep(0.05)

        # Simulate occasional failure (for demo)
        charge_id = f"ch_{uuid.uuid4().hex[:8]}"
        self.charges.append({
            "charge_id": charge_id,
            "user_id": user_id,
            "amount": amount,
            "currency": currency,
            "processed_at": time.time(),
        })
        print(f"    [PaymentService] *** ACTUAL CHARGE PROCESSED: "
              f"{currency}{amount} for {user_id} → {charge_id} ***")
        return {"status_code": 200, "body": {"charge_id": charge_id, "status": "success"}}


if __name__ == "__main__":
    print("=" * 65)
    print("   IDEMPOTENCY KEY SYSTEM — Demo")
    print("=" * 65)

    store = IdempotencyStore()
    middleware = IdempotencyMiddleware(store)
    payment = PaymentService()

    # ── Scenario 1: Normal payment (first time) ───────────────────────────
    print("\n  ─── Scenario 1: Normal Payment ───")
    key_1 = str(uuid.uuid4())
    print(f"  Client generates Idempotency-Key: {key_1}")

    for attempt in range(1, 4):
        print(f"\n  Attempt #{attempt} (same key — simulating network retry):")
        result = middleware.execute(
            idempotency_key=key_1,
            operation=lambda: payment.charge("user_42", 100.00, "$"),
        )
        print(f"  → HTTP {result['status_code']} | body={result['body']} "
              f"| duplicate={result['was_duplicate']}")

    print(f"\n  Total ACTUAL charges processed: {len(payment.charges)} "
          f"(expected: 1, even with 3 attempts)")

    # ── Scenario 2: Different key → new charge ────────────────────────────
    print("\n\n  ─── Scenario 2: New Payment with Different Key ───")
    key_2 = str(uuid.uuid4())
    print(f"  New Idempotency-Key: {key_2}")
    result = middleware.execute(
        idempotency_key=key_2,
        operation=lambda: payment.charge("user_42", 50.00, "$"),
    )
    print(f"  → HTTP {result['status_code']} | {result['body']}")
    print(f"  Total actual charges: {len(payment.charges)} (now 2, correctly)")

    # ── Scenario 3: Concurrent requests with same key ─────────────────────
    print("\n\n  ─── Scenario 3: Concurrent Duplicate Detection ───")
    key_3 = str(uuid.uuid4())
    results = []
    threads = []

    def concurrent_request():
        r = middleware.execute(
            idempotency_key=key_3,
            operation=lambda: payment.charge("user_99", 200.00, "$"),
        )
        results.append(r)

    for _ in range(5):
        t = threading.Thread(target=concurrent_request)
        threads.append(t)
    for t in threads: t.start()
    for t in threads: t.join()

    actual_executions = sum(1 for r in results if not r['was_duplicate'])
    print(f"\n  5 concurrent requests → {actual_executions} actual charge(s) "
          f"(should be 1)")

    # ── Middleware stats ───────────────────────────────────────────────────
    print(f"\n\n  ─── Middleware Stats ───")
    stats = middleware.stats()
    print(f"  Total requests:  {stats['total_requests']}")
    print(f"  Deduplicated:    {stats['deduplicated']}")
    print(f"  Dedup ratio:     {stats['dedup_ratio']}")
    print(f"  Store:           {stats['store_stats']}")
    print(f"  Actual charges:  {len(payment.charges)}")

    print("\n" + "=" * 65)
    print("  KEY TAKEAWAY: Idempotency-Key prevents duplicate charges")
    print("  even when clients retry failed or timed-out requests.")
    print("  The server processes each key EXACTLY ONCE.")
    print("=" * 65)
