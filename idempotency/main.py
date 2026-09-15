"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                           IDEMPOTENCY SUITE                                  ║
║                      All Concepts — Main Runner                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WHAT IS IDEMPOTENCY?                                                        ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  An operation is IDEMPOTENT if applying it multiple times produces the       ║
║  SAME result as applying it once.                                            ║
║                                                                              ║
║  f(f(x)) = f(x)                                                             ║
║                                                                              ║
║  WHY IT MATTERS:                                                             ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  In distributed systems, RETRIES are inevitable:                            ║
║    - Network timeouts: client can't tell if request was received             ║
║    - Load balancer retries: upstream failure → automatic retry               ║
║    - Message queue redelivery: at-least-once queues send duplicates          ║
║    - Client retry logic: exponential backoff on 5xx errors                  ║
║                                                                              ║
║  Without idempotency: retries cause duplicate charges, duplicate orders,    ║
║  double emails, double account creation...                                   ║
║                                                                              ║
║  With idempotency: retries are SAFE — same result every time.               ║
║                                                                              ║
║  MODULES:                                                                    ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  idempotency_key.py        → Idempotency key middleware (PROCESSING/        ║
║                              COMPLETE/FAILED lifecycle + deduplication)     ║
║  http_methods.py           → HTTP GET/PUT/DELETE/POST/PATCH semantics       ║
║  database_patterns.py      → UPSERT, unique constraints, optimistic lock    ║
║  distributed_idempotency.py→ At-most/at-least/exactly-once + Outbox        ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import time
import uuid
import random
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from idempotency_key import IdempotencyStore, IdempotencyMiddleware, PaymentService
from http_methods import UserRepository, HTTPMethodDemo
from database_patterns import UpsertPattern, UniqueConstraintPattern, OptimisticLockingPattern
from distributed_idempotency import (
    SimpleBroker, AtMostOnceProducer, AtLeastOnceProducer,
    ExactlyOnceConsumer, TransactionalOutbox, Message
)


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def subsection(title: str):
    print(f"\n  ─── {title} ───")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 1: IDEMPOTENCY KEY SYSTEM
# ─────────────────────────────────────────────────────────────────────────────
def demo_idempotency_key():
    section("MODULE 1: IDEMPOTENCY KEY SYSTEM")
    print("  Concept: Unique key per operation — safe to retry indefinitely")

    store = IdempotencyStore()
    middleware = IdempotencyMiddleware(store)
    payment = PaymentService()

    key = str(uuid.uuid4())
    print(f"\n  Idempotency-Key: {key[:16]}...")
    print("  Sending 'charge $100' 3 times with the SAME key (client retries):\n")

    for attempt in range(1, 4):
        result = middleware.execute(
            idempotency_key=key,
            operation=lambda: payment.charge("user_42", 100.00, "$"),
        )
        print(f"  Attempt #{attempt}: HTTP {result['status_code']} "
              f"| duplicate={result['was_duplicate']}")

    print(f"\n  Actual charges processed: {len(payment.charges)} "
          f"(EXPECTED: 1 despite 3 attempts ✅)")
    stats = middleware.stats()
    print(f"  Dedup ratio: {stats['dedup_ratio']}")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 2: HTTP METHODS
# ─────────────────────────────────────────────────────────────────────────────
def demo_http_methods():
    section("MODULE 2: HTTP METHOD IDEMPOTENCY")
    print("  Concept: HTTP defines which methods are safe to retry")

    db = UserRepository()
    api = HTTPMethodDemo(db)

    subsection("GET — Safe + Idempotent")
    r1 = api.GET("/users/1", 1)
    r2 = api.GET("/users/1", 1)
    print(f"  GET x2 → both {r1.status_code} | state changes={len(db.operation_log)} (zero)")

    subsection("PUT — Idempotent (full replace)")
    payload = {"name": "Alice V2", "email": "alice.v2@ex.com", "role": "admin", "balance": 500.0}
    api.PUT("/users/1", 1, payload)
    api.PUT("/users/1", 1, payload)
    api.PUT("/users/1", 1, payload)
    final = api.GET("/users/1", 1)
    print(f"  PUT x3 → final state: {final.body['name']} (same after each call)")

    subsection("DELETE — Idempotent (resource stays absent)")
    r_del1 = api.DELETE("/users/2", 2)
    r_del2 = api.DELETE("/users/2", 2)  # Already gone
    print(f"  DELETE #1: HTTP {r_del1.status_code} | DELETE #2: HTTP {r_del2.status_code}")
    print(f"  Server state is the same — user 2 is absent in both cases")

    subsection("POST — NOT idempotent (creates duplicates!)")
    body = {"name": "Dave", "email": "dave@ex.com"}
    pre_count = len([l for l in db.operation_log if l.startswith("CREATE")])
    api.POST("/users", body)
    api.POST("/users", body)
    api.POST("/users", body)
    post_count = len([l for l in db.operation_log if l.startswith("CREATE")])
    print(f"  POST x3 → {post_count - pre_count} Dave accounts created (should be 1!)")
    print(f"  ⚠ Use Idempotency-Key header with POST to prevent this")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 3: DATABASE PATTERNS
# ─────────────────────────────────────────────────────────────────────────────
def demo_database_patterns():
    section("MODULE 3: DATABASE IDEMPOTENCY PATTERNS")
    print("  Concept: DB-level safety net — last line of defense")

    subsection("UPSERT — Insert or Update")
    upsert = UpsertPattern()
    for i in range(3):
        row, is_new = upsert.upsert("alice@ex.com", "Alice Smith", "+1-555-0001")
        print(f"  Attempt {i+1}: is_new={is_new} | rows={upsert.count()}")
    print(f"  → {upsert.insert_count} INSERT, {upsert.update_count} UPDATEs, 1 total row ✅")

    subsection("Unique Constraint — DB rejects duplicates")
    uc = UniqueConstraintPattern()
    idem_key = str(uuid.uuid4())
    results = []
    for i in range(3):
        payment, is_new = uc.safe_insert_or_get(idem_key, "user_99", 75.00)
        results.append(payment.id)
    unique_ids = len(set(results))
    print(f"  3 attempts → {unique_ids} unique payment record ✅")

    subsection("Optimistic Locking — Version column prevents lost updates")
    ol = OptimisticLockingPattern()
    order = ol.create_order("user_1", 199.99)
    print(f"  Created: {order}")
    updated = ol.update_status(order.id, "confirmed", expected_version=1)
    print(f"  Updated: {updated}")
    try:
        ol.update_status(order.id, "shipped", expected_version=1)  # Stale!
    except Exception as e:
        print(f"  Stale version detected: {e.__class__.__name__} ✅")
    final = ol.update_with_retry(order.id, "shipped")
    print(f"  After retry: {final}")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 4: DISTRIBUTED DELIVERY
# ─────────────────────────────────────────────────────────────────────────────
def demo_distributed():
    section("MODULE 4: DISTRIBUTED DELIVERY GUARANTEES")
    print("  Concept: Networks drop packets — choose your guarantee carefully")
    random.seed(99)

    subsection("At-Most-Once (fire & forget, losses OK)")
    broker = SimpleBroker()
    amo = AtMostOnceProducer("telemetry", network_drop_rate=0.3)
    for i in range(6):
        amo.send(broker, "metrics", {"value": i})
    s = amo.stats()
    print(f"  Sent {s['sent']} | Dropped {s['dropped']} | Broker got: {broker.receive_count}")
    print(f"  → Acceptable for metrics/telemetry. NEVER for payments.")

    subsection("At-Least-Once + Idempotent Consumer = Exactly-Once Semantics")
    consumer = ExactlyOnceConsumer("order_consumer")
    processed = []

    def handler(m: Message):
        processed.append(m.payload.get("id"))

    # Simulate 3 messages, each delivered twice (duplicate)
    msg_ids = [str(uuid.uuid4()) for _ in range(3)]
    print("  Round 1 (first delivery):")
    for i, mid in enumerate(msg_ids):
        consumer.process(Message(mid, "orders", {"id": f"order_{i}"}), handler)
    print("  Round 2 (duplicate delivery — same IDs):")
    for i, mid in enumerate(msg_ids):
        consumer.process(Message(mid, "orders", {"id": f"order_{i}"}), handler)

    s = consumer.stats()
    print(f"\n  Received {s['total_received']} messages | "
          f"Processed {s['processed']} | Duplicates skipped {s['duplicates_skipped']}")
    print(f"  Unique orders processed: {len(set(processed))} (EXPECTED: 3 ✅)")

    subsection("Transactional Outbox — Atomic DB write + event publish")
    outbox = TransactionalOutbox()
    outbox.create_order_with_event("user_1", 150.0)
    outbox.create_order_with_event("user_2", 300.0)
    print(f"  Before relay: {outbox.stats()}")
    outbox.relay_outbox()
    print(f"  After relay:  {outbox.stats()}")
    print(f"  → DB writes and events are always consistent")


# ─────────────────────────────────────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    section("IDEMPOTENCY — COMPLETE REFERENCE")
    print()
    print("  CORE CONCEPT:")
    print("    f(f(x)) = f(x)  |  Multiple calls = same result as one call")
    print("    Critical when: retries, at-least-once queues, network failures")

    print("\n  HTTP METHOD IDEMPOTENCY:")
    rows = [
        ("GET",     "YES", "YES"),
        ("PUT",     "YES", "NO"),
        ("DELETE",  "YES", "NO"),
        ("POST",    "NO",  "NO"),
        ("PATCH",   "DEPENDS (if absolute)", "NO"),
    ]
    print(f"    {'Method':<8} {'Idempotent':<22} {'Safe'}")
    for r in rows:
        print(f"    {r[0]:<8} {r[1]:<22} {r[2]}")

    print("\n  IDEMPOTENCY KEY PATTERN:")
    print("    1. Client generates UUID per operation")
    print("    2. Sends: Idempotency-Key: <uuid> in header")
    print("    3. Server checks Redis: PROCESSING / COMPLETE / FAILED")
    print("    4. COMPLETE → return cached response (no re-processing)")
    print("    Used by: Stripe, PayPal, Twilio, AWS")

    print("\n  DATABASE PATTERNS:")
    db_rows = [
        ("UPSERT",             "INSERT ON CONFLICT DO UPDATE",  "Profile sync"),
        ("Unique Constraint",  "DB rejects duplicate inserts",  "Payment dedup"),
        ("Optimistic Lock",    "Version column + WHERE check",  "Concurrent edits"),
    ]
    for r in db_rows:
        print(f"    {r[0]:<20} {r[1]:<32} → {r[2]}")

    print("\n  DISTRIBUTED DELIVERY:")
    dist_rows = [
        ("At-Most-Once",  "Fire & forget, may lose",        "Metrics"),
        ("At-Least-Once", "Retry until ACK, may duplicate", "Events + idempotent consumer"),
        ("Exactly-Once",  "ALO + dedup consumer",           "Payments, orders"),
        ("Outbox Pattern","Atomic DB + event write",        "Microservices events"),
    ]
    for r in dist_rows:
        print(f"    {r[0]:<16} {r[1]:<34} → {r[2]}")

    print()
    print("  Run individual files for detailed demos:")
    for f in ["idempotency_key.py", "http_methods.py",
              "database_patterns.py", "distributed_idempotency.py"]:
        print(f"    python {f}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       IDEMPOTENCY — All Concepts Demo")
    print("=" * 65)

    demo_idempotency_key()
    demo_http_methods()
    demo_database_patterns()
    demo_distributed()
    print_summary()
