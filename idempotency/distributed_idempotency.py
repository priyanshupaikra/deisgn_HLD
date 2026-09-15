"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       IDEMPOTENCY — MODULE 4                                 ║
║             DISTRIBUTED DELIVERY GUARANTEES                                  ║
║          (At-Most-Once / At-Least-Once / Exactly-Once)                       ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
In distributed systems, messages between services can be LOST, DUPLICATED,
or DELIVERED OUT OF ORDER due to network failures. Every messaging system
must choose a delivery guarantee — and each has trade-offs.

THE THREE DELIVERY GUARANTEES:
────────────────────────────────

  ┌──────────────────┬────────────────┬────────────────┬────────────────────┐
  │ Guarantee        │ Messages Lost? │ Duplicates?    │ Complexity         │
  ├──────────────────┼────────────────┼────────────────┼────────────────────┤
  │ At-Most-Once     │ Possible       │ Never          │ Low (fire-forget)  │
  │ At-Least-Once    │ Never          │ Possible       │ Medium (retry)     │
  │ Exactly-Once     │ Never          │ Never          │ High (2PC / idmp)  │
  └──────────────────┴────────────────┴────────────────┴────────────────────┘

AT-MOST-ONCE:
─────────────
  "Send and forget" — message is sent once, no retry if it fails.
  If the network drops the message → IT IS LOST FOREVER.

  HOW:
    Producer sends message
    Does NOT wait for ACK
    Consumer processes (or doesn't — no retry)

  USE CASES:
    - Metrics/telemetry (losing one data point is acceptable)
    - Log shipping (occasional loss is OK)
    - WebRTC video frames (stale frame is useless anyway)
    - UDP-based protocols (DNS, NTP)

  EXAMPLE:
    UDP → "fire and forget" — no acknowledgment, no retransmission.

AT-LEAST-ONCE:
──────────────
  Message is retried until acknowledged. May be delivered MULTIPLE TIMES.
  Consumer MUST be idempotent to handle duplicates safely.

  HOW:
    Producer sends message
    Waits for ACK with timeout
    If no ACK (or timeout) → RETRY (possibly sending duplicate)
    Consumer processes; if already seen → skip (idempotent consumer)

  USE CASES:
    - Payment notifications (must not lose, but can deduplicate)
    - Email delivery
    - Kafka (default: at-least-once with consumer commits)
    - SQS standard queues (at-least-once delivery)

  THE CRITICAL REQUIREMENT:
    Consumers MUST be idempotent — processing the same message twice
    must produce the same result as processing it once.

EXACTLY-ONCE:
─────────────
  Holy grail of messaging. Every message delivered exactly once.
  Achieved through a combination of:
    1. Idempotency keys on the producer side
    2. Deduplication on the consumer side
    3. Two-Phase Commit (2PC) or Transactional Outbox

  HOW (Kafka approach):
    Producers: assigned a PID (Producer ID) + sequence numbers
    Broker: deduplicates within a session using sequence numbers
    Consumer transactions: consume + process + commit atomically

  USE CASES:
    - Financial transactions (charge exactly $100, not $200)
    - Order creation (create exactly 1 order per customer request)
    - Kafka with enable.idempotence=true + transactions

  COST:
    Much higher latency and complexity.
    In practice: "at-least-once + idempotent consumer" is preferred
    over true "exactly-once" for most systems.

TRANSACTIONAL OUTBOX PATTERN:
───────────────────────────────
  Problem: atomically writing to DB + publishing to message queue.
    Write to DB fails → queue message lost
    Write to queue fails → DB has data but event never sent

  Solution: OUTBOX TABLE in the SAME database.
    1. Within ONE DB transaction: write business data + write to outbox table
    2. Separate "outbox relay" process reads outbox → publishes to queue → deletes

  This guarantees DB write and message publish either BOTH happen or NEITHER.

REAL-WORLD USAGE:
──────────────────
  Kafka:
    enable.idempotence=true         → Exactly-once on producer side
    isolation.level=read_committed  → Exactly-once on consumer side
    .commitSync() / .commitAsync()  → At-least-once if commit before crash

  SQS:
    Standard queue → at-least-once (may duplicate)
    FIFO queue     → exactly-once (within deduplication window)

  RabbitMQ:
    publisher confirms → at-least-once
    consumer ack       → at-least-once (nack → requeue)

"""

import time
import uuid
import random
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Message model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Message:
    """Represents a message in a distributed messaging system."""
    message_id: str          # Unique message identifier (for deduplication)
    topic: str               # Message topic / queue name
    payload: dict            # Actual data
    producer_id: str = ""    # Which producer sent this
    sequence_num: int = 0    # Sequence number (for exactly-once)
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0     # How many times retried

    def __repr__(self):
        return (f"Message(id={self.message_id[:8]}..., topic={self.topic}, "
                f"payload={self.payload}, retry={self.retry_count})")


# ─────────────────────────────────────────────────────────────────────────────
# GUARANTEE 1: AT-MOST-ONCE DELIVERY
# ─────────────────────────────────────────────────────────────────────────────

class AtMostOnceProducer:
    """
    At-Most-Once Producer: Fire and forget.

    Sends the message ONCE. If it fails or is dropped → GONE.
    No retry, no acknowledgment waiting.

    Real-world: UDP, StatsD metrics, raw syslog over UDP.
    """

    def __init__(self, name: str, network_drop_rate: float = 0.3):
        """
        :param network_drop_rate: Probability of network dropping the message (0–1)
        """
        self.name = name
        self.network_drop_rate = network_drop_rate
        self.sent_count = 0
        self.dropped_count = 0

    def send(self, broker, topic: str, payload: dict) -> bool:
        """
        Send once. No retry. No acknowledgment.
        :return: True if delivered, False if dropped.
        """
        msg = Message(
            message_id=str(uuid.uuid4()),
            topic=topic,
            payload=payload,
            producer_id=self.name,
        )
        self.sent_count += 1

        # Simulate network drop
        if random.random() < self.network_drop_rate:
            self.dropped_count += 1
            print(f"  [AtMostOnce] DROPPED: {msg.message_id[:8]}... (network failure)")
            return False  # Message LOST — no retry

        # Deliver to broker
        broker.receive(msg)
        return True

    def stats(self) -> dict:
        return {
            "sent": self.sent_count,
            "dropped": self.dropped_count,
            "loss_rate": f"{self.dropped_count/max(self.sent_count,1)*100:.1f}%"
        }


# ─────────────────────────────────────────────────────────────────────────────
# GUARANTEE 2: AT-LEAST-ONCE DELIVERY
# ─────────────────────────────────────────────────────────────────────────────

class AtLeastOnceProducer:
    """
    At-Least-Once Producer: Retry until acknowledged.

    Sends the message and waits for ACK with timeout.
    If no ACK → retry with exponential backoff.
    Result: message is GUARANTEED to be delivered, but MAY be delivered multiple times.

    Real-world: Kafka (default), RabbitMQ with publisher confirms.
    """

    def __init__(self, name: str, max_retries: int = 3,
                 network_drop_rate: float = 0.4, base_delay: float = 0.01):
        self.name = name
        self.max_retries = max_retries
        self.network_drop_rate = network_drop_rate
        self.base_delay = base_delay
        self.sent_count = 0
        self.retry_count = 0
        self.failed_count = 0   # Failed even after all retries

    def send(self, broker, topic: str, payload: dict,
             message_id: str = None) -> bool:
        """
        Send with retry until ACK or max_retries exceeded.

        :param message_id: Pass explicit ID to enable consumer deduplication.
                           If retried, same ID → consumer can detect duplicate.
        """
        msg = Message(
            message_id=message_id or str(uuid.uuid4()),
            topic=topic,
            payload=payload,
            producer_id=self.name,
        )
        self.sent_count += 1

        for attempt in range(self.max_retries + 1):
            msg.retry_count = attempt

            # Simulate network: drop packet with some probability
            if random.random() < self.network_drop_rate:
                if attempt < self.max_retries:
                    delay = self.base_delay * (2 ** attempt)  # Exponential backoff
                    print(f"  [AtLeastOnce] Attempt {attempt+1} DROPPED, "
                          f"retry in {delay*1000:.0f}ms...")
                    self.retry_count += 1
                    time.sleep(delay)
                    continue
                else:
                    print(f"  [AtLeastOnce] All {self.max_retries+1} attempts failed!")
                    self.failed_count += 1
                    return False

            # Delivered! Get ACK from broker.
            broker.receive(msg)
            if attempt > 0:
                print(f"  [AtLeastOnce] Delivered on attempt #{attempt+1} "
                      f"(ID={msg.message_id[:8]}...)")
            return True

        return False

    def stats(self) -> dict:
        return {
            "total_sends": self.sent_count,
            "retries": self.retry_count,
            "failed_after_retries": self.failed_count,
        }


# ─────────────────────────────────────────────────────────────────────────────
# GUARANTEE 3: EXACTLY-ONCE (via idempotent consumer)
# ─────────────────────────────────────────────────────────────────────────────

class ExactlyOnceConsumer:
    """
    Exactly-Once Consumer via IDEMPOTENT MESSAGE PROCESSING.

    True "exactly-once" at the infrastructure level is extremely complex
    (requires distributed transactions or 2PC). In practice, most systems
    achieve "exactly-once SEMANTICS" via:

      at-least-once delivery + idempotent consumer = effectively exactly-once

    HOW IT WORKS:
      1. Consumer maintains a "seen_ids" set (Redis SET, DB table)
      2. On receiving a message:
         a. Check if message_id is in seen_ids
         b. If YES → SKIP (duplicate, already processed)
         c. If NO  → PROCESS + ADD to seen_ids (atomically)

    THE ATOMIC REQUIREMENT:
      Steps (c) must be atomic: "process + mark as seen" must be ONE transaction.
      If consumer crashes between process and mark → message reprocessed on restart.

      Solution: Use the same database transaction for:
        - Business logic (insert order, deduct balance)
        - Mark message as seen (insert into processed_messages table)
      Both commit together → guaranteed exactly-once processing.

    DEDUPLICATION WINDOW:
      seen_ids cannot grow forever → use TTL (e.g., 24 hours).
      Any message older than 24h is assumed to be a new message (not duplicate).
      Set your window based on your max expected message delay.
    """

    def __init__(self, name: str, dedup_window_seconds: float = 3600.0):
        self.name = name
        self.dedup_window = dedup_window_seconds
        # {message_id → processed_at_timestamp}
        self._seen_ids: dict[str, float] = {}
        self._lock = threading.Lock()
        self.processed_count = 0     # Unique messages actually processed
        self.duplicate_count = 0     # Duplicate messages skipped

    def _is_duplicate(self, message_id: str) -> bool:
        """Check if message_id was seen within the deduplication window."""
        with self._lock:
            if message_id not in self._seen_ids:
                return False
            seen_at = self._seen_ids[message_id]
            # If outside the window → treat as new (window expired)
            return time.time() - seen_at < self.dedup_window

    def _mark_seen(self, message_id: str) -> None:
        """Atomically mark this message_id as processed."""
        with self._lock:
            self._seen_ids[message_id] = time.time()

    def process(self, message: Message, handler: Callable[[Message], None]) -> str:
        """
        Process a message with exactly-once semantics.

        :param message:  The incoming message
        :param handler:  Business logic handler (called only if not duplicate)
        :return:         "processed" | "duplicate_skipped"
        """
        if self._is_duplicate(message.message_id):
            # DUPLICATE: already processed this message_id → skip
            self.duplicate_count += 1
            print(f"  [{self.name}] DUPLICATE SKIPPED: {message.message_id[:8]}... "
                  f"(retry #{message.retry_count})")
            return "duplicate_skipped"

        # NOT a duplicate → process atomically
        # In production: wrap handler + _mark_seen in ONE DB transaction
        handler(message)
        self._mark_seen(message.message_id)   # Atomic with handler in real DB
        self.processed_count += 1
        print(f"  [{self.name}] PROCESSED: {message.message_id[:8]}... "
              f"(retry #{message.retry_count})")
        return "processed"

    def stats(self) -> dict:
        return {
            "processed": self.processed_count,
            "duplicates_skipped": self.duplicate_count,
            "total_received": self.processed_count + self.duplicate_count,
        }


# ─────────────────────────────────────────────────────────────────────────────
# TRANSACTIONAL OUTBOX PATTERN
# ─────────────────────────────────────────────────────────────────────────────

class TransactionalOutbox:
    """
    Transactional Outbox Pattern.

    PROBLEM IT SOLVES:
      How do you ATOMICALLY:
        a. Write to your database (create order)
        b. Publish an event to a message queue (order_created event)

      If you do them separately:
        - Write DB succeeds, queue publish fails → event lost, DB has orphan data
        - Queue publish succeeds, DB write fails → event sent but no data

    SOLUTION:
      Write to BOTH tables in ONE transaction:
        1. INSERT INTO orders (...)       ← business data
        2. INSERT INTO outbox (event...)  ← event to be sent later

      Separate "Outbox Relay" process:
        - Periodically: SELECT unsent events from outbox
        - Publish each to message queue
        - On success: DELETE (or mark as sent) from outbox
        - On failure: leave in outbox → retry on next poll

    GUARANTEES:
      - DB write and event are always consistent (same transaction)
      - At-least-once delivery (relay may publish twice if it crashes mid-delete)
      - Consumer needs idempotency to handle relay duplicates

    USED BY:
      - Debezium (CDC — Change Data Capture) reads DB changelog → Kafka
      - Most modern event-driven microservice architectures
    """

    def __init__(self):
        # Simulated DB: orders table
        self.orders: dict[str, dict] = {}
        # Simulated outbox table (in same DB as orders)
        self.outbox: list[dict] = []
        # Simulated message queue (external)
        self.published_events: list[dict] = []
        self._lock = threading.Lock()

    def create_order_with_event(self, user_id: str, total: float) -> dict:
        """
        Atomically create order + write outbox event in ONE transaction.
        Returns the created order.
        """
        with self._lock:
            # === SINGLE DB TRANSACTION START ===

            # Step 1: Insert into orders table
            order_id = str(uuid.uuid4())
            order = {
                "id": order_id,
                "user_id": user_id,
                "total": total,
                "status": "created",
                "created_at": time.time(),
            }
            self.orders[order_id] = order

            # Step 2: Insert into outbox table (same transaction!)
            outbox_entry = {
                "outbox_id": str(uuid.uuid4()),
                "event_type": "order.created",
                "payload": {"order_id": order_id, "user_id": user_id, "total": total},
                "created_at": time.time(),
                "sent": False,
            }
            self.outbox.append(outbox_entry)

            # === TRANSACTION COMMIT (both inserts committed together) ===

            print(f"  [Outbox] TX committed: order {order_id[:8]}... + outbox event")
            return order

    def relay_outbox(self) -> int:
        """
        Outbox Relay: reads pending events → publishes to queue → marks sent.
        In production: runs as a scheduled job or CDC connector (Debezium).

        :return: Number of events relayed
        """
        with self._lock:
            unsent = [e for e in self.outbox if not e["sent"]]

        relayed = 0
        for event in unsent:
            # Publish to message queue
            self.published_events.append(event["payload"])
            print(f"  [OutboxRelay] Published: {event['event_type']} "
                  f"→ order={event['payload']['order_id'][:8]}...")

            # Mark as sent in outbox (separate from publishing)
            event["sent"] = True
            relayed += 1

        return relayed

    def stats(self) -> dict:
        return {
            "orders": len(self.orders),
            "outbox_total": len(self.outbox),
            "outbox_pending": sum(1 for e in self.outbox if not e["sent"]),
            "published_events": len(self.published_events),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Simulated Broker
# ─────────────────────────────────────────────────────────────────────────────

class SimpleBroker:
    """Simulates a message broker (Kafka/RabbitMQ/SQS)."""

    def __init__(self):
        self.queues: dict[str, list] = defaultdict(list)
        self.receive_count = 0

    def receive(self, message: Message) -> None:
        """Broker receives and stores a message."""
        self.queues[message.topic].append(message)
        self.receive_count += 1

    def consume(self, topic: str) -> list[Message]:
        """Consume all messages from a topic."""
        msgs = list(self.queues.get(topic, []))
        self.queues[topic] = []
        return msgs


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    random.seed(42)  # Reproducible demo
    print("=" * 65)
    print("   DISTRIBUTED DELIVERY GUARANTEES — Demo")
    print("=" * 65)

    # ── AT-MOST-ONCE ──────────────────────────────────────────────────────
    print("\n\n  ═══ GUARANTEE 1: AT-MOST-ONCE (fire & forget) ═══")
    print("  Drop rate=30%. Some messages WILL be lost. No retry.\n")

    broker1 = SimpleBroker()
    at_most_once = AtMostOnceProducer("producer_metrics", network_drop_rate=0.3)

    for i in range(10):
        at_most_once.send(broker1, "metrics", {"counter": i, "value": i * 10})

    stats = at_most_once.stats()
    print(f"\n  Sent: {stats['sent']} | Dropped: {stats['dropped']} "
          f"| Loss rate: {stats['loss_rate']}")
    print(f"  Broker received: {broker1.receive_count} messages")
    print(f"  → Some metrics lost. ACCEPTABLE for metrics, NOT for payments.")

    # ── AT-LEAST-ONCE ─────────────────────────────────────────────────────
    print("\n\n  ═══ GUARANTEE 2: AT-LEAST-ONCE (retry until ACK) ═══")
    print("  Drop rate=40%, max 3 retries. Duplicates possible.\n")

    broker2 = SimpleBroker()
    at_least_once = AtLeastOnceProducer(
        "producer_orders", max_retries=3, network_drop_rate=0.4
    )

    # Use SAME message_id for retries (enables consumer deduplication)
    idem_key = str(uuid.uuid4())
    for i in range(3):
        at_least_once.send(broker2, "order_events",
                           {"order_id": f"order_{i}", "event": "created"},
                           message_id=f"msg_{i}_{idem_key[:8]}")

    stats = at_least_once.stats()
    print(f"\n  Total sends: {stats['total_sends']} | "
          f"Retries: {stats['retries']} | "
          f"Broker received: {broker2.receive_count}")
    print(f"  → Broker may have duplicates! Consumer must be idempotent.")

    # ── EXACTLY-ONCE (idempotent consumer) ───────────────────────────────
    print("\n\n  ═══ GUARANTEE 3: EXACTLY-ONCE SEMANTICS ═══")
    print("  At-least-once delivery + idempotent consumer = effectively exactly-once\n")

    broker3 = SimpleBroker()
    at_least_once2 = AtLeastOnceProducer("producer_payments", network_drop_rate=0.5)
    consumer = ExactlyOnceConsumer("payment_consumer", dedup_window_seconds=3600)

    # Business logic handler (processes payment)
    processed_orders = []

    def payment_handler(msg: Message) -> None:
        processed_orders.append(msg.payload["order_id"])

    # Producer sends the SAME 3 messages with SAME IDs (simulating retries)
    fixed_ids = [str(uuid.uuid4()) for _ in range(3)]
    print("  Phase 1: First delivery attempt for 3 payment messages:")
    for i, msg_id in enumerate(fixed_ids):
        msg = Message(
            message_id=msg_id,
            topic="payments",
            payload={"order_id": f"order_{i+1}", "amount": (i+1)*50},
            retry_count=0,
        )
        consumer.process(msg, payment_handler)

    print(f"\n  Phase 2: Broker retries all 3 messages (duplicates!):")
    for i, msg_id in enumerate(fixed_ids):
        msg = Message(
            message_id=msg_id,   # SAME ID as before
            topic="payments",
            payload={"order_id": f"order_{i+1}", "amount": (i+1)*50},
            retry_count=1,       # This is a retry
        )
        consumer.process(msg, payment_handler)

    stats = consumer.stats()
    print(f"\n  Consumer stats: {stats}")
    print(f"  Orders actually processed: {len(set(processed_orders))} "
          f"(expected: 3, despite 6 deliveries)")

    # ── TRANSACTIONAL OUTBOX ──────────────────────────────────────────────
    print("\n\n  ═══ TRANSACTIONAL OUTBOX PATTERN ═══")
    print("  Atomically write to DB + publish event via outbox table\n")

    outbox = TransactionalOutbox()

    # Create 3 orders — each atomically writes order + outbox event
    for i in range(3):
        outbox.create_order_with_event(f"user_{i+1}", total=(i+1)*100.0)

    print(f"\n  Before relay: {outbox.stats()}")

    # Relay outbox to message queue
    print("\n  Outbox relay running...")
    relayed = outbox.relay_outbox()
    print(f"  Relayed {relayed} events to message queue")
    print(f"  After relay: {outbox.stats()}")

    # Summary
    print("\n" + "=" * 65)
    print("  DELIVERY GUARANTEE COMPARISON")
    print("=" * 65)
    rows = [
        ("Guarantee",     "Lost?", "Duplicates?", "Complexity", "Example"),
        ("─"*14,          "─"*5,   "─"*11,        "─"*10,       "─"*18),
        ("At-Most-Once",  "Yes",   "No",           "Low",        "UDP, metrics"),
        ("At-Least-Once", "No",    "Possible",     "Medium",     "Kafka, SQS, AMQP"),
        ("Exactly-Once",  "No",    "No",           "High",       "Kafka txns, FIFO SQS"),
    ]
    for r in rows:
        print(f"  {r[0]:<15} {r[1]:<6} {r[2]:<12} {r[3]:<11} {r[4]}")
    print("\n  RULE: At-least-once + idempotent consumer ≈ exactly-once")
    print("        (simpler and preferred in most production systems)")
    print("=" * 65)
