"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    MESSAGE QUEUE (Kafka/RabbitMQ) — MODULE 1                 ║
║                          CORE CONCEPTS & PRIMITIVES                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
WHAT IS A MESSAGE QUEUE?
─────────────────────────
A message queue is an ASYNCHRONOUS communication mechanism between services.
Instead of Service A calling Service B directly (synchronous/tight coupling):
  SYNCHRONOUS (tight coupling):
    Service A ──calls──▶ Service B  (A blocks until B responds)
    If B is slow → A is slow
    If B is down → A fails
  ASYNCHRONOUS (via message queue):
    Service A ──puts message──▶ [QUEUE] ──picks up──▶ Service B
    A doesn't wait for B. A just enqueues and continues.
    If B is slow → messages queue up, A is unaffected
    If B is down → messages wait, processed when B recovers
WHY USE A MESSAGE QUEUE?
─────────────────────────
  1. DECOUPLING       → Producer and consumer don't know each other
  2. BUFFERING        → Absorb traffic spikes (queue acts as shock absorber)
  3. ASYNC PROCESSING → Long tasks (email, video encode) run in background
  4. LOAD LEVELING    → Consumers process at their own rate
  5. FAULT TOLERANCE  → Messages persist even if consumer is temporarily down
  6. FANOUT           → One message → many consumers (pub/sub)
CORE PRIMITIVES:
─────────────────
  PRODUCER   → Creates and sends messages to the broker
  BROKER     → Stores and routes messages (the queue server: Kafka, RabbitMQ)
  CONSUMER   → Reads and processes messages from the broker
  TOPIC      → Named channel messages are sent to (Kafka)
  QUEUE      → Named buffer consumers read from (RabbitMQ)
  MESSAGE    → The unit of data: headers + body
  ACK        → Acknowledgment: consumer tells broker "I processed this"
  NACK       → Negative acknowledgment: consumer tells broker "requeue this"
KAFKA vs RABBITMQ — KEY DIFFERENCES:
──────────────────────────────────────
  ┌─────────────────────┬────────────────────────┬─────────────────────────┐
  │ Feature             │ Kafka                  │ RabbitMQ                │
  ├─────────────────────┼────────────────────────┼─────────────────────────┤
  │ Model               │ Log (append-only)      │ Queue (FIFO)            │
  │ Routing             │ Topic + Partition      │ Exchange + Binding Key  │
  │ Message Retention   │ Days/forever (replay!) │ Until consumed          │
  │ Consumer Groups     │ Built-in               │ Via competing consumers │
  │ Ordering            │ Per-partition          │ Per-queue               │
  │ Throughput          │ Millions/sec           │ Thousands/sec           │
  │ Use Case            │ Streaming, event log   │ Task queues, RPC        │
  │ Pull/Push           │ Consumer pulls         │ Broker pushes           │
  └─────────────────────┴────────────────────────┴─────────────────────────┘
"""
import time
import uuid
import threading
from dataclasses import dataclass, field
from typing import Any, Optional
from collections import defaultdict, deque
from enum import Enum
# ─────────────────────────────────────────────────────────────────────────────
# Core Message Model
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Message:
    """
    A message — the fundamental unit of data in any messaging system.
    STRUCTURE:
      - message_id:  Globally unique ID for deduplication and tracking
      - topic:       Logical channel this message belongs to
      - key:         Routing/partition key (Kafka: determines partition)
      - value:       The actual payload (bytes in real systems, dict here)
      - headers:     Metadata (correlation_id, content-type, trace-id)
      - timestamp:   When the message was produced
      - partition:   Which partition (Kafka) or queue (RabbitMQ) stored it
      - offset:      Position in the partition log (Kafka-specific)
    """
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    topic: str = ""
    key: str = ""            # Used for partitioning and ordering
    value: Any = None        # The payload
    headers: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    partition: int = 0       # Assigned by broker
    offset: int = -1         # Position in partition log (Kafka)
    retry_count: int = 0     # Number of delivery attempts
    def __repr__(self):
        return (f"Message(id={self.message_id[:8]}..., "
                f"topic={self.topic}, key={self.key!r}, "
                f"partition={self.partition}, offset={self.offset})")
# ─────────────────────────────────────────────────────────────────────────────
# Delivery Acknowledgment
# ─────────────────────────────────────────────────────────────────────────────
class AckStatus(Enum):
    """Consumer acknowledgment status."""
    ACK   = "ACK"    # Processed successfully → remove from queue
    NACK  = "NACK"   # Processing failed → requeue (or send to DLQ)
    REJECT = "REJECT" # Permanently failed → send to DLQ, don't requeue
@dataclass
class DeliveryAck:
    """Acknowledgment sent from consumer back to broker."""
    message_id: str
    status: AckStatus
    consumer_id: str
    reason: str = ""     # For NACK/REJECT: why it failed
# ─────────────────────────────────────────────────────────────────────────────
# Base Producer
# ─────────────────────────────────────────────────────────────────────────────
class BaseProducer:
    """
    Base message producer.
    Responsibilities:
      1. Create messages with correct metadata
      2. Choose routing key / partition key
      3. Serialize payload
      4. Handle send failures (retry, dead-letter)
      5. Track delivery confirmation (optional: acks)
    PRODUCER DELIVERY SEMANTICS (Kafka):
      acks=0  → Fire and forget (fastest, may lose messages)
      acks=1  → Leader broker ACKs (may lose if leader fails before replication)
      acks=-1 → All replicas ACK (slowest, strongest durability guarantee)
    """
    def __init__(self, producer_id: str):
        self.producer_id = producer_id
        self.sent_count = 0
        self.failed_count = 0
        self._lock = threading.Lock()
    def _build_message(self, topic: str, value: Any, key: str = "") -> Message:
        """Build a message with standard metadata."""
        return Message(
            message_id=str(uuid.uuid4()),
            topic=topic,
            key=key,
            value=value,
            headers={
                "producer_id":   self.producer_id,
                "content_type":  "application/json",
                "produced_at":   str(time.time()),
            },
            timestamp=time.time(),
        )
    def send(self, broker, topic: str, value: Any, key: str = "") -> Message:
        """Send a message to the broker."""
        msg = self._build_message(topic, value, key)
        with self._lock:
            self.sent_count += 1
        broker.publish(msg)
        return msg
    def stats(self) -> dict:
        return {"sent": self.sent_count, "failed": self.failed_count}
# ─────────────────────────────────────────────────────────────────────────────
# Base Consumer
# ─────────────────────────────────────────────────────────────────────────────
class BaseConsumer:
    """
    Base message consumer.
    Responsibilities:
      1. Subscribe to topics/queues
      2. Pull or receive pushed messages
      3. Process messages (business logic)
      4. ACK or NACK based on processing result
      5. Track offsets (Kafka) or message IDs (RabbitMQ) for fault tolerance
    CONSUMER COMMIT STRATEGIES (Kafka):
      Auto-commit:    Offsets committed periodically (may re-process on crash)
      Manual commit:  App commits only after successful processing (exactly-once)
      Sync commit:    Blocks until broker confirms commit (safest)
      Async commit:   Non-blocking, but may lose commit on crash
    """
    def __init__(self, consumer_id: str):
        self.consumer_id = consumer_id
        self.processed_count = 0
        self.failed_count = 0
        self.last_processed_offset: dict[int, int] = {}  # {partition → offset}
    def on_message(self, message: Message) -> AckStatus:
        """
        Override this method with your processing logic.
        Must return ACK, NACK, or REJECT.
        """
        raise NotImplementedError
    def _handle(self, message: Message) -> DeliveryAck:
        """Wrapper: calls on_message and builds an ACK."""
        try:
            status = self.on_message(message)
            if status == AckStatus.ACK:
                self.processed_count += 1
                self.last_processed_offset[message.partition] = message.offset
            else:
                self.failed_count += 1
        except Exception as e:
            status = AckStatus.NACK
            self.failed_count += 1
        return DeliveryAck(
            message_id=message.message_id,
            status=status,
            consumer_id=self.consumer_id,
        )
    def stats(self) -> dict:
        return {
            "consumer_id": self.consumer_id,
            "processed": self.processed_count,
            "failed": self.failed_count,
            "offsets": self.last_processed_offset,
        }
# ─────────────────────────────────────────────────────────────────────────────
# Dead Letter Queue (DLQ)
# ─────────────────────────────────────────────────────────────────────────────
class DeadLetterQueue:
    """
    Dead Letter Queue (DLQ) — where messages go to die gracefully.
    CONCEPT:
      When a message CANNOT be processed (after max retries, or REJECT),
      it is moved to a DLQ instead of being discarded or blocking the main queue.
    WHY DLQ?
      Without DLQ: bad messages block the queue forever (poison pill problem)
      With DLQ:    bad messages are quarantined, rest of queue flows normally
    WHAT TO DO WITH DLQ MESSAGES:
      1. Alert the on-call team
      2. Analyze: schema change? bug? malformed data?
      3. Fix the bug
      4. Replay from DLQ back to main queue (if appropriate)
    DLQ METADATA:
      Each DLQ message should include:
        - Original topic/queue
        - Original message_id
        - Failure reason
        - Number of retries attempted
        - Timestamp when moved to DLQ
    USED BY:
      - AWS SQS: Dead Letter Queue with MaxReceiveCount
      - RabbitMQ: x-dead-letter-exchange binding
      - Kafka: manual DLQ topic (kafka.dlq.<topic>)
    """
    def __init__(self, name: str = "dead_letter_queue"):
        self.name = name
        self._messages: list[dict] = []
        self._lock = threading.Lock()
    def enqueue(self, message: Message, reason: str) -> None:
        """Move a failed message to the DLQ with context."""
        with self._lock:
            self._messages.append({
                "dlq_entry_id":  str(uuid.uuid4()),
                "original_msg":  message,
                "original_topic": message.topic,
                "failure_reason": reason,
                "retry_count":   message.retry_count,
                "dlq_at":        time.time(),
            })
        print(f"  [DLQ:{self.name}] ☠ Message {message.message_id[:8]}... "
              f"sent to DLQ. Reason: {reason}")
    def drain(self) -> list[dict]:
        """Return all DLQ messages (for inspection / replay)."""
        with self._lock:
            msgs = list(self._messages)
            return msgs
    def count(self) -> int:
        with self._lock:
            return len(self._messages)
# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 65)
    print("   CORE MESSAGING CONCEPTS — Demo")
    print("=" * 65)
    print("\n  Message structure:")
    msg = Message(
        topic="order.events",
        key="user_42",
        value={"order_id": "ord_001", "amount": 99.99, "status": "created"},
        headers={"trace_id": "abc-123", "version": "1.0"},
    )
    print(f"  {msg}")
    print(f"  Headers: {msg.headers}")
    print(f"  Payload: {msg.value}")
    print("\n  Dead Letter Queue:")
    dlq = DeadLetterQueue("payments.dlq")
    bad_msg = Message(topic="payments", key="user_1",
                      value={"corrupt": True}, retry_count=3)
    dlq.enqueue(bad_msg, "Max retries exceeded: schema validation failed")
    dlq.enqueue(bad_msg, "Consumer threw unhandled exception")
    print(f"  DLQ count: {dlq.count()} messages quarantined")
    for entry in dlq.drain():
        print(f"  → {entry['original_topic']} | reason: {entry['failure_reason']}")
    print("\n" + "=" * 65)
    print("  KEY CONCEPTS RECAP:")
    print("  Producer  → Creates messages")
    print("  Broker    → Stores + routes (Kafka/RabbitMQ server)")
    print("  Consumer  → Processes messages, sends ACK/NACK")
    print("  DLQ       → Quarantine for permanently failed messages")
    print("  ACK       → 'I processed this' → remove from queue")
    print("  NACK      → 'Retry this' → requeue")
    print("  REJECT    → 'This is bad' → send to DLQ")
    print("=" * 65)
