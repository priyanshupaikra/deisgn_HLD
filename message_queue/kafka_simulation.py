"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    MESSAGE QUEUE (Kafka/RabbitMQ) — MODULE 2                 ║
║                         KAFKA SIMULATION                                     ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS KAFKA?
──────────────
Apache Kafka is a DISTRIBUTED EVENT STREAMING PLATFORM.
It is fundamentally different from traditional message queues:

  Traditional MQ (RabbitMQ):    Producer → Queue → Consumer (message DELETED after consume)
  Kafka:                         Producer → Topic → Log (message RETAINED, replay possible)

KAFKA'S CORE ARCHITECTURE:
────────────────────────────
  TOPIC      → Logical name for a stream of events (e.g., "order.events")
  PARTITION  → A topic is split into N partitions for parallelism
               Each partition is an ORDERED, IMMUTABLE LOG of messages
               Messages are APPENDED to the end, never modified
  OFFSET     → Position of a message in a partition (0-indexed, monotonically increasing)
  SEGMENT    → Physical file on disk; partition is split into segments
  BROKER     → A Kafka server. A cluster has multiple brokers.
  REPLICATION→ Each partition is replicated across R brokers (fault tolerance)
  LEADER     → One broker handles reads/writes for a partition
  FOLLOWER   → Replicas that mirror the leader (take over if leader fails)

PARTITIONING STRATEGIES:
─────────────────────────
  How Kafka decides which partition a message goes to:

  1. EXPLICIT KEY (most common):
     hash(message.key) % num_partitions
     Messages with the SAME key ALWAYS go to the SAME partition
     → Guarantees ordering for a specific entity (e.g., all orders from user_42)

  2. ROUND ROBIN (no key):
     Messages distributed evenly across partitions
     → No ordering guarantee but maximizes throughput

  3. CUSTOM PARTITIONER:
     App-defined logic (e.g., "VIP users → partition 0")

WHY PARTITIONS? (Parallelism):
───────────────────────────────
  1 topic, 4 partitions, 4 consumers in a group → each consumer reads 1 partition
  → 4x throughput compared to 1 partition, 4 consumers

  RULE: # consumers in group ≤ # partitions
  If consumers > partitions → extra consumers are idle (no partition to read)

CONSUMER GROUPS:
─────────────────
  A CONSUMER GROUP is a set of consumers that collectively read a topic.
  Each partition is consumed by EXACTLY ONE consumer in the group.
  Different groups independently consume ALL messages (fan-out).

  Example: topic "order.events" has 3 partitions
    Group "analytics-service":   consumer A → P0, consumer B → P1, consumer C → P2
    Group "email-service":       consumer X → P0+P1, consumer Y → P2
    (both groups read ALL messages independently)

KAFKA OFFSET MANAGEMENT:
─────────────────────────
  Offsets are stored in a special topic: __consumer_offsets
  Each consumer group tracks: {topic, partition, consumer_group → committed_offset}

  On crash/restart: consumer reads from last committed offset → no message loss
  But if committed BEFORE processing: may lose messages (at-most-once)
  If committed AFTER processing: may reprocess messages on crash (at-least-once)

KAFKA RETENTION:
─────────────────
  Messages are retained for a configurable time (default 7 days).
  REPLAY: consumers can seek backward and replay old messages.
  This is a UNIQUE feature — RabbitMQ does not support replay.

LOG COMPACTION:
────────────────
  Instead of time-based deletion, compaction keeps the LATEST value per key.
  Useful for changelog topics (like a database changelog).
  Example: user profile updates — only keep the most recent profile per user_id.

"""

import time
import uuid
import hashlib
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from core_concepts import Message, BaseProducer, BaseConsumer, AckStatus, DeadLetterQueue


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Partition — the core storage unit
# ─────────────────────────────────────────────────────────────────────────────

class KafkaPartition:
    """
    A single Kafka partition — an ordered, immutable log of messages.

    PROPERTIES:
      - Append-only: messages are only added to the end
      - Each message gets a monotonically increasing OFFSET
      - Messages are retained until TTL expires or log compaction runs
      - Consumers track their position via offsets (not message deletion)
    """

    def __init__(self, topic: str, partition_id: int, retention_seconds: float = None):
        self.topic = topic
        self.partition_id = partition_id
        self.retention_seconds = retention_seconds  # None = keep forever (demo)
        self._log: list[Message] = []               # The append-only log
        self._lock = threading.Lock()
        self._next_offset = 0

    def append(self, message: Message) -> int:
        """Append a message to the log. Returns the assigned offset."""
        with self._lock:
            message.partition = self.partition_id
            message.offset = self._next_offset
            self._log.append(message)
            self._next_offset += 1
            return message.offset

    def read(self, from_offset: int, max_records: int = 100) -> list[Message]:
        """
        Read messages starting from a given offset.
        This is how consumers fetch — they specify WHERE to start reading.
        """
        with self._lock:
            if from_offset >= len(self._log):
                return []
            return self._log[from_offset: from_offset + max_records]

    def high_watermark(self) -> int:
        """
        The High Watermark Offset (HWO) is the offset of the next message to be written.
        Consumers can only read up to the high watermark.
        (Messages above HWO may not yet be replicated to all followers.)
        """
        return self._next_offset

    def size(self) -> int:
        return len(self._log)

    def compact(self) -> int:
        """
        LOG COMPACTION: keep only the latest message per key.
        Returns number of messages removed.
        """
        with self._lock:
            # Find latest offset for each key
            latest: dict[str, int] = {}
            for i, msg in enumerate(self._log):
                if msg.key:
                    latest[msg.key] = i

            # Keep messages that are the latest for their key (or have no key)
            original_len = len(self._log)
            compacted = []
            for i, msg in enumerate(self._log):
                if not msg.key or latest.get(msg.key) == i:
                    compacted.append(msg)

            self._log = compacted
            removed = original_len - len(self._log)
            return removed

    def __repr__(self):
        return (f"KafkaPartition(topic={self.topic}, "
                f"id={self.partition_id}, "
                f"messages={len(self._log)}, "
                f"HWM={self._next_offset})")


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Topic
# ─────────────────────────────────────────────────────────────────────────────

class KafkaTopic:
    """
    A Kafka Topic — a named stream of events split into partitions.

    ANALOGY: A topic is like a database table, but for events.
    Each partition is like a shard of that table.
    """

    def __init__(self, name: str, num_partitions: int = 3,
                 replication_factor: int = 1):
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor  # How many broker copies
        self.partitions: list[KafkaPartition] = [
            KafkaPartition(name, i) for i in range(num_partitions)
        ]

    def get_partition(self, key: str) -> int:
        """
        PARTITION SELECTION:
          - If key is provided: hash(key) % num_partitions (consistent routing)
          - If no key: round-robin (for even distribution)

        Using a key ensures all messages for the same entity (user, order)
        go to the same partition → guaranteed ordering for that entity.
        """
        if not key:
            # Round-robin: use a simple incrementing counter
            return int(time.time() * 1000) % self.num_partitions

        # Hash-based: deterministic, same key → same partition always
        hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hash_val % self.num_partitions

    def append(self, message: Message) -> tuple[int, int]:
        """Route message to correct partition. Returns (partition_id, offset)."""
        partition_id = self.get_partition(message.key)
        offset = self.partitions[partition_id].append(message)
        return partition_id, offset

    def stats(self) -> dict:
        return {
            "name": self.name,
            "num_partitions": self.num_partitions,
            "partitions": [
                {"id": p.partition_id, "size": p.size(), "hwm": p.high_watermark()}
                for p in self.partitions
            ],
            "total_messages": sum(p.size() for p in self.partitions),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Consumer Group Coordinator
# ─────────────────────────────────────────────────────────────────────────────

class ConsumerGroupCoordinator:
    """
    Kafka Consumer Group Coordinator.

    Manages the assignment of partitions to consumers within a group.
    Handles REBALANCING when consumers join or leave.

    REBALANCING:
      When the group membership changes (consumer joins, crashes, or leaves),
      Kafka redistributes partition assignments — this is called REBALANCING.

      During rebalancing: all consumers STOP processing (stop-the-world).
      After rebalancing: each consumer resumes from its last committed offset.

    PARTITION ASSIGNMENT STRATEGIES:
      RangeAssignor:       P0+P1 → C0, P2+P3 → C1 (consecutive ranges)
      RoundRobinAssignor:  P0 → C0, P1 → C1, P2 → C0, P3 → C1 (round-robin)
      StickyAssignor:      Minimizes reassignment on rebalance
    """

    def __init__(self, group_id: str):
        self.group_id = group_id
        # {consumer_id → [partition_ids]}
        self.assignments: dict[str, list[int]] = {}
        # {(partition_id) → committed_offset}
        self.committed_offsets: dict[tuple, int] = {}
        self._lock = threading.Lock()

    def assign(self, consumer_ids: list[str], topic: KafkaTopic) -> dict[str, list[int]]:
        """
        Assign partitions to consumers (range assignment strategy).
        Called during startup and rebalancing.
        """
        with self._lock:
            n_parts = topic.num_partitions
            n_consumers = len(consumer_ids)
            new_assignments: dict[str, list[int]] = {cid: [] for cid in consumer_ids}

            # Range assignment: distribute partitions as evenly as possible
            for partition_id in range(n_parts):
                consumer_idx = partition_id % n_consumers
                new_assignments[consumer_ids[consumer_idx]].append(partition_id)

            self.assignments = new_assignments

            # Print assignment map
            print(f"  [ConsumerGroup:{self.group_id}] Partition assignment:")
            for cid, parts in new_assignments.items():
                print(f"    {cid} → partitions {parts}")

            return new_assignments

    def commit_offset(self, consumer_id: str, topic_name: str,
                       partition_id: int, offset: int) -> None:
        """
        COMMIT OFFSET:
          Record that this consumer group has processed up to this offset.
          Stored in __consumer_offsets topic (simulated as a dict here).
          On restart: consumer resumes from (committed_offset + 1).
        """
        with self._lock:
            key = (topic_name, partition_id)
            self.committed_offsets[key] = offset

    def get_committed_offset(self, topic_name: str, partition_id: int) -> int:
        """Get last committed offset. Returns -1 if never committed (read from beginning)."""
        return self.committed_offsets.get((topic_name, partition_id), -1)


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Broker (simulated)
# ─────────────────────────────────────────────────────────────────────────────

class KafkaBroker:
    """
    Simulated Kafka Broker.

    In production: many brokers form a cluster, coordinated by ZooKeeper or
    KRaft (Kafka's own consensus protocol since 2.8).

    Responsibilities:
      - Manage topics and partitions
      - Accept messages from producers
      - Serve messages to consumers
      - Track consumer group offsets
      - Replicate partitions across brokers
    """

    def __init__(self, broker_id: int = 0):
        self.broker_id = broker_id
        self.topics: dict[str, KafkaTopic] = {}
        self._lock = threading.Lock()

    def create_topic(self, name: str, num_partitions: int = 3,
                     replication_factor: int = 1) -> KafkaTopic:
        """Create a topic. Returns existing if already present."""
        with self._lock:
            if name not in self.topics:
                self.topics[name] = KafkaTopic(name, num_partitions, replication_factor)
                print(f"  [Kafka] Topic created: '{name}' "
                      f"({num_partitions} partitions, rf={replication_factor})")
            return self.topics[name]

    def publish(self, message: Message) -> tuple[int, int]:
        """
        PRODUCER SEND:
          Append message to the correct partition of the topic.
          Returns (partition_id, offset).
        """
        if message.topic not in self.topics:
            raise ValueError(f"Topic '{message.topic}' does not exist. Create it first.")

        topic = self.topics[message.topic]
        partition_id, offset = topic.append(message)
        message.topic = message.topic  # Already set
        return partition_id, offset

    def fetch(self, topic_name: str, partition_id: int,
              from_offset: int, max_records: int = 50) -> list[Message]:
        """
        CONSUMER FETCH (PULL model):
          Kafka uses a PULL model — consumers request messages.
          Consumers track their own offset and fetch from where they left off.
        """
        topic = self.topics.get(topic_name)
        if not topic:
            return []
        return topic.partitions[partition_id].read(from_offset, max_records)

    def get_topic_stats(self) -> dict:
        return {name: topic.stats() for name, topic in self.topics.items()}


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Producer
# ─────────────────────────────────────────────────────────────────────────────

class KafkaProducer(BaseProducer):
    """
    Kafka Producer.

    KEY FEATURES:
      - Partitioning by message key (hash-based)
      - Batching (accumulate messages, send in batches for throughput)
      - Compression (snappy, gzip, lz4, zstd)
      - Idempotent producer (enable.idempotence=true → exactly-once on producer side)
    """

    def __init__(self, producer_id: str, broker: KafkaBroker):
        super().__init__(producer_id)
        self.broker = broker

    def send(self, topic: str, value: Any, key: str = "") -> Message:
        """Send a message to a Kafka topic."""
        msg = self._build_message(topic, value, key)
        partition_id, offset = self.broker.publish(msg)
        self.sent_count += 1
        print(f"  [KafkaProducer:{self.producer_id}] SENT: "
              f"topic={topic}, key={key!r}, "
              f"partition={partition_id}, offset={offset}")
        return msg


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Consumer
# ─────────────────────────────────────────────────────────────────────────────

class KafkaConsumer(BaseConsumer):
    """
    Kafka Consumer.

    KEY FEATURES:
      - PULL model: consumer controls fetch rate
      - Offset tracking: exactly where to resume after crash
      - Consumer groups: parallel processing across partitions
      - Rebalancing: automatic partition reassignment
    """

    def __init__(self, consumer_id: str, broker: KafkaBroker,
                 group_coordinator: ConsumerGroupCoordinator,
                 handler: Callable[[Message], bool] = None):
        super().__init__(consumer_id)
        self.broker = broker
        self.coordinator = group_coordinator
        self.handler = handler   # Business logic handler
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def on_message(self, message: Message) -> AckStatus:
        """Process a message. Returns ACK/NACK/REJECT."""
        if self.handler:
            try:
                success = self.handler(message)
                return AckStatus.ACK if success else AckStatus.NACK
            except Exception as e:
                return AckStatus.NACK
        return AckStatus.ACK  # Default: always ACK

    def poll(self, topic_name: str, partition_id: int,
             max_records: int = 10) -> list[Message]:
        """
        POLL for messages from a specific partition.

        Reads from last committed offset + 1.
        After processing, commits the new offset.
        """
        from_offset = self.coordinator.get_committed_offset(
            topic_name, partition_id
        ) + 1

        messages = self.broker.fetch(topic_name, partition_id,
                                     from_offset, max_records)
        for msg in messages:
            ack = self._handle(msg)
            if ack.status == AckStatus.ACK:
                # Commit offset after successful processing
                self.coordinator.commit_offset(
                    self.consumer_id, topic_name, partition_id, msg.offset
                )
                print(f"  [Consumer:{self.consumer_id}] ACK partition={partition_id} "
                      f"offset={msg.offset} | payload={msg.value}")
            else:
                print(f"  [Consumer:{self.consumer_id}] NACK partition={partition_id} "
                      f"offset={msg.offset} | will retry")

        return messages


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   KAFKA SIMULATION — Demo")
    print("=" * 65)

    broker = KafkaBroker(broker_id=0)

    # ── Create topic ──────────────────────────────────────────────────────
    print("\n  ─── Topic Creation ───\n")
    broker.create_topic("order.events", num_partitions=3, replication_factor=1)

    # ── Produce messages with keys ────────────────────────────────────────
    print("\n  ─── Producing Messages (keyed by user_id) ───\n")
    producer = KafkaProducer("order-service", broker)

    events = [
        ("user_1", {"order_id": "ord_001", "event": "created",   "amount": 50}),
        ("user_2", {"order_id": "ord_002", "event": "created",   "amount": 80}),
        ("user_1", {"order_id": "ord_001", "event": "confirmed", "amount": 50}),
        ("user_3", {"order_id": "ord_003", "event": "created",   "amount": 120}),
        ("user_2", {"order_id": "ord_002", "event": "shipped",   "amount": 80}),
        ("user_1", {"order_id": "ord_001", "event": "delivered", "amount": 50}),
    ]

    for key, value in events:
        producer.send("order.events", value, key=key)

    print()
    stats = broker.get_topic_stats()["order.events"]
    print(f"  Topic stats: total={stats['total_messages']} messages")
    for p in stats["partitions"]:
        print(f"    Partition {p['id']}: {p['size']} messages (HWM={p['hwm']})")

    # ── Consumer Group ────────────────────────────────────────────────────
    print("\n  ─── Consumer Group (2 consumers, 3 partitions) ───\n")
    topic_obj = broker.topics["order.events"]
    coord = ConsumerGroupCoordinator("analytics-group")
    coord.assign(["consumer_A", "consumer_B"], topic_obj)

    def order_handler(msg: Message) -> bool:
        # Business logic: process the order event
        return True  # ACK

    consumer_a = KafkaConsumer("consumer_A", broker, coord, order_handler)
    consumer_b = KafkaConsumer("consumer_B", broker, coord, order_handler)

    print("\n  Consumer A polling its assigned partitions:")
    for part_id in coord.assignments.get("consumer_A", []):
        consumer_a.poll("order.events", part_id)

    print("\n  Consumer B polling its assigned partitions:")
    for part_id in coord.assignments.get("consumer_B", []):
        consumer_b.poll("order.events", part_id)

    print(f"\n  Consumer A stats: {consumer_a.stats()}")
    print(f"  Consumer B stats: {consumer_b.stats()}")

    # ── Ordering guarantee ────────────────────────────────────────────────
    print("\n  ─── Ordering Guarantee: All user_1 events → same partition ───\n")
    user1_partition = topic_obj.get_partition("user_1")
    user1_msgs = topic_obj.partitions[user1_partition].read(0)
    print(f"  All events for user_1 are in partition {user1_partition}:")
    for m in user1_msgs:
        if m.key == "user_1":
            print(f"    offset={m.offset} | {m.value['event']}")

    # ── Log Compaction ────────────────────────────────────────────────────
    print("\n  ─── Log Compaction: Keep only latest value per key ───\n")
    broker.create_topic("user.profile", num_partitions=1)
    p = KafkaProducer("profile-service", broker)
    for name in ["Alice", "Alice V2", "Alice V3", "Bob", "Bob V2"]:
        user = "user_1" if "Alice" in name else "user_2"
        p.send("user.profile", {"name": name}, key=user)

    partition_0 = broker.topics["user.profile"].partitions[0]
    print(f"  Before compaction: {partition_0.size()} messages")
    removed = partition_0.compact()
    print(f"  After compaction: {partition_0.size()} messages ({removed} removed)")
    print(f"  Remaining (latest per key):")
    for m in partition_0.read(0):
        print(f"    key={m.key} → {m.value}")

    print("\n" + "=" * 65)
    print("  KAFKA KEY CONCEPTS:")
    print("  Partition   → Ordered log; key → deterministic partition")
    print("  Offset      → Position; consumer resumes from last offset")
    print("  Cons. Group → Parallel consumers share partitions")
    print("  Retention   → Messages kept N days; REPLAY is possible")
    print("  Compaction  → Keep latest value per key (changelog)")
    print("=" * 65)
