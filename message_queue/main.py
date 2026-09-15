"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                   MESSAGE QUEUE (Kafka/RabbitMQ) SUITE                       ║
║                       All Concepts — Main Runner                             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  QUICK REFERENCE:                                                            ║
║                                                                              ║
║  WHEN TO USE KAFKA:                                                          ║
║    ✅ Event sourcing / audit log (need message replay)                      ║
║    ✅ High-throughput streaming (millions of events/sec)                    ║
║    ✅ Multiple independent consumers of the same event stream               ║
║    ✅ Event-driven microservices with decoupled teams                       ║
║    ✅ Real-time analytics pipelines                                          ║
║    Examples: Uber's trip events, LinkedIn's activity feed                   ║
║                                                                              ║
║  WHEN TO USE RABBITMQ:                                                       ║
║    ✅ Task queues (job distribution, background workers)                    ║
║    ✅ Complex routing (direct/topic/headers exchanges)                      ║
║    ✅ RPC-style request-reply over messaging                                ║
║    ✅ Shorter retention (message deleted after consume)                     ║
║    ✅ Priority queues, delay queues natively                                ║
║    Examples: Email sending, image processing, order fulfillment             ║
║                                                                              ║
║  MODULES:                                                                    ║
║    core_concepts.py       → Message, Producer, Consumer, DLQ basics        ║
║    kafka_simulation.py    → Partitions, Offsets, Consumer Groups, Compact  ║
║    rabbitmq_simulation.py → Direct, Fanout, Topic, Headers exchanges       ║
║    patterns.py            → PubSub, Competing Consumers, Priority,         ║
║                              Delay Queue, Request-Reply RPC                 ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
import sys
import os
import time
import threading
sys.path.insert(0, os.path.dirname(__file__))
from core_concepts import Message, DeadLetterQueue, AckStatus
from kafka_simulation import KafkaBroker, KafkaProducer, KafkaConsumer, ConsumerGroupCoordinator
from rabbitmq_simulation import RabbitMQBroker
from patterns import (
    PubSubBroker, CompetingConsumersQueue,
    PriorityQueue, DelayQueue, RPCServer
)
def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")
def subsection(title: str):
    print(f"\n  ─── {title} ───")
# ─────────────────────────────────────────────────────────────────────────────
# DEMO 1: KAFKA
# ─────────────────────────────────────────────────────────────────────────────
def demo_kafka():
    section("MODULE 2: KAFKA — Log-based Streaming")
    print("  Key features: Partitions, Offsets, Consumer Groups, Replay, Compaction")
    broker = KafkaBroker()
    broker.create_topic("payments", num_partitions=3)
    subsection("Producing with keys (same key → same partition = ordered)")
    producer = KafkaProducer("payment-service", broker)
    events = [
        ("user_1", {"event": "initiated", "amount": 100}),
        ("user_2", {"event": "initiated", "amount": 200}),
        ("user_1", {"event": "completed", "amount": 100}),  # Same partition as user_1 initiated
        ("user_3", {"event": "initiated", "amount": 50}),
    ]
    for key, value in events:
        producer.send("payments", value, key=key)
    stats = broker.get_topic_stats()["payments"]
    print(f"\n  Total messages: {stats['total_messages']} across 3 partitions")
    for p in stats["partitions"]:
        print(f"    Partition {p['id']}: {p['size']} msg(s)")
    subsection("Consumer Group — 2 consumers share 3 partitions")
    topic = broker.topics["payments"]
    coord = ConsumerGroupCoordinator("billing-group")
    coord.assign(["billing-A", "billing-B"], topic)
    processed = []
    consumer_a = KafkaConsumer("billing-A", broker, coord,
                               handler=lambda m: processed.append(m.value) or True)
    consumer_b = KafkaConsumer("billing-B", broker, coord,
                               handler=lambda m: processed.append(m.value) or True)
    for p in coord.assignments.get("billing-A", []):
        consumer_a.poll("payments", p)
    for p in coord.assignments.get("billing-B", []):
        consumer_b.poll("payments", p)
    print(f"\n  Total events processed by group: {len(processed)}")
    print(f"  billing-A processed: {consumer_a.stats()['processed']}")
    print(f"  billing-B processed: {consumer_b.stats()['processed']}")
# ─────────────────────────────────────────────────────────────────────────────
# DEMO 2: RABBITMQ
# ─────────────────────────────────────────────────────────────────────────────
def demo_rabbitmq():
    section("MODULE 3: RABBITMQ — Exchange-based Routing")
    print("  Key features: Direct, Fanout, Topic, Headers exchanges + ACK/DLQ")
    broker = RabbitMQBroker()
    subsection("Fanout: 1 order event → email + inventory + analytics")
    broker.declare_exchange("order.events", "fanout")
    q_email = broker.declare_queue("order-email-queue")
    q_inv   = broker.declare_queue("order-inventory-queue")
    q_anal  = broker.declare_queue("order-analytics-queue")
    for q in [q_email, q_inv, q_anal]:
        broker.exchanges["order.events"].bind(q)
    broker.publish("order.events", "", {"order_id": "ord_001", "total": 99.99})
    print(f"\n  1 publish → email:{q_email.depth()}, "
          f"inventory:{q_inv.depth()}, analytics:{q_anal.depth()} (all got it)")
    subsection("Topic: Routing log events with wildcards")
    broker.declare_exchange("logs", "topic")
    q_errors = broker.declare_queue("error-logs")
    q_all    = broker.declare_queue("all-logs")
    broker.exchanges["logs"].bind(q_errors, "*.error")
    broker.exchanges["logs"].bind(q_all,    "#")
    for rk in ["payment.error", "order.created", "db.error", "system.info"]:
        broker.publish("logs", rk, {"msg": rk})
    print(f"\n  After 4 events: errors={q_errors.depth()} (*.error), all={q_all.depth()} (#)")
    subsection("ACK/NACK with DLQ (max 2 retries)")
    broker.declare_exchange("tasks", "direct")
    q_tasks = broker.declare_queue("task-queue", max_retries=2)
    broker.exchanges["tasks"].bind(q_tasks, "task")
    broker.publish("tasks", "task", {"job": "bad-job"})
    attempt = 0
    while q_tasks.depth() > 0 or len(q_tasks._unacked) > 0:
        msg = q_tasks.get()
        if not msg:
            break
        attempt += 1
        print(f"  Processing attempt {attempt}: {msg.value['job']}")
        q_tasks.nack(msg.headers["delivery_tag"], requeue=(attempt < 2))
    print(f"  DLQ count: {q_tasks.dlq.count()} (bad-job went to DLQ after max retries)")
# ─────────────────────────────────────────────────────────────────────────────
# DEMO 3: PATTERNS
# ─────────────────────────────────────────────────────────────────────────────
def demo_patterns():
    section("MODULE 4: ADVANCED MESSAGING PATTERNS")
    subsection("Priority Queue — critical requests first")
    pq = PriorityQueue("api-requests")
    pq.enqueue({"task": "free-tier-search"},    priority=3)
    pq.enqueue({"task": "premium-checkout"},    priority=1)
    pq.enqueue({"task": "payment-alert"},       priority=0)
    pq.enqueue({"task": "standard-browse"},     priority=2)
    print("  Dequeue order:")
    for msg in pq.drain_in_order():
        p = msg.headers["priority"]
        label = ["CRITICAL", "HIGH", "NORMAL", "LOW"][p]
        print(f"    [{label}] {msg.value['task']}")
    subsection("Delay Queue — scheduled retry")
    dq = DelayQueue("retry-jobs")
    dq.enqueue({"job": "retry #1"}, delay_seconds=0.1)
    dq.enqueue({"job": "retry #2"}, delay_seconds=0.3)
    dq.enqueue({"job": "retry #3"}, delay_seconds=0.6)
    for wait, label in [(0.15, "t=0.15s"), (0.35, "t=0.5s"), (0.5, "t=1.0s")]:
        time.sleep(wait)
        ready = [m.value["job"] for m in dq.poll()]
        if ready:
            print(f"  Polling at {label}: {ready}")
    subsection("Competing Consumers — parallel worker pool")
    results = []
    lock = threading.Lock()
    def handler(msg: Message):
        time.sleep(0.02)
        with lock:
            results.append(msg.value)
    pool = CompetingConsumersQueue("jobs")
    pool.start_workers(4, handler)
    for i in range(8):
        pool.enqueue(f"job_{i}")
    time.sleep(0.4)
    pool.stop()
    print(f"  8 jobs, 4 workers → {pool.stats().get('total_processed', len(results))} processed")
# ─────────────────────────────────────────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    section("MESSAGE QUEUE — COMPLETE REFERENCE")
    print("\n  KAFKA vs RABBITMQ:")
    rows = [
        ("Feature",        "Kafka",                  "RabbitMQ"),
        ("─"*14,           "─"*24,                   "─"*22),
        ("Model",          "Append-only log",         "FIFO queue"),
        ("Retention",      "Days/forever (replay)",   "Until consumed"),
        ("Routing",        "Topic + Partition",       "Exchange + Binding"),
        ("Throughput",     "Millions/sec",            "Thousands/sec"),
        ("Ordering",       "Per-partition",           "Per-queue"),
        ("Replay",         "YES (seek offset)",       "NO"),
        ("Priority",       "Manual workaround",       "Native (x-max-priority)"),
        ("Delay",          "Manual workaround",       "Native (x-message-ttl)"),
    ]
    for r in rows:
        print(f"  {r[0]:<15} {r[1]:<25} {r[2]}")
    print("\n  RABBITMQ EXCHANGE TYPES:")
    for exc in [
        ("Direct",   "Exact routing key match",   "Task dispatch by type"),
        ("Fanout",   "Broadcast to all queues",   "Event notifications"),
        ("Topic",    "Wildcard patterns (* #)",   "Flexible event routing"),
        ("Headers",  "Match by header values",    "Complex filter conditions"),
    ]:
        print(f"    {exc[0]:<9} → {exc[1]:<28} | {exc[2]}")
    print("\n  MESSAGING PATTERNS:")
    for pat in [
        ("Pub/Sub",            "1 → N fan-out",                    "Decouple publishers/consumers"),
        ("Competing Consumers","N workers share 1 queue",           "Horizontal scale-out"),
        ("Priority Queue",     "High-pri dequeued first",          "SLA tiers, VIP traffic"),
        ("Delay Queue",        "Visible after TTL",                "Retry backoff, scheduling"),
        ("Request-Reply",      "RPC via correlation_id+reply_to",  "Async RPC between services"),
    ]:
        print(f"    {pat[0]:<20} {pat[1]:<34} → {pat[2]}")
    print("\n  DELIVERY GUARANTEES (recap from idempotency module):")
    print("    At-Most-Once  → Fire-and-forget. May lose messages.")
    print("    At-Least-Once → Retry until ACK. May duplicate → need idempotent consumer.")
    print("    Exactly-Once  → Kafka transactions OR at-least-once + dedup consumer.")
    print()
    print("  Run individual files:")
    for f in ["core_concepts.py", "kafka_simulation.py",
              "rabbitmq_simulation.py", "patterns.py"]:
        print(f"    python {f}")
    print()
# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       MESSAGE QUEUE (Kafka/RabbitMQ) — All Concepts")
    print("=" * 65)
    demo_kafka()
    demo_rabbitmq()
    demo_patterns()
    print_summary()
