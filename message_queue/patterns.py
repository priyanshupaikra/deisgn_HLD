"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    MESSAGE QUEUE (Kafka/RabbitMQ) — MODULE 4                 ║
║                        ADVANCED MESSAGING PATTERNS                           ║
╚══════════════════════════════════════════════════════════════════════════════╝
CONCEPT:
--------
Beyond basic produce/consume, message queues enable powerful DESIGN PATTERNS
that solve real distributed system challenges. These patterns are language-
and broker-agnostic (work with Kafka, RabbitMQ, SQS, Pub/Sub, etc.).
PATTERNS COVERED:
──────────────────
  1. PUB/SUB (Publish-Subscribe)  → One publisher, multiple independent subscribers
  2. COMPETING CONSUMERS           → Multiple workers share a queue for throughput
  3. PRIORITY QUEUE                → High-priority messages processed before low-priority
  4. DELAY QUEUE                   → Messages delivered AFTER a specified delay
  5. REQUEST-REPLY (RPC over MQ)   → Synchronous RPC using async messaging
REAL-WORLD USAGE:
──────────────────
  PUB/SUB:
    → Kafka topic with multiple consumer groups
    → SNS (AWS Simple Notification Service) → multiple SQS queues
    → Use: Order created → notify email, inventory, analytics all independently
  COMPETING CONSUMERS:
    → Multiple workers reading from the same SQS queue
    → Worker pool pattern — scale horizontally
    → Use: Image resizing (100 workers, 1 queue = 100x throughput)
  PRIORITY QUEUE:
    → RabbitMQ queue with x-max-priority
    → AWS SQS FIFO doesn't support priority — must use multiple queues
    → Use: Premium users' requests processed before free users
  DELAY QUEUE:
    → RabbitMQ: x-message-ttl + dead-letter exchange (DLX)
    → SQS: DelaySeconds parameter
    → Kafka: no native delay — use separate delay topic
    → Use: Retry with backoff (retry in 30s), scheduled notifications,
           "order confirmed" email sent 1 minute after order is created
  REQUEST-REPLY (RPC over MQ):
    → Correlation ID + reply-to queue pattern
    → Use: When you want RPC-style communication but with async resilience
           E.g., checkout service asks inventory service "is product in stock?"
"""
import time
import uuid
import threading
import heapq
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from core_concepts import Message, DeadLetterQueue
# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 1: PUB/SUB (Publish-Subscribe)
# ─────────────────────────────────────────────────────────────────────────────
class PubSubBroker:
    """
    Pub/Sub Pattern.
    CONCEPT:
      Publishers send events to a TOPIC.
      Multiple SUBSCRIBERS each independently receive ALL events.
      Publishers and subscribers are completely DECOUPLED — they don't know
      about each other. The broker handles fan-out.
    KEY PROPERTY:
      Adding a new subscriber requires ZERO changes to the publisher.
      This is perfect for event-driven architectures.
    PUSH vs PULL:
      PUSH: Broker pushes messages to subscribers as they arrive (low latency)
      PULL: Subscribers poll the broker when ready (backpressure control)
    DIAGRAM:
      Publisher ──▶ [Topic] ──▶ Subscriber A (email-service)
                              ──▶ Subscriber B (analytics-service)
                              ──▶ Subscriber C (notification-service)
                              ──▶ NEW SUBSCRIBER (zero publisher change!)
    """
    def __init__(self):
        # {topic → [subscriber callbacks]}
        self._subscribers: dict[str, list[tuple[str, Callable]]] = defaultdict(list)
        self._event_log: list[dict] = []   # Keep a record of all events
    def subscribe(self, topic: str, subscriber_id: str,
                  handler: Callable[[Message], None]) -> None:
        """Register a subscriber to receive all messages for a topic."""
        self._subscribers[topic].append((subscriber_id, handler))
        print(f"  [PubSub] '{subscriber_id}' subscribed to topic='{topic}'")
    def publish(self, topic: str, event_type: str, data: Any) -> int:
        """
        Publish an event. Delivered to ALL subscribers of the topic.
        Each subscriber receives an INDEPENDENT copy.
        """
        msg = Message(
            message_id=str(uuid.uuid4()),
            topic=topic,
            key=event_type,
            value=data,
            headers={"event_type": event_type},
        )
        self._event_log.append({"topic": topic, "event_type": event_type, "data": data})
        subscribers = self._subscribers.get(topic, [])
        print(f"\n  [PubSub] PUBLISH: topic={topic}, event={event_type} "
              f"→ {len(subscribers)} subscriber(s)")
        for sub_id, handler in subscribers:
            try:
                handler(msg)
            except Exception as e:
                print(f"  [PubSub] ⚠ Subscriber '{sub_id}' failed: {e}")
        return len(subscribers)
    def subscriber_count(self, topic: str) -> int:
        return len(self._subscribers.get(topic, []))
# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 2: COMPETING CONSUMERS (Worker Pool)
# ─────────────────────────────────────────────────────────────────────────────
class CompetingConsumersQueue:
    """
    Competing Consumers Pattern (Worker Pool).
    CONCEPT:
      Multiple consumers ALL read from the SAME queue.
      Each message is delivered to EXACTLY ONE consumer.
      Workers compete to grab the next available message.
    WHY IT'S USEFUL:
      Horizontal scaling: add more workers → process messages faster.
      If one worker crashes → other workers pick up the remaining messages.
      Natural load balancing: fast workers process more messages.
    DIAGRAM:
      [Queue] ──▶ Worker 1 (processing msg A)
              ──▶ Worker 2 (processing msg B)
              ──▶ Worker 3 (processing msg C)
      (Each message goes to exactly ONE worker)
    THROUGHPUT:
      1 worker  → 10 msg/sec
      10 workers → ~100 msg/sec (linear scaling for IO-bound tasks)
    IN PRACTICE:
      - AWS SQS + Lambda (auto-scaling competing consumers)
      - Celery workers (Python task queue)
      - RabbitMQ basic_qos(prefetch_count=1) for fair dispatch
    """
    def __init__(self, name: str):
        self.name = name
        self._queue: deque[Message] = deque()
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._workers: list[threading.Thread] = []
        self._stats: dict[str, int] = defaultdict(int)
        self._running = False
    def enqueue(self, value: Any, priority: int = 0) -> None:
        """Add a message to the queue."""
        msg = Message(
            message_id=str(uuid.uuid4()),
            topic=self.name,
            value=value,
        )
        with self._condition:
            self._queue.append(msg)
            self._stats["enqueued"] += 1
            self._condition.notify()  # Wake up one waiting worker
    def _worker_loop(self, worker_id: str,
                     handler: Callable[[Message], None]) -> None:
        """
        Worker thread: continuously poll for messages, process them.
        Blocks when queue is empty (using condition variable).
        """
        while self._running:
            with self._condition:
                while not self._queue and self._running:
                    self._condition.wait(timeout=0.1)  # Block until message arrives
                if not self._queue:
                    continue
                msg = self._queue.popleft()  # EXACTLY ONE worker gets this message
            # Process outside the lock (allows other workers to grab messages)
            try:
                handler(msg)
                self._stats[f"worker_{worker_id}_processed"] += 1
                self._stats["total_processed"] += 1
            except Exception as e:
                self._stats["errors"] += 1
    def start_workers(self, num_workers: int,
                       handler: Callable[[Message], None]) -> None:
        """Start N competing consumer worker threads."""
        self._running = True
        for i in range(num_workers):
            worker_id = f"W{i+1}"
            t = threading.Thread(
                target=self._worker_loop,
                args=(worker_id, handler),
                daemon=True,
                name=f"worker-{worker_id}",
            )
            t.start()
            self._workers.append(t)
        print(f"  [CompetingConsumers:{self.name}] Started {num_workers} workers")
    def stop(self) -> None:
        """Signal all workers to stop."""
        with self._condition:
            self._running = False
            self._condition.notify_all()
    def depth(self) -> int:
        with self._lock:
            return len(self._queue)
    def stats(self) -> dict:
        return dict(self._stats)
# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 3: PRIORITY QUEUE
# ─────────────────────────────────────────────────────────────────────────────
class PriorityQueue:
    """
    Priority Queue.
    CONCEPT:
      Messages have a priority level. Higher-priority messages are
      ALWAYS processed BEFORE lower-priority messages, regardless of
      when they were enqueued.
    PRIORITY LEVELS (common convention):
      0 = CRITICAL (payment failures, alerts)
      1 = HIGH     (premium user requests)
      2 = NORMAL   (regular users)
      3 = LOW      (background jobs, analytics)
    IMPLEMENTATION:
      Python's heapq (min-heap) — smallest value = highest priority.
      For max-priority: negate the priority value.
    IN RABBITMQ:
      Queue arguments: x-max-priority: 10
      Message property: priority=9
    REAL EXAMPLE:
      Netflix uses priority queues to ensure premium subscribers
      get bandwidth allocation before free-tier users during peak hours.
    """
    def __init__(self, name: str):
        self.name = name
        self._heap: list[tuple[int, int, Message]] = []   # (priority, sequence, msg)
        self._sequence = 0   # Tie-breaker: FIFO within same priority
        self._lock = threading.Lock()
        self.enqueued = 0
        self.dequeued = 0
    def enqueue(self, value: Any, priority: int = 2) -> None:
        """
        Enqueue with priority.
        Lower number = higher priority (min-heap).
        Tie-break by sequence number (FIFO within same priority).
        """
        msg = Message(
            message_id=str(uuid.uuid4()),
            topic=self.name,
            value=value,
            headers={"priority": priority},
        )
        with self._lock:
            heapq.heappush(self._heap, (priority, self._sequence, msg))
            self._sequence += 1
            self.enqueued += 1
    def dequeue(self) -> Optional[Message]:
        """Pop the highest-priority message (lowest priority number)."""
        with self._lock:
            if not self._heap:
                return None
            priority, seq, msg = heapq.heappop(self._heap)
            self.dequeued += 1
            return msg
    def drain_in_order(self) -> list[Message]:
        """Drain all messages in priority order (for demonstration)."""
        messages = []
        while True:
            msg = self.dequeue()
            if not msg:
                break
            messages.append(msg)
        return messages
    def depth(self) -> int:
        with self._lock:
            return len(self._heap)
# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 4: DELAY QUEUE
# ─────────────────────────────────────────────────────────────────────────────
class DelayQueue:
    """
    Delay Queue — messages become visible only AFTER a specified delay.
    CONCEPT:
      Message is enqueued with a delay_seconds parameter.
      It is INVISIBLE to consumers until delay_seconds have passed.
      After the delay, it appears in the queue and is processed normally.
    USE CASES:
      - Retry with exponential backoff:
          1st failure → delay 30s → retry
          2nd failure → delay 60s → retry
          3rd failure → delay 120s → retry
      - Scheduled notifications:
          "Send welcome email 5 minutes after signup"
      - Deferred cleanup:
          "Delete temp file 1 hour after processing"
      - Order cancellation window:
          "If not confirmed in 10 minutes → cancel order"
    IN RABBITMQ:
      Method 1: x-message-ttl on a staging queue + dead-letter to main queue
        → Messages "die" after TTL → go to main queue via dead-letter routing
      Method 2: RabbitMQ Delayed Message Plugin (x-delayed-message exchange)
    IN AWS SQS:
      DelaySeconds = 0 to 900 (max 15 minutes)
    IN KAFKA:
      No native delay support.
      Common workaround: separate delay topic + consumer that checks timestamps.
    """
    def __init__(self, name: str):
        self.name = name
        # Heap: (deliver_at_timestamp, sequence, Message)
        self._heap: list[tuple[float, int, Message]] = []
        self._sequence = 0
        self._lock = threading.Lock()
    def enqueue(self, value: Any, delay_seconds: float) -> Message:
        """
        Enqueue with delay.
        Message will not be visible until (now + delay_seconds).
        """
        deliver_at = time.time() + delay_seconds
        msg = Message(
            message_id=str(uuid.uuid4()),
            topic=self.name,
            value=value,
            headers={
                "delay_seconds": delay_seconds,
                "deliver_at":    deliver_at,
                "enqueued_at":   time.time(),
            },
        )
        with self._lock:
            heapq.heappush(self._heap, (deliver_at, self._sequence, msg))
            self._sequence += 1
        print(f"  [DelayQueue:{self.name}] Scheduled: delay={delay_seconds}s "
              f"→ {msg.value}")
        return msg
    def poll(self) -> list[Message]:
        """
        Return all messages whose delay has elapsed.
        Messages not yet due remain in the queue.
        """
        now = time.time()
        ready = []
        with self._lock:
            while self._heap and self._heap[0][0] <= now:
                deliver_at, seq, msg = heapq.heappop(self._heap)
                ready.append(msg)
        return ready
    def pending_count(self) -> int:
        with self._lock:
            return len(self._heap)
# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 5: REQUEST-REPLY (RPC over Message Queue)
# ─────────────────────────────────────────────────────────────────────────────
class RPCServer:
    """
    Request-Reply (RPC) Pattern over a Message Queue.
    CONCEPT:
      Enables synchronous-looking RPC using asynchronous messaging.
      The client sends a request with:
        - reply_to:       name of the queue to send the response to
        - correlation_id: unique ID to match response to request
      The server processes the request and sends response to reply_to queue
      with the same correlation_id.
      The client waits for a response on its reply_to queue matching
      its correlation_id.
    DIAGRAM:
      Client ──[request + reply_to=client_123 + correlation_id=abc]──▶ [Request Queue]
                                                                              │
                                                                        [Server processes]
                                                                              │
      Client ◀──[response + correlation_id=abc]──────────────────── [client_123 queue]
    ADVANTAGES:
      - Server and client can be on different machines, languages, runtimes
      - Server can be restarted without losing requests (in queue)
      - Can add multiple server instances without client change (competing consumers)
    USED BY:
      - RabbitMQ RPC tutorial (official pattern)
      - Apache Camel, Spring AMQP
      - Enterprise Service Bus (ESB) patterns
    """
    def __init__(self, request_queue_name: str):
        self.request_queue_name = request_queue_name
        self._request_queue: deque[Message] = deque()
        self._reply_queues: dict[str, deque[Message]] = {}
        self._lock = threading.Lock()
    def _get_or_create_reply_queue(self, queue_name: str) -> deque:
        with self._lock:
            if queue_name not in self._reply_queues:
                self._reply_queues[queue_name] = deque()
            return self._reply_queues[queue_name]
    def call(self, payload: Any, timeout_seconds: float = 2.0) -> Optional[Any]:
        """
        CLIENT SIDE: Send request and wait for response.
        Uses a temporary reply queue unique to this call.
        """
        correlation_id = str(uuid.uuid4())
        reply_queue_name = f"rpc.reply.{uuid.uuid4().hex[:8]}"
        reply_queue = self._get_or_create_reply_queue(reply_queue_name)
        # Send request
        request_msg = Message(
            message_id=correlation_id,
            topic=self.request_queue_name,
            value=payload,
            headers={
                "reply_to":       reply_queue_name,
                "correlation_id": correlation_id,
            },
        )
        with self._lock:
            self._request_queue.append(request_msg)
        print(f"  [RPCClient] REQUEST sent: correlation_id={correlation_id[:8]}...")
        # Wait for response (poll reply queue)
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            with self._lock:
                if reply_queue:
                    response_msg = reply_queue.popleft()
                    if response_msg.headers.get("correlation_id") == correlation_id:
                        print(f"  [RPCClient] RESPONSE received: {response_msg.value}")
                        return response_msg.value
            time.sleep(0.01)  # Poll interval
        print(f"  [RPCClient] ⏰ TIMEOUT waiting for correlation_id={correlation_id[:8]}...")
        return None
    def serve(self, handler: Callable[[Any], Any]) -> None:
        """
        SERVER SIDE: Process pending requests and send responses.
        In production: this runs in an infinite loop in a separate process.
        """
        with self._lock:
            requests = list(self._request_queue)
            self._request_queue.clear()
        for request_msg in requests:
            print(f"  [RPCServer] Processing request: {request_msg.value}")
            result = handler(request_msg.value)
            # Send response to the reply_to queue
            reply_to = request_msg.headers.get("reply_to")
            correlation_id = request_msg.headers.get("correlation_id")
            reply_queue = self._get_or_create_reply_queue(reply_to)
            response_msg = Message(
                message_id=str(uuid.uuid4()),
                topic=reply_to,
                value=result,
                headers={"correlation_id": correlation_id},
            )
            with self._lock:
                reply_queue.append(response_msg)
            print(f"  [RPCServer] Response sent to '{reply_to}': {result}")
# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # ── PATTERN 1: PUB/SUB ────────────────────────────────────────────────
    print("=" * 65)
    print("   PATTERN 1: PUB/SUB — One event, many subscribers")
    print("=" * 65)
    pubsub = PubSubBroker()
    email_received = []
    analytics_received = []
    notif_received = []
    pubsub.subscribe("user.events", "email-service",
                     lambda m: email_received.append(m.value))
    pubsub.subscribe("user.events", "analytics-service",
                     lambda m: analytics_received.append(m.value))
    pubsub.subscribe("user.events", "notification-service",
                     lambda m: notif_received.append(m.value))
    pubsub.publish("user.events", "user.registered",
                   {"user_id": "usr_1", "email": "alice@ex.com"})
    pubsub.publish("user.events", "user.login",
                   {"user_id": "usr_1", "ip": "10.0.0.1"})
    print(f"\n  Email got:      {len(email_received)} events")
    print(f"  Analytics got:  {len(analytics_received)} events")
    print(f"  Notif got:      {len(notif_received)} events")
    print(f"  → Publisher published 2 events; each subscriber got 2 independently")
    # ── PATTERN 2: COMPETING CONSUMERS ────────────────────────────────────
    print("\n\n" + "=" * 65)
    print("   PATTERN 2: COMPETING CONSUMERS — Worker Pool")
    print("=" * 65)
    pool_results = []
    pool_lock = threading.Lock()
    def image_resize_job(msg: Message) -> None:
        time.sleep(0.01)   # Simulate processing time
        with pool_lock:
            pool_results.append({
                "job": msg.value,
                "worker": threading.current_thread().name,
            })
    pool = CompetingConsumersQueue("image-resize-queue")
    pool.start_workers(3, image_resize_job)
    for i in range(9):
        pool.enqueue(f"resize_image_{i}.jpg")
    time.sleep(0.3)  # Let workers process
    pool.stop()
    stats = pool.stats()
    print(f"\n  Enqueued: 9 jobs, Processed: {stats.get('total_processed', 0)}")
    workers_used = set(r["worker"] for r in pool_results)
    print(f"  Workers used: {workers_used}")
    print(f"  → Workload distributed across workers automatically")
    # ── PATTERN 3: PRIORITY QUEUE ─────────────────────────────────────────
    print("\n\n" + "=" * 65)
    print("   PATTERN 3: PRIORITY QUEUE — VIP first")
    print("=" * 65)
    pq = PriorityQueue("request-queue")
    # Enqueue in REVERSE priority order — but should dequeue in priority order
    pq.enqueue({"task": "background-report",  "tier": "free"},    priority=3)
    pq.enqueue({"task": "standard-search",    "tier": "free"},    priority=2)
    pq.enqueue({"task": "premium-checkout",   "tier": "premium"}, priority=1)
    pq.enqueue({"task": "payment-failure",    "tier": "system"},  priority=0)
    pq.enqueue({"task": "premium-profile",    "tier": "premium"}, priority=1)
    pq.enqueue({"task": "free-user-browse",   "tier": "free"},    priority=2)
    print(f"\n  Dequeuing {pq.depth()} messages in PRIORITY ORDER:")
    for msg in pq.drain_in_order():
        p = msg.headers["priority"]
        label = ["CRITICAL", "HIGH", "NORMAL", "LOW"][p]
        print(f"    [{label}] {msg.value['task']} ({msg.value['tier']})")
    # ── PATTERN 4: DELAY QUEUE ────────────────────────────────────────────
    print("\n\n" + "=" * 65)
    print("   PATTERN 4: DELAY QUEUE — Scheduled delivery")
    print("=" * 65)
    dq = DelayQueue("retry-queue")
    dq.enqueue({"job": "retry-payment",     "attempt": 1}, delay_seconds=0.1)
    dq.enqueue({"job": "welcome-email",     "user": "new"}, delay_seconds=0.2)
    dq.enqueue({"job": "send-report",       "type": "weekly"}, delay_seconds=0.5)
    dq.enqueue({"job": "far-future-task",   "type": "hourly"}, delay_seconds=10.0)
    print(f"\n  Pending: {dq.pending_count()} messages")
    print("  Polling at t=0s:", [m.value["job"] for m in dq.poll()])
    time.sleep(0.25)
    print("  Polling at t=0.25s:", [m.value["job"] for m in dq.poll()])
    time.sleep(0.3)
    print("  Polling at t=0.55s:", [m.value["job"] for m in dq.poll()])
    print(f"  Still pending: {dq.pending_count()} (the 10s far-future task)")
    # ── PATTERN 5: REQUEST-REPLY ──────────────────────────────────────────
    print("\n\n" + "=" * 65)
    print("   PATTERN 5: REQUEST-REPLY (RPC over Message Queue)")
    print("=" * 65)
    rpc = RPCServer("inventory.requests")
    def inventory_handler(payload: dict) -> dict:
        """Simulate inventory service checking stock."""
        product_id = payload.get("product_id")
        stock = {"p1": 100, "p2": 0, "p3": 50}.get(product_id, -1)
        return {"product_id": product_id, "in_stock": stock > 0, "quantity": stock}
    # Client sends request
    print("\n  Client sending RPC request for product p1:")
    request_thread = threading.Thread(
        target=lambda: rpc.serve(inventory_handler),
        daemon=True
    )
    # Start server in background, then client call
    def run_rpc():
        time.sleep(0.05)  # Brief delay so client sends first
        rpc.serve(inventory_handler)
    server_thread = threading.Thread(target=run_rpc, daemon=True)
    server_thread.start()
    result = rpc.call({"product_id": "p1"}, timeout_seconds=1.0)
    print(f"\n  RPC result: {result}")
    print("\n\n" + "=" * 65)
    print("  MESSAGING PATTERNS SUMMARY")
    print("=" * 65)
    rows = [
        ("Pattern",           "Message Flow",                   "Use Case"),
        ("─"*16,              "─"*30,                           "─"*20),
        ("Pub/Sub",           "1 publisher → N subscribers",    "Event fan-out"),
        ("Competing Consumers","N workers share 1 queue",       "Parallel processing"),
        ("Priority Queue",    "High priority dequeued first",   "SLA tiers, VIP users"),
        ("Delay Queue",       "Message visible after delay",    "Retry backoff, schedules"),
        ("Request-Reply",     "RPC via correlation_id",         "Service-to-service calls"),
    ]
    for r in rows:
        print(f"  {r[0]:<18} {r[1]:<32} {r[2]}")
    print("=" * 65)
