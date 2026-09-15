"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    MESSAGE QUEUE (Kafka/RabbitMQ) — MODULE 3                 ║
║                        RABBITMQ SIMULATION                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
WHAT IS RABBITMQ?
──────────────────
RabbitMQ is a traditional MESSAGE BROKER implementing the AMQP protocol
(Advanced Message Queuing Protocol).
Unlike Kafka, RabbitMQ is a "smart broker, dumb consumer" system:
  - The BROKER does the routing logic (exchanges, bindings)
  - The CONSUMER just reads from its queue
RABBITMQ ARCHITECTURE:
────────────────────────
  Producer ──▶ Exchange ──(binding)──▶ Queue ──▶ Consumer
  Components:
    EXCHANGE  → Receives messages from producers; routes to queues
    QUEUE     → Buffer that stores messages until consumed
    BINDING   → Rule that connects an exchange to a queue
    ROUTING KEY → Message attribute used by exchanges for routing decisions
FOUR EXCHANGE TYPES:
─────────────────────
  1. DIRECT     → Route to queue whose binding key EXACTLY MATCHES routing key
  2. FANOUT     → Broadcast to ALL bound queues (ignore routing key)
  3. TOPIC      → Route using PATTERN matching (*, # wildcards)
  4. HEADERS    → Route based on message HEADER values (not routing key)
EXCHANGE TYPE DETAILS:
───────────────────────
  DIRECT EXCHANGE:
    Binding: queue "email-queue" bound with key "email"
    Message with routing_key="email" → ONLY to email-queue
    Message with routing_key="sms"   → NOT to email-queue
    Use: Task-specific routing (payment, email, sms)
  FANOUT EXCHANGE:
    Binding: queue "log-1", "log-2", "log-3" all bound (no routing key)
    Message sent to fanout → ALL queues receive a copy
    Use: Broadcasting events (new user → send email AND create feed AND update analytics)
  TOPIC EXCHANGE:
    Binding keys support wildcards:
      *  → matches exactly ONE word
      #  → matches ZERO or MORE words
    Example bindings:
      "order.*"        matches "order.created", "order.shipped" but NOT "order.item.added"
      "order.#"        matches "order.created", "order.item.added", "order.item.variant.sold"
      "*.error"        matches "payment.error", "shipping.error" but NOT "order.item.error"
      "#"              matches everything
    Use: Flexible routing for event-driven architectures
  HEADERS EXCHANGE:
    Routing based on message header key-value pairs.
    Supports "x-match: all" (all headers must match) or "x-match: any" (any header matches)
    Use: Complex routing conditions that can't be expressed as routing keys
MESSAGE ACKNOWLEDGMENT:
────────────────────────
  Consumer MUST ACK each message:
    channel.basic_ack(delivery_tag)  → Message removed from queue
    channel.basic_nack(delivery_tag, requeue=True)  → Requeued at front
    channel.basic_nack(delivery_tag, requeue=False) → Moved to DLQ
  If consumer crashes without ACK → message is REQUEUED automatically
  (RabbitMQ tracks unacked messages per connection)
DURABILITY:
────────────
  Durable exchange   → Survives broker restart
  Durable queue      → Survives broker restart
  Persistent message → Saved to disk (survives broker restart)
  ALL THREE must be true for guaranteed message survival.
"""
import time
import uuid
import threading
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from core_concepts import Message, DeadLetterQueue, AckStatus
# ─────────────────────────────────────────────────────────────────────────────
# RabbitMQ Queue
# ─────────────────────────────────────────────────────────────────────────────
class RabbitQueue:
    """
    A RabbitMQ Queue — stores messages until a consumer processes them.
    PROPERTIES:
      durable:    Survives broker restart (stored to disk)
      exclusive:  Only one consumer allowed
      auto_delete: Deleted when last consumer disconnects
      max_retries: Max delivery attempts before moving to DLQ
    """
    def __init__(self, name: str, durable: bool = True,
                 exclusive: bool = False, auto_delete: bool = False,
                 max_retries: int = 3):
        self.name = name
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.max_retries = max_retries
        self._messages: deque[Message] = deque()
        self._unacked: dict[str, Message] = {}   # delivery_tag → Message
        self._lock = threading.Lock()
        self.enqueued_count = 0
        self.acked_count = 0
        self.nacked_count = 0
        self.dlq = DeadLetterQueue(f"{name}.dlq")
    def put(self, message: Message) -> None:
        """Enqueue a message (called by exchange when routing)."""
        with self._lock:
            # Give message a delivery tag for this queue
            message.headers["delivery_tag"] = str(uuid.uuid4())
            self._messages.append(message)
            self.enqueued_count += 1
    def get(self) -> Optional[Message]:
        """
        Pop the next message for delivery to a consumer.
        Moves message to unacked dict — must be ACK'd or NACK'd.
        """
        with self._lock:
            if not self._messages:
                return None
            msg = self._messages.popleft()
            delivery_tag = msg.headers.get("delivery_tag")
            self._unacked[delivery_tag] = msg
            return msg
    def ack(self, delivery_tag: str) -> None:
        """Consumer ACK: message processed successfully → remove it."""
        with self._lock:
            msg = self._unacked.pop(delivery_tag, None)
            if msg:
                self.acked_count += 1
    def nack(self, delivery_tag: str, requeue: bool = True) -> None:
        """
        Consumer NACK:
          requeue=True  → Put back at front of queue (retry)
          requeue=False → Send to Dead Letter Queue (give up)
        """
        with self._lock:
            msg = self._unacked.pop(delivery_tag, None)
            if msg:
                self.nacked_count += 1
                msg.retry_count += 1
                if requeue and msg.retry_count < self.max_retries:
                    # Requeue at front for retry
                    self._messages.appendleft(msg)
                    print(f"  [Queue:{self.name}] NACK requeued "
                          f"(retry {msg.retry_count}/{self.max_retries}): "
                          f"{msg.message_id[:8]}...")
                else:
                    # Move to DLQ
                    reason = ("max retries exceeded" if msg.retry_count >= self.max_retries
                              else "requeue=False")
                    self.dlq.enqueue(msg, reason)
    def requeue_unacked(self) -> int:
        """
        Called when a consumer disconnects without ACK.
        All unacked messages are requeued (RabbitMQ does this automatically).
        """
        with self._lock:
            count = len(self._unacked)
            for msg in self._unacked.values():
                self._messages.appendleft(msg)
            self._unacked.clear()
            return count
    def depth(self) -> int:
        """Number of messages waiting to be consumed."""
        with self._lock:
            return len(self._messages)
    def stats(self) -> dict:
        return {
            "queue": self.name,
            "depth": self.depth(),
            "unacked": len(self._unacked),
            "enqueued": self.enqueued_count,
            "acked": self.acked_count,
            "nacked": self.nacked_count,
            "dlq_count": self.dlq.count(),
        }
    def __repr__(self):
        return f"RabbitQueue({self.name}, depth={self.depth()})"
# ─────────────────────────────────────────────────────────────────────────────
# Exchange Types
# ─────────────────────────────────────────────────────────────────────────────
class DirectExchange:
    """
    DIRECT EXCHANGE — exact routing key match.
    Binding: queue bound with a specific routing key.
    Message: routed ONLY to queues where binding_key == message.routing_key.
    Use: Specific task routing (log level routing: info/warning/error)
    """
    def __init__(self, name: str):
        self.name = name
        self.exchange_type = "direct"
        # {routing_key → [RabbitQueue]}
        self._bindings: dict[str, list[RabbitQueue]] = defaultdict(list)
    def bind(self, queue: RabbitQueue, routing_key: str) -> None:
        """Bind a queue to this exchange with a routing key."""
        self._bindings[routing_key].append(queue)
        print(f"  [DirectExchange:{self.name}] Bound '{queue.name}' ← key='{routing_key}'")
    def publish(self, message: Message, routing_key: str) -> int:
        """Route message to queues with exact routing_key match."""
        queues = self._bindings.get(routing_key, [])
        for q in queues:
            # Each queue gets its OWN COPY of the message
            import copy
            q.put(copy.deepcopy(message))
        return len(queues)
class FanoutExchange:
    """
    FANOUT EXCHANGE — broadcast to ALL bound queues.
    Routing key is IGNORED. Every bound queue gets a copy.
    Use: Broadcasting (new user → email + SMS + analytics + audit all at once)
    """
    def __init__(self, name: str):
        self.name = name
        self.exchange_type = "fanout"
        self._queues: list[RabbitQueue] = []
    def bind(self, queue: RabbitQueue, routing_key: str = "") -> None:
        """Bind a queue. Routing key is ignored for fanout."""
        self._queues.append(queue)
        print(f"  [FanoutExchange:{self.name}] Bound '{queue.name}' (key ignored)")
    def publish(self, message: Message, routing_key: str = "") -> int:
        """Broadcast to ALL bound queues."""
        import copy
        for q in self._queues:
            q.put(copy.deepcopy(message))
        return len(self._queues)
class TopicExchange:
    """
    TOPIC EXCHANGE — pattern matching with wildcards.
    Binding keys use:
      *  → matches exactly one word segment
      #  → matches zero or more word segments
    Examples:
      binding "order.*"   matches: "order.created", "order.shipped"
                          no match: "order.item.added"
      binding "order.#"   matches: "order.created", "order.item.added", "order.a.b.c"
      binding "*.error"   matches: "payment.error", "order.error"
                          no match: "db.query.error"
      binding "#"         matches: everything
    Use: Flexible event-driven routing (order.* → analytics, #.error → alert)
    """
    def __init__(self, name: str):
        self.name = name
        self.exchange_type = "topic"
        # [(pattern, RabbitQueue)]
        self._bindings: list[tuple[str, RabbitQueue]] = []
    def bind(self, queue: RabbitQueue, pattern: str) -> None:
        """Bind a queue to a topic pattern."""
        self._bindings.append((pattern, queue))
        print(f"  [TopicExchange:{self.name}] Bound '{queue.name}' ← pattern='{pattern}'")
    def _matches(self, pattern: str, routing_key: str) -> bool:
        """
        Check if a routing key matches a topic pattern.
        Converts AMQP wildcards to regex:
          * → [^.]+ (one word segment: no dots)
          # → .* (anything including empty and multiple segments)
        """
        # Escape dots in the pattern first, then replace wildcards
        regex = re.escape(pattern)
        regex = regex.replace(r'\#', '.*')           # # → .*
        regex = regex.replace(r'\*', r'[^.]+')       # * → [^.]+
        regex = f'^{regex}$'
        return bool(re.match(regex, routing_key))
    def publish(self, message: Message, routing_key: str) -> int:
        """Route message to queues whose pattern matches the routing key."""
        import copy
        delivered = 0
        seen_queues = set()  # Prevent duplicate delivery to same queue
        for pattern, queue in self._bindings:
            if self._matches(pattern, routing_key) and queue.name not in seen_queues:
                queue.put(copy.deepcopy(message))
                seen_queues.add(queue.name)
                delivered += 1
        return delivered
class HeadersExchange:
    """
    HEADERS EXCHANGE — route based on message header key-value pairs.
    Routing key is ignored. Instead, message headers are matched against
    binding arguments.
    x-match: "all"  → ALL header key-value pairs must match
    x-match: "any"  → ANY one header key-value pair must match
    Use: Complex conditions that can't be expressed as a simple string.
    Example: route messages only if content_type=pdf AND size=large.
    """
    def __init__(self, name: str):
        self.name = name
        self.exchange_type = "headers"
        # [(match_mode, {headers}, RabbitQueue)]
        self._bindings: list[tuple[str, dict, RabbitQueue]] = []
    def bind(self, queue: RabbitQueue, headers: dict,
             x_match: str = "all") -> None:
        """
        Bind with header-matching criteria.
        :param headers:  {key: value} pairs to match
        :param x_match:  "all" or "any"
        """
        self._bindings.append((x_match, headers, queue))
        print(f"  [HeadersExchange:{self.name}] Bound '{queue.name}' "
              f"← headers={headers}, x-match={x_match}")
    def publish(self, message: Message, routing_key: str = "") -> int:
        """Route based on message headers."""
        import copy
        delivered = 0
        msg_headers = message.headers
        for x_match, bind_headers, queue in self._bindings:
            matches = []
            for k, v in bind_headers.items():
                matches.append(str(msg_headers.get(k)) == str(v))
            route = all(matches) if x_match == "all" else any(matches)
            if route:
                queue.put(copy.deepcopy(message))
                delivered += 1
        return delivered
# ─────────────────────────────────────────────────────────────────────────────
# RabbitMQ Broker
# ─────────────────────────────────────────────────────────────────────────────
class RabbitMQBroker:
    """
    Simulated RabbitMQ Broker (AMQP server).
    In production: RabbitMQ is a single node or cluster.
    Supports virtual hosts (vhosts) for multi-tenancy.
    Uses Mnesia (Erlang distributed DB) for metadata storage.
    """
    def __init__(self, name: str = "rabbitmq"):
        self.name = name
        self.queues: dict[str, RabbitQueue] = {}
        self.exchanges: dict[str, Any] = {}
        self._lock = threading.Lock()
    def declare_queue(self, name: str, durable: bool = True,
                      max_retries: int = 3) -> RabbitQueue:
        with self._lock:
            if name not in self.queues:
                self.queues[name] = RabbitQueue(name, durable, max_retries=max_retries)
                print(f"  [RabbitMQ] Queue declared: '{name}' (durable={durable})")
            return self.queues[name]
    def declare_exchange(self, name: str, exchange_type: str) -> Any:
        with self._lock:
            if name not in self.exchanges:
                types = {
                    "direct":  DirectExchange,
                    "fanout":  FanoutExchange,
                    "topic":   TopicExchange,
                    "headers": HeadersExchange,
                }
                cls = types.get(exchange_type.lower())
                if not cls:
                    raise ValueError(f"Unknown exchange type: {exchange_type}")
                self.exchanges[name] = cls(name)
                print(f"  [RabbitMQ] Exchange declared: '{name}' type={exchange_type}")
            return self.exchanges[name]
    def publish(self, exchange_name: str, routing_key: str,
                value: Any, headers: dict = None) -> int:
        """Publish a message through an exchange."""
        exchange = self.exchanges.get(exchange_name)
        if not exchange:
            raise ValueError(f"Exchange '{exchange_name}' not found")
        msg = Message(
            message_id=str(uuid.uuid4()),
            topic=exchange_name,
            key=routing_key,
            value=value,
            headers=headers or {},
        )
        delivered_to = exchange.publish(msg, routing_key)
        if delivered_to == 0:
            print(f"  [RabbitMQ] ⚠ Message unroutable: "
                  f"exchange={exchange_name}, key={routing_key!r}")
        return delivered_to
    def consume(self, queue_name: str, handler: Callable[[Message], bool],
                num_messages: int = None) -> None:
        """
        Consume messages from a queue.
        handler: returns True → ACK, False → NACK
        """
        queue = self.queues.get(queue_name)
        if not queue:
            raise ValueError(f"Queue '{queue_name}' not found")
        count = 0
        while True:
            msg = queue.get()
            if not msg:
                break
            if num_messages and count >= num_messages:
                # Put it back (simulates prefetch limit)
                queue.nack(msg.headers["delivery_tag"], requeue=True)
                break
            delivery_tag = msg.headers["delivery_tag"]
            try:
                success = handler(msg)
                if success:
                    queue.ack(delivery_tag)
                    print(f"  [Consumer→{queue_name}] ACK: {msg.value}")
                else:
                    queue.nack(delivery_tag, requeue=True)
            except Exception as e:
                queue.nack(delivery_tag, requeue=False)  # Send to DLQ
            count += 1
    def queue_stats(self) -> dict:
        return {name: q.stats() for name, q in self.queues.items()}
# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    broker = RabbitMQBroker()
    # ── DIRECT EXCHANGE ────────────────────────────────────────────────────
    print("=" * 65)
    print("   DIRECT EXCHANGE — Exact Routing Key Match")
    print("=" * 65)
    broker.declare_exchange("tasks", "direct")
    q_email = broker.declare_queue("email-tasks")
    q_sms   = broker.declare_queue("sms-tasks")
    q_push  = broker.declare_queue("push-tasks")
    broker.exchanges["tasks"].bind(q_email, "email")
    broker.exchanges["tasks"].bind(q_sms,   "sms")
    broker.exchanges["tasks"].bind(q_push,  "push")
    broker.publish("tasks", "email", {"to": "alice@ex.com", "subject": "Welcome"})
    broker.publish("tasks", "sms",   {"to": "+1555123", "body": "Your OTP is 4521"})
    broker.publish("tasks", "push",  {"device": "iphone_1", "title": "New message"})
    broker.publish("tasks", "fax",   {"number": "555-0000"})  # Unroutable!
    print(f"\n  Queue depths: email={q_email.depth()}, sms={q_sms.depth()}, push={q_push.depth()}")
    broker.consume("email-tasks", lambda m: True)
    # ── FANOUT EXCHANGE ────────────────────────────────────────────────────
    print("\n\n" + "=" * 65)
    print("   FANOUT EXCHANGE — Broadcast to ALL Queues")
    print("=" * 65)
    broker.declare_exchange("user.events", "fanout")
    q_analytics = broker.declare_queue("analytics-queue")
    q_audit     = broker.declare_queue("audit-queue")
    q_feed      = broker.declare_queue("feed-queue")
    for q in [q_analytics, q_audit, q_feed]:
        broker.exchanges["user.events"].bind(q)
    broker.publish("user.events", "", {"event": "user.registered", "user_id": "usr_42"})
    print(f"\n  After 1 publish: analytics={q_analytics.depth()}, "
          f"audit={q_audit.depth()}, feed={q_feed.depth()} (all got it)")
    # ── TOPIC EXCHANGE ─────────────────────────────────────────────────────
    print("\n\n" + "=" * 65)
    print("   TOPIC EXCHANGE — Wildcard Pattern Routing")
    print("=" * 65)
    broker.declare_exchange("app.logs", "topic")
    q_all_errors   = broker.declare_queue("all-errors-queue")
    q_order_events = broker.declare_queue("order-events-queue")
    q_all_logs     = broker.declare_queue("all-logs-queue")
    broker.exchanges["app.logs"].bind(q_all_errors,   "*.error")
    broker.exchanges["app.logs"].bind(q_order_events, "order.*")
    broker.exchanges["app.logs"].bind(q_all_logs,     "#")
    routing_keys = [
        "order.created",    # → order-events, all-logs
        "order.error",      # → all-errors, order-events, all-logs
        "payment.error",    # → all-errors, all-logs
        "payment.success",  # → all-logs only
        "system.health",    # → all-logs only
    ]
    for rk in routing_keys:
        broker.publish("app.logs", rk, {"routing_key": rk, "msg": f"Event: {rk}"})
    print(f"\n  Queue depths after 5 publishes:")
    print(f"    all-errors-queue:    {q_all_errors.depth()} (order.error + payment.error)")
    print(f"    order-events-queue:  {q_order_events.depth()} (order.created + order.error)")
    print(f"    all-logs-queue:      {q_all_logs.depth()} (all 5)")
    # ── HEADERS EXCHANGE ───────────────────────────────────────────────────
    print("\n\n" + "=" * 65)
    print("   HEADERS EXCHANGE — Route by Message Headers")
    print("=" * 65)
    broker.declare_exchange("reports", "headers")
    q_pdf_large  = broker.declare_queue("pdf-large-queue")
    q_csv_any    = broker.declare_queue("csv-queue")
    q_any_urgent = broker.declare_queue("urgent-queue")
    broker.exchanges["reports"].bind(q_pdf_large,  {"format": "pdf", "size": "large"}, x_match="all")
    broker.exchanges["reports"].bind(q_csv_any,    {"format": "csv"},                  x_match="any")
    broker.exchanges["reports"].bind(q_any_urgent, {"priority": "urgent"},             x_match="any")
    broker.publish("reports", "", {"report": "Q1 Sales"}, headers={"format": "pdf", "size": "large"})
    broker.publish("reports", "", {"report": "User CSV"},  headers={"format": "csv"})
    broker.publish("reports", "", {"report": "URGENT!"},   headers={"priority": "urgent"})
    broker.publish("reports", "", {"report": "PDF Small"}, headers={"format": "pdf", "size": "small"})
    print(f"\n  Queue depths:")
    print(f"    pdf-large-queue: {q_pdf_large.depth()} (only large PDFs)")
    print(f"    csv-queue:       {q_csv_any.depth()} (all CSVs)")
    print(f"    urgent-queue:    {q_any_urgent.depth()} (all urgent)")
    print("\n" + "=" * 65)
    print("  RABBITMQ EXCHANGE COMPARISON")
    print("=" * 65)
    rows = [
        ("Exchange",  "Routing Logic",        "Use Case"),
        ("─"*9,       "─"*22,                 "─"*26),
        ("Direct",    "Exact key match",       "Task routing, log levels"),
        ("Fanout",    "Broadcast to all",      "Event notifications"),
        ("Topic",     "Wildcard patterns",     "Flexible event routing"),
        ("Headers",   "Header key-value",      "Complex conditions"),
    ]
    for r in rows:
        print(f"  {r[0]:<10} {r[1]:<24} {r[2]}")
    print("=" * 65)
