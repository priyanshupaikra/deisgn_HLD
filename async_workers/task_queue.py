"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                      ASYNCHRONOUS WORKERS — MODULE 2                         ║
║                    TASK QUEUE WITH RETRY & SCHEDULING                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
A TASK QUEUE is the heart of an async worker system.
It coordinates between:
  - PRODUCERS (web servers, API endpoints) that enqueue tasks
  - CONSUMERS (workers) that dequeue and execute them

CELERY ARCHITECTURE (production standard):
────────────────────────────────────────────
  [Web Server] ──task.delay()──▶ [Broker: Redis/RabbitMQ]
                                          ↓
                               [Celery Worker Process]
                                          ↓
                               [Result Backend: Redis/DB]
                                          ↓
                               [Web Server polls for result]

TASK STATES (Celery-inspired):
────────────────────────────────
  PENDING  → In queue, not yet picked up
  STARTED  → Worker has begun execution
  SUCCESS  → Completed without error
  FAILURE  → Raised an exception after all retries
  RETRY    → Being retried (counting down to next attempt)
  REVOKED  → Cancelled (app explicitly cancelled it)

RETRY STRATEGIES:
──────────────────
  FIXED:        Retry after exactly N seconds (simple, predictable)
  EXPONENTIAL:  Retry after 2^n seconds (2s, 4s, 8s, 16s, 32s)
                Prevents thundering herd when many tasks fail simultaneously
  EXPONENTIAL + JITTER: Retry after (2^n + random(0,1)) seconds
                Prevents synchronized retries from multiple workers

  Celery:
    @app.task(bind=True, max_retries=3, default_retry_delay=60)
    def my_task(self):
        try:
            do_work()
        except Exception as exc:
            raise self.retry(exc=exc, countdown=2**self.request.retries)

TASK RESULT STORAGE:
─────────────────────
  After a task completes, the result is stored in the RESULT BACKEND.
  Web server can poll: result = AsyncResult(task_id).get(timeout=10)

  Result backends:
    Redis      → Fast, in-memory (default in most setups)
    Database   → Persistent but slower
    Memcached  → Fast but no persistence
    RPC        → Reply directly to caller (for request-reply pattern)

TASK PRIORITIES:
─────────────────
  Priority queues ensure critical tasks are processed first.
  Implementation:
    - RabbitMQ: x-max-priority queue argument
    - Redis: Multiple queues + worker checks high-priority first
    - Celery: queue routing + worker queue flags

"""

import time
import uuid
import heapq
import threading
import random
import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
from worker_types import Task, TaskStatus


# ─────────────────────────────────────────────────────────────────────────────
# Retry Strategies
# ─────────────────────────────────────────────────────────────────────────────

class RetryStrategy:
    """Base class for retry delay calculations."""

    def delay(self, attempt: int) -> float:
        raise NotImplementedError


class FixedRetry(RetryStrategy):
    """
    Fixed delay between retries.
    Simple and predictable. Use when failure is likely transient and short.

    Delay: base_delay seconds every time.
    """
    def __init__(self, base_delay: float = 5.0):
        self.base_delay = base_delay

    def delay(self, attempt: int) -> float:
        return self.base_delay

    def __repr__(self):
        return f"FixedRetry(delay={self.base_delay}s)"


class ExponentialBackoffRetry(RetryStrategy):
    """
    Exponential backoff with optional jitter.

    WHY EXPONENTIAL:
      Fixed retry can cause "thundering herd" — many workers all retrying
      at exactly the same time, hammering a service that's recovering.
      Exponential backoff spreads retries out over time.

    WHY JITTER:
      Without jitter: workers synchronized → all retry at t=2s, t=4s, t=8s
      With jitter: workers desynchronized → retries spread across time window

    FORMULA (with jitter):
      delay = min(cap, base * 2^attempt) + random(0, base)

    AWS SDK uses: "Full Jitter" → delay = random(0, min(cap, base * 2^attempt))
    """

    def __init__(self, base_delay: float = 1.0, max_delay: float = 60.0,
                 jitter: bool = True):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter

    def delay(self, attempt: int) -> float:
        # Exponential: 1s, 2s, 4s, 8s, 16s... capped at max_delay
        exp_delay = min(self.max_delay, self.base_delay * (2 ** attempt))
        if self.jitter:
            # Add random jitter to prevent synchronized retries
            jitter_amount = random.uniform(0, self.base_delay)
            return exp_delay + jitter_amount
        return exp_delay

    def __repr__(self):
        return (f"ExponentialBackoff(base={self.base_delay}s, "
                f"max={self.max_delay}s, jitter={self.jitter})")


# ─────────────────────────────────────────────────────────────────────────────
# Task Result Backend
# ─────────────────────────────────────────────────────────────────────────────

class TaskResultBackend:
    """
    Task Result Backend — stores task results and status.

    In production: Redis (fast) or PostgreSQL (persistent).

    Consumers query: result = backend.get(task_id, timeout=10)
    Workers store:   backend.set(task_id, result, status)

    TTL: Results auto-expire after some time (default 24h in Celery)
         to prevent unbounded memory growth.
    """

    def __init__(self, ttl_seconds: float = 86400.0):
        self._store: dict[str, dict] = {}
        self._ttl = ttl_seconds
        self._lock = threading.Lock()

    def store(self, task_id: str, status: TaskStatus,
              result: Any = None, error: str = "") -> None:
        """Store task result."""
        with self._lock:
            self._store[task_id] = {
                "task_id":    task_id,
                "status":     status.value,
                "result":     result,
                "error":      error,
                "stored_at":  time.time(),
                "expires_at": time.time() + self._ttl,
            }

    def get(self, task_id: str,
            timeout: float = 0.0) -> Optional[dict]:
        """
        Get result. Optionally polls for a result (blocking with timeout).
        Returns None if not found or expired.
        """
        deadline = time.time() + timeout
        while True:
            with self._lock:
                record = self._store.get(task_id)
                if record:
                    if time.time() > record["expires_at"]:
                        del self._store[task_id]
                        return None
                    # Only return if terminal state
                    if record["status"] in (TaskStatus.SUCCESS.value,
                                            TaskStatus.FAILED.value,
                                            TaskStatus.REVOKED.value):
                        return record

            if time.time() >= deadline:
                return None  # Timeout
            time.sleep(0.01)  # Poll interval

    def all_results(self) -> list[dict]:
        with self._lock:
            return list(self._store.values())


# ─────────────────────────────────────────────────────────────────────────────
# Task Queue
# ─────────────────────────────────────────────────────────────────────────────

class TaskQueue:
    """
    Priority Task Queue backed by a min-heap.

    Messages are stored as (priority, created_at, task) tuples.
    Lower priority number = processed first.
    created_at is used as tie-breaker (FIFO within same priority).

    THREAD SAFETY:
      A Lock + Condition variable ensures:
        - Only one thread dequeues at a time
        - Worker threads sleep when queue is empty
        - Producer threads wake sleeping workers via notify()
    """

    def __init__(self, name: str):
        self.name = name
        self._heap: list[tuple] = []
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._revoked: set = set()
        self.total_enqueued = 0
        self.total_dequeued = 0

    def enqueue(self, task: Task) -> None:
        """
        Add task to queue. Workers sleeping on wait() are woken up.
        Priority = (task.priority, task.created_at) for stable ordering.
        """
        with self._not_empty:
            heapq.heappush(self._heap, (task.priority, task.created_at, task))
            self.total_enqueued += 1
            self._not_empty.notify()  # Wake one sleeping worker

    def dequeue(self, timeout: float = 1.0) -> Optional[Task]:
        """
        Pop the highest-priority task.
        Blocks for up to `timeout` seconds if queue is empty.
        Returns None on timeout.
        """
        with self._not_empty:
            deadline = time.time() + timeout
            while not self._heap:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                self._not_empty.wait(timeout=remaining)

            while self._heap:
                priority, created_at, task = heapq.heappop(self._heap)
                self.total_dequeued += 1
                if task.task_id in self._revoked:
                    task.status = TaskStatus.REVOKED
                    continue  # Skip revoked tasks
                return task

        return None

    def revoke(self, task_id: str) -> None:
        """Cancel a pending task (if not yet started)."""
        with self._lock:
            self._revoked.add(task_id)

    def depth(self) -> int:
        with self._lock:
            return len(self._heap)


# ─────────────────────────────────────────────────────────────────────────────
# Worker with Retry
# ─────────────────────────────────────────────────────────────────────────────

class RetryWorker:
    """
    A worker process that fetches tasks from a queue, executes them,
    and handles failures with configurable retry strategies.

    EXECUTION LOOP:
      1. Dequeue next task (block if empty)
      2. Execute task.func(*args, **kwargs)
      3a. Success → store result in backend
      3b. Failure (retry_count < max_retries):
            compute retry delay → re-enqueue with delay
      3c. Failure (retry_count >= max_retries):
            store FAILED in backend (give up)

    TASK TIMEOUT:
      Worker runs task with a thread-level timeout.
      If task exceeds timeout → forcefully mark as FAILED.
      (Real Celery uses SIGTERM + SIGKILL for process-level timeout)

    WORKER CONCURRENCY:
      Multiple RetryWorker threads share a single TaskQueue.
      They compete for tasks (competing consumers pattern).
    """

    def __init__(self, worker_id: str, queue: TaskQueue,
                 result_backend: TaskResultBackend,
                 retry_strategy: RetryStrategy = None):
        self.worker_id = worker_id
        self.queue = queue
        self.backend = result_backend
        self.retry_strategy = retry_strategy or ExponentialBackoffRetry()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.processed = 0
        self.retried = 0
        self.failed_permanently = 0

    def start(self) -> None:
        """Start the worker loop in a background thread."""
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"RetryWorker-{self.worker_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        """Signal worker to stop and wait for clean shutdown."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=timeout)

    def _execute(self, task: Task) -> Any:
        """Execute with optional timeout."""
        if task.timeout:
            result_holder = []
            error_holder = []

            def target():
                try:
                    result_holder.append(task.func(*task.args, **task.kwargs))
                except Exception as e:
                    error_holder.append(e)

            t = threading.Thread(target=target)
            t.start()
            t.join(timeout=task.timeout)
            if t.is_alive():
                raise TimeoutError(f"Task '{task.name}' timed out after {task.timeout}s")
            if error_holder:
                raise error_holder[0]
            return result_holder[0] if result_holder else None
        else:
            return task.func(*task.args, **task.kwargs)

    def _run_loop(self) -> None:
        """Main worker loop: dequeue → execute → handle result."""
        while self._running:
            task = self.queue.dequeue(timeout=0.5)
            if not task:
                continue  # Timeout, check _running again

            if task.status == TaskStatus.REVOKED:
                print(f"  [Worker:{self.worker_id}] REVOKED: '{task.name}'")
                continue

            task.status = TaskStatus.RUNNING
            task.started_at = time.time()

            print(f"  [Worker:{self.worker_id}] START: '{task.name}' "
                  f"(attempt {task.retry_count + 1}/{task.max_retries + 1})")

            try:
                result = self._execute(task)
                task.result = result
                task.status = TaskStatus.SUCCESS
                task.finished_at = time.time()
                self.processed += 1

                self.backend.store(task.task_id, TaskStatus.SUCCESS, result=result)
                print(f"  [Worker:{self.worker_id}] ✓ SUCCESS: '{task.name}' "
                      f"in {task.duration_ms:.0f}ms | result={result}")

            except Exception as e:
                task.error = str(e)
                task.finished_at = time.time()

                if task.retry_count < task.max_retries:
                    # RETRY: compute delay and re-enqueue
                    delay = self.retry_strategy.delay(task.retry_count)
                    task.retry_count += 1
                    task.status = TaskStatus.RETRYING
                    self.retried += 1

                    print(f"  [Worker:{self.worker_id}] ↺ RETRY #{task.retry_count}: "
                          f"'{task.name}' (delay={delay:.1f}s) | error: {e}")

                    # Re-enqueue after delay
                    def delayed_requeue(t=task, d=delay):
                        time.sleep(d)
                        t.status = TaskStatus.PENDING
                        t.started_at = None
                        t.finished_at = None
                        self.queue.enqueue(t)

                    threading.Thread(target=delayed_requeue, daemon=True).start()

                else:
                    # PERMANENTLY FAILED
                    task.status = TaskStatus.FAILED
                    self.failed_permanently += 1
                    self.backend.store(task.task_id, TaskStatus.FAILED, error=str(e))
                    print(f"  [Worker:{self.worker_id}] ✗ FAILED: '{task.name}' "
                          f"after {task.max_retries + 1} attempts | {e}")

    def stats(self) -> dict:
        return {
            "worker_id":         self.worker_id,
            "processed":         self.processed,
            "retried":           self.retried,
            "failed_permanently": self.failed_permanently,
            "retry_strategy":    str(self.retry_strategy),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Periodic Scheduler
# ─────────────────────────────────────────────────────────────────────────────

class PeriodicScheduler:
    """
    Periodic / Cron-style task scheduler.

    Runs tasks on a schedule: every N seconds, or at specific times.
    Similar to:
      - Celery Beat (periodic task scheduler)
      - APScheduler (Python scheduler)
      - Cron jobs on Linux

    SCHEDULE TYPES:
      interval:  Run every N seconds
      cron:      Run at specific times (daily at 9am, weekly on Monday)
      one_shot:  Run once after a delay

    BEAT SCHEDULER (Celery Beat):
      Celery Beat is a separate process that:
        1. Reads the schedule configuration
        2. At the right time: sends a task message to the broker
        3. Workers pick it up and execute

      This ensures periodic tasks survive worker restarts.
    """

    @dataclass
    class ScheduledJob:
        name: str
        func: Callable
        args: tuple = field(default_factory=tuple)
        interval_seconds: float = 60.0
        last_run: float = 0.0
        run_count: int = 0

        def is_due(self) -> bool:
            return time.time() >= self.last_run + self.interval_seconds

    def __init__(self):
        self._jobs: list['PeriodicScheduler.ScheduledJob'] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def add_job(self, name: str, func: Callable,
                interval_seconds: float, args: tuple = ()) -> None:
        """Register a periodic job."""
        job = self.ScheduledJob(
            name=name, func=func, args=args,
            interval_seconds=interval_seconds,
            last_run=time.time() - interval_seconds,  # Run immediately on first tick
        )
        with self._lock:
            self._jobs.append(job)
        print(f"  [Scheduler] Registered '{name}' every {interval_seconds}s")

    def start(self, tick_interval: float = 0.1) -> None:
        """Start the scheduler loop (checks due jobs every tick_interval seconds)."""
        self._running = True

        def loop():
            while self._running:
                with self._lock:
                    for job in self._jobs:
                        if job.is_due():
                            job.last_run = time.time()
                            job.run_count += 1
                            try:
                                result = job.func(*job.args)
                                print(f"  [Scheduler] ✓ '{job.name}' run #{job.run_count} "
                                      f"| result={result}")
                            except Exception as e:
                                print(f"  [Scheduler] ✗ '{job.name}' failed: {e}")
                time.sleep(tick_interval)

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

def flaky_task(name: str, fail_times: int = 2) -> str:
    """Task that fails the first N times, then succeeds."""
    if not hasattr(flaky_task, "_counts"):
        flaky_task._counts = defaultdict(int)
    flaky_task._counts[name] += 1
    if flaky_task._counts[name] <= fail_times:
        raise RuntimeError(f"Simulated failure #{flaky_task._counts[name]} for '{name}'")
    return f"'{name}' succeeded on attempt {flaky_task._counts[name]}"


if __name__ == "__main__":
    print("=" * 65)
    print("   TASK QUEUE WITH RETRY & SCHEDULING — Demo")
    print("=" * 65)

    # ── Task Queue + Worker with Retry ─────────────────────────────────────
    print("\n  ═══ TASK QUEUE WITH EXPONENTIAL BACKOFF RETRY ═══\n")

    queue   = TaskQueue("main-queue")
    backend = TaskResultBackend()
    strategy = ExponentialBackoffRetry(base_delay=0.1, max_delay=1.0, jitter=False)

    # Start 2 workers
    workers = [
        RetryWorker(f"W{i}", queue, backend, strategy)
        for i in range(1, 3)
    ]
    for w in workers:
        w.start()

    # Enqueue tasks with different priorities and failure modes
    tasks = [
        Task(name="send_invoice",    func=flaky_task, args=("send_invoice", 2),
             max_retries=3, priority=1),
        Task(name="resize_image",    func=flaky_task, args=("resize_image", 0),
             max_retries=2, priority=2),
        Task(name="payment_webhook", func=flaky_task, args=("payment_webhook", 1),
             max_retries=3, priority=0),  # CRITICAL
    ]

    task_ids = []
    for t in tasks:
        queue.enqueue(t)
        task_ids.append(t.task_id)
        print(f"  Enqueued: '{t.name}' (priority={t.priority})")

    # Wait for all tasks to complete (poll results)
    print("\n  Waiting for results...\n")
    time.sleep(2.5)

    for tid in task_ids:
        result = backend.get(tid, timeout=0)
        if result:
            print(f"  Result: [{result['status']}] {result.get('result') or result.get('error')}")

    print("\n  Worker stats:")
    for w in workers:
        w.stop()
        print(f"    {w.stats()}")

    # ── Periodic Scheduler ─────────────────────────────────────────────────
    print("\n\n  ═══ PERIODIC SCHEDULER (Celery Beat style) ═══\n")
    scheduler = PeriodicScheduler()

    scheduler.add_job("cleanup_expired_sessions",
                      lambda: f"deleted {random.randint(10, 100)} sessions",
                      interval_seconds=0.3)
    scheduler.add_job("send_daily_digest",
                      lambda: f"digest sent to {random.randint(100,1000)} users",
                      interval_seconds=0.5)
    scheduler.add_job("refresh_recommendation_cache",
                      lambda: "cache refreshed",
                      interval_seconds=0.7)

    scheduler.start()
    time.sleep(1.2)  # Let it run for a bit
    scheduler.stop()

    # ── Retry Delay Comparison ─────────────────────────────────────────────
    print("\n\n  ═══ RETRY STRATEGY COMPARISON ═══\n")
    random.seed(42)
    for name, strat in [
        ("Fixed(5s)",        FixedRetry(5.0)),
        ("ExpBackoff(1s)",   ExponentialBackoffRetry(1.0, 60.0, jitter=False)),
        ("ExpBackoff+Jitter",ExponentialBackoffRetry(1.0, 60.0, jitter=True)),
    ]:
        delays = [round(strat.delay(i), 2) for i in range(5)]
        print(f"  {name:<22}: attempts 1-5 → delays={delays}s")

    print("\n" + "=" * 65)
    print("  TASK QUEUE KEY CONCEPTS:")
    print("  Priority Queue  → Critical tasks first (heapq)")
    print("  Retry Strategy  → Fixed / Exponential Backoff + Jitter")
    print("  Result Backend  → Redis/DB stores task results with TTL")
    print("  Revocation      → Cancel pending tasks before execution")
    print("  Periodic Sched. → Celery Beat: enqueues tasks on a cron schedule")
    print("=" * 65)
