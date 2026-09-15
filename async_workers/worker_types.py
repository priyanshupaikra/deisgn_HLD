"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                      ASYNCHRONOUS WORKERS — MODULE 1                         ║
║                         WORKER TYPES & EXECUTION MODELS                      ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT ARE ASYNCHRONOUS WORKERS?
───────────────────────────────
An asynchronous worker is a process or thread that performs tasks
INDEPENDENTLY of the main application flow.

The main application ENQUEUES work and returns immediately.
Workers pick up and execute that work in the background.

SYNCHRONOUS vs ASYNCHRONOUS:
──────────────────────────────
  SYNCHRONOUS (tight coupling, blocks user):
    HTTP Request → App → Sends Email → waits 2s → HTTP Response (slow!)

  ASYNCHRONOUS (decoupled, fast response):
    HTTP Request → App → Enqueues "send_email" job → HTTP 202 Accepted (fast!)
                                    ↓
                         [Worker] picks up job → Sends Email → done

WHY ASYNC WORKERS?
───────────────────
  1. USER EXPERIENCE: Don't make users wait for slow tasks
     (video encoding, PDF generation, emails, bulk imports)

  2. RESILIENCE: If email service is down, the job stays in queue
     and retries later. Without workers → request fails.

  3. SCALABILITY: Scale workers independently from the web server
     (add 10 more workers for heavy job processing)

  4. RATE CONTROL: Workers process at controlled rate
     (don't overwhelm downstream APIs)

  5. FAULT ISOLATION: Worker crash doesn't affect web server

WORKER EXECUTION MODELS:
─────────────────────────
  Model             | Concurrency Unit | Best For
  ──────────────────────────────────────────────────────────────────
  Thread Pool       | OS threads       | IO-bound tasks (network, disk)
  Process Pool      | OS processes     | CPU-bound tasks (image, video, ML)
  Async (asyncio)   | Coroutines       | High-concurrency IO (10k+ tasks)
  Celery            | Distributed      | Production task queues (any workload)

PYTHON GIL NOTE:
─────────────────
  Python has the Global Interpreter Lock (GIL):
    - Thread Pool: GIL limits CPU parallelism → use for IO-bound work only
    - Process Pool: No GIL (separate process) → use for CPU-bound work
    - asyncio: Single thread, cooperative multitasking → for high-concurrency IO

REAL-WORLD EXAMPLES:
─────────────────────
  Thread Pool   → Django Channels, Gunicorn workers, database connection pools
  Process Pool  → Celery with prefork (default), video transcoding
  asyncio       → FastAPI, aiohttp servers, Celery with gevent/eventlet
  Celery        → Millions of tasks/day at Instagram, Pinterest, Coursera

"""

import time
import uuid
import threading
import concurrent.futures
import asyncio
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict


# ─────────────────────────────────────────────────────────────────────────────
# Task model
# ─────────────────────────────────────────────────────────────────────────────

class TaskStatus(Enum):
    PENDING   = "PENDING"    # Waiting in queue
    RUNNING   = "RUNNING"    # Currently being executed
    SUCCESS   = "SUCCESS"    # Completed successfully
    FAILED    = "FAILED"     # Failed after all retries
    RETRYING  = "RETRYING"   # Scheduled for retry
    REVOKED   = "REVOKED"    # Cancelled by application


@dataclass
class Task:
    """
    A unit of background work.

    FIELDS:
      task_id:     Unique identifier (used for status lookup and deduplication)
      name:        Human-readable task name (e.g., "send_welcome_email")
      func:        The callable to execute
      args/kwargs: Arguments to pass to func
      priority:    Lower = higher priority (0=critical, 3=low)
      max_retries: Maximum retry attempts on failure
      retry_delay: Seconds to wait between retries (can be exponential)
      timeout:     Max seconds to run before forceful termination
      eta:         Earliest time to execute (None = execute immediately)
    """
    task_id:     str = field(default_factory=lambda: str(uuid.uuid4()))
    name:        str = ""
    func:        Optional[Callable] = None
    args:        tuple = field(default_factory=tuple)
    kwargs:      dict = field(default_factory=dict)
    priority:    int = 2          # 0=critical, 1=high, 2=normal, 3=low
    max_retries: int = 3
    retry_count: int = 0
    retry_delay: float = 1.0      # seconds between retries
    timeout:     Optional[float] = None
    eta:         Optional[float] = None  # Earliest time to execute
    status:      TaskStatus = TaskStatus.PENDING
    created_at:  float = field(default_factory=time.time)
    started_at:  Optional[float] = None
    finished_at: Optional[float] = None
    result:      Any = None
    error:       str = ""

    @property
    def duration_ms(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at) * 1000
        return None

    def __repr__(self):
        return (f"Task({self.name}, status={self.status.value}, "
                f"retry={self.retry_count}/{self.max_retries})")


# ─────────────────────────────────────────────────────────────────────────────
# WORKER TYPE 1: THREAD POOL WORKER
# ─────────────────────────────────────────────────────────────────────────────

class ThreadPoolWorker:
    """
    Thread Pool Worker — uses Python's ThreadPoolExecutor.

    BEST FOR: IO-bound tasks
      - Network calls (HTTP requests, database queries)
      - File system operations
      - Waiting for external services

    HOW IT WORKS:
      A pool of N threads is pre-created (avoid thread creation overhead).
      Tasks are submitted to the pool → executor picks an idle thread → runs task.
      Thread returns to pool after task completion (reused for next task).

    THREAD POOL SIZE:
      Rule of thumb: num_threads = num_CPUs * (1 + wait_time / compute_time)
      For mostly IO (90% wait): num_threads = 4 CPUs * (1 + 9) = 40 threads
      For balanced (50% wait):  num_threads = 4 CPUs * (1 + 1) = 8 threads

    LIMITATIONS (Python GIL):
      Multiple threads share memory but only ONE runs Python bytecode at a time.
      For CPU-bound tasks → use ProcessPoolWorker instead.
    """

    def __init__(self, name: str, max_workers: int = 4):
        self.name = name
        self.max_workers = max_workers
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"worker-{name}",
        )
        self._futures: dict[str, concurrent.futures.Future] = {}
        self._task_registry: dict[str, Task] = {}
        self._lock = threading.Lock()
        self.completed = 0
        self.failed = 0

    def submit(self, task: Task) -> str:
        """
        Submit a task for execution.
        Returns immediately — task runs in background thread.
        """
        task.status = TaskStatus.RUNNING
        task.started_at = time.time()

        def run_task():
            try:
                result = task.func(*task.args, **task.kwargs)
                task.result = result
                task.status = TaskStatus.SUCCESS
                task.finished_at = time.time()
                with self._lock:
                    self.completed += 1
                print(f"  [ThreadPool:{self.name}] ✓ Task '{task.name}' "
                      f"done in {task.duration_ms:.0f}ms")
                return result
            except Exception as e:
                task.error = str(e)
                task.status = TaskStatus.FAILED
                task.finished_at = time.time()
                with self._lock:
                    self.failed += 1
                print(f"  [ThreadPool:{self.name}] ✗ Task '{task.name}' failed: {e}")
                raise

        future = self._executor.submit(run_task)
        with self._lock:
            self._futures[task.task_id] = future
            self._task_registry[task.task_id] = task
        return task.task_id

    def wait_all(self, timeout: float = None) -> None:
        """Block until all submitted tasks complete."""
        futures = list(self._futures.values())
        concurrent.futures.wait(futures, timeout=timeout)

    def stats(self) -> dict:
        return {
            "name": self.name,
            "max_workers": self.max_workers,
            "completed": self.completed,
            "failed": self.failed,
            "total_submitted": len(self._task_registry),
        }

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)


# ─────────────────────────────────────────────────────────────────────────────
# WORKER TYPE 2: PROCESS POOL WORKER
# ─────────────────────────────────────────────────────────────────────────────

class ProcessPoolWorker:
    """
    Process Pool Worker — uses Python's ProcessPoolExecutor.

    BEST FOR: CPU-bound tasks
      - Image/video processing
      - Data compression
      - Machine learning inference
      - Cryptographic operations
      - PDF generation

    HOW IT WORKS:
      Separate OS processes are spawned (NO shared memory, NO GIL).
      Each process has its own Python interpreter.
      Communication via pickled objects (serialization overhead).

    PROCESS POOL SIZE:
      Rule of thumb: num_processes = num_CPUs
      Over-provisioning processes causes context-switching overhead.

    WHEN TO USE vs THREAD POOL:
      Task spends time COMPUTING → ProcessPool (bypass GIL)
      Task spends time WAITING   → ThreadPool (share GIL, lighter weight)

    NOTE:
      Cannot pickle lambda functions or closures in real ProcessPoolExecutor.
      This simulation uses threads to illustrate the concept without
      the serialization complexity.
    """

    def __init__(self, name: str, max_workers: int = None):
        import os
        self.name = name
        self.max_workers = max_workers or os.cpu_count() or 4
        # NOTE: In real code, use concurrent.futures.ProcessPoolExecutor
        # Here we simulate with threads to avoid pickle limitations in demo
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix=f"proc-{name}",
        )
        self.completed = 0
        self.failed = 0
        self._lock = threading.Lock()

    def map(self, func: Callable, items: list,
            chunksize: int = 1) -> list:
        """
        MAP: Apply func to every item in parallel.
        Equivalent to [func(item) for item in items] but parallel.

        :param chunksize: How many items per process (batching for efficiency)
        :return: Results in the same order as input
        """
        futures = []
        results = []

        print(f"  [ProcessPool:{self.name}] Processing {len(items)} items "
              f"across {self.max_workers} workers...")

        start = time.time()
        future_list = [self._executor.submit(func, item) for item in items]

        for future in concurrent.futures.as_completed(future_list):
            try:
                result = future.result()
                results.append(result)
                with self._lock:
                    self.completed += 1
            except Exception as e:
                with self._lock:
                    self.failed += 1
                results.append(None)

        elapsed = (time.time() - start) * 1000
        print(f"  [ProcessPool:{self.name}] Completed {len(items)} items "
              f"in {elapsed:.0f}ms (parallel)")
        return results

    def stats(self) -> dict:
        return {"name": self.name, "workers": self.max_workers,
                "completed": self.completed, "failed": self.failed}

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True)


# ─────────────────────────────────────────────────────────────────────────────
# WORKER TYPE 3: ASYNC (asyncio) WORKER
# ─────────────────────────────────────────────────────────────────────────────

class AsyncWorker:
    """
    Async Worker — uses Python's asyncio event loop.

    BEST FOR: High-concurrency IO-bound tasks
      - Handling thousands of concurrent HTTP requests
      - WebSocket connections
      - Database queries with async drivers (asyncpg, motor)

    HOW IT WORKS:
      Single thread runs an EVENT LOOP.
      Tasks are COROUTINES that yield control while waiting (await).
      While one coroutine waits for IO, the event loop runs another.
      Zero thread-switching overhead → very efficient for IO-bound work.

    ASYNC vs THREAD POOL:
      asyncio: 1 thread, 10,000 coroutines (very lightweight)
      ThreadPool: 10,000 threads would consume ~80GB RAM!

    asyncio KEY PRIMITIVES:
      async def func():   → defines a coroutine
      await something     → yields control back to event loop
      asyncio.gather()    → run multiple coroutines concurrently
      asyncio.Queue       → thread-safe queue for async producers/consumers
      asyncio.Semaphore   → limit concurrent operations (rate limiting)

    CONCURRENCY LIMIT (backpressure):
      Without limit: 10,000 coroutines hammer database simultaneously
      With Semaphore(100): at most 100 concurrent DB queries at any time
    """

    def __init__(self, name: str, concurrency: int = 10):
        """
        :param concurrency: Max simultaneous tasks (via asyncio.Semaphore)
        """
        self.name = name
        self.concurrency = concurrency
        self.completed = 0
        self.failed = 0

    async def _run_with_semaphore(self, semaphore: asyncio.Semaphore,
                                   task_name: str,
                                   coro) -> Any:
        """Run a coroutine with concurrency limit."""
        async with semaphore:
            try:
                result = await coro
                self.completed += 1
                return result
            except Exception as e:
                self.failed += 1
                raise

    async def run_batch(self, tasks: list[tuple[str, Any]]) -> list[Any]:
        """
        Run a batch of (name, coroutine) pairs concurrently.
        Limits to self.concurrency concurrent tasks at a time.

        asyncio.gather() runs all tasks concurrently and collects results.
        """
        semaphore = asyncio.Semaphore(self.concurrency)
        print(f"  [AsyncWorker:{self.name}] Running {len(tasks)} tasks "
              f"(concurrency limit={self.concurrency})")
        coros = [
            self._run_with_semaphore(semaphore, name, coro)
            for name, coro in tasks
        ]
        results = await asyncio.gather(*coros, return_exceptions=True)
        return list(results)

    def stats(self) -> dict:
        return {"name": self.name, "concurrency": self.concurrency,
                "completed": self.completed, "failed": self.failed}


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

# Simulated IO-bound task (network request, DB query)
def io_bound_task(item: str) -> dict:
    time.sleep(0.05)   # 50ms simulated IO wait
    return {"item": item, "status": "processed", "thread": threading.current_thread().name}


# Simulated CPU-bound task (compression, encoding)
def cpu_bound_task(n: int) -> int:
    """Compute sum of squares (simulates CPU work)."""
    return sum(i * i for i in range(n))


# Simulated async IO task (aiohttp-style)
async def async_fetch(url: str) -> dict:
    await asyncio.sleep(0.02)   # Non-blocking IO wait
    return {"url": url, "status": 200, "body": f"Response from {url}"}


if __name__ == "__main__":
    print("=" * 65)
    print("   WORKER TYPES — Demo")
    print("=" * 65)

    # ── Thread Pool ────────────────────────────────────────────────────────
    print("\n  ═══ THREAD POOL WORKER (IO-bound) ═══")
    print("  4 workers processing 8 IO-bound tasks (each takes 50ms)")
    print("  Sequential would take: 8 × 50ms = 400ms")
    print("  Parallel with 4 threads: ~100ms\n")

    pool = ThreadPoolWorker("io-pool", max_workers=4)
    start = time.time()
    for i in range(8):
        task = Task(
            name=f"fetch_user_{i}",
            func=io_bound_task,
            args=(f"user_{i}",),
        )
        pool.submit(task)
    pool.wait_all()
    elapsed = (time.time() - start) * 1000
    print(f"\n  Completed in {elapsed:.0f}ms (vs ~400ms sequential)")
    print(f"  Stats: {pool.stats()}")
    pool.shutdown()

    # ── Process Pool ───────────────────────────────────────────────────────
    print("\n\n  ═══ PROCESS POOL WORKER (CPU-bound) ═══")
    print("  4 workers processing 8 CPU-bound tasks in parallel\n")

    proc_pool = ProcessPoolWorker("cpu-pool", max_workers=4)
    items = [1_000_000] * 8  # Compute sum-of-squares for 1M numbers, 8 times
    start = time.time()
    results = proc_pool.map(cpu_bound_task, items)
    elapsed = (time.time() - start) * 1000
    print(f"  Results (first 3): {results[:3]}")
    print(f"  Completed in {elapsed:.0f}ms | Stats: {proc_pool.stats()}")
    proc_pool.shutdown()

    # ── Async Worker ──────────────────────────────────────────────────────
    print("\n\n  ═══ ASYNC WORKER (high-concurrency IO) ═══")
    print("  10 concurrent coroutines, each simulating a 20ms network fetch\n")

    async def run_async_demo():
        worker = AsyncWorker("async-pool", concurrency=10)
        urls = [f"https://api.example.com/data/{i}" for i in range(10)]
        tasks = [(f"fetch_{i}", async_fetch(url)) for i, url in enumerate(urls)]

        start = time.time()
        results = await worker.run_batch(tasks)
        elapsed = (time.time() - start) * 1000
        print(f"  Fetched {len(results)} URLs concurrently in {elapsed:.0f}ms "
              f"(sequential would be {len(urls) * 20}ms)")
        print(f"  Stats: {worker.stats()}")

    asyncio.run(run_async_demo())

    print("\n" + "=" * 65)
    print("  WORKER TYPE COMPARISON")
    print("=" * 65)
    rows = [
        ("Model",         "Concurrency Unit", "Best For",            "GIL?"),
        ("─"*13,          "─"*17,             "─"*22,                "─"*4),
        ("Thread Pool",   "OS Threads",       "IO-bound (network)",  "Yes"),
        ("Process Pool",  "OS Processes",     "CPU-bound (compute)", "No"),
        ("asyncio",       "Coroutines",       "High-conc IO (10k+)", "N/A"),
    ]
    for r in rows:
        print(f"  {r[0]:<14} {r[1]:<18} {r[2]:<23} {r[3]}")
    print("=" * 65)
