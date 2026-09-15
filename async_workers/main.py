"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                      ASYNCHRONOUS WORKERS SUITE                              ║
║                       All Concepts — Main Runner                             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WHAT IS AN ASYNC WORKER?                                                    ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  A process/thread that performs work INDEPENDENTLY of the main app.         ║
║  Web server enqueues job → returns immediately → worker runs in background. ║
║                                                                              ║
║  PRODUCTION STACK (Python):                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Broker:  Redis or RabbitMQ         (message transport)                     ║
║  Workers: Celery                    (task execution)                         ║
║  Beat:    Celery Beat               (periodic scheduler)                    ║
║  Backend: Redis or PostgreSQL       (result storage)                         ║
║  Monitor: Flower                    (real-time dashboard)                   ║
║                                                                              ║
║  MODULES:                                                                    ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  worker_types.py → Thread Pool (IO), Process Pool (CPU), asyncio (10k IO)  ║
║  task_queue.py   → Priority queue, Retry (exp backoff), Periodic scheduler  ║
║  patterns.py     → Fan-Out, Pipeline, Saga, BG+Poll, Batch                 ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os
import time
import threading
import asyncio

sys.path.insert(0, os.path.dirname(__file__))

from worker_types import Task, TaskStatus, ThreadPoolWorker, AsyncWorker
from task_queue import (
    TaskQueue, TaskResultBackend, RetryWorker,
    ExponentialBackoffRetry, FixedRetry, PeriodicScheduler
)
from patterns import (
    FanOutFanIn, TaskPipeline, SagaOrchestrator,
    BackgroundJobService, BatchProcessor
)


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def subsection(title: str):
    print(f"\n  ─── {title} ───")


# ─────────────────────────────────────────────────────────────────────────────
def demo_worker_types():
    section("MODULE 1: WORKER TYPES")
    print("  Thread Pool → IO-bound | Process Pool → CPU-bound | asyncio → high-concurrency")

    subsection("Thread Pool (4 workers, 8 IO tasks)")
    pool = ThreadPoolWorker("demo", max_workers=4)
    start = time.time()
    for i in range(8):
        task = Task(name=f"io_task_{i}",
                    func=lambda n=i: (time.sleep(0.05), f"result_{n}")[1])
        pool.submit(task)
    pool.wait_all()
    elapsed = (time.time() - start) * 1000
    print(f"  8 tasks × 50ms = 400ms sequential → parallel: {elapsed:.0f}ms "
          f"({400/elapsed:.1f}x faster)")
    print(f"  {pool.stats()}")
    pool.shutdown()

    subsection("asyncio (10 concurrent fetch coroutines)")
    async def run_async():
        async def fake_fetch(n):
            await asyncio.sleep(0.02)
            return f"data_{n}"

        worker = AsyncWorker("demo", concurrency=10)
        tasks = [(f"fetch_{i}", fake_fetch(i)) for i in range(10)]
        start = time.time()
        results = await worker.run_batch(tasks)
        elapsed = (time.time() - start) * 1000
        print(f"  10 tasks × 20ms = 200ms sequential → parallel: {elapsed:.0f}ms")

    asyncio.run(run_async())


# ─────────────────────────────────────────────────────────────────────────────
def demo_task_queue():
    section("MODULE 2: TASK QUEUE WITH RETRY")
    print("  Priority queue + exponential backoff + result backend")

    queue   = TaskQueue("demo-queue")
    backend = TaskResultBackend()
    strategy = ExponentialBackoffRetry(base_delay=0.05, max_delay=0.5, jitter=False)

    workers = [RetryWorker(f"W{i}", queue, backend, strategy) for i in range(2)]
    for w in workers: w.start()

    from collections import defaultdict
    call_counts: dict[str, int] = defaultdict(int)

    def flaky(name, fail_times):
        call_counts[name] += 1
        if call_counts[name] <= fail_times:
            raise RuntimeError(f"failure #{call_counts[name]}")
        return f"'{name}' OK on attempt {call_counts[name]}"

    tasks = [
        Task(name="critical_alert", func=flaky, args=("critical_alert", 0),
             max_retries=2, priority=0),
        Task(name="send_email",     func=flaky, args=("send_email", 2),
             max_retries=3, priority=1),
        Task(name="gen_report",     func=flaky, args=("gen_report", 1),
             max_retries=2, priority=2),
    ]

    subsection("Enqueuing 3 tasks (priority order)")
    for t in tasks:
        queue.enqueue(t)
        print(f"  Queued: [{t.priority}] '{t.name}'")

    time.sleep(1.5)
    for w in workers: w.stop()

    subsection("Results from backend")
    for t in tasks:
        r = backend.get(t.task_id, timeout=0)
        status = r["status"] if r else "NO RESULT"
        print(f"  '{t.name}': [{status}]")

    subsection("Retry Strategy: delays for attempts 1-5")
    for name, s in [("Fixed(0.5s)", FixedRetry(0.5)),
                     ("ExpBackoff(0.1s)", ExponentialBackoffRetry(0.1, 10.0, False))]:
        delays = [round(s.delay(i), 2) for i in range(5)]
        print(f"  {name:<24}: {delays}")


# ─────────────────────────────────────────────────────────────────────────────
def demo_patterns():
    section("MODULE 3: ASYNC WORKER PATTERNS")

    subsection("Pipeline: Video Upload")
    TaskPipeline("video") \
        .pipe("validate",  lambda v: {**v, "ok": True}) \
        .pipe("transcode", lambda v: {**v, "fmt": "mp4"}) \
        .pipe("cdn_push",  lambda v: {**v, "url": "https://cdn.ex.com/vid.mp4"}) \
        .run({"file": "raw.mov"})

    subsection("Saga: Order with Compensation (shipping fails)")
    saga = SagaOrchestrator("order")
    saga \
        .add_step("reserve_stock",
                  lambda c: {**c, "stock": "reserved"},
                  lambda c: {**c, "stock": "released"}) \
        .add_step("charge_payment",
                  lambda c: {**c, "charge": "ch_001"},
                  lambda c: {**c, "charge": "refunded"}) \
        .add_step("create_shipment",
                  lambda c: (_ for _ in ()).throw(RuntimeError("Shipping down")),
                  lambda c: {**c, "ship": "cancelled"})

    try:
        saga.execute({"order_id": "o_1"})
    except RuntimeError as e:
        print(f"  Saga aborted & compensated: {e}")

    subsection("Background Job + Poll (HTTP 202 pattern)")
    svc = BackgroundJobService()
    job_id = svc.submit("pdf_export", lambda: (time.sleep(0.2), {"pages": 50})[1])
    status = svc.poll_until_done(job_id)
    print(f"  Job status: {status['status']} | result: {status.get('result')}")

    subsection("Batch Processor (size=4, wait=0.3s)")
    rows = []
    bp = BatchProcessor("events", max_size=4, max_wait_seconds=0.3,
                         processor=lambda b: rows.extend(b) or f"{len(b)} rows bulk-inserted")
    for i in range(9):
        bp.add({"event": f"e{i}"})
        time.sleep(0.04)
    bp.stop()
    print(f"  {len(rows)} events processed in {bp._flush_count} DB round-trips")


# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    section("ASYNC WORKERS — COMPLETE REFERENCE")
    print()
    print("  WORKER TYPES:")
    for row in [
        ("Thread Pool", "OS Threads",   "IO-bound (network/disk)",   "GIL limits CPU parallelism"),
        ("Process Pool","OS Processes", "CPU-bound (encode/compute)","Bypasses GIL"),
        ("asyncio",     "Coroutines",   "High-conc IO (10k+)",       "1 thread, cooperative"),
    ]:
        print(f"    {row[0]:<13} {row[1]:<14} {row[2]:<26} ← {row[3]}")

    print("\n  RETRY STRATEGIES:")
    for row in [
        ("Fixed",              "Retry every N seconds",              "Predictable, simple"),
        ("Exponential Backoff","2^n seconds between retries",        "Reduces thundering herd"),
        ("Exp Backoff+Jitter", "2^n + random spread",               "Desynchronizes retries"),
    ]:
        print(f"    {row[0]:<22} {row[1]:<36} → {row[2]}")

    print("\n  TASK QUEUE CONCEPTS:")
    for row in [
        ("Priority Queue", "Min-heap; lower number = processed first"),
        ("Result Backend", "Redis/DB stores task state + result with TTL"),
        ("Revocation",     "Cancel pending task before worker picks it up"),
        ("Periodic Sched.","Celery Beat: enqueues tasks on cron schedule"),
    ]:
        print(f"    {row[0]:<18} → {row[1]}")

    print("\n  ASYNC PATTERNS:")
    for row in [
        ("Fan-Out/Fan-In", "Split → parallel exec → aggregate", "Recommendations, map-reduce"),
        ("Pipeline",       "A → B → C chained output→input",   "ETL, video processing"),
        ("Saga",           "Dist. transaction + compensation",  "Order, travel booking"),
        ("BG + Poll",      "HTTP 202 + GET /status/{id}",       "Reports, ML training"),
        ("Batch",          "Accumulate → bulk process",         "DB inserts, emails"),
    ]:
        print(f"    {row[0]:<18} {row[1]:<34} → {row[2]}")

    print()
    print("  Run individual files:")
    for f in ["worker_types.py", "task_queue.py", "patterns.py"]:
        print(f"    python {f}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       ASYNCHRONOUS WORKERS — All Concepts Demo")
    print("=" * 65)

    demo_worker_types()
    demo_task_queue()
    demo_patterns()
    print_summary()
