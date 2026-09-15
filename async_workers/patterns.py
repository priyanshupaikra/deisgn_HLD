"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                      ASYNCHRONOUS WORKERS — MODULE 3                         ║
║                        COMMON ASYNC WORKER PATTERNS                          ║
╚══════════════════════════════════════════════════════════════════════════════╝

PATTERNS COVERED:
──────────────────
  1. FAN-OUT / FAN-IN      → Parallel sub-tasks, collect all results
  2. PIPELINE (Chaining)   → Task A result → Task B input → Task C input
  3. SAGA PATTERN          → Multi-step distributed transaction with compensation
  4. BACKGROUND JOB + POLL → Submit job, poll for result (async HTTP pattern)
  5. BATCH PROCESSING      → Accumulate items, process as a batch

REAL-WORLD MAPPINGS:
─────────────────────
  Fan-Out:      Celery group() | asyncio.gather() | AWS Step Functions parallel
  Pipeline:     Celery chain() | Kafka Streams | AWS Step Functions sequence
  Saga:         Microservices choreography/orchestration
  Background:   HTTP 202 Accepted + polling /status/{job_id}
  Batch:        Kafka consumer poll() batches | AWS SQS batch read

"""

import time
import uuid
import threading
import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from collections import defaultdict


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 1: FAN-OUT / FAN-IN
# ─────────────────────────────────────────────────────────────────────────────

class FanOutFanIn:
    """
    Fan-Out / Fan-In Pattern.

    FAN-OUT: Split one task into N parallel sub-tasks.
    FAN-IN:  Collect results from all N sub-tasks and aggregate.

    VISUAL:
                    ┌─▶ Sub-Task A ─┐
      Parent Task ──├─▶ Sub-Task B ─├──▶ Aggregated Result
                    └─▶ Sub-Task C ─┘

    USE CASES:
      - Send emails to 1000 users: fan-out 1000 sub-tasks, fan-in completion count
      - Product recommendations: fan-out to 5 ML models, fan-in best result
      - Map-Reduce: Map = fan-out, Reduce = fan-in
      - Parallel API enrichment: fetch user, orders, preferences simultaneously

    IN CELERY:
      group([task1.s(arg), task2.s(arg), task3.s(arg)]).apply_async()
      Celery collects all results via a chord callback.

    asyncio.gather():
      results = await asyncio.gather(fetch_user(), fetch_orders(), fetch_prefs())
    """

    def __init__(self, name: str):
        self.name = name

    def execute(self,
                parent_input: Any,
                splitter: Callable[[Any], list],
                sub_task: Callable[[Any], Any],
                aggregator: Callable[[list], Any],
                max_workers: int = 4) -> Any:
        """
        Execute fan-out/fan-in.

        :param parent_input: Input to the parent task
        :param splitter:     Splits input into sub-task inputs
        :param sub_task:     Function applied to each sub-task input (in parallel)
        :param aggregator:   Combines all sub-task results into final output
        :param max_workers:  Parallelism level

        Flow:
          sub_inputs = splitter(parent_input)        ← split
          sub_results = parallel_map(sub_task, sub_inputs)  ← fan-out + execute
          final = aggregator(sub_results)            ← fan-in
        """
        # FAN-OUT: split into sub-tasks
        sub_inputs = splitter(parent_input)
        print(f"  [FanOut:{self.name}] Split into {len(sub_inputs)} sub-tasks")

        # PARALLEL EXECUTION: each sub-task runs in its own thread
        results = [None] * len(sub_inputs)
        errors = [None] * len(sub_inputs)
        threads = []

        start = time.time()

        def worker(idx, inp):
            try:
                results[idx] = sub_task(inp)
            except Exception as e:
                errors[idx] = e

        for i, inp in enumerate(sub_inputs):
            t = threading.Thread(target=worker, args=(i, inp))
            threads.append(t)

        # Start in batches (respect max_workers)
        for batch_start in range(0, len(threads), max_workers):
            batch = threads[batch_start: batch_start + max_workers]
            for t in batch:
                t.start()
            for t in batch:
                t.join()

        elapsed = (time.time() - start) * 1000
        completed = sum(1 for r in results if r is not None)
        print(f"  [FanOut:{self.name}] {completed}/{len(sub_inputs)} sub-tasks "
              f"completed in {elapsed:.0f}ms")

        # FAN-IN: aggregate results
        valid_results = [r for r in results if r is not None]
        final = aggregator(valid_results)
        print(f"  [FanIn:{self.name}]  Aggregated result: {final}")
        return final


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 2: PIPELINE (Task Chaining)
# ─────────────────────────────────────────────────────────────────────────────

class TaskPipeline:
    """
    Pipeline / Task Chain Pattern.

    Each task's OUTPUT becomes the next task's INPUT.
    Tasks run SEQUENTIALLY but can be distributed across workers.

    VISUAL:
      Input ──▶ [Stage 1] ──▶ [Stage 2] ──▶ [Stage 3] ──▶ Output

    USE CASES:
      - Video processing: download → transcode → thumbnail → upload → notify
      - Order fulfillment: validate → charge → allocate stock → ship → email
      - Data ETL: extract → transform → load → index

    IN CELERY:
      chain(task1.s(input), task2.s(), task3.s()).apply_async()
      Each task receives the previous result as its first argument.

    ERROR HANDLING:
      If any stage fails → pipeline aborts (fail-fast).
      Can add compensation steps for partial rollback (Saga pattern).
    """

    def __init__(self, name: str):
        self.name = name
        self._stages: list[tuple[str, Callable]] = []

    def pipe(self, stage_name: str, func: Callable) -> 'TaskPipeline':
        """Add a stage to the pipeline. Returns self for chaining."""
        self._stages.append((stage_name, func))
        return self  # Fluent interface: pipeline.pipe(...).pipe(...)

    def run(self, initial_input: Any) -> Any:
        """
        Execute pipeline stages sequentially.
        Each stage receives the previous stage's output.
        """
        print(f"\n  [Pipeline:{self.name}] Starting with input: {initial_input}")
        current = initial_input

        for i, (stage_name, func) in enumerate(self._stages, 1):
            stage_start = time.time()
            try:
                current = func(current)
                elapsed = (time.time() - stage_start) * 1000
                print(f"  [Pipeline:{self.name}] Stage {i}/{len(self._stages)}: "
                      f"'{stage_name}' ✓ → {current!r} ({elapsed:.0f}ms)")
            except Exception as e:
                print(f"  [Pipeline:{self.name}] Stage {i}: '{stage_name}' ✗ FAILED: {e}")
                print(f"  [Pipeline:{self.name}] Pipeline ABORTED at stage {i}")
                raise

        print(f"  [Pipeline:{self.name}] Completed → {current!r}")
        return current


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 3: SAGA PATTERN
# ─────────────────────────────────────────────────────────────────────────────

class SagaOrchestrator:
    """
    Saga Pattern — distributed transactions with compensation.

    PROBLEM:
      A distributed transaction spans multiple services.
      If step 3 of 5 fails, how do you UNDO steps 1 and 2?

    SOLUTION (Saga):
      Each step has a COMPENSATING transaction (undo operation).
      If any step fails:
        - Don't complete the forward steps
        - Run compensating transactions for COMPLETED steps in reverse order

    TWO SAGA APPROACHES:
      CHOREOGRAPHY: Each service publishes events and reacts to others.
                    Decoupled, but hard to monitor/debug. Complex error handling.

      ORCHESTRATION: A central Saga Orchestrator coordinates all steps.
                     Simpler to understand and debug. Single point of failure.
                     (This implementation is Orchestration-based)

    VISUAL:
      ┌─────────────────────────────────────────────────────────────────┐
      │ FORWARD:   Step1 ──▶ Step2 ──▶ Step3 ✗ (FAILS HERE)           │
      │ COMPENSATE:          Comp2 ◀── Comp1 (run in reverse order)   │
      └─────────────────────────────────────────────────────────────────┘

    USE CASES:
      - Order placement: reserve stock → charge payment → create shipment
        If shipment fails → refund payment → release stock
      - Travel booking: book flight → book hotel → book car
        If car fails → cancel hotel → cancel flight

    IN PRODUCTION:
      - AWS Step Functions (state machine with compensation)
      - Temporal.io (workflow orchestration)
      - Conductor (Netflix open-source orchestrator)
    """

    @dataclass
    class SagaStep:
        name: str
        action: Callable       # Forward transaction
        compensate: Callable   # Undo transaction

    def __init__(self, name: str):
        self.name = name
        self._steps: list['SagaOrchestrator.SagaStep'] = []

    def add_step(self, name: str,
                 action: Callable,
                 compensate: Callable) -> 'SagaOrchestrator':
        self._steps.append(self.SagaStep(name, action, compensate))
        return self

    def execute(self, context: dict) -> dict:
        """
        Run the saga.
        On failure: runs compensations for all completed steps in reverse.

        :param context: Shared data passed between steps (like a request context)
        :return:        Updated context after all steps, or raises on failure.
        """
        completed_steps = []
        print(f"\n  [Saga:{self.name}] Starting ({len(self._steps)} steps)")

        for i, step in enumerate(self._steps, 1):
            print(f"  [Saga:{self.name}] Step {i}/{len(self._steps)}: '{step.name}'")
            try:
                context = step.action(context)
                completed_steps.append(step)
                print(f"  [Saga:{self.name}] ✓ '{step.name}' completed")
            except Exception as e:
                print(f"  [Saga:{self.name}] ✗ '{step.name}' FAILED: {e}")
                print(f"  [Saga:{self.name}] Running compensations ({len(completed_steps)} steps)...")

                # COMPENSATE in reverse order
                for comp_step in reversed(completed_steps):
                    try:
                        context = comp_step.compensate(context)
                        print(f"  [Saga:{self.name}] ↩ Compensated '{comp_step.name}'")
                    except Exception as comp_err:
                        print(f"  [Saga:{self.name}] ⚠ Compensation '{comp_step.name}' failed: {comp_err}")
                        # In production: alert operations team, manual intervention needed

                raise RuntimeError(f"Saga '{self.name}' failed at step '{step.name}': {e}")

        print(f"  [Saga:{self.name}] All steps completed ✓")
        return context


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 4: BACKGROUND JOB + POLL (HTTP 202 Accepted)
# ─────────────────────────────────────────────────────────────────────────────

class BackgroundJobService:
    """
    Background Job + Status Polling Pattern.

    FLOW:
      Client ──POST /jobs──▶ Server returns 202 Accepted + {job_id}
      Server enqueues job to background worker
      Client polls: GET /jobs/{job_id}/status
        → {status: "PENDING"} → keep polling
        → {status: "RUNNING"} → keep polling
        → {status: "COMPLETE", result: {...}} → done!

    HTTP STATUS CODES:
      202 Accepted         → Request accepted, not yet processed
      200 + {status: running} → Still in progress
      200 + {status: done}    → Completed, here's the result
      303 See Other        → Job done, redirect to result URL

    WHY 202 INSTEAD OF 200?
      202 means "I got your request but I'm not done yet."
      200 means "I'm done and here's the result."
      For long-running tasks, 202 + poll is much better UX than blocking.

    USED BY:
      - GitHub Actions API: POST /repos/{owner}/{repo}/actions/runs → 202
      - AWS Batch: POST /v1/jobs → jobId, poll via GET /v1/jobs/{jobId}
      - Stripe: Some operations return 202 with a webhook for completion
    """

    def __init__(self):
        self._jobs: dict[str, dict] = {}
        self._lock = threading.Lock()

    def submit(self, job_name: str, func: Callable, args: tuple = ()) -> str:
        """
        Accept a job request. Returns job_id immediately (HTTP 202).
        Runs func in background.
        """
        job_id = str(uuid.uuid4())
        with self._lock:
            self._jobs[job_id] = {
                "job_id":     job_id,
                "name":       job_name,
                "status":     "PENDING",
                "submitted":  time.time(),
                "result":     None,
                "error":      None,
            }

        def run():
            with self._lock:
                self._jobs[job_id]["status"] = "RUNNING"
                self._jobs[job_id]["started"] = time.time()

            print(f"  [BGJob] 🔄 Starting job '{job_name}' (id={job_id[:8]}...)")
            try:
                result = func(*args)
                with self._lock:
                    self._jobs[job_id].update({
                        "status": "COMPLETE",
                        "result": result,
                        "finished": time.time(),
                    })
                print(f"  [BGJob] ✓ Job '{job_name}' COMPLETE → {result}")
            except Exception as e:
                with self._lock:
                    self._jobs[job_id].update({
                        "status": "FAILED",
                        "error": str(e),
                        "finished": time.time(),
                    })
                print(f"  [BGJob] ✗ Job '{job_name}' FAILED: {e}")

        threading.Thread(target=run, daemon=True).start()
        print(f"  [BGJob] HTTP 202 Accepted → job_id={job_id[:8]}...")
        return job_id

    def get_status(self, job_id: str) -> dict:
        """Poll job status. Returns current state + result if complete."""
        with self._lock:
            job = self._jobs.get(job_id, {})
            return dict(job)

    def poll_until_done(self, job_id: str, timeout: float = 10.0,
                         poll_interval: float = 0.1) -> dict:
        """Client-side polling loop (simulates frontend polling)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self.get_status(job_id)
            if status.get("status") in ("COMPLETE", "FAILED"):
                return status
            time.sleep(poll_interval)
        return {"status": "TIMEOUT", "job_id": job_id}


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 5: BATCH PROCESSING
# ─────────────────────────────────────────────────────────────────────────────

class BatchProcessor:
    """
    Batch Processing Pattern.

    Instead of processing items one-by-one, accumulate them into batches
    and process the whole batch together for efficiency.

    WHY BATCH?
      - DB: 1000 individual INSERTs = 1000 round trips = ~5000ms
            1 batch INSERT(1000) = 1 round trip = ~50ms (100x faster!)
      - Email service: 1000 individual API calls = rate limited, slow
                       1 batch send-to-many API call = 1 request, fast
      - ML inference: Batch size 32 uses GPU memory better than 1 at a time

    TRIGGER CONDITIONS (when to flush the batch):
      SIZE-BASED:  Flush when batch reaches max_size items
      TIME-BASED:  Flush every N seconds regardless of batch size
      HYBRID:      Flush when max_size reached OR N seconds elapsed (most common)
                   Prevents items from waiting forever in a small batch.

    IN KAFKA:
      Consumer polls and gets up to max.poll.records = 500 per batch.
      Process all 500 in one DB transaction (very efficient).

    IN CELERY:
      @app.task
      def process_batch(items): bulk_insert(items)
      celery.chunks(process_item.s(i), 100)  # 100 items per batch
    """

    def __init__(self, name: str, max_size: int = 10,
                 max_wait_seconds: float = 1.0,
                 processor: Callable[[list], Any] = None):
        """
        :param max_size:         Max items before automatic flush
        :param max_wait_seconds: Max time (seconds) before flush even if not full
        :param processor:        Function that handles the full batch
        """
        self.name = name
        self.max_size = max_size
        self.max_wait = max_wait_seconds
        self.processor = processor or (lambda batch: f"processed {len(batch)} items")
        self._batch: list[Any] = []
        self._lock = threading.Lock()
        self._last_flush = time.time()
        self._total_processed = 0
        self._flush_count = 0

        # Background timer thread: time-based flush
        self._running = True
        self._timer = threading.Thread(target=self._timer_loop, daemon=True)
        self._timer.start()

    def add(self, item: Any) -> None:
        """Add item to current batch. Auto-flushes if size limit reached."""
        with self._lock:
            self._batch.append(item)
            should_flush = len(self._batch) >= self.max_size

        if should_flush:
            self._flush("size-limit")

    def _flush(self, reason: str = "manual") -> int:
        """Process current batch and reset."""
        with self._lock:
            if not self._batch:
                return 0
            batch = list(self._batch)
            self._batch.clear()
            self._last_flush = time.time()

        size = len(batch)
        print(f"  [BatchProcessor:{self.name}] FLUSH ({reason}): "
              f"{size} items → processing...")
        result = self.processor(batch)
        self._total_processed += size
        self._flush_count += 1
        print(f"  [BatchProcessor:{self.name}] ✓ Batch done: {result}")
        return size

    def _timer_loop(self) -> None:
        """Flush periodically (time-based trigger)."""
        while self._running:
            time.sleep(0.05)
            with self._lock:
                elapsed = time.time() - self._last_flush
                has_items = bool(self._batch)

            if has_items and elapsed >= self.max_wait:
                self._flush("time-limit")

    def flush_now(self) -> int:
        """Force-flush remaining items (call on shutdown)."""
        return self._flush("shutdown")

    def stop(self) -> None:
        self._running = False
        self.flush_now()

    def stats(self) -> dict:
        return {
            "name":            self.name,
            "total_processed": self._total_processed,
            "flush_count":     self._flush_count,
            "pending":         len(self._batch),
        }


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    # ── Fan-Out / Fan-In ──────────────────────────────────────────────────
    print("=" * 65)
    print("   PATTERN 1: FAN-OUT / FAN-IN")
    print("=" * 65)

    fan = FanOutFanIn("recommendation-engine")
    fan.execute(
        parent_input={"user_id": "u_42"},
        splitter=lambda inp: [
            {"model": "collaborative", "user_id": inp["user_id"]},
            {"model": "content-based", "user_id": inp["user_id"]},
            {"model": "trending",      "user_id": inp["user_id"]},
            {"model": "popular",       "user_id": inp["user_id"]},
        ],
        sub_task=lambda inp: (
            time.sleep(0.05),  # Simulate model inference
            {"model": inp["model"], "items": [f"item_{i}" for i in range(3)]}
        )[1],
        aggregator=lambda results: {
            "merged_recommendations": list({
                item
                for r in results
                for item in r["items"]
            }),
            "models_used": len(results),
        },
    )

    # ── Pipeline ──────────────────────────────────────────────────────────
    print("\n\n" + "=" * 65)
    print("   PATTERN 2: PIPELINE (Task Chaining)")
    print("=" * 65)

    TaskPipeline("video-upload") \
        .pipe("validate",    lambda v: {**v, "validated": True}) \
        .pipe("transcode",   lambda v: {**v, "format": "mp4", "bitrate": "1080p"}) \
        .pipe("thumbnail",   lambda v: {**v, "thumbnail": "thumb_001.jpg"}) \
        .pipe("cdn_upload",  lambda v: {**v, "cdn_url": "https://cdn.ex.com/vid_001.mp4"}) \
        .pipe("notify_user", lambda v: {**v, "notified": True}) \
        .run({"file": "raw_video.mov", "user_id": "u_42"})

    # ── Saga ──────────────────────────────────────────────────────────────
    print("\n\n" + "=" * 65)
    print("   PATTERN 3: SAGA — Order Placement with Compensation")
    print("=" * 65)

    saga = SagaOrchestrator("place-order")
    saga \
        .add_step(
            "reserve_stock",
            action=lambda ctx: {**ctx, "stock_reserved": True},
            compensate=lambda ctx: {**ctx, "stock_reserved": False, "stock_released": True},
        ) \
        .add_step(
            "charge_payment",
            action=lambda ctx: {**ctx, "payment_charged": True, "charge_id": "ch_123"},
            compensate=lambda ctx: {**ctx, "payment_refunded": True},
        ) \
        .add_step(
            "create_shipment",
            action=lambda ctx: (_ for _ in ()).throw(RuntimeError("Shipping service down!")),
            compensate=lambda ctx: {**ctx, "shipment_cancelled": True},
        )

    try:
        saga.execute({"order_id": "ord_001", "user_id": "u_42", "amount": 99.99})
    except RuntimeError as e:
        print(f"\n  Saga rolled back: {e}")

    # ── Background Job + Poll ─────────────────────────────────────────────
    print("\n\n" + "=" * 65)
    print("   PATTERN 4: BACKGROUND JOB + POLL (HTTP 202)")
    print("=" * 65)
    svc = BackgroundJobService()

    def generate_report():
        time.sleep(0.3)  # Simulate slow report generation
        return {"rows": 50000, "format": "PDF", "size_mb": 12.5}

    job_id = svc.submit("monthly-report", generate_report)
    print(f"\n  Polling for result...")
    result = svc.poll_until_done(job_id)
    print(f"  Final status: {result['status']} | result: {result.get('result')}")

    # ── Batch Processing ──────────────────────────────────────────────────
    print("\n\n" + "=" * 65)
    print("   PATTERN 5: BATCH PROCESSING")
    print("=" * 65)

    db_inserts = []
    def bulk_db_insert(batch: list) -> str:
        db_inserts.extend(batch)
        return f"INSERT {len(batch)} rows in 1 DB round-trip"

    batcher = BatchProcessor("event-batcher", max_size=5, max_wait_seconds=0.5,
                              processor=bulk_db_insert)

    print("\n  Adding 13 events (batch_size=5, max_wait=0.5s):")
    for i in range(13):
        batcher.add({"event": f"click_{i}", "user": f"u_{i%3}"})
        time.sleep(0.05)

    batcher.stop()
    print(f"\n  Stats: {batcher.stats()}")
    print(f"  Total DB rows inserted: {len(db_inserts)} in {batcher._flush_count} batches")
    print(f"  (vs 13 individual inserts without batching)")

    print("\n" + "=" * 65)
    print("  ASYNC PATTERNS SUMMARY")
    print("=" * 65)
    for name, desc, use in [
        ("Fan-Out/Fan-In", "Parallel sub-tasks → aggregate",  "ML models, enrichment"),
        ("Pipeline",       "A→B→C sequential chained tasks",  "ETL, video processing"),
        ("Saga",           "Distributed txn + compensation",   "Order, booking flows"),
        ("Background+Poll","202 Accepted → poll /status",     "Reports, imports"),
        ("Batch",          "Accumulate → bulk process",        "DB inserts, emails"),
    ]:
        print(f"  {name:<18} {desc:<36} → {use}")
    print("=" * 65)
