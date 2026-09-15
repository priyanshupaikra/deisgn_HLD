"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    DISTRIBUTED TRACING — MODULE 1                            ║
║                       TRACES, SPANS & COLLECTORS                             ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS DISTRIBUTED TRACING?
────────────────────────────
In a microservices architecture, a single user request might touch 10 different
services before a response is returned. If the request is slow, how do you know
which of the 10 services caused the delay?

Logs aren't enough. You need to see the "Waterfall" or "Flame Graph" of the 
entire request across network boundaries.

CORE CONCEPTS:
──────────────
  1. TRACE: The complete lifecycle of a single user request across all services.
            Represented by a globally unique 32-character hex string (Trace ID).

  2. SPAN:  A single unit of work (e.g., a DB query, an HTTP call, a function run).
            Has a start time, end time, name, and its own unique Span ID.
            Spans have "Parent-Child" relationships. 
            (e.g., API Gateway Span is the Parent of the Auth Service Span).

  3. CONTEXT PROPAGATION (W3C Trace Context):
     How does Service B know it's part of the same trace as Service A?
     Service A injects HTTP Headers (like `traceparent`) into its request to Service B.
     Service B extracts this header and uses the same Trace ID.

  4. COLLECTOR: A background system (like Jaeger, Zipkin, or Datadog) that 
     receives finished Spans from all services and reconstructs the Flame Graph.
"""

import time
import uuid
import contextvars
from dataclasses import dataclass, field
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# 1. TRACE CONTEXT (Thread-safe Context Propagation)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TraceContext:
    trace_id: str
    parent_span_id: Optional[str] = None

# Using contextvars ensures async tasks/threads don't mix up their trace IDs.
_trace_ctx_var = contextvars.ContextVar("trace_ctx", default=None)

def get_trace_context() -> Optional[TraceContext]:
    return _trace_ctx_var.get()

def set_trace_context(ctx: TraceContext):
    _trace_ctx_var.set(ctx)


# ─────────────────────────────────────────────────────────────────────────────
# 2. THE COLLECTOR (Simulates Jaeger / Zipkin / Datadog)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SpanData:
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    service_name: str
    operation_name: str
    start_time_ms: float
    end_time_ms: float
    duration_ms: float
    tags: dict

class TraceCollector:
    """
    In real life, spans are sent via gRPC or UDP to a background agent.
    This simulates the backend database that stores all spans.
    """
    def __init__(self):
        self.spans = []

    def record_span(self, span_data: SpanData):
        self.spans.append(span_data)

    def print_waterfall(self, trace_id: str):
        """Reconstructs and prints a Jaeger-like waterfall UI in the console."""
        print(f"\n  [Jaeger Waterfall View] Trace ID: {trace_id}")
        print("  " + "-" * 75)
        
        trace_spans = [s for s in self.spans if s.trace_id == trace_id]
        if not trace_spans:
            print("  No spans found.")
            return

        # Find the root span (earliest start time, usually no parent)
        root_span = min(trace_spans, key=lambda s: s.start_time_ms)
        trace_start = root_span.start_time_ms
        trace_duration = max(s.end_time_ms for s in trace_spans) - trace_start

        # Build parent-child tree
        children_map = {s.span_id: [] for s in trace_spans}
        for s in trace_spans:
            if s.parent_span_id and s.parent_span_id in children_map:
                children_map[s.parent_span_id].append(s)

        # Recursive function to print spans
        def print_span(span: SpanData, depth: int):
            # Calculate where to draw the bar
            offset_ms = span.start_time_ms - trace_start
            
            # Simple text visualization mapping ms to chars (max 40 chars width)
            # Assuming max width represents trace_duration
            if trace_duration > 0:
                left_pad = int((offset_ms / trace_duration) * 40)
                bar_len = max(1, int((span.duration_ms / trace_duration) * 40))
            else:
                left_pad, bar_len = 0, 1
                
            indent = "  " * depth
            name = f"{span.service_name}::{span.operation_name}"
            
            # Formatting: [Service::Op] 45.2ms |    [=====]
            print(f"  {indent}{name:<30} {span.duration_ms:>6.1f}ms | {' '*left_pad}[{'='*bar_len}]")
            
            # Print tags
            if span.tags:
                print(f"  {indent}  ↳ tags: {span.tags}")
            
            # Print children sorted by start time
            children = sorted(children_map.get(span.span_id, []), key=lambda s: s.start_time_ms)
            for child in children:
                print_span(child, depth + 1)

        print_span(root_span, 0)
        print("  " + "-" * 75 + "\n")

# Global collector instance
collector = TraceCollector()


# ─────────────────────────────────────────────────────────────────────────────
# 3. SPAN (Context Manager)
# ─────────────────────────────────────────────────────────────────────────────

class Span:
    """
    A single operation within a trace.
    Acts as a context manager (with Span(...):) to automatically record start/end times.
    """
    def __init__(self, service_name: str, operation_name: str, tags: dict = None):
        self.service_name = service_name
        self.operation_name = operation_name
        self.tags = tags or {}
        
        self.span_id = uuid.uuid4().hex[:16] # 64-bit span ID
        self.start_time = 0.0
        
        # Determine Trace Context
        ctx = get_trace_context()
        if ctx:
            # We are part of an existing trace
            self.trace_id = ctx.trace_id
            self.parent_span_id = ctx.parent_span_id
        else:
            # We are the ROOT SPAN (e.g., API Gateway receiving new request)
            self.trace_id = uuid.uuid4().hex # 128-bit trace ID
            self.parent_span_id = None
            
        # Store original context to restore it later
        self._previous_ctx = ctx

    def __enter__(self):
        self.start_time = time.time() * 1000 # milliseconds
        
        # Set this span as the parent for any downstream code executed within this block
        new_ctx = TraceContext(trace_id=self.trace_id, parent_span_id=self.span_id)
        set_trace_context(new_ctx)
        
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = time.time() * 1000
        duration = end_time - self.start_time
        
        if exc_type:
            self.tags["error"] = True
            self.tags["error.message"] = str(exc_val)
            
        span_data = SpanData(
            trace_id=self.trace_id,
            span_id=self.span_id,
            parent_span_id=self.parent_span_id,
            service_name=self.service_name,
            operation_name=self.operation_name,
            start_time_ms=self.start_time,
            end_time_ms=end_time,
            duration_ms=duration,
            tags=self.tags
        )
        
        # "Send" to background collector
        collector.record_span(span_data)
        
        # Restore previous context
        set_trace_context(self._previous_ctx)
