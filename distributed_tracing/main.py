"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    DISTRIBUTED TRACING SUITE                                 ║
║                    All Concepts — Main Runner                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  THE PROBLEM WITH MICROSERVICES:                                             ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Logs are isolated per server. If an API request is slow, searching logs     ║
║  won't tell you *where* the time was spent across network boundaries.        ║
║                                                                              ║
║  MODULES IN THIS DIRECTORY:                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  tracing_core.py  → Simulates Trace Context, Spans, and Jaeger Collector.    ║
║  microservices.py → Passes Context across simulated HTTP boundaries.         ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from tracing_core import collector
import microservices


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


# ─────────────────────────────────────────────────────────────────────────────
def demo_distributed_tracing():
    section("MODULE 1: DISTRIBUTED TRACING (FLAME GRAPH)")
    
    print("  Triggering an API Gateway Checkout Request...")
    print("  (This will call Auth Service, which calls DB.")
    print("   Then it will call Billing Service, which also calls DB.)\n")
    
    # 1. Execute the trace
    trace_id = microservices.api_gateway_checkout(user_id="U_4921", amount=149.99)
    
    # 2. Render the Waterfall
    print("  Trace completed! Rendering Jaeger Waterfall View...")
    collector.print_waterfall(trace_id)
    
    print("  Look at the waterfall above. Notice how:")
    print("  1. The API Gateway encompasses the entire request.")
    print("  2. Auth and Billing happen sequentially.")
    print("  3. The Database calls are nested perfectly inside their parents.")


# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    section("DISTRIBUTED TRACING — COMPLETE REFERENCE")
    print()
    
    print("  KEY CONCEPTS:")
    for row in [
        ("Trace ID",  "A unique ID representing the ENTIRE user journey."),
        ("Span",      "A single block of work (e.g., a DB query, an HTTP request)."),
        ("Span ID",   "A unique ID for that specific block of work."),
        ("Parent ID", "Points to the span that triggered this span, creating a tree."),
    ]:
        print(f"    {row[0]:<10} {row[1]}")

    print("\n  CONTEXT PROPAGATION (W3C Standard):")
    for row in [
        ("traceparent", "The HTTP header injected into every outbound network call."),
        ("",            "Format: 00-{TraceId}-{ParentSpanId}-01"),
        ("",            "Allows Service B to attach its spans to Service A's trace."),
    ]:
        print(f"    {row[0]:<12} {row[1]}")

    print("\n  TOOLS:")
    for row in [
        ("OpenTelemetry", "The standard library (SDK) developers put in their code"),
        ("",              "to automatically intercept HTTP/DB calls and create spans."),
        ("Jaeger/Zipkin", "The backend 'Collector' UI where you view the Flame Graphs."),
    ]:
        print(f"    {row[0]:<14} {row[1]}")

    print()
    print("  Run individual file:")
    print("    python microservices.py")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       DISTRIBUTED TRACING — All Concepts")
    print("=" * 65)

    demo_distributed_tracing()
    print_summary()
