"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                   MONITORING & LOGGING SUITE                                 ║
║                    All Concepts — Main Runner                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  THE THREE PILLARS OF OBSERVABILITY:                                         ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  1. LOGS: Record of discrete events (e.g., User 123 logged in).              ║
║           Tools: Elasticsearch, Logstash, Kibana (ELK), Splunk, Datadog      ║
║  2. METRICS: Aggregated numbers over time (e.g., 500 requests/sec).          ║
║           Tools: Prometheus, Grafana, Datadog                                ║
║  3. TRACES: Tracking a single request as it jumps between microservices.     ║
║           Tools: Jaeger, Zipkin, OpenTelemetry                               ║
║                                                                              ║
║  MODULES IN THIS DIRECTORY:                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  structured_logging.py → JSON logs and ContextVar Correlation IDs.          ║
║  metrics.py            → Prometheus-style Counters, Gauges, Histograms.      ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

# Import modules to ensure they are loadable (we won't run their full scripts 
# here directly to avoid double-printing, we will just call their core functions)
import structured_logging
import metrics


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


# ─────────────────────────────────────────────────────────────────────────────
def demo_logs_and_traces():
    section("MODULE 1: STRUCTURED LOGGING & DISTRIBUTED TRACING")
    print("  Showing how JSON logging combined with Correlation IDs allows")
    print("  you to trace a single request across multiple microservices.\n")
    
    print("  --- Triggering API Gateway for User A ---")
    structured_logging.api_gateway_handle_request("User_A_777", 25.0)

    print("\n  --- Triggering API Gateway for User B (Failing request) ---")
    structured_logging.api_gateway_handle_request("User_B_999", 5000.0)


# ─────────────────────────────────────────────────────────────────────────────
def demo_metrics():
    section("MODULE 2: METRICS (Prometheus Model)")
    print("  Showing how Counters (Totals), Gauges (Current State), and ")
    print("  Histograms (Latencies) are accumulated in memory and scraped.\n")

    server = metrics.WebServerMetrics()
    server.simulate_traffic(5)
    server.scrape_metrics_endpoint()


# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    section("MONITORING & LOGGING — COMPLETE REFERENCE")
    print()
    
    print("  1. STRUCTURED LOGGING:")
    for row in [
        ("Why use JSON?",    "Easy for machines (Elasticsearch) to parse and index."),
        ("What is inside?",  "Timestamp, Level, Service, Msg, + Custom Fields (e.g., user_id)"),
        ("Correlation ID",   "A unique ID generated at the gateway and passed to all downstream"),
        ("",                 "services. Allows you to search 'req_123' and see the whole trace."),
    ]:
        print(f"    {row[0]:<17} {row[1]}")

    print("\n  2. METRICS (PROMETHEUS MODEL):")
    for row in [
        ("Counter",   "Number that only goes UP. (e.g., Total Requests, Total Errors)"),
        ("Gauge",     "Number that goes UP and DOWN. (e.g., Active Users, Memory Usage)"),
        ("Histogram", "Groups latency/sizes into buckets. (e.g., 99th percentile latency)"),
        ("Labels",    "Key-value tags on metrics to allow slicing in Grafana (e.g., status='500')"),
    ]:
        print(f"    {row[0]:<17} {row[1]}")

    print()
    print("  Run individual files:")
    print("    python structured_logging.py")
    print("    python metrics.py")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       MONITORING & LOGGING — All Concepts")
    print("=" * 65)

    demo_logs_and_traces()
    demo_metrics()
    print_summary()
