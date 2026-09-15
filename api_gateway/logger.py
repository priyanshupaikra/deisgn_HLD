"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       API GATEWAY — COMPONENT 5                              ║
║                        LOGGING & METRICS COLLECTOR                           ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
Every request that flows through the API Gateway should be LOGGED and its
metrics RECORDED. This provides visibility into:
  - Who is calling what
  - How fast are services responding
  - Which endpoints are most used
  - Error rates by service / client / route
  - Anomaly detection (sudden traffic spikes, unusual error rates)

WHAT GETS LOGGED:
──────────────────
  Per-request log entry (structured JSON in production):
    - Timestamp (ISO-8601)
    - Request ID (for distributed tracing)
    - Client ID + IP address
    - HTTP Method + Path (original and rewritten)
    - Target service name
    - HTTP Response Status Code
    - Response time in milliseconds (latency)
    - Request body size
    - Error details (if applicable)

METRICS COLLECTED:
──────────────────
  1. Request counters:
     - Total requests (by route, service, client, status code)
  2. Latency percentiles:
     - p50 (median), p95, p99, p99.9 latency
     - This is more useful than average (averages hide outliers)
  3. Error rates:
     - 4xx client errors vs 5xx server errors
  4. Throughput:
     - Requests per second (RPS) over a rolling window

LOG LEVELS:
───────────
  DEBUG   → Request/response body details (too verbose for production)
  INFO    → Successful requests (normal flow)
  WARNING → 4xx client errors (bad requests, unauthorized)
  ERROR   → 5xx server errors, circuit breaker trips

WHY STRUCTURED LOGGING MATTERS:
────────────────────────────────
  Human-readable logs are hard to search and aggregate.
  Structured JSON logs can be ingested by:
    - Elasticsearch + Kibana  (ELK stack)
    - Datadog, New Relic, Splunk
    - AWS CloudWatch Logs Insights
    - Google Cloud Logging

  Example structured log:
    {
      "timestamp": "2026-08-12T10:00:00Z",
      "request_id": "abc-123",
      "client_id": "usr_42",
      "method": "GET",
      "path": "/users/42",
      "status": 200,
      "latency_ms": 45.2,
      "service": "user-service"
    }

REAL-WORLD EXAMPLES:
────────────────────
  - AWS API Gateway: access logs to CloudWatch
  - Kong: File Log plugin, HTTP Log plugin
  - NGINX: access.log, error.log
  - Envoy Proxy: gRPC access log service

"""

import time
import json
import statistics
from datetime import datetime, timezone
from collections import defaultdict, deque
from dataclasses import dataclass, field
from router import Request, Response


@dataclass
class LogEntry:
    """
    A single structured log record for one request-response cycle.
    In production this would be serialized to JSON and shipped to a log aggregator.
    """
    timestamp: str
    request_id: str
    client_id: str
    client_ip: str
    method: str
    original_path: str
    routed_path: str
    service_name: str
    status_code: int
    latency_ms: float
    request_size_bytes: int = 0
    error: str = ""
    level: str = "INFO"            # DEBUG, INFO, WARNING, ERROR

    def to_dict(self) -> dict:
        return {
            "timestamp":    self.timestamp,
            "request_id":   self.request_id,
            "client_id":    self.client_id,
            "client_ip":    self.client_ip,
            "method":       self.method,
            "path":         self.routed_path,
            "original_path": self.original_path,
            "service":      self.service_name,
            "status":       self.status_code,
            "latency_ms":   self.latency_ms,
            "level":        self.level,
            "error":        self.error or None,
        }

    def to_log_line(self) -> str:
        """Human-readable single-line log format (similar to Apache/NGINX Combined Log)."""
        err = f" ERROR={self.error}" if self.error else ""
        return (f"[{self.timestamp}] {self.level:<7} "
                f"{self.client_ip:<18} "
                f'"{self.method} {self.original_path}" '
                f"{self.status_code} "
                f"{self.latency_ms:.1f}ms "
                f"svc={self.service_name} "
                f"rid={self.request_id[:8]}"
                f"{err}")


class MetricsCollector:
    """
    In-memory metrics collector for the API Gateway.

    Tracks:
      - Request counts by (service, status_code, method)
      - Latency samples (for percentile computation)
      - Error rates
      - Requests per second (using a sliding time window)

    In production: metrics would be pushed to Prometheus / Datadog / CloudWatch.
    """

    def __init__(self, rps_window_seconds: int = 60):
        # Counters: keyed by (service, status_category, method)
        self.request_counts: dict[str, int] = defaultdict(int)

        # Latency samples: keyed by service name
        self.latency_samples: dict[str, list[float]] = defaultdict(list)

        # Error counters: keyed by service
        self.error_4xx: dict[str, int] = defaultdict(int)
        self.error_5xx: dict[str, int] = defaultdict(int)

        # Sliding window for RPS calculation
        # Stores timestamps of all requests in the last `rps_window_seconds`
        self.rps_window = rps_window_seconds
        self.request_timestamps: deque = deque()

        self.total_requests: int = 0
        self.started_at: float = time.time()

    def record(self, entry: LogEntry) -> None:
        """Record metrics from a completed request."""
        svc = entry.service_name
        code = entry.status_code

        # Increment request count by service + status category
        status_cat = f"{code // 100}xx"    # e.g., "2xx", "4xx", "5xx"
        self.request_counts[f"{svc}:{status_cat}"] += 1
        self.request_counts[f"total:{status_cat}"] += 1
        self.total_requests += 1

        # Record latency sample for this service
        self.latency_samples[svc].append(entry.latency_ms)
        self.latency_samples["all"].append(entry.latency_ms)

        # Track errors
        if 400 <= code < 500:
            self.error_4xx[svc] += 1
        elif code >= 500:
            self.error_5xx[svc] += 1

        # Track timestamp for RPS calculation
        now = time.time()
        self.request_timestamps.append(now)

        # Evict timestamps outside the sliding window
        cutoff = now - self.rps_window
        while self.request_timestamps and self.request_timestamps[0] < cutoff:
            self.request_timestamps.popleft()

    def get_rps(self) -> float:
        """Calculate current requests-per-second over the sliding window."""
        now = time.time()
        cutoff = now - self.rps_window
        # Count timestamps within the window
        recent = sum(1 for ts in self.request_timestamps if ts >= cutoff)
        elapsed = min(self.rps_window, now - self.started_at)
        return round(recent / elapsed, 2) if elapsed > 0 else 0

    def get_latency_percentiles(self, service: str = "all") -> dict:
        """
        Compute latency percentiles for a service (or all services combined).

        Percentiles are far more useful than averages for latency:
          - p50 = median (50% of requests are faster than this)
          - p95 = 95% of requests complete within this time
          - p99 = the "worst" 1% of users' experience
        """
        samples = sorted(self.latency_samples.get(service, []))
        if not samples:
            return {"p50": 0, "p95": 0, "p99": 0, "min": 0, "max": 0, "avg": 0}

        def percentile(data, p):
            if not data:
                return 0
            idx = int(len(data) * p / 100)
            idx = min(idx, len(data) - 1)
            return round(data[idx], 2)

        return {
            "p50":  percentile(samples, 50),
            "p95":  percentile(samples, 95),
            "p99":  percentile(samples, 99),
            "min":  round(min(samples), 2),
            "max":  round(max(samples), 2),
            "avg":  round(statistics.mean(samples), 2),
            "count": len(samples),
        }

    def print_dashboard(self) -> None:
        """Print a summary metrics dashboard."""
        elapsed = time.time() - self.started_at
        print(f"\n  {'─'*55}")
        print(f"  GATEWAY METRICS DASHBOARD  (uptime: {elapsed:.1f}s)")
        print(f"  {'─'*55}")
        print(f"  Total Requests : {self.total_requests}")
        print(f"  RPS            : {self.get_rps()}/s")
        print(f"  4xx Errors     : {sum(self.error_4xx.values())}")
        print(f"  5xx Errors     : {sum(self.error_5xx.values())}")

        # Latency percentiles for all requests
        p = self.get_latency_percentiles("all")
        print(f"\n  Latency (all services):")
        print(f"    p50={p['p50']}ms  p95={p['p95']}ms  p99={p['p99']}ms  "
              f"avg={p['avg']}ms  max={p['max']}ms")

        # Per-service breakdown
        print(f"\n  Per-Service Status Codes:")
        seen_services = set()
        for key, count in sorted(self.request_counts.items()):
            if ":" in key:
                svc, cat = key.split(":", 1)
                if svc != "total":
                    seen_services.add(svc)
                    print(f"    {svc:<22} {cat}: {count}")

        print(f"  {'─'*55}")


class GatewayLogger:
    """
    Structured logger for the API Gateway.

    Collects LogEntry records and feeds them to the MetricsCollector.
    In production, log entries would also be:
      - Written to rotating log files
      - Streamed to a log aggregator (Kafka, Fluentd, Logstash)
      - Sent to APM tools (Datadog, New Relic, Dynatrace)
    """

    def __init__(self, enable_console: bool = True, enable_json: bool = False):
        """
        :param enable_console: Print human-readable logs to stdout
        :param enable_json:    Print JSON-structured logs (for log aggregators)
        """
        self.enable_console = enable_console
        self.enable_json = enable_json
        self.metrics = MetricsCollector()
        self.log_buffer: list[LogEntry] = []   # In-memory log store (for demo)

    def log(self, request: Request, response: Response,
            service_name: str, original_path: str, start_time: float) -> LogEntry:
        """
        Create a structured log entry from a completed request-response cycle.

        Determines log level from status code:
          2xx → INFO
          4xx → WARNING  (client error)
          5xx → ERROR    (server error)

        :param request:       The processed request
        :param response:      The final response
        :param service_name:  The backend service that handled it
        :param original_path: The original URL before any rewriting
        :param start_time:    time.time() when request entered the gateway
        :return:              The created LogEntry
        """
        latency_ms = round((time.time() - start_time) * 1000, 2)
        status = response.status_code

        # Determine log level based on status code
        if status >= 500:
            level = "ERROR"
            error = response.body.get("error", "")
        elif status >= 400:
            level = "WARNING"
            error = response.body.get("message", "")
        else:
            level = "INFO"
            error = ""

        entry = LogEntry(
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            request_id=request.request_id,
            client_id=request.client_id or "anonymous",
            client_ip=request.client_ip,
            method=request.method,
            original_path=original_path,
            routed_path=request.path,
            service_name=service_name or "gateway",
            status_code=status,
            latency_ms=latency_ms,
            error=error,
            level=level,
        )

        # Store and record
        self.log_buffer.append(entry)
        self.metrics.record(entry)

        # Output to console
        if self.enable_console:
            print(f"  {entry.to_log_line()}")

        if self.enable_json:
            print(f"  JSON: {json.dumps(entry.to_dict())}")

        return entry


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import random

    print("=" * 60)
    print("   LOGGING & METRICS — API Gateway Component Demo")
    print("=" * 60)

    logger = GatewayLogger(enable_console=True)

    # Simulate a series of requests with varying latencies and statuses
    scenarios = [
        ("GET",    "/users",          "user-service",    200),
        ("GET",    "/users/42",       "user-service",    200),
        ("POST",   "/orders",         "order-service",   201),
        ("GET",    "/products/p1",    "product-service", 200),
        ("GET",    "/users/999",      "user-service",    404),
        ("POST",   "/orders",         "order-service",   503),
        ("DELETE", "/users/42",       "user-service",    403),
        ("GET",    "/products/p5",    "product-service", 200),
        ("POST",   "/orders",         "order-service",   201),
        ("GET",    "/nonexistent",    "gateway",         404),
    ]

    print(f"\n  Simulating {len(scenarios)} requests:\n")
    for method, path, service, status in scenarios:
        # Simulate random latency (10ms - 500ms)
        latency = random.uniform(0.010, 0.500)
        start = time.time() - latency  # Backdate start to simulate latency

        req = Request(method, path, client_ip="10.0.0.1")
        req.client_id = "usr_42"
        req.request_id = f"req-{random.randint(1000,9999)}"

        body = {"error": "Service Error"} if status >= 400 else {"data": "ok"}
        resp = Response(status, body)

        logger.log(req, resp, service, path, start)

    logger.metrics.print_dashboard()

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY: Every request is logged with structured data.")
    print("  Latency percentiles (p95, p99) reveal real user experience.")
    print("  Metrics feed dashboards, alerts, and auto-scaling decisions.")
    print("=" * 60)
