"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                   MONITORING & LOGGING — MODULE 2                            ║
║                METRICS (Counters, Gauges, Histograms)                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT ARE METRICS?
─────────────────
While Logs tell the "Story", Metrics give you the "Vital Signs".
They are numeric representations of data measured over time.

Instead of writing a log for every single HTTP request (which costs disk space
and search compute), you increment a number in memory.
Every 15 seconds, a system like PROMETHEUS "scrapes" (pulls) these numbers.

THE 3 CORE METRIC TYPES (Prometheus Model):
───────────────────────────────────────────
  1. COUNTER:
     - A number that ONLY goes UP (or resets to 0 on restart).
     - Use for: Total HTTP requests, Total Errors, Total bytes sent.
     - You usually query the RATE of a counter (e.g., requests per second).

  2. GAUGE:
     - A number that can go UP or DOWN.
     - Use for: Current active users, Current memory usage, Queue depth.

  3. HISTOGRAM (or Summary / Timer):
     - Tracks the distribution of values (sizes or durations) into "Buckets".
     - Use for: Request Latency (how many requests took <10ms, <50ms, <100ms?)
     - Allows calculating Percentiles (p95, p99 latency).

DIMENSIONS (LABELS / TAGS):
───────────────────────────
Metrics can have labels to slice and dice the data.
Instead of one metric: `http_requests_total`
You use labels: `http_requests_total{method="GET", endpoint="/api/users", status="200"}`
This allows Grafana dashboards to show errors per endpoint.
"""

import time
import random
from collections import defaultdict


# ─────────────────────────────────────────────────────────────────────────────
# 1. COUNTER
# ─────────────────────────────────────────────────────────────────────────────

class Counter:
    """A metric that only goes up."""
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        # Store values based on labels: { "method=GET,status=200": 5 }
        self._values = defaultdict(int)

    def inc(self, amount: int = 1, **labels):
        """Increment the counter. Cannot be negative."""
        if amount < 0: raise ValueError("Counters can only increase")
        
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        self._values[label_str] += amount

    def get_snapshot(self) -> dict:
        return dict(self._values)


# ─────────────────────────────────────────────────────────────────────────────
# 2. GAUGE
# ─────────────────────────────────────────────────────────────────────────────

class Gauge:
    """A metric that can go up and down."""
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self._values = defaultdict(float)

    def set(self, value: float, **labels):
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        self._values[label_str] = value

    def inc(self, amount: float = 1.0, **labels):
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        self._values[label_str] += amount

    def dec(self, amount: float = 1.0, **labels):
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        self._values[label_str] -= amount

    def get_snapshot(self) -> dict:
        return dict(self._values)


# ─────────────────────────────────────────────────────────────────────────────
# 3. HISTOGRAM
# ─────────────────────────────────────────────────────────────────────────────

class Histogram:
    """
    Groups observations (like latency in seconds) into predefined buckets.
    """
    def __init__(self, name: str, description: str, buckets: list[float]):
        self.name = name
        self.description = description
        # Ensure buckets are sorted and end with infinity
        self.buckets = sorted(buckets)
        if self.buckets[-1] != float('inf'):
            self.buckets.append(float('inf'))
            
        # Structure: { label_str: { bucket_limit: count } }
        self._counts = defaultdict(lambda: defaultdict(int))
        self._sum = defaultdict(float)
        self._count = defaultdict(int)

    def observe(self, value: float, **labels):
        """Record a new value (e.g., request took 0.04 seconds)."""
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        
        self._sum[label_str] += value
        self._count[label_str] += 1
        
        # Prometheus cumulative buckets: 
        # If value is 0.04, it increments the <=0.05 bucket, <=0.1 bucket, etc.
        for bucket_limit in self.buckets:
            if value <= bucket_limit:
                self._counts[label_str][bucket_limit] += 1

    def get_snapshot(self) -> dict:
        """Format similarly to how Prometheus exposes it."""
        snapshot = {}
        for label_str in self._count.keys():
            snapshot[label_str] = {
                "count": self._count[label_str],
                "sum": self._sum[label_str],
                "buckets": {f"le_{b}": self._counts[label_str][b] for b in self.buckets}
            }
        return snapshot


# ─────────────────────────────────────────────────────────────────────────────
# SIMULATED APPLICATION EXPOSING METRICS
# ─────────────────────────────────────────────────────────────────────────────

class WebServerMetrics:
    def __init__(self):
        # 1. Counter for total requests
        self.requests_total = Counter(
            "http_requests_total", 
            "Total number of HTTP requests"
        )
        
        # 2. Gauge for currently active requests
        self.active_requests = Gauge(
            "http_active_requests", 
            "Number of currently executing HTTP requests"
        )
        
        # 3. Histogram for latency (buckets in seconds: 10ms, 50ms, 100ms, 500ms)
        self.request_duration_seconds = Histogram(
            "http_request_duration_seconds",
            "HTTP request latency in seconds",
            buckets=[0.01, 0.05, 0.1, 0.5]
        )

    def simulate_traffic(self, num_requests: int):
        print(f"  [Simulating {num_requests} web requests...]\n")
        
        for _ in range(num_requests):
            endpoint = random.choice(["/home", "/api/users", "/api/checkout"])
            method = "GET" if endpoint != "/api/checkout" else "POST"
            status = random.choice([200, 200, 200, 200, 404, 500])
            
            # Start request
            self.active_requests.inc(endpoint=endpoint)
            
            # Simulate processing time (faster for /home, slower for /checkout)
            base_latency = 0.02 if endpoint == "/home" else 0.08
            latency = base_latency + random.uniform(0, 0.1)
            if status == 500: latency += 0.4 # Errors take longer
            
            # End request (Record metrics)
            self.active_requests.dec(endpoint=endpoint)
            self.requests_total.inc(method=method, endpoint=endpoint, status=status)
            self.request_duration_seconds.observe(latency, method=method, endpoint=endpoint)

    def scrape_metrics_endpoint(self):
        """
        Simulates the /metrics endpoint that Prometheus scrapes every 15s.
        """
        print("=" * 50)
        print(" GET /metrics (Prometheus Scrape) ")
        print("=" * 50)
        
        print(f"# HELP {self.requests_total.name} {self.requests_total.description}")
        print(f"# TYPE {self.requests_total.name} counter")
        for labels, val in self.requests_total.get_snapshot().items():
            print(f"{self.requests_total.name}{{{labels}}} {val}")
            
        print(f"\n# HELP {self.active_requests.name} {self.active_requests.description}")
        print(f"# TYPE {self.active_requests.name} gauge")
        for labels, val in self.active_requests.get_snapshot().items():
            print(f"{self.active_requests.name}{{{labels}}} {val}")
            
        print(f"\n# HELP {self.request_duration_seconds.name} {self.request_duration_seconds.description}")
        print(f"# TYPE {self.request_duration_seconds.name} histogram")
        for labels, data in self.request_duration_seconds.get_snapshot().items():
            # Prometheus histogram format
            for bucket_key, bucket_val in data["buckets"].items():
                b_limit = bucket_key.replace("le_", "")
                print(f"{self.request_duration_seconds.name}_bucket{{{labels},le=\"{b_limit}\"}} {bucket_val}")
            print(f"{self.request_duration_seconds.name}_sum{{{labels}}} {data['sum']:.4f}")
            print(f"{self.request_duration_seconds.name}_count{{{labels}}} {data['count']}")
        print("=" * 50)


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   METRICS (Prometheus Style) — Demo")
    print("=" * 65)

    server = WebServerMetrics()
    
    # Let's say there are currently 5 users connected and downloading a big file
    server.active_requests.set(5, endpoint="/downloads")
    
    # Simulate a burst of traffic
    server.simulate_traffic(15)
    
    # Scrape the metrics (This is what Prometheus reads)
    server.scrape_metrics_endpoint()
