"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    LOAD BALANCING — ALGORITHM 3                              ║
║                         LEAST CONNECTIONS                                    ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
Least Connections is a DYNAMIC load balancing algorithm that sends each new
request to the server that currently has the FEWEST active (in-flight) connections.

Unlike Round Robin (which ignores real-time state), Least Connections actually
looks at the current workload of each server before making a routing decision.

HOW IT WORKS (Visual):
──────────────────────
  Active connections at time T:
    S1 → [■■■■■]   5 connections
    S2 → [■■]      2 connections   ← LOWEST → new request goes here
    S3 → [■■■]     3 connections

  When a connection FINISHES on any server, that server's count decreases.
  The next new request will again go to whichever has the minimum count.

DYNAMIC NATURE:
───────────────
  Connection lifecycle:
    Request arrives   → server.active_connections += 1
    Request completes → server.active_connections -= 1

  The load balancer always routes to min(active_connections).
  This naturally adapts to slow servers accumulating more connections.

VARIANT — WEIGHTED LEAST CONNECTIONS:
──────────────────────────────────────
  Score = active_connections / weight
  Route to server with lowest score.
  Higher weight = more capacity = can tolerate more connections.

  Example (S1 weight=3, S2 weight=1):
    S1 has 6 connections → score = 6/3 = 2.0
    S2 has 1 connection  → score = 1/1 = 1.0  ← pick S2

ADVANTAGES:
-----------
  ✅ Adapts to real server load (not just static rotation)
  ✅ Naturally handles slow/long-running requests — busy servers get less traffic
  ✅ Performs well when request durations vary significantly
  ✅ Better than Round Robin for long-lived connections (WebSockets, DB connections)

DISADVANTAGES:
--------------
  ❌ Requires tracking connection state → more memory and state
  ❌ Slightly higher overhead per request (must check all servers)
  ❌ Does not account for server CPU/memory — only connection COUNT
     (a server with 2 heavy requests may be worse than one with 5 light ones)
  ❌ Race conditions if not thread-safe (two requests may pick the same server)

REAL-WORLD USE CASE:
--------------------
  - NGINX Plus (least_conn directive)
  - HAProxy (leastconn balance algorithm)
  - Database connection pooling (PgBouncer)
  - Long-lived API connections (file uploads, streaming)
  - Any system where request duration varies widely

"""

import threading
import time
import random
from typing import Optional


class Server:
    """Backend server tracking active connection count."""

    def __init__(self, server_id: str, host: str, port: int, weight: int = 1):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.weight = weight                  # For Weighted Least Connections variant
        self.active_connections = 0           # Current in-flight connections
        self.total_requests = 0               # Lifetime completed requests
        self.is_healthy = True
        self._lock = threading.Lock()         # Per-server lock for atomic updates

    def connect(self) -> None:
        """Increment active connection counter (request starts)."""
        with self._lock:
            self.active_connections += 1
            self.total_requests += 1

    def disconnect(self) -> None:
        """Decrement active connection counter (request completes)."""
        with self._lock:
            self.active_connections = max(0, self.active_connections - 1)

    @property
    def weighted_score(self) -> float:
        """
        Weighted score for Weighted Least Connections.
        Lower score = more desirable server.
        Formula: active_connections / weight
        """
        return self.active_connections / self.weight if self.weight > 0 else float('inf')

    def __repr__(self):
        status = "UP" if self.is_healthy else "DOWN"
        return (f"Server({self.server_id} | {self.host}:{self.port} | "
                f"active={self.active_connections} | total={self.total_requests} | {status})")


class LeastConnectionsLoadBalancer:
    """
    Least Connections Load Balancer.

    Routes each new request to the healthy server with the fewest
    currently active connections.

    Supports both:
      - Standard Least Connections (ignores server weight)
      - Weighted Least Connections (considers weight, enabled via use_weights flag)
    """

    def __init__(self, use_weights: bool = False):
        """
        :param use_weights: If True, use Weighted Least Connections (score = conns/weight)
                            If False, use standard Least Connections (score = conns)
        """
        self.servers: list[Server] = []
        self.use_weights = use_weights
        self.lock = threading.Lock()

    def add_server(self, server: Server) -> None:
        self.servers.append(server)
        print(f"  [+] Added: {server}")

    def mark_unhealthy(self, server_id: str) -> None:
        for s in self.servers:
            if s.server_id == server_id:
                s.is_healthy = False
                print(f"  [!] Server {server_id} marked DOWN")

    def mark_healthy(self, server_id: str) -> None:
        for s in self.servers:
            if s.server_id == server_id:
                s.is_healthy = True
                print(f"  [+] Server {server_id} marked HEALTHY")

    def get_server(self) -> Optional[Server]:
        """
        Select the server with the minimum active connections.

        Algorithm:
          1. Filter to healthy servers only
          2. If weighted mode: score = active_connections / weight
             If standard mode: score = active_connections
          3. Find server with minimum score
          4. Call server.connect() to increment its active count
          5. Return the server to the caller
             (caller must call server.disconnect() when done)

        Thread-safety: A global lock prevents two concurrent requests
        from both seeing the same "minimum" and both picking the same server.

        :return: The chosen Server, or None if all servers are down
        """
        with self.lock:
            healthy = [s for s in self.servers if s.is_healthy]
            if not healthy:
                return None

            # Score function: weighted or plain connection count
            if self.use_weights:
                selected = min(healthy, key=lambda s: s.weighted_score)
            else:
                selected = min(healthy, key=lambda s: s.active_connections)

            # Immediately increment to reflect the new connection
            selected.connect()
            return selected

    def release(self, server: Server) -> None:
        """
        Mark a request as completed on the given server.
        Must be called after the request finishes to decrement active count.
        """
        server.disconnect()

    def print_state(self) -> None:
        """Print current active connection distribution."""
        print("    Current State:", " | ".join(
            f"{s.server_id}={s.active_connections}" for s in self.servers
        ))

    def get_stats(self) -> None:
        print("\n  --- Server Stats ---")
        total = sum(s.total_requests for s in self.servers)
        for s in self.servers:
            pct = (s.total_requests / total * 100) if total > 0 else 0
            bar = "█" * int(pct // 5)
            print(f"    {s.server_id} (w={s.weight}): active={s.active_connections}, "
                  f"total={s.total_requests} ({pct:.1f}%) {bar}")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
def simulate_request(lb: LeastConnectionsLoadBalancer, req_id: int, duration: float):
    """
    Simulate a request with a given processing duration.
    This runs in a separate thread to model concurrent connections.
    """
    server = lb.get_server()
    if not server:
        print(f"    Request #{req_id:02d}: No healthy servers!")
        return
    print(f"    Request #{req_id:02d} → {server.server_id} "
          f"(active={server.active_connections}, duration={duration:.1f}s)")
    time.sleep(duration)    # Simulate request processing time
    lb.release(server)      # MUST release when done


if __name__ == "__main__":
    print("=" * 60)
    print("   LEAST CONNECTIONS — Load Balancer Demo")
    print("=" * 60)

    # ─── Standard Least Connections ───────────────────────────────────────
    print("\n  --- Standard Least Connections ---")
    lb = LeastConnectionsLoadBalancer(use_weights=False)
    lb.add_server(Server("S1", "10.0.0.1", 8080))
    lb.add_server(Server("S2", "10.0.0.2", 8080))
    lb.add_server(Server("S3", "10.0.0.3", 8080))

    print(f"\n  Launching concurrent requests with varying durations:\n")
    threads = []
    durations = [0.5, 2.0, 0.3, 1.5, 0.2, 0.8, 3.0, 0.1, 1.0, 0.4]
    for i, dur in enumerate(durations, 1):
        t = threading.Thread(target=simulate_request, args=(lb, i, dur))
        threads.append(t)
        t.start()
        time.sleep(0.05)   # Small stagger so we can see the effect

    for t in threads:
        t.join()

    lb.get_stats()

    # ─── Weighted Least Connections ────────────────────────────────────────
    print("\n\n  --- Weighted Least Connections ---")
    print("  (S1=weight 3, S2=weight 2, S3=weight 1)\n")
    wlb = LeastConnectionsLoadBalancer(use_weights=True)
    wlb.add_server(Server("S1", "10.0.0.1", 8080, weight=3))
    wlb.add_server(Server("S2", "10.0.0.2", 8080, weight=2))
    wlb.add_server(Server("S3", "10.0.0.3", 8080, weight=1))

    # Sequential requests for clean visibility
    for i in range(1, 13):
        s = wlb.get_server()
        print(f"    Request #{i:02d} → {s.server_id} "
              f"(score={s.weighted_score:.2f}, active={s.active_connections})")
        # Don't release — keep connections open to show accumulation effect

    wlb.get_stats()

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY: Routes to server with fewest active connections.")
    print("  Adapts dynamically to server load — better than Round Robin")
    print("  for workloads with variable request processing times.")
    print("=" * 60)
