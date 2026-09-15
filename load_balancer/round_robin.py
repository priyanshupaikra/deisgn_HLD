"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    LOAD BALANCING — ALGORITHM 1                              ║
║                           ROUND ROBIN                                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
Round Robin is the simplest and most widely used load balancing algorithm.
Requests are distributed to servers in a circular, sequential order.
Every server gets exactly one request before the cycle repeats.

HOW IT WORKS (Visual):
──────────────────────
  Servers:   [S1]  [S2]  [S3]
  Request 1   →    S1
  Request 2        →    S2
  Request 3              →    S3
  Request 4   →    S1   (cycle restarts)
  Request 5        →    S2
  ...

  The pointer (index) moves one step forward with each request.
  When it reaches the last server, it wraps back to the first.

KEY ASSUMPTION:
───────────────
  All servers are assumed to be IDENTICAL in capacity and capability.
  Round Robin does NOT account for differences in server load or power.
  → If servers have different capacities, use Weighted Round Robin instead.

ADVANTAGES:
-----------
  ✅ Extremely simple to implement (just a rotating pointer)
  ✅ Distributes load evenly across all servers
  ✅ Predictable and fair — each server gets equal share
  ✅ No per-request state needed (just a global counter)
  ✅ Very low overhead

DISADVANTAGES:
--------------
  ❌ Does NOT account for varying server capacities
  ❌ Does NOT account for current server load (a slow request on S1 may
     already be processing 100 connections, but Round Robin still sends to it)
  ❌ Can be suboptimal for long-lived connections (e.g., WebSockets)
  ❌ No session stickiness (same client may hit different servers)

REAL-WORLD USE CASE:
--------------------
  - Default mode in NGINX upstream load balancing
  - HAProxy default configuration
  - DNS Round Robin (multiple IPs for one domain)
  - Kubernetes Service default load balancing

"""

import threading
from typing import Optional


class Server:
    """Represents a backend server in the pool."""

    def __init__(self, server_id: str, host: str, port: int):
        """
        :param server_id: Unique label (e.g., "S1", "web-01")
        :param host:      IP or hostname of the server
        :param port:      Port the server listens on
        """
        self.server_id = server_id
        self.host = host
        self.port = port
        self.is_healthy = True        # Health status flag
        self.total_requests = 0       # Lifetime request count (for stats)

    def __repr__(self):
        status = "HEALTHY" if self.is_healthy else "DOWN"
        return f"Server({self.server_id} | {self.host}:{self.port} | {status} | reqs={self.total_requests})"


class RoundRobinLoadBalancer:
    """
    Round Robin Load Balancer.

    Maintains a circular pointer over the list of healthy servers.
    Each call to get_server() advances the pointer by one.

    Attributes:
        servers  : List of all registered Server objects
        index    : Current position in the rotation (the "pointer")
        lock     : Thread lock for safe concurrent access
    """

    def __init__(self):
        self.servers: list[Server] = []
        self.index: int = 0           # Rotating pointer
        self.lock = threading.Lock()  # Thread-safe pointer advancement

    def add_server(self, server: Server) -> None:
        """Register a new server into the pool."""
        self.servers.append(server)
        print(f"  [+] Added: {server}")

    def remove_server(self, server_id: str) -> None:
        """Remove a server from the pool by its ID."""
        self.servers = [s for s in self.servers if s.server_id != server_id]
        # Clamp the index to avoid IndexError after removal
        if self.servers:
            self.index = self.index % len(self.servers)
        print(f"  [-] Removed server: {server_id}")

    def _get_healthy_servers(self) -> list[Server]:
        """Return only servers that are marked as healthy."""
        return [s for s in self.servers if s.is_healthy]

    def get_server(self) -> Optional[Server]:
        """
        Select the next server using round-robin rotation.

        Algorithm:
          1. Filter to only HEALTHY servers
          2. If none available → return None (all servers down)
          3. Pick server at current index (pointer)
          4. Advance the pointer by 1 (wrap around with modulo)
          5. Record the request on the chosen server

        Thread-safe: uses a lock to prevent race conditions on self.index
        when multiple threads call this simultaneously.

        :return: The selected Server object, or None if no healthy servers
        """
        with self.lock:
            healthy = self._get_healthy_servers()
            if not healthy:
                return None  # All servers are down — return None

            # Clamp index in case healthy list is smaller than self.index
            self.index = self.index % len(healthy)

            # Pick the server at the current pointer position
            selected = healthy[self.index]

            # Advance the pointer (wrap around using modulo)
            self.index = (self.index + 1) % len(healthy)

            # Track total requests for stats
            selected.total_requests += 1
            return selected

    def mark_unhealthy(self, server_id: str) -> None:
        """Simulate a server going down (health check failure)."""
        for s in self.servers:
            if s.server_id == server_id:
                s.is_healthy = False
                print(f"  [!] Server {server_id} marked as DOWN")

    def mark_healthy(self, server_id: str) -> None:
        """Simulate a server recovering."""
        for s in self.servers:
            if s.server_id == server_id:
                s.is_healthy = True
                print(f"  [+] Server {server_id} marked as HEALTHY")

    def get_stats(self) -> None:
        """Print request distribution across all servers."""
        print("\n  --- Server Stats ---")
        total = sum(s.total_requests for s in self.servers)
        for s in self.servers:
            pct = (s.total_requests / total * 100) if total > 0 else 0
            bar = "█" * int(pct // 5)
            print(f"    {s.server_id}: {s.total_requests:4d} requests ({pct:5.1f}%) {bar}")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("   ROUND ROBIN — Load Balancer Demo")
    print("=" * 60)

    lb = RoundRobinLoadBalancer()

    # Register 3 backend servers
    lb.add_server(Server("S1", "192.168.1.10", 8080))
    lb.add_server(Server("S2", "192.168.1.11", 8080))
    lb.add_server(Server("S3", "192.168.1.12", 8080))

    print(f"\n  Sending 9 requests (expect 3 each):\n")
    for i in range(1, 10):
        server = lb.get_server()
        print(f"    Request #{i:02d} → {server.server_id} ({server.host}:{server.port})")

    lb.get_stats()

    # Simulate a server going down
    print(f"\n  Simulating S2 going DOWN...")
    lb.mark_unhealthy("S2")

    print(f"\n  Sending 6 more requests (S1 and S3 only):\n")
    for i in range(10, 16):
        server = lb.get_server()
        print(f"    Request #{i:02d} → {server.server_id} ({server.host}:{server.port})")

    lb.get_stats()

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY: Equal rotation, no intelligence about load.")
    print("  Best when all servers are identical in capacity.")
    print("=" * 60)
