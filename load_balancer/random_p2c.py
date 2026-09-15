"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    LOAD BALANCING — ALGORITHM 5                              ║
║                      RANDOM + RANDOM WITH TWO CHOICES                       ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
Random selection is exactly what it sounds like — pick a server at random
for each request. It's simple and requires no state, but its variant —
"Power of Two Random Choices" — is a surprisingly powerful improvement used
in large-scale distributed systems like Twitter, Nginx, and Envoy Proxy.

ALGORITHM 1 — Simple Random:
─────────────────────────────
  Pick any healthy server with equal probability.
  random.choice(healthy_servers)

  No coordination needed between load balancer instances.
  Slightly worse than Round Robin in theory, but in practice nearly identical.

ALGORITHM 2 — Power of Two Random Choices (P2C):
──────────────────────────────────────────────────
  Invented by Michael Mitzenmacher (1996) — a breakthrough in distributed computing.

  Instead of picking 1 random server, pick 2 random servers, then
  CHOOSE THE BETTER ONE based on a metric (e.g., least active connections).

  Pseudo-code:
    s1 = random_server()
    s2 = random_server()  (different from s1)
    return s1 if s1.connections < s2.connections else s2

WHY DOES P2C WORK SO WELL?
────────────────────────────
  Mathematical proof shows that picking 2 random choices and taking the
  better one reduces the MAXIMUM LOAD on any server from:
    O(log N / log log N)   → Simple Random (where N = servers)
  to:
    O(log log N)           → Power of Two Choices

  This is an EXPONENTIAL improvement in load balance quality!

  Example: 1000 servers, Simple Random:   worst server might have ~14x avg load
                          Power of Two:   worst server might have ~3.3x avg load

  The intuition: even though we only look at 2 servers, the "greedy" choice
  of picking the least loaded of those 2 dramatically flattens the load curve.

ADVANTAGES:
-----------
  ✅ No shared state needed (load balancer instances don't coordinate)
  ✅ P2C is near-optimal for dynamic load balancing with minimal overhead
  ✅ Scales to massive distributed systems (Nginx, Envoy, Twitter)
  ✅ Very resilient — no single point of failure
  ✅ Simple to implement, surprisingly effective

DISADVANTAGES:
--------------
  ❌ Simple Random: No guarantee of even distribution (can cluster requests)
  ❌ No session affinity (different from IP Hash)
  ❌ Requires querying active_connections metric from servers (for P2C)

REAL-WORLD USE CASE:
--------------------
  - Twitter's Finagle load balancer: uses P2C (Least Loaded variant)
  - Envoy Proxy: least_request + P2C as default algorithm
  - NGINX Plus random_two directive
  - HAProxy random load balancing mode
  - Distributed hash tables and peer-to-peer networks

"""

import random
import threading
from typing import Optional


class Server:
    """Backend server with connection tracking."""

    def __init__(self, server_id: str, host: str, port: int):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.active_connections = 0
        self.total_requests = 0
        self.is_healthy = True
        self._lock = threading.Lock()

    def connect(self) -> None:
        """Increment active connections (request starts)."""
        with self._lock:
            self.active_connections += 1
            self.total_requests += 1

    def disconnect(self) -> None:
        """Decrement active connections (request done)."""
        with self._lock:
            self.active_connections = max(0, self.active_connections - 1)

    def __repr__(self):
        return (f"Server({self.server_id} | {self.host}:{self.port} | "
                f"active={self.active_connections} | total={self.total_requests})")


class RandomLoadBalancer:
    """
    Simple Random Load Balancer.

    Picks a uniformly random healthy server for each request.
    No state, no coordination — every request is independent.
    """

    def __init__(self):
        self.servers: list[Server] = []
        self.lock = threading.Lock()

    def add_server(self, server: Server) -> None:
        self.servers.append(server)
        print(f"  [+] Added: {server}")

    def get_server(self) -> Optional[Server]:
        """
        Randomly select any healthy server.
        Uses Python's random.choice — uniform distribution over healthy pool.
        """
        with self.lock:
            healthy = [s for s in self.servers if s.is_healthy]
            if not healthy:
                return None
            selected = random.choice(healthy)
            selected.total_requests += 1
            return selected

    def get_stats(self) -> None:
        print("\n  [Simple Random] Server Stats:")
        total = sum(s.total_requests for s in self.servers)
        for s in self.servers:
            pct = (s.total_requests / total * 100) if total > 0 else 0
            bar = "█" * int(pct // 5)
            print(f"    {s.server_id}: {s.total_requests:4d} reqs ({pct:.1f}%) {bar}")


class PowerOfTwoChoicesLoadBalancer:
    """
    Power of Two Random Choices (P2C) Load Balancer.

    For each request:
      1. Pick 2 distinct healthy servers at random
      2. Compare their active_connections
      3. Route to the one with fewer connections

    This achieves near-optimal load balance without any global coordination,
    making it ideal for distributed load balancers and service meshes.

    Used by Twitter Finagle, Envoy Proxy, and NGINX Plus.
    """

    def __init__(self):
        self.servers: list[Server] = []
        self.lock = threading.Lock()

    def add_server(self, server: Server) -> None:
        self.servers.append(server)
        print(f"  [+] Added: {server}")

    def mark_unhealthy(self, server_id: str) -> None:
        for s in self.servers:
            if s.server_id == server_id:
                s.is_healthy = False

    def mark_healthy(self, server_id: str) -> None:
        for s in self.servers:
            if s.server_id == server_id:
                s.is_healthy = True

    def get_server(self) -> Optional[Server]:
        """
        Power of Two Choices selection.

        Steps:
          1. Get the healthy server pool
          2. If only 1 server: use it directly
          3. Randomly sample 2 distinct servers from the pool
          4. Compare their active_connections
          5. Pick the one with FEWER active connections (the better choice)
          6. Increment its active connection count

        :return: The selected server (caller must call release() when done)
        """
        with self.lock:
            healthy = [s for s in self.servers if s.is_healthy]
            if not healthy:
                return None

            # Edge case: only one server available
            if len(healthy) == 1:
                selected = healthy[0]
            else:
                # Step 3: Sample 2 distinct random candidates
                # random.sample guarantees no duplicates
                candidates = random.sample(healthy, 2)
                s1, s2 = candidates[0], candidates[1]

                # Step 4 & 5: Pick the one with fewer active connections
                # Ties broken randomly (compare in the order sampled)
                selected = s1 if s1.active_connections <= s2.active_connections else s2

            # Step 6: Register the new connection
            selected.connect()
            return selected

    def release(self, server: Server) -> None:
        """Mark request as completed (decrement active connections)."""
        server.disconnect()

    def get_stats(self) -> None:
        print("\n  [Power of Two Choices] Server Stats:")
        total = sum(s.total_requests for s in self.servers)
        for s in self.servers:
            pct = (s.total_requests / total * 100) if total > 0 else 0
            bar = "█" * int(pct // 5)
            print(f"    {s.server_id}: {s.total_requests:4d} reqs ({pct:.1f}%) {bar}")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("   RANDOM + POWER OF TWO CHOICES — Load Balancer Demo")
    print("=" * 60)

    # ─── Simple Random ─────────────────────────────────────────────────────
    print("\n  --- Simple Random ---")
    rand_lb = RandomLoadBalancer()
    rand_lb.add_server(Server("S1", "10.0.0.1", 8080))
    rand_lb.add_server(Server("S2", "10.0.0.2", 8080))
    rand_lb.add_server(Server("S3", "10.0.0.3", 8080))

    print(f"\n  Sending 15 requests:\n")
    for i in range(1, 16):
        s = rand_lb.get_server()
        print(f"    Request #{i:02d} → {s.server_id}")

    rand_lb.get_stats()

    # ─── Power of Two Choices ──────────────────────────────────────────────
    print("\n\n  --- Power of Two Random Choices (P2C) ---")
    print("  (Simulates concurrent connections with different active loads)\n")

    p2c_lb = PowerOfTwoChoicesLoadBalancer()
    p2c_lb.add_server(Server("S1", "10.0.0.1", 8080))
    p2c_lb.add_server(Server("S2", "10.0.0.2", 8080))
    p2c_lb.add_server(Server("S3", "10.0.0.3", 8080))

    # Manually set different active loads to show P2C's intelligence
    # S1 is already busy with 8 connections, S2 with 5, S3 with 1
    for _ in range(8): p2c_lb.servers[0].connect()   # Preload S1
    for _ in range(5): p2c_lb.servers[1].connect()   # Preload S2
    for _ in range(1): p2c_lb.servers[2].connect()   # Preload S3

    print(f"  Initial load: S1=8 connections, S2=5 connections, S3=1 connection")
    print(f"  P2C should strongly prefer S3 (least loaded):\n")

    selected_counts = {"S1": 0, "S2": 0, "S3": 0}
    for i in range(1, 16):
        s = p2c_lb.get_server()
        selected_counts[s.server_id] += 1
        print(f"    Request #{i:02d} → {s.server_id} "
              f"(active={s.active_connections})")
        p2c_lb.release(s)   # Release so connections drain back

    p2c_lb.get_stats()

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY:")
    print("  Simple Random: uniform, no coordination needed.")
    print("  P2C: exponentially better load balance than Simple Random")
    print("       by sampling just 2 servers. Used by Envoy, Twitter Finagle.")
    print("=" * 60)
