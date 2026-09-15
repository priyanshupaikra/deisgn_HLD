"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    LOAD BALANCING — ALGORITHM 2                              ║
║                        WEIGHTED ROUND ROBIN                                  ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
Weighted Round Robin extends Round Robin by assigning a WEIGHT to each server
that reflects its relative capacity. A server with a higher weight receives
proportionally MORE requests than a server with a lower weight.

This solves Round Robin's core weakness: assuming all servers are equal.

HOW IT WORKS (Visual):
──────────────────────
  Server weights:  S1=3,  S2=2,  S3=1   (total = 6 slots)

  Expanded rotation:  [S1, S1, S1, S2, S2, S3]
                        ↑
                      pointer rotates through these 6 slots

  Request 1 → S1
  Request 2 → S1
  Request 3 → S1
  Request 4 → S2
  Request 5 → S2
  Request 6 → S3
  Request 7 → S1  (cycle restarts)
  ...

  Over 6 requests: S1 handles 50%, S2 handles 33%, S3 handles 17%

TWO COMMON IMPLEMENTATIONS:
────────────────────────────
  1. Expanded List (used here):
     Pre-generate a rotation list by repeating each server `weight` times.
     Simple but wastes memory for large weights.

  2. Smooth Weighted Round Robin (NGINX's implementation):
     Dynamic weight tracking — no pre-generated list, avoids bunching.
     In each step, pick the server with highest "current_weight",
     then subtract total_weight from it.
     → Produces smoother distribution: S1,S2,S1,S3,S1,S2 instead of S1,S1,S1,S2,S2,S3

  This file implements BOTH — simple expanded list + smooth SWRR.

ADVANTAGES:
-----------
  ✅ Servers with more capacity (CPU, RAM) get proportionally more traffic
  ✅ Still simple to understand and implement
  ✅ Smooth WRR eliminates server bunching

DISADVANTAGES:
--------------
  ❌ Still does NOT account for real-time server load
     (a powerful server that's currently overloaded still gets more requests)
  ❌ Weights must be configured manually; can become stale over time
  ❌ Not ideal if request processing times vary greatly

REAL-WORLD USE CASE:
--------------------
  - NGINX Upstream weighted load balancing  (weight= directive)
  - HAProxy weighted server pools
  - Cloud load balancers with heterogeneous VM sizes
  - Gradual traffic shifting (canary deployments: old=90, new=10)

"""

import threading
from typing import Optional


class WeightedServer:
    """Backend server with a weight reflecting its relative capacity."""

    def __init__(self, server_id: str, host: str, port: int, weight: int):
        """
        :param weight: Relative capacity (e.g., weight=3 means 3x the traffic of weight=1)
        """
        self.server_id = server_id
        self.host = host
        self.port = port
        self.weight = weight          # Configured weight
        self.current_weight = 0       # Used by Smooth WRR algorithm
        self.is_healthy = True
        self.total_requests = 0

    def __repr__(self):
        return (f"WeightedServer({self.server_id} | {self.host}:{self.port} "
                f"| weight={self.weight} | reqs={self.total_requests})")


# ─────────────────────────────────────────────────────────────────────────────
# IMPLEMENTATION 1: Simple Weighted Round Robin (Expanded List)
# ─────────────────────────────────────────────────────────────────────────────
class WeightedRoundRobinSimple:
    """
    Simple Weighted Round Robin using a pre-expanded rotation list.

    Build a list like [S1, S1, S1, S2, S2, S3] and rotate through it.
    Intuitive but not smooth — servers get requests in big blocks.
    """

    def __init__(self):
        self.servers: list[WeightedServer] = []
        self._rotation: list[WeightedServer] = []  # Expanded rotation list
        self.index = 0
        self.lock = threading.Lock()

    def add_server(self, server: WeightedServer) -> None:
        """Add server and rebuild the rotation list."""
        self.servers.append(server)
        self._rebuild_rotation()

    def _rebuild_rotation(self) -> None:
        """
        Rebuild the expanded rotation list whenever servers change.

        Each server appears `weight` times in the list.
        Example: weight=[3,2,1] → [S1,S1,S1,S2,S2,S3]
        """
        self._rotation = []
        for s in self.servers:
            if s.is_healthy:
                self._rotation.extend([s] * s.weight)  # Repeat server `weight` times
        self.index = 0  # Reset pointer

    def get_server(self) -> Optional[WeightedServer]:
        """Select next server from the weighted rotation list."""
        with self.lock:
            if not self._rotation:
                return None
            server = self._rotation[self.index]
            self.index = (self.index + 1) % len(self._rotation)
            server.total_requests += 1
            return server

    def get_stats(self) -> None:
        print("\n  [Simple WRR] Server Stats:")
        total = sum(s.total_requests for s in self.servers)
        for s in self.servers:
            pct = (s.total_requests / total * 100) if total > 0 else 0
            bar = "█" * int(pct // 5)
            print(f"    {s.server_id} (w={s.weight}): {s.total_requests:4d} reqs "
                  f"({pct:5.1f}%) {bar}")


# ─────────────────────────────────────────────────────────────────────────────
# IMPLEMENTATION 2: Smooth Weighted Round Robin (NGINX-style)
# ─────────────────────────────────────────────────────────────────────────────
class SmoothWeightedRoundRobin:
    """
    Smooth Weighted Round Robin (SWRR) — NGINX's algorithm.

    No pre-generated list. Each request dynamically picks the server with
    the highest current_weight, then penalizes it by total_weight.

    Algorithm per request:
      1. For each server: current_weight += weight
      2. Pick server with highest current_weight
      3. Subtract total_weight from selected server's current_weight

    This produces smooth interleaving: S1,S2,S1,S3,S1,S2 instead of
    S1,S1,S1,S2,S2,S3 — reducing hot-spots on any single server.

    Example trace (weights: S1=3, S2=2, S3=1, total=6):
      Step 1: current=[3,2,1] → pick S1 → current=[3-6,2,1]=[-3,2,1]
      Step 2: current=[0,4,2] → pick S2 → current=[0,4-6,2]=[0,-2,2]
      Step 3: current=[3,0,3] → pick S1 → current=[-3,0,3]
      Step 4: current=[0,2,4] → pick S3 → current=[0,2,-2]
      Step 5: current=[3,4,0] → pick S2 → current=[3,-2,0]
      Step 6: current=[6,0,1] → pick S1 → back to start
    """

    def __init__(self):
        self.servers: list[WeightedServer] = []
        self.lock = threading.Lock()

    def add_server(self, server: WeightedServer) -> None:
        server.current_weight = 0
        self.servers.append(server)

    def _total_weight(self) -> int:
        return sum(s.weight for s in self.servers if s.is_healthy)

    def get_server(self) -> Optional[WeightedServer]:
        """
        SWRR selection:
          1. Increment every server's current_weight by its configured weight
          2. Pick the server with the maximum current_weight
          3. Deduct total_weight from the chosen server's current_weight
        """
        with self.lock:
            healthy = [s for s in self.servers if s.is_healthy]
            if not healthy:
                return None

            total = self._total_weight()

            # Step 1: Bump everyone's current weight
            for s in healthy:
                s.current_weight += s.weight

            # Step 2: Pick server with highest current_weight
            selected = max(healthy, key=lambda s: s.current_weight)

            # Step 3: Penalize selected server by total weight (smoothing step)
            selected.current_weight -= total

            selected.total_requests += 1
            return selected

    def get_stats(self) -> None:
        print("\n  [Smooth WRR] Server Stats:")
        total = sum(s.total_requests for s in self.servers)
        for s in self.servers:
            pct = (s.total_requests / total * 100) if total > 0 else 0
            bar = "█" * int(pct // 5)
            print(f"    {s.server_id} (w={s.weight}): {s.total_requests:4d} reqs "
                  f"({pct:5.1f}%) {bar}")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("   WEIGHTED ROUND ROBIN — Load Balancer Demo")
    print("=" * 60)
    print("  Weights: S1=3 (powerful), S2=2 (medium), S3=1 (weak)\n")

    # ─── Simple WRR ───────────────────────────────────────────────────────
    print("  --- Simple WRR (block distribution) ---")
    simple_lb = WeightedRoundRobinSimple()
    simple_lb.add_server(WeightedServer("S1", "10.0.0.1", 8080, weight=3))
    simple_lb.add_server(WeightedServer("S2", "10.0.0.2", 8080, weight=2))
    simple_lb.add_server(WeightedServer("S3", "10.0.0.3", 8080, weight=1))

    print(f"\n  Sending 12 requests:\n")
    for i in range(1, 13):
        s = simple_lb.get_server()
        print(f"    Request #{i:02d} → {s.server_id}")

    simple_lb.get_stats()

    # ─── Smooth WRR ───────────────────────────────────────────────────────
    print("\n\n  --- Smooth WRR (NGINX-style, interleaved) ---")
    smooth_lb = SmoothWeightedRoundRobin()
    smooth_lb.add_server(WeightedServer("S1", "10.0.0.1", 8080, weight=3))
    smooth_lb.add_server(WeightedServer("S2", "10.0.0.2", 8080, weight=2))
    smooth_lb.add_server(WeightedServer("S3", "10.0.0.3", 8080, weight=1))

    print(f"\n  Sending 12 requests:\n")
    for i in range(1, 13):
        s = smooth_lb.get_server()
        print(f"    Request #{i:02d} → {s.server_id}")

    smooth_lb.get_stats()

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY: Weights control traffic proportion.")
    print("  Smooth WRR avoids bunching (S1,S2,S1,S3 vs S1,S1,S1,S2,S2,S3).")
    print("  Perfect for heterogeneous server fleets or canary deployments.")
    print("=" * 60)
