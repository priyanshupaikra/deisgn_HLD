"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                        LOAD BALANCER SUITE                                   ║
║                  All 5 Algorithms — Main Runner                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WHAT IS LOAD BALANCING?                                                     ║
║  ────────────────────────                                                    ║
║  Load balancing distributes incoming network traffic across multiple         ║
║  backend servers to:                                                         ║
║    • Prevent any single server from being overloaded                         ║
║    • Maximize throughput and minimize response latency                       ║
║    • Ensure high availability (if one server fails, others serve traffic)    ║
║    • Enable horizontal scaling (add more servers to handle more load)        ║
║                                                                              ║
║  ALGORITHMS COVERED:                                                         ║
║  ───────────────────                                                         ║
║  1. Round Robin          — Sequential rotation across servers                ║
║  2. Weighted Round Robin — Proportional distribution by server capacity      ║
║  3. Least Connections    — Always route to least-busy server (dynamic)       ║
║  4. IP Hash / Consistent — Same client always hits the same server           ║
║  5. Random / P2C         — Random pick + Power of Two Choices variant        ║
║                                                                              ║
║  COMPARISON TABLE:                                                           ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  Algorithm          | Dynamic? | Session | Complexity | Best For            ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  Round Robin        | No       | No      | Trivial    | Uniform, short reqs ║
║  Weighted RR        | No       | No      | Simple     | Heterogeneous pool  ║
║  Least Connections  | Yes      | No      | Medium     | Long/variable reqs  ║
║  IP Hash            | No       | Yes     | Simple     | Session affinity    ║
║  Consistent Hash    | Partial  | Yes     | Medium     | Distributed caches  ║
║  Random             | No       | No      | Trivial    | Stateless, simple   ║
║  P2C                | Yes      | No      | Simple     | Large-scale systems ║
║                                                                              ║
║  HOW TO RUN INDIVIDUAL ALGORITHMS:                                           ║
║    python round_robin.py                                                     ║
║    python weighted_round_robin.py                                            ║
║    python least_connections.py                                               ║
║    python ip_hash_consistent.py                                              ║
║    python random_p2c.py                                                      ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os

# Add current directory to path so imports work
sys.path.insert(0, os.path.dirname(__file__))

# ─── Import all implementations ───────────────────────────────────────────────
from round_robin import RoundRobinLoadBalancer, Server as RRServer
from weighted_round_robin import WeightedRoundRobinSimple, SmoothWeightedRoundRobin, WeightedServer
from least_connections import LeastConnectionsLoadBalancer, Server as LCServer
from ip_hash_consistent import IPHashLoadBalancer, ConsistentHashLoadBalancer, Server as IHServer
from random_p2c import RandomLoadBalancer, PowerOfTwoChoicesLoadBalancer, Server as RandServer


def section(title: str) -> None:
    print(f"\n{'═' * 58}")
    print(f"  {title}")
    print(f"{'═' * 58}")


def subsection(algo: str, requests: int) -> None:
    print(f"\n  ─── {algo} ({requests} requests) ───")


# ─────────────────────────────────────────────────────────────────────────────
# 1. ROUND ROBIN
# ─────────────────────────────────────────────────────────────────────────────
def demo_round_robin():
    section("ALGORITHM 1: ROUND ROBIN")
    print("  Concept: Sequential rotation — each server takes turns in order.")
    print("  Best for: identical servers, uniform short-lived requests.\n")

    lb = RoundRobinLoadBalancer()
    lb.add_server(RRServer("S1", "10.0.0.1", 8080))
    lb.add_server(RRServer("S2", "10.0.0.2", 8080))
    lb.add_server(RRServer("S3", "10.0.0.3", 8080))

    for i in range(1, 10):
        s = lb.get_server()
        print(f"    Request #{i:02d} → {s.server_id}")

    lb.get_stats()


# ─────────────────────────────────────────────────────────────────────────────
# 2. WEIGHTED ROUND ROBIN
# ─────────────────────────────────────────────────────────────────────────────
def demo_weighted_round_robin():
    section("ALGORITHM 2: WEIGHTED ROUND ROBIN")
    print("  Concept: Proportional rotation by server weight/capacity.")
    print("  Best for: heterogeneous server pools, canary deployments.\n")
    print("  Weights → S1=3 (powerful), S2=2 (medium), S3=1 (weak)")

    # Smooth WRR (NGINX-style)
    subsection("Smooth Weighted RR (NGINX-style)", 12)
    lb = SmoothWeightedRoundRobin()
    lb.add_server(WeightedServer("S1", "10.0.0.1", 8080, weight=3))
    lb.add_server(WeightedServer("S2", "10.0.0.2", 8080, weight=2))
    lb.add_server(WeightedServer("S3", "10.0.0.3", 8080, weight=1))

    for i in range(1, 13):
        s = lb.get_server()
        print(f"    Request #{i:02d} → {s.server_id}")

    lb.get_stats()


# ─────────────────────────────────────────────────────────────────────────────
# 3. LEAST CONNECTIONS
# ─────────────────────────────────────────────────────────────────────────────
def demo_least_connections():
    section("ALGORITHM 3: LEAST CONNECTIONS")
    print("  Concept: Always route to the server with fewest active connections.")
    print("  Best for: variable request durations, WebSockets, long-lived connections.\n")

    lb = LeastConnectionsLoadBalancer(use_weights=False)
    lb.add_server(LCServer("S1", "10.0.0.1", 8080))
    lb.add_server(LCServer("S2", "10.0.0.2", 8080))
    lb.add_server(LCServer("S3", "10.0.0.3", 8080))

    # Preload different connection counts to demonstrate the algorithm
    for _ in range(5): lb.servers[0].connect()  # S1 has 5 active
    for _ in range(2): lb.servers[1].connect()  # S2 has 2 active
    for _ in range(8): lb.servers[2].connect()  # S3 has 8 active

    print("  Initial load: S1=5, S2=2, S3=8 connections")
    print("  New requests should strongly prefer S2 (least loaded):\n")

    for i in range(1, 8):
        s = lb.get_server()
        print(f"    Request #{i:02d} → {s.server_id} "
              f"(active connections: {s.active_connections})")

    lb.get_stats()


# ─────────────────────────────────────────────────────────────────────────────
# 4. IP HASH / CONSISTENT HASHING
# ─────────────────────────────────────────────────────────────────────────────
def demo_ip_hash():
    section("ALGORITHM 4: IP HASH / CONSISTENT HASHING")
    print("  Concept: Same client IP → same server (session affinity).")
    print("  Consistent Hash minimizes remapping when servers change.\n")

    # Consistent Hashing
    lb = ConsistentHashLoadBalancer(vnodes=150)
    lb.add_server(IHServer("S1", "10.0.0.1", 8080))
    lb.add_server(IHServer("S2", "10.0.0.2", 8080))
    lb.add_server(IHServer("S3", "10.0.0.3", 8080))

    client_ips = [
        "192.168.1.1", "10.5.0.2", "172.16.0.3",
        "192.168.1.1", "10.5.0.2", "203.0.113.9",
    ]
    print("  Notice: repeated IPs always route to the same server:\n")
    for i, ip in enumerate(client_ips, 1):
        s = lb.get_server(ip)
        print(f"    Request #{i:02d} | {ip:<18} → {s.server_id}")

    lb.get_stats()


# ─────────────────────────────────────────────────────────────────────────────
# 5. RANDOM + POWER OF TWO CHOICES (P2C)
# ─────────────────────────────────────────────────────────────────────────────
def demo_random_p2c():
    section("ALGORITHM 5: RANDOM + POWER OF TWO CHOICES")
    print("  Concept: Pick 2 random servers, choose the least-loaded one.")
    print("  Achieves near-optimal balance with O(log log N) max load.\n")

    lb = PowerOfTwoChoicesLoadBalancer()
    lb.add_server(RandServer("S1", "10.0.0.1", 8080))
    lb.add_server(RandServer("S2", "10.0.0.2", 8080))
    lb.add_server(RandServer("S3", "10.0.0.3", 8080))

    # Skew initial load — S1 very busy, S3 free
    for _ in range(10): lb.servers[0].connect()
    for _ in range(3):  lb.servers[1].connect()
    # S3 has 0 connections

    print("  Initial: S1=10 connections, S2=3, S3=0")
    print("  P2C will avoid S1 and prefer S3:\n")

    for i in range(1, 10):
        s = lb.get_server()
        print(f"    Request #{i:02d} → {s.server_id} "
              f"(active={s.active_connections})")
        lb.release(s)

    lb.get_stats()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 58)
    print("       LOAD BALANCER — All 5 Algorithms Demo")
    print("=" * 58)
    print("""
  WHEN TO USE WHICH ALGORITHM:
  ─────────────────────────────
  Round Robin        → Identical servers, short requests (default NGINX)
  Weighted RR        → Different server capacities, canary deployments
  Least Connections  → Variable request durations (DB, WebSockets)
  IP Hash            → Session affinity (shopping carts, user state)
  Consistent Hash    → Distributed caches, microservices partitioning
  Random             → Stateless, no coordination needed
  P2C                → Large-scale, Envoy/Finagle-style load balancing
    """)

    demo_round_robin()
    demo_weighted_round_robin()
    demo_least_connections()
    demo_ip_hash()
    demo_random_p2c()

    # ── Final Comparison Table ─────────────────────────────────────────────
    section("FINAL COMPARISON SUMMARY")
    rows = [
        ("Algorithm",        "Dynamic?", "Session?", "Complexity", "Best For"),
        ("─" * 19,           "─" * 8,   "─" * 8,    "─" * 10,    "─" * 26),
        ("Round Robin",      "No",       "No",        "Trivial",   "Uniform, short reqs"),
        ("Weighted RR",      "No",       "No",        "Simple",    "Heterogeneous servers"),
        ("Least Conn",       "Yes",      "No",        "Medium",    "Long/variable requests"),
        ("IP Hash",          "No",       "Yes",       "Simple",    "Session affinity"),
        ("Consistent Hash",  "Partial",  "Yes",       "Medium",    "Distributed caches"),
        ("Random",           "No",       "No",        "Trivial",   "Stateless, simple"),
        ("P2C",              "Yes",      "No",        "Simple",    "Large-scale systems"),
    ]
    for row in rows:
        print(f"  {row[0]:<20} {row[1]:<9} {row[2]:<10} {row[3]:<11} {row[4]}")

    print(f"\n  Run individual files for in-depth demos and explanations.")
    print("=" * 58 + "\n")
