"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    LOAD BALANCING — ALGORITHM 4                              ║
║                      IP HASH / CONSISTENT HASHING                           ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
IP Hash (also called Sticky Sessions or Session Affinity) uses a HASH of the
client's IP address to consistently route that client to the SAME backend server.

Every time the same IP sends a request, it will always land on the same server —
as long as the server pool doesn't change.

HOW IT WORKS (Basic IP Hash):
──────────────────────────────
  server_index = hash(client_ip) % number_of_servers

  Client 192.168.1.10 → hash("192.168.1.10") % 3 = 1 → always S2
  Client 10.0.0.5     → hash("10.0.0.5")     % 3 = 0 → always S1
  Client 172.16.0.1   → hash("172.16.0.1")   % 3 = 2 → always S3

PROBLEM WITH SIMPLE MODULO HASHING:
─────────────────────────────────────
  When a server is ADDED or REMOVED, the modulo value changes for ALL clients!
  Example: 3 servers → 4 servers:
    Client A: hash % 3 = 1 → S2    After: hash % 4 = 3 → S4 (MOVED!)
    Client B: hash % 3 = 0 → S1    After: hash % 4 = 1 → S2 (MOVED!)
  → 100% of sessions are disrupted (cache misses, session loss)

CONSISTENT HASHING (The Solution):
────────────────────────────────────
  - Imagine a CIRCULAR RING (hash ring) from 0 to 2^32
  - Each SERVER is placed at one or more points on the ring (using hash of server_id)
  - Each REQUEST is placed at a point on the ring (using hash of client_ip)
  - The request goes to the NEAREST SERVER clockwise on the ring

  Adding/removing a server only affects the clients that "belonged" to it.
  On average: only K/N clients are remapped (K=keys, N=servers).
  Simple modulo: 100% of keys remapped. Consistent hashing: ~1/N keys remapped.

VIRTUAL NODES (Vnodes):
────────────────────────
  Problem: A small number of servers leads to UNEVEN distribution on the ring.
  Solution: Each physical server is represented by MULTIPLE virtual nodes.
  Example: S1 → vnode_S1_0, vnode_S1_1, vnode_S1_2, ... (evenly spread on ring)
  More vnodes = smoother, more balanced distribution.

ADVANTAGES:
-----------
  ✅ Session Affinity: same client always hits same server
     → Critical for: shopping carts, login sessions, WebSocket connections
  ✅ Cache Locality: cached data for a client stays on one server (cache hit ↑)
  ✅ Consistent Hashing: adding/removing servers only affects ~1/N of clients
  ✅ Stateless routing decision (no shared state needed between LB instances)

DISADVANTAGES:
--------------
  ❌ Simple IP Hash can cause uneven distribution (many clients behind a NAT)
  ❌ Adding/removing servers still causes some disruption (use with care)
  ❌ Not adaptive to server load (a server stays sticky even if overloaded)
  ❌ VPN/proxy users may have unpredictable routing

REAL-WORLD USE CASE:
--------------------
  - NGINX ip_hash directive (simple modulo-based)
  - Amazon DynamoDB, Apache Cassandra (consistent hashing for data partitioning)
  - Memcached/Redis clustering
  - Distributed CDN routing (Akamai, Cloudflare)
  - Microservices service mesh (consistent session routing)

"""

import hashlib
import bisect
import threading
from typing import Optional


class Server:
    """Backend server registered in the hash ring."""

    def __init__(self, server_id: str, host: str, port: int):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.total_requests = 0
        self.is_healthy = True

    def __repr__(self):
        return f"Server({self.server_id} | {self.host}:{self.port} | reqs={self.total_requests})"


# ─────────────────────────────────────────────────────────────────────────────
# IMPLEMENTATION 1: Simple IP Hash (modulo-based)
# ─────────────────────────────────────────────────────────────────────────────
class IPHashLoadBalancer:
    """
    Simple IP Hash using modulo arithmetic.

    server_index = hash(client_ip) % len(servers)

    Fast and simple but causes full reshuffling on server pool changes.
    """

    def __init__(self):
        self.servers: list[Server] = []
        self.lock = threading.Lock()

    def add_server(self, server: Server) -> None:
        with self.lock:
            self.servers.append(server)
            print(f"  [+] Added: {server}")

    def get_server(self, client_ip: str) -> Optional[Server]:
        """
        Hash the client IP and pick a server using modulo.

        :param client_ip: The client's IP address string
        :return:          The server this client is mapped to
        """
        with self.lock:
            healthy = [s for s in self.servers if s.is_healthy]
            if not healthy:
                return None

            # Hash the IP to a number and map to server index
            ip_hash = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
            index = ip_hash % len(healthy)
            server = healthy[index]
            server.total_requests += 1
            return server

    def get_stats(self) -> None:
        print("\n  [IP Hash] Server Stats:")
        total = sum(s.total_requests for s in self.servers)
        for s in self.servers:
            pct = (s.total_requests / total * 100) if total > 0 else 0
            bar = "█" * int(pct // 5)
            print(f"    {s.server_id}: {s.total_requests:4d} reqs ({pct:.1f}%) {bar}")


# ─────────────────────────────────────────────────────────────────────────────
# IMPLEMENTATION 2: Consistent Hashing with Virtual Nodes
# ─────────────────────────────────────────────────────────────────────────────
class ConsistentHashLoadBalancer:
    """
    Consistent Hashing Load Balancer with Virtual Nodes.

    Architecture:
      - Hash ring: a sorted list of (hash_value, server_id) tuples
      - Each physical server is represented by `vnodes` virtual points on the ring
      - Requests are hashed, then we find the nearest clockwise server on the ring

    Adding/removing a server only remaps ~1/N of traffic instead of 100%.

    Attributes:
        vnodes    : Number of virtual nodes per server (more = better balance)
        ring      : Sorted list of hash positions (the hash ring)
        ring_map  : Dict mapping hash_position → Server object
        servers   : Dict of all registered servers
    """

    def __init__(self, vnodes: int = 150):
        """
        :param vnodes: Virtual nodes per server.
                       Higher value = more even distribution but more memory.
                       Typical production values: 100–300
        """
        self.vnodes = vnodes
        self.ring: list[int] = []          # Sorted list of hash positions
        self.ring_map: dict[int, Server] = {}  # position → server
        self.servers: dict[str, Server] = {}
        self.lock = threading.Lock()

    def _hash(self, key: str) -> int:
        """
        Hash a string key to a position on the ring [0, 2^32).
        Uses MD5 truncated to 32 bits for speed.
        """
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**32)

    def add_server(self, server: Server) -> None:
        """
        Add a server to the ring by placing `vnodes` virtual points.

        Each virtual node key: "{server_id}_vnode_{i}"
        This spreads the server evenly across the ring.
        """
        with self.lock:
            self.servers[server.server_id] = server
            for i in range(self.vnodes):
                vnode_key = f"{server.server_id}_vnode_{i}"
                pos = self._hash(vnode_key)
                self.ring.append(pos)
                self.ring_map[pos] = server

            self.ring.sort()  # Keep ring sorted for binary search
            print(f"  [+] Added: {server} ({self.vnodes} vnodes on ring)")

    def remove_server(self, server_id: str) -> None:
        """
        Remove a server and all its virtual nodes from the ring.
        Only ~1/N of clients need to be remapped.
        """
        with self.lock:
            if server_id not in self.servers:
                return
            for i in range(self.vnodes):
                vnode_key = f"{server_id}_vnode_{i}"
                pos = self._hash(vnode_key)
                if pos in self.ring_map:
                    del self.ring_map[pos]
                if pos in self.ring:
                    self.ring.remove(pos)
            del self.servers[server_id]
            print(f"  [-] Removed server {server_id} from ring")

    def get_server(self, client_ip: str) -> Optional[Server]:
        """
        Find the server for the given client IP using consistent hashing.

        Algorithm:
          1. Hash the client_ip to a ring position
          2. Using binary search (bisect), find the nearest position >= client_hash
          3. If no position is found (client_hash > all positions) → wrap to ring[0]
          4. Return the server at that position

        :param client_ip: Client IP address (or any session key)
        :return:          The server this client maps to
        """
        with self.lock:
            if not self.ring:
                return None

            # Hash the client IP to a ring position
            client_pos = self._hash(client_ip)

            # Binary search for nearest clockwise server position
            # bisect_right returns the insertion point — that's our target position
            idx = bisect.bisect_right(self.ring, client_pos)

            # Wrap around: if idx == len(ring), go back to ring[0]
            if idx == len(self.ring):
                idx = 0

            pos = self.ring[idx]
            server = self.ring_map[pos]

            # Skip unhealthy servers (scan clockwise)
            visited = set()
            while not server.is_healthy:
                visited.add(pos)
                idx = (idx + 1) % len(self.ring)
                pos = self.ring[idx]
                server = self.ring_map[pos]
                if pos in visited:
                    return None  # All servers are down

            server.total_requests += 1
            return server

    def get_stats(self) -> None:
        print("\n  [Consistent Hash] Server Stats:")
        total = sum(s.total_requests for s in self.servers.values())
        for s in self.servers.values():
            pct = (s.total_requests / total * 100) if total > 0 else 0
            bar = "█" * int(pct // 5)
            print(f"    {s.server_id}: {s.total_requests:4d} reqs ({pct:.1f}%) {bar}")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("   IP HASH / CONSISTENT HASHING — Load Balancer Demo")
    print("=" * 60)

    # Sample client IPs
    client_ips = [
        "192.168.1.1", "10.0.0.1", "172.16.0.5", "192.168.1.1",
        "203.0.113.7", "10.0.0.1", "198.51.100.3", "192.168.1.1",
        "192.168.1.50", "10.10.10.10"
    ]

    # ─── Simple IP Hash ────────────────────────────────────────────────────
    print("\n  --- Simple IP Hash ---")
    iphash = IPHashLoadBalancer()
    iphash.add_server(Server("S1", "10.0.0.1", 8080))
    iphash.add_server(Server("S2", "10.0.0.2", 8080))
    iphash.add_server(Server("S3", "10.0.0.3", 8080))

    print(f"\n  Routing 10 requests (notice same IP → same server):\n")
    for i, ip in enumerate(client_ips, 1):
        s = iphash.get_server(ip)
        print(f"    Request #{i:02d} | IP={ip:<18} → {s.server_id}")

    iphash.get_stats()

    # ─── Consistent Hashing ────────────────────────────────────────────────
    print("\n\n  --- Consistent Hashing (with Virtual Nodes) ---")
    ch = ConsistentHashLoadBalancer(vnodes=150)
    ch.add_server(Server("S1", "10.0.0.1", 8080))
    ch.add_server(Server("S2", "10.0.0.2", 8080))
    ch.add_server(Server("S3", "10.0.0.3", 8080))

    print(f"\n  Routing 10 requests:\n")
    for i, ip in enumerate(client_ips, 1):
        s = ch.get_server(ip)
        print(f"    Request #{i:02d} | IP={ip:<18} → {s.server_id}")

    ch.get_stats()

    # ─── Demonstrate server removal — minimal disruption ──────────────────
    print(f"\n  --- Removing S2 from ring (Consistent Hash) ---")
    # Capture old mappings
    print(f"  Before removal:")
    for ip in client_ips[:5]:
        s = ch.get_server(ip)
        print(f"    {ip:<20} → {s.server_id}")

    ch.remove_server("S2")

    print(f"\n  After removal (only clients that were on S2 move):")
    for ip in client_ips[:5]:
        s = ch.get_server(ip)
        print(f"    {ip:<20} → {s.server_id}")

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY:")
    print("  IP Hash: Same client → same server. Simple but disrupts all")
    print("           sessions on server pool change.")
    print("  Consistent Hash: Only ~1/N sessions remapped when servers")
    print("                   are added/removed. Production-grade.")
    print("=" * 60)
