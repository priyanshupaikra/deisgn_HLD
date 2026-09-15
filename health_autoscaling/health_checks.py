"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                 HEALTH CHECKS & AUTO SCALING — MODULE 1                      ║
║                     LIVENESS & READINESS PROBES                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT ARE HEALTH CHECKS?
───────────────────────
In distributed systems, servers fail all the time. A Load Balancer (or Kubernetes)
needs to know if a server is healthy before sending traffic to it.
It does this by periodically pinging a specific endpoint, like `/health`.

TYPES OF PROBES (Kubernetes Model):
───────────────────────────────────
  1. LIVENESS PROBE ("Are you alive?")
     - Checks if the application has crashed or is deadlocked.
     - Action if failed: The infrastructure KILLS and RECREATES the container/server.
     - Example: App is stuck in an infinite loop. It stops responding to /health.
       K8s restarts it.

  2. READINESS PROBE ("Are you ready to receive traffic?")
     - Checks if the app is fully initialized and its dependencies (DB, Cache) are up.
     - Action if failed: The Load Balancer STOPS SENDING TRAFFIC to this node,
       but does NOT kill it.
     - Example: App is booting up and loading a massive cache into memory.
       It shouldn't receive traffic yet, but it shouldn't be killed either.
"""

import time
import random
from typing import Tuple


class ServerState:
    BOOTING = "booting"
    RUNNING = "running"
    DB_DOWN = "database_down"
    DEADLOCKED = "deadlocked"


class ServerInstance:
    """
    Simulates a backend server (e.g., an EC2 instance or K8s pod).
    """
    def __init__(self, id: str):
        self.id = id
        self.state = ServerState.BOOTING
        
        # Simulates background initialization time
        self.boot_completed_at = time.time() + 0.5 
        
    def _update_internal_state(self):
        """Simulates background state changes."""
        now = time.time()
        
        # If it was booting and time passed, it's now running
        if self.state == ServerState.BOOTING and now > self.boot_completed_at:
            self.state = ServerState.RUNNING

    # ─────────────────────────────────────────────────────────────────────────────
    # LIVENESS ENDPOINT
    # ─────────────────────────────────────────────────────────────────────────────
    def check_liveness(self) -> Tuple[int, str]:
        """
        Liveness Probe: Should respond quickly. Only checks if the core process
        is responding. Does NOT check external databases.
        """
        self._update_internal_state()
        
        if self.state == ServerState.DEADLOCKED:
            # Simulate a stuck thread that cannot respond to HTTP requests
            return 500, "Timeout"
            
        return 200, "Alive"

    # ─────────────────────────────────────────────────────────────────────────────
    # READINESS ENDPOINT
    # ─────────────────────────────────────────────────────────────────────────────
    def check_readiness(self) -> Tuple[int, str]:
        """
        Readiness Probe: Checks if the app is ready to serve user traffic.
        This includes checking database connections and cache warmups.
        """
        self._update_internal_state()
        
        if self.state == ServerState.BOOTING:
            return 503, "Service Unavailable: Still booting / warming cache"
            
        if self.state == ServerState.DB_DOWN:
            return 503, "Service Unavailable: Database connection failed"
            
        if self.state == ServerState.DEADLOCKED:
            return 500, "Timeout"
            
        return 200, "Ready"


class LoadBalancer:
    """
    Simulates an ALB (Application Load Balancer) or K8s Service that monitors
    nodes and only routes traffic to healthy ones.
    """
    def __init__(self):
        self.nodes = []

    def register(self, node: ServerInstance):
        self.nodes.append(node)
        print(f"  [LB] Registered Node {node.id}")

    def route_request(self) -> str:
        """Finds a ready node to route the request to."""
        ready_nodes = []
        for node in self.nodes:
            # LB uses the READINESS probe to decide routing
            status, _ = node.check_readiness()
            if status == 200:
                ready_nodes.append(node)
                
        if not ready_nodes:
            return "502 Bad Gateway (No healthy upstream nodes)"
            
        chosen = random.choice(ready_nodes)
        return f"200 OK (Served by {chosen.id})"


class Orchestrator:
    """
    Simulates Kubernetes / AWS ASG health monitoring system.
    Runs in the background checking Liveness.
    """
    def __init__(self, lb: LoadBalancer):
        self.lb = lb

    def run_health_checks(self):
        print("\n  [Orchestrator] Running health check sweep...")
        
        for node in self.lb.nodes.copy():
            # 1. Check Liveness
            live_status, live_msg = node.check_liveness()
            
            # 2. Check Readiness (just for logging in this demo)
            ready_status, ready_msg = node.check_readiness()
            
            print(f"    Node {node.id}: Liveness={live_status}, Readiness={ready_status}")
            
            if live_status != 200:
                print(f"    [!] Node {node.id} failed Liveness Probe. KILLING AND RESTARTING.")
                # Remove dead node
                self.lb.nodes.remove(node)
                # Spin up replacement
                new_node = ServerInstance(f"{node.id}_v2")
                self.lb.register(new_node)


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   LIVENESS & READINESS PROBES — Demo")
    print("=" * 65)
    
    lb = LoadBalancer()
    k8s = Orchestrator(lb)
    
    # 1. Start two nodes
    n1 = ServerInstance("Server_A")
    n2 = ServerInstance("Server_B")
    lb.register(n1)
    lb.register(n2)
    
    # 2. Immediate Traffic (Nodes are still booting)
    print("\n  --- Sending traffic immediately (Nodes are Booting) ---")
    print(f"  Request 1: {lb.route_request()}")
    
    # 3. Wait for boot to finish
    print("\n  (Waiting 0.6 seconds for cache warmup...)")
    time.sleep(0.6)
    print(f"  Request 2: {lb.route_request()}")
    
    # 4. Simulate Database failure on Server A
    print("\n  --- Simulating DB connection loss on Server_A ---")
    n1.state = ServerState.DB_DOWN
    
    print("  (Server_A fails readiness, but passes liveness. It is NOT killed, just ignored by LB)")
    k8s.run_health_checks()
    print(f"  Request 3: {lb.route_request()}  <- Notice it avoids Server_A")
    
    # 5. Simulate Deadlock on Server B
    print("\n  --- Simulating Deadlock on Server_B ---")
    n2.state = ServerState.DEADLOCKED
    
    print("  (Server_B fails liveness. Orchestrator kills it and replaces it)")
    k8s.run_health_checks()
