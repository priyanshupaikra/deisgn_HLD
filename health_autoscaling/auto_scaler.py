"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                 HEALTH CHECKS & AUTO SCALING — MODULE 2                      ║
║                     AUTO SCALING GROUPS (ASG) / HPA                          ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS AUTO SCALING?
─────────────────────
Traffic on the internet is rarely flat. A website might have 100 users at 3 AM
and 10,000 users at 9 AM. 

Instead of permanently paying for 100 servers (over-provisioning), Auto Scaling
allows the infrastructure to automatically add servers (Scale Out) when traffic 
spikes, and remove servers (Scale In) when traffic drops.

TYPES OF SCALING:
─────────────────
  1. HORIZONTAL SCALING (Scale Out/In):
     - Adding MORE machines to the pool.
     - Handled by AWS Auto Scaling Groups (ASG) or K8s Horizontal Pod Autoscaler (HPA).
     - Best for stateless web applications.

  2. VERTICAL SCALING (Scale Up/Down):
     - Making the existing machine BIGGER (more RAM, more CPU).
     - Usually requires downtime (rebooting the VM).
     - Best for stateful databases (e.g., migrating from a 2-core DB to an 8-core DB).

HOW DOES IT KNOW WHEN TO SCALE?
───────────────────────────────
The Auto Scaler listens to METRICS (CloudWatch / Prometheus).
  - Target Tracking: "Keep average CPU utilization at 70%."
  - Step Scaling: "If CPU > 85% for 3 minutes, add 2 instances."
  - Predictive Scaling: "Based on historical ML data, scale up every day at 8:30 AM."

COOLDOWN PERIODS:
─────────────────
After scaling out, the ASG waits for a "Cooldown Period" (e.g., 5 minutes) before 
evaluating metrics again. This prevents it from endlessly launching servers while
the first batch is still booting up (which would result in way too many servers).
"""

import time
import math


class ServerNode:
    """A simulated backend server processing traffic."""
    def __init__(self, id: str, max_rps: float = 100.0):
        self.id = id
        self.max_rps = max_rps # Maximum requests per second this server can handle
        self.current_rps = 0.0

    @property
    def cpu_utilization(self) -> float:
        """CPU is directly proportional to the traffic it's handling."""
        return (self.current_rps / self.max_rps) * 100.0


class AutoScalingGroup:
    """
    Simulates AWS ASG or Kubernetes HPA.
    Monitors a group of servers and scales horizontally based on CPU metrics.
    """
    def __init__(self, target_cpu: float = 70.0, min_size: int = 1, max_size: int = 10):
        self.target_cpu = target_cpu
        self.min_size = min_size
        self.max_size = max_size
        self.nodes = [ServerNode(f"Node_1")]
        
        self.next_node_id = 2

    def distribute_traffic(self, total_rps: float):
        """Simulate a Load Balancer distributing traffic evenly to all nodes."""
        rps_per_node = total_rps / len(self.nodes) if self.nodes else 0
        for node in self.nodes:
            node.current_rps = rps_per_node

    def get_average_cpu(self) -> float:
        """Metrics aggregator (like CloudWatch)."""
        if not self.nodes: return 0.0
        total_cpu = sum(node.cpu_utilization for node in self.nodes)
        return total_cpu / len(self.nodes)

    def evaluate_scaling(self):
        """
        The Scaling Policy Engine.
        In reality, this runs on a cron schedule (e.g., every 15-60 seconds).
        """
        avg_cpu = self.get_average_cpu()
        print(f"  [Metrics] Cluster Size: {len(self.nodes)} | Avg CPU: {avg_cpu:.1f}%")

        # Basic Target Tracking logic
        if avg_cpu > self.target_cpu and len(self.nodes) < self.max_size:
            self._scale_out(avg_cpu)
            
        elif avg_cpu < (self.target_cpu - 20) and len(self.nodes) > self.min_size:
            # We subtract 20 to create a "deadband" so it doesn't flap rapidly
            # between scaling up and down if CPU is hovering right at 70%.
            self._scale_in(avg_cpu)

    def _scale_out(self, current_cpu: float):
        """Add nodes to the pool to reduce overall CPU."""
        # Calculate how many nodes we actually need to reach the target CPU
        # Desired Nodes = Current Nodes * (Current CPU / Target CPU)
        desired_capacity = math.ceil(len(self.nodes) * (current_cpu / self.target_cpu))
        
        # Don't exceed max_size
        desired_capacity = min(desired_capacity, self.max_size)
        nodes_to_add = desired_capacity - len(self.nodes)

        if nodes_to_add > 0:
            print(f"  [ASG ALARM] CPU too high! Scaling OUT by adding {nodes_to_add} nodes...")
            for _ in range(nodes_to_add):
                self.nodes.append(ServerNode(f"Node_{self.next_node_id}"))
                self.next_node_id += 1

    def _scale_in(self, current_cpu: float):
        """Remove nodes from the pool to save money."""
        desired_capacity = math.ceil(len(self.nodes) * (current_cpu / self.target_cpu))
        desired_capacity = max(desired_capacity, self.min_size)
        
        nodes_to_remove = len(self.nodes) - desired_capacity

        if nodes_to_remove > 0:
            print(f"  [ASG ALARM] CPU too low (wasting money). Scaling IN by removing {nodes_to_remove} nodes...")
            for _ in range(nodes_to_remove):
                self.nodes.pop()


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   AUTO SCALING GROUP (Target CPU: 70%) — Demo")
    print("=" * 65)
    
    asg = AutoScalingGroup(target_cpu=70.0, min_size=1, max_size=5)

    # 1. Low traffic (Night time)
    print("\n  --- 3:00 AM (Low Traffic: 40 requests/sec) ---")
    asg.distribute_traffic(40)
    asg.evaluate_scaling()
    
    # 2. Traffic Spike (Morning rush)
    print("\n  --- 9:00 AM (Massive Spike: 280 requests/sec) ---")
    asg.distribute_traffic(280)
    asg.evaluate_scaling()
    
    print("\n  (Traffic redistributed across new nodes)")
    asg.distribute_traffic(280) 
    asg.evaluate_scaling()
    
    # 3. Traffic drops back down
    print("\n  --- 11:00 PM (Traffic drops: 60 requests/sec) ---")
    asg.distribute_traffic(60)
    asg.evaluate_scaling()
    
    print("\n  (Traffic redistributed across remaining nodes)")
    asg.distribute_traffic(60)
    asg.evaluate_scaling()
