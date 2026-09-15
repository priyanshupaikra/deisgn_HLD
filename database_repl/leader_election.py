"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                   DATABASE REPLICATION — MODULE 2                            ║
║                LEADER ELECTION & FAILOVER (High Availability)                ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT HAPPENS WHEN THE LEADER DIES?
──────────────────────────────────
If your Master/Leader database crashes, your application can no longer accept writes.
To maintain High Availability, the system must automatically:
1. Detect that the Leader is dead (Heartbeats).
2. Elect a new Leader from the remaining Followers.
3. Reroute write traffic to the new Leader.

This is called "Failover".

CONSENSUS ALGORITHMS (Raft / Paxos / ZooKeeper):
────────────────────────────────────────────────
How do the followers agree on who the new leader should be? They hold an election.
Usually, the follower with the most up-to-date replication log wins.

Split Brain Problem:
If a network partition happens (Node A can't talk to Node B, but both can talk to 
the app), they might BOTH think they are the leader. This destroys data integrity.
Consensus algorithms prevent this by requiring a "Quorum" (majority vote, e.g. 
3 out of 5 nodes must agree).
"""

import time
import random
import threading

class NodeStatus:
    LEADER = "LEADER"
    FOLLOWER = "FOLLOWER"
    DEAD = "DEAD"


class DbNode:
    def __init__(self, node_id: str):
        self.id = node_id
        self.status = NodeStatus.FOLLOWER
        self.log_index = 0 # Simulates how up-to-date this node is

    def ping(self) -> bool:
        """Simulate heartbeat. Returns True if alive."""
        return self.status != NodeStatus.DEAD


class ClusterManager:
    """
    Simulates a coordinator like Apache ZooKeeper or Redis Sentinel.
    """
    def __init__(self, nodes: list[DbNode]):
        self.nodes = nodes
        self.current_leader = None
        self.heartbeat_interval = 0.5
        
        # Initial Election
        self._elect_leader()
        
        # Start heartbeat monitoring
        threading.Thread(target=self._monitor_heartbeats, daemon=True).start()

    def get_leader(self) -> DbNode:
        return self.current_leader

    def _monitor_heartbeats(self):
        """Continuously pings the leader."""
        while True:
            time.sleep(self.heartbeat_interval)
            
            if self.current_leader and not self.current_leader.ping():
                print(f"  [ClusterManager] 🚨 ALERT! Leader {self.current_leader.id} failed to respond to heartbeat!")
                self.current_leader = None
                self._elect_leader()

    def _elect_leader(self):
        """
        Hold an election. The node with the highest log_index (most up to date) wins.
        """
        print("  [ClusterManager] Initiating Leader Election...")
        alive_nodes = [n for n in self.nodes if n.status != NodeStatus.DEAD]
        
        if not alive_nodes:
            print("  [ClusterManager] FATAL: All nodes are dead. Cluster is down.")
            return

        # Sort by log_index descending (most up-to-date first)
        alive_nodes.sort(key=lambda n: n.log_index, reverse=True)
        
        new_leader = alive_nodes[0]
        
        # In a real system, there is voting here to ensure quorum
        time.sleep(0.5) # Simulate election time
        
        for n in self.nodes:
            if n.status != NodeStatus.DEAD:
                n.status = NodeStatus.FOLLOWER
                
        new_leader.status = NodeStatus.LEADER
        self.current_leader = new_leader
        print(f"  [ClusterManager] 👑 Node {new_leader.id} elected as the new Leader! (Log Index: {new_leader.log_index})")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   LEADER ELECTION & FAILOVER — Demo")
    print("=" * 65)

    # 1. Setup a cluster with 3 nodes
    node1 = DbNode("DB_1")
    node2 = DbNode("DB_2")
    node3 = DbNode("DB_3")
    
    # Simulate that node 2 happened to replicate a bit faster and has more logs
    node1.log_index = 100
    node2.log_index = 105
    node3.log_index = 90
    
    cluster = ClusterManager([node1, node2, node3])
    
    # 2. Normal Operation
    print(f"\n  App is routing WRITES to: {cluster.get_leader().id}")
    time.sleep(1)
    
    # 3. Simulate Master Crash
    print("\n  --- SIMULATING HARDWARE FAILURE ON MASTER ---")
    cluster.get_leader().status = NodeStatus.DEAD
    
    # 4. Wait for Cluster Manager to detect and failover
    print("  (Waiting for heartbeats to fail...)")
    time.sleep(2)
    
    # 5. Application resumes
    print(f"\n  App is now routing WRITES to: {cluster.get_leader().id}")
    
    # Note: Because Node 1 (index 100) and Node 3 (index 90) were left,
    # Node 1 won the election because it had the higher log index.
