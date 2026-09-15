"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    DATABASE REPLICATION SUITE                                ║
║                    All Concepts — Main Runner                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WHY REPLICATE DATABASES?                                                    ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  1. High Availability (If master dies, promote a slave to master).           ║
║  2. Read Scalability (Route heavy SELECT queries to slaves to protect        ║
║     the master so it can focus on INSERT/UPDATE/DELETE).                     ║
║                                                                              ║
║  MODULES IN THIS DIRECTORY:                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  replication_types.py → Synchronous vs Asynchronous & Replication Lag.       ║
║  leader_election.py   → ZooKeeper/Sentinel style Failover.                   ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import time
import replication_types as repl
import leader_election as leader


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def subsection(title: str):
    print(f"\n  ─── {title} ───")


# ─────────────────────────────────────────────────────────────────────────────
def demo_replication():
    section("MODULE 1: REPLICATION TYPES & LAG")
    
    subsection("1. Synchronous (Safe, but Slow)")
    f_sync = repl.DatabaseNode("Follower")
    l_sync = repl.SynchronousLeader("Master", f_sync)
    l_sync.write_sync("config", "v1.0")
    print(f"  [App] Read from Follower immediately: {f_sync.read('config')}")

    subsection("2. Asynchronous (Fast, but Eventual Consistency)")
    f_async = repl.DatabaseNode("Follower")
    l_async = repl.AsynchronousLeader("Master", f_async)
    l_async.write_async("config", "v2.0")
    
    print(f"  [App] Read from Follower immediately: {f_async.read('config')}  <-- LAG!")
    print("  [App] Waiting 1.5s...")
    time.sleep(1.5)
    print(f"  [App] Read from Follower again: {f_async.read('config')}  <-- CAUGHT UP!")


# ─────────────────────────────────────────────────────────────────────────────
def demo_failover():
    section("MODULE 2: LEADER ELECTION & FAILOVER")
    
    n1 = leader.DbNode("DB_Node_1")
    n1.log_index = 50
    n2 = leader.DbNode("DB_Node_2")
    n2.log_index = 80 # Most up to date follower
    n3 = leader.DbNode("DB_Node_3")
    n3.log_index = 40
    
    print("  Booting Cluster Manager...")
    cluster = leader.ClusterManager([n1, n2, n3])
    
    print(f"  [App] Current Leader is: {cluster.get_leader().id}")
    
    subsection("Simulating Master Hardware Failure")
    cluster.get_leader().status = leader.NodeStatus.DEAD
    
    print("  Waiting 1 second for heartbeats to fail...")
    time.sleep(1.5)
    
    print(f"  [App] New Leader is: {cluster.get_leader().id}")
    print("  (Notice it picked DB_Node_2 because it had the highest log_index)")


# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    section("DATABASE REPLICATION — COMPLETE REFERENCE")
    print()
    
    print("  REPLICATION STRATEGIES:")
    for row in [
        ("Synchronous",  "Master waits for Follower to save to disk. 100% safe, but slow."),
        ("Asynchronous", "Master returns to user instantly, replicates in background. Fast."),
        ("Semi-Sync",    "Master waits for *at least one* Follower to save, then returns."),
    ]:
        print(f"    {row[0]:<14} {row[1]}")

    print("\n  CONSISTENCY MODELS:")
    for row in [
        ("Strong",     "Reads always see the latest write. (Synchronous provides this)."),
        ("Eventual",   "Reads might see old data (Lag), but will eventually catch up (Async)."),
        ("Read-Own",   "Read-Your-Own-Writes. A user who just updated their profile reads"),
        ("",           "from the Master for 1 minute, while others read from the Follower."),
    ]:
        print(f"    {row[0]:<14} {row[1]}")

    print("\n  LEADER ELECTION (Consensus):")
    for row in [
        ("Heartbeats", "Nodes constantly ping each other. If Master misses pings, it's dead."),
        ("Quorum",     "To avoid Split-Brain (two masters), a strict majority of nodes (N/2+1)"),
        ("",           "must agree on who the new master is."),
        ("Algorithms", "Raft, Paxos, ZAB (ZooKeeper Atomic Broadcast)."),
    ]:
        print(f"    {row[0]:<14} {row[1]}")

    print()
    print("  Run individual files:")
    print("    python replication_types.py")
    print("    python leader_election.py")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       DATABASE REPLICATION — All Concepts")
    print("=" * 65)

    demo_replication()
    demo_failover()
    print_summary()
