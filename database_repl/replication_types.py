"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                   DATABASE REPLICATION — MODULE 1                            ║
║                SYNC VS ASYNC REPLICATION & REPLICATION LAG                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS DATABASE REPLICATION?
─────────────────────────────
If you only have one database server, what happens when its hard drive dies? 
All your data is gone, and your website is down. 
Replication is keeping copies of the same data on multiple machines.

LEADER-FOLLOWER (Master-Slave) ARCHITECTURE:
────────────────────────────────────────────
- LEADER (Master): Handles all WRITE requests (INSERT, UPDATE, DELETE). 
  It writes to its own disk, then sends the changes (Write-Ahead Log / Binlog) 
  to the Followers.
- FOLLOWER (Slave/Replica): Receives changes from the Leader and applies them.
  Usually handles READ requests (SELECT) to take load off the Leader.

1. SYNCHRONOUS REPLICATION:
   - Leader writes to disk, sends to Follower, and WAITS for the Follower to confirm 
     it wrote to disk BEFORE telling the user "Success".
   - Pros: 100% Data safety. If Leader dies, Follower is exactly up to date.
   - Cons: Slow. If the Follower's network is slow, the user is stuck waiting.

2. ASYNCHRONOUS REPLICATION:
   - Leader writes to disk and immediately tells the user "Success". 
   - It sends the update to the Follower in the background.
   - Pros: Extremely fast writes. 
   - Cons: "Replication Lag". If user reads from the Follower instantly, they 
     might see old data. If Leader dies before sending the update, data is lost.
"""

import time
import threading
from typing import Dict, Any


class DatabaseNode:
    def __init__(self, name: str):
        self.name = name
        self.data: Dict[str, Any] = {}

    def write(self, key: str, value: Any):
        self.data[key] = value
        # print(f"    [{self.name}] Wrote {key}={value}")

    def read(self, key: str) -> Any:
        return self.data.get(key, "NOT_FOUND")


class SynchronousLeader(DatabaseNode):
    def __init__(self, name: str, follower: DatabaseNode):
        super().__init__(name)
        self.follower = follower

    def write_sync(self, key: str, value: Any):
        start_time = time.time()
        print(f"  [Leader] Sync Write initiated for {key}={value}")
        
        # 1. Write to local disk
        self.write(key, value)
        
        # 2. Replicate to Follower (Simulate network delay)
        time.sleep(0.5) 
        self.follower.write(key, value)
        
        # 3. Wait for ACK, then return
        duration = time.time() - start_time
        print(f"  [Leader] ✅ Sync Write Completed in {duration:.2f}s (Both nodes have it)")


class AsynchronousLeader(DatabaseNode):
    def __init__(self, name: str, follower: DatabaseNode):
        super().__init__(name)
        self.follower = follower

    def write_async(self, key: str, value: Any):
        start_time = time.time()
        print(f"  [Leader] Async Write initiated for {key}={value}")
        
        # 1. Write to local disk
        self.write(key, value)
        
        # 2. Fire and forget replication (Background thread)
        def replicate():
            time.sleep(1.0) # Simulate a slow network or heavy load
            self.follower.write(key, value)
            print(f"    [Background] Follower finally caught up and wrote {key}={value}")
            
        threading.Thread(target=replicate, daemon=True).start()
        
        # 3. Return immediately!
        duration = time.time() - start_time
        print(f"  [Leader] ✅ Async Write Completed in {duration:.4f}s (Returned to user immediately)")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   SYNC vs ASYNC REPLICATION — Demo")
    print("=" * 65)

    # 1. SYNCHRONOUS REPLICATION
    print("\n  --- SYNCHRONOUS REPLICATION (Safe but Slow) ---")
    follower_A = DatabaseNode("Follower_A")
    leader_sync = SynchronousLeader("Leader_Sync", follower_A)
    
    leader_sync.write_sync("user_101", "Alice")
    
    print(f"  User reads from Follower immediately: {follower_A.read('user_101')}")
    # Result: "Alice" (Guaranteed to be there)


    # 2. ASYNCHRONOUS REPLICATION (Fast but Eventual Consistency)
    print("\n  --- ASYNCHRONOUS REPLICATION (Fast but has Lag) ---")
    follower_B = DatabaseNode("Follower_B")
    leader_async = AsynchronousLeader("Leader_Async", follower_B)
    
    leader_async.write_async("user_202", "Bob")
    
    # User immediately refreshes the page and their read request hits the Follower
    print("  User refreshes page instantly. Reading from Follower...")
    read_result = follower_B.read("user_202")
    print(f"  Result: {read_result}  <-- This is REPLICATION LAG! (Read your own writes problem)")
    
    print("\n  Waiting 1.5 seconds...")
    time.sleep(1.5)
    
    print("  User refreshes page again. Reading from Follower...")
    read_result = follower_B.read("user_202")
    print(f"  Result: {read_result}  <-- The system is now 'Eventually Consistent'")
