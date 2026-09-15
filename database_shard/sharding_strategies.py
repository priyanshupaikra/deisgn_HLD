"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     DATABASE SHARDING — MODULE 1                             ║
║                     RANGE & HASH-BASED SHARDING                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS SHARDING?
─────────────────
When a database grows too large to fit on a single hard drive (e.g., 50 TB of data)
or receives too many writes for a single CPU to handle, Replication is not enough
(because every replica still has to hold ALL the data and process ALL the writes).

Solution: Horizontal Partitioning (Sharding).
You split the database into multiple smaller databases (shards).
- Shard 1 holds Users A-M
- Shard 2 holds Users N-Z
Now you have double the storage and double the write capacity!

THE SHARD KEY:
──────────────
To find data, the application needs a "Routing Key" or "Shard Key".
If you shard by `user_id`, you must provide the `user_id` in every query so the 
router knows which database to connect to.
If you query without a shard key (e.g., "Find all users aged 25"), the router has 
to query EVERY shard (Scatter-Gather). This is very slow.

SHARDING STRATEGIES:
────────────────────
1. RANGE-BASED SHARDING:
   - Split by continuous values (e.g., A-M, N-Z, or Jan-Jun, Jul-Dec).
   - Pros: Easy to understand. Great for range queries ("Give me users from Jan").
   - Cons: Hotspots. If you shard by alphabet, "S" (Smith) might get 10x more 
           data than "Q". If you shard by date, the "Current Month" shard will 
           take 100% of the write traffic, defeating the purpose of sharding!

2. HASH-BASED SHARDING:
   - Run the Shard Key through a Hash Function (e.g., hash(user_id) % num_shards).
   - Pros: Perfectly even distribution of data and traffic. No hotspots.
   - Cons: Resharding is painful. If you add a new shard, the modulo (`%`) changes
           for almost every key, forcing you to move massive amounts of data.
           (Consistent Hashing fixes this).
"""

import hashlib
from typing import Dict, List, Any


class DatabaseShard:
    """A single physical database server holding a slice of the data."""
    def __init__(self, name: str):
        self.name = name
        self.data: Dict[str, Any] = {}

    def insert(self, key: str, value: Any):
        self.data[key] = value

    def read(self, key: str) -> Any:
        return self.data.get(key)
        
    def count(self) -> int:
        return len(self.data)


# ─────────────────────────────────────────────────────────────────────────────
# 1. RANGE BASED ROUTER
# ─────────────────────────────────────────────────────────────────────────────

class RangeRouter:
    """
    Routes based on the first letter of the username.
    Shard 1: A-H, Shard 2: I-P, Shard 3: Q-Z
    """
    def __init__(self):
        self.shard1 = DatabaseShard("Shard_A_H")
        self.shard2 = DatabaseShard("Shard_I_P")
        self.shard3 = DatabaseShard("Shard_Q_Z")

    def _get_shard(self, username: str) -> DatabaseShard:
        first_letter = username.upper()[0]
        if "A" <= first_letter <= "H":
            return self.shard1
        elif "I" <= first_letter <= "P":
            return self.shard2
        else:
            return self.shard3

    def insert_user(self, username: str, user_data: dict):
        shard = self._get_shard(username)
        shard.insert(username, user_data)
        print(f"  [Range Router] '{username}' routed to {shard.name}")

    def print_distribution(self):
        print("\n  [Range] Data Distribution (Notice the Hotspots!):")
        for s in [self.shard1, self.shard2, self.shard3]:
            print(f"    {s.name}: {s.count()} users")


# ─────────────────────────────────────────────────────────────────────────────
# 2. HASH BASED ROUTER
# ─────────────────────────────────────────────────────────────────────────────

class HashRouter:
    """
    Routes based on a cryptographic hash of the User ID.
    """
    def __init__(self, num_shards: int = 3):
        self.shards = [DatabaseShard(f"Shard_Hash_{i}") for i in range(num_shards)]

    def _get_shard(self, user_id: str) -> DatabaseShard:
        # 1. Hash the key to get a large integer
        hash_bytes = hashlib.md5(user_id.encode('utf-8')).digest()
        hash_int = int.from_bytes(hash_bytes, byteorder='big')
        
        # 2. Modulo by number of shards
        shard_index = hash_int % len(self.shards)
        return self.shards[shard_index]

    def insert_user(self, user_id: str, user_data: dict):
        shard = self._get_shard(user_id)
        shard.insert(user_id, user_data)
        # print(f"  [Hash Router] '{user_id}' routed to {shard.name}")

    def print_distribution(self):
        print("\n  [Hash] Data Distribution (Perfectly Even!):")
        for s in self.shards:
            print(f"    {s.name}: {s.count()} users")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   SHARDING STRATEGIES — Demo")
    print("=" * 65)
    
    # Let's pretend we have a bunch of common American names.
    # Notice there are very few Q, X, Z names, and lots of A, B, C names.
    users_to_insert = [
        "Alice", "Adam", "Amanda", "Bob", "Brian", "Charlie", "David",
        "Edward", "Frank", "George", "Henry", "Isabella", "Jack",
        "Kevin", "Liam", "Michael", "Quinn", "Zack"
    ]
    
    print("\n  --- 1. RANGE-BASED SHARDING ---")
    range_router = RangeRouter()
    for name in users_to_insert:
        range_router.insert_user(name, {"age": 30})
        
    range_router.print_distribution()
    
    
    print("\n  --- 2. HASH-BASED SHARDING ---")
    print("  (Hashing the same list of names using MD5 % 3)")
    hash_router = HashRouter(num_shards=3)
    for name in users_to_insert:
        hash_router.insert_user(name, {"age": 30})
        
    hash_router.print_distribution()
    
    print("\n  --- THE SCATTER-GATHER PROBLEM ---")
    print("  If you query 'SELECT * FROM users WHERE age = 30':")
    print("  Because 'age' is NOT the shard key, the application must query")
    print("  Shard 1 AND Shard 2 AND Shard 3, then combine the results.")
    print("  This destroys performance. Always query by the Shard Key!")
