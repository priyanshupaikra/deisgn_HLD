"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     DATABASE FUNDAMENTALS — MODULE 2                         ║
║                       ISOLATION LEVELS (I in ACID)                           ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS ISOLATION?
──────────────────
If two users are querying/updating the exact same row in the database at the 
exact same millisecond, what happens? Isolation determines how strictly the DB 
prevents them from stepping on each other's toes.

Higher isolation = More Data Accuracy, but Much Slower (locks).
Lower isolation  = Faster, but Risk of reading weird/incorrect data.

THE 3 READ PHENOMENA (The Problems):
────────────────────────────────────
1. DIRTY READ: 
   Tx A updates a row but hasn't committed yet. Tx B reads that uncommitted data. 
   Then Tx A rolls back. Tx B is now using data that technically never existed!

2. NON-REPEATABLE READ:
   Tx A reads a row. Tx B updates that row and commits. Tx A reads the same row 
   again and gets a DIFFERENT value inside the same transaction.

3. PHANTOM READ:
   Tx A queries "count all users where age > 30" (Result: 5). 
   Tx B inserts a new 35-year-old user and commits.
   Tx A runs the exact same query again and gets a DIFFERENT count (Result: 6).

THE 4 ISOLATION LEVELS (The Solutions):
───────────────────────────────────────
1. READ UNCOMMITTED: No locks. Fastest. Suffers from all 3 phenomena.
2. READ COMMITTED:   Prevents Dirty Reads. (Default in Postgres).
3. REPEATABLE READ:  Prevents Dirty + Non-Repeatable Reads. (Default in MySQL).
4. SERIALIZABLE:     Prevents all 3. Slowest. Transactions execute as if they 
                     were happening one-by-one in a strict sequence.
"""

import threading
import time

class ConcurrentDatabase:
    def __init__(self):
        self.ticket_inventory = 10
        self.lock = threading.Lock() # Simulates Row-Level Locking

    def read_committed_purchase(self, user: str, amount: int):
        """
        Simulating READ COMMITTED. 
        It reads the data, makes a decision, and then tries to write.
        (This is vulnerable to the 'Lost Update' anomaly!)
        """
        print(f"  [{user}] Checking inventory... Found {self.ticket_inventory} tickets.")
        time.sleep(0.1) # Simulate think time / network latency
        
        if self.ticket_inventory >= amount:
            print(f"  [{user}] Inventory sufficient. Buying {amount} tickets...")
            # Another transaction might have changed the inventory during the sleep!
            self.ticket_inventory -= amount
            print(f"  [{user}] ✅ Success! {self.ticket_inventory} tickets left.")
        else:
            print(f"  [{user}] ❌ Failed. Not enough tickets.")

    def serializable_purchase(self, user: str, amount: int):
        """
        Simulating SERIALIZABLE (or SELECT FOR UPDATE).
        Uses a lock to ensure no one else can read or write until this is done.
        """
        with self.lock:
            print(f"  [{user}] (Locked Row) Checking inventory... Found {self.ticket_inventory} tickets.")
            time.sleep(0.1)
            
            if self.ticket_inventory >= amount:
                print(f"  [{user}] Inventory sufficient. Buying {amount} tickets...")
                self.ticket_inventory -= amount
                print(f"  [{user}] ✅ Success! {self.ticket_inventory} tickets left.")
            else:
                print(f"  [{user}] ❌ Failed. Not enough tickets.")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   ISOLATION LEVELS (Concurrency Anomalies) — Demo")
    print("=" * 65)

    print("\n  --- SCENARIO 1: The 'Lost Update' Anomaly (Read Committed) ---")
    print("  Two users try to buy 8 tickets at the same time. There are only 10.")
    
    db1 = ConcurrentDatabase()
    
    # Start two threads at the same time
    t1 = threading.Thread(target=db1.read_committed_purchase, args=("Alice", 8))
    t2 = threading.Thread(target=db1.read_committed_purchase, args=("Bob", 8))
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    print("  🚨 DISASTER! Both users read '10', both thought it was safe to buy.")
    print(f"  Final Inventory: {db1.ticket_inventory} (We sold 16 tickets when we only had 10!)")
    
    
    print("\n  --- SCENARIO 2: Serializable Isolation (Locks) ---")
    print("  We use locks to ensure transactions happen sequentially.")
    
    db2 = ConcurrentDatabase()
    
    t3 = threading.Thread(target=db2.serializable_purchase, args=("Alice", 8))
    t4 = threading.Thread(target=db2.serializable_purchase, args=("Bob", 8))
    
    t3.start()
    t4.start()
    t3.join()
    t4.join()
    
    print(f"  🎉 SUCCESS! Final Inventory: {db2.ticket_inventory} (Bob's transaction was rejected).")
