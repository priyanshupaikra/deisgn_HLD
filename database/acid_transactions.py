"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     DATABASE FUNDAMENTALS — MODULE 1                         ║
║                       ACID TRANSACTIONS & ROLLBACKS                          ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS ACID?
─────────────
ACID is a set of properties that guarantee database transactions are processed
reliably. Relational databases (like PostgreSQL and MySQL) guarantee ACID.

  A - ATOMICITY: "All or Nothing." 
      If a transaction involves 3 steps and step 3 fails, the database automatically
      undoes (ROLLBACKS) steps 1 and 2. You never have a partially completed transaction.
      
  C - CONSISTENCY: "Rules are obeyed."
      A transaction can only bring the database from one valid state to another.
      If a column has a UNIQUE constraint, a transaction cannot violate it.
      
  I - ISOLATION: "Invisible to others."
      If Transaction A and Transaction B are happening at the exact same time,
      they shouldn't interfere with each other. (See isolation_levels.py)
      
  D - DURABILITY: "Permanent."
      Once a transaction is COMMITTED, it will survive a power outage or crash.
      The DB writes it to a non-volatile Write-Ahead Log (WAL) on disk before 
      saying "Success".
"""

import copy
from typing import Dict


class RelationalDB:
    """Simulates a database engine with basic Transaction support."""
    
    def __init__(self):
        # The permanent, durable storage (simulated in memory)
        self.storage: Dict[str, float] = {
            "Alice": 500.0,
            "Bob": 500.0
        }
        
        # The temporary workspace for an active transaction
        self.transaction_workspace = None
        self.in_transaction = False

    def print_state(self):
        print(f"    [DB STATE] Alice: ${self.storage['Alice']:.2f} | Bob: ${self.storage['Bob']:.2f}")

    def begin_transaction(self):
        print("\n  [DB] BEGIN TRANSACTION;")
        self.in_transaction = True
        # Create a snapshot to work on (Simulating MVCC/Undo Logs)
        self.transaction_workspace = copy.deepcopy(self.storage)

    def update_balance(self, user: str, amount: float):
        if not self.in_transaction:
            raise Exception("Cannot write outside a transaction!")
            
        print(f"  [DB] UPDATE balances SET amount = amount + {amount} WHERE user = '{user}';")
        
        # Simulating a business logic error (Consistency Check)
        if self.transaction_workspace[user] + amount < 0:
            raise ValueError(f"INSUFFICIENT FUNDS for {user}")
            
        self.transaction_workspace[user] += amount

    def commit(self):
        """DURABILITY: Save the workspace to permanent storage."""
        if not self.in_transaction: return
        print("  [DB] COMMIT; (Writing to WAL and Disk)")
        self.storage = self.transaction_workspace
        self.in_transaction = False
        self.transaction_workspace = None

    def rollback(self):
        """ATOMICITY: Discard the workspace."""
        if not self.in_transaction: return
        print("  [DB] ❌ ROLLBACK; (Discarding changes)")
        self.in_transaction = False
        self.transaction_workspace = None


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   ACID: ATOMICITY & ROLLBACKS — Demo")
    print("=" * 65)
    
    db = RelationalDB()
    print("  Initial State:")
    db.print_state()

    print("\n  --- SCENARIO 1: Successful Transfer (All or Nothing: ALL) ---")
    try:
        db.begin_transaction()
        db.update_balance("Alice", -100.0) # Step 1: Deduct from Alice
        db.update_balance("Bob", 100.0)    # Step 2: Add to Bob
        db.commit()                        # Step 3: Success!
    except Exception as e:
        print(f"  [App Error] {e}")
        db.rollback()
        
    db.print_state()

    print("\n  --- SCENARIO 2: Failed Transfer (All or Nothing: NOTHING) ---")
    print("  (Alice tries to send $600 to Bob, but she only has $400)")
    try:
        db.begin_transaction()
        # Step 1: Add to Bob FIRST (just to show atomicity)
        db.update_balance("Bob", 600.0)
        
        # Step 2: Deduct from Alice. This will raise a ValueError!
        db.update_balance("Alice", -600.0) 
        
        db.commit()
    except Exception as e:
        print(f"  [App Error] Caught exception: {e}")
        db.rollback() # Undo the $600 we just gave Bob in the workspace
        
    print("\n  Final State (Notice Bob didn't keep the $600 because of the rollback):")
    db.print_state()
