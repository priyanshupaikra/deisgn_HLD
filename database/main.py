"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    DATABASE FUNDAMENTALS SUITE                               ║
║                    All Concepts — Main Runner                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WHY USE A RELATIONAL DATABASE?                                              ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Databases aren't just for storing data; they are for storing data SAFELY.   ║
║  ACID properties ensure that even if the power cuts out mid-save, or 10,000  ║
║  users try to buy the same ticket at once, your data remains mathematically  ║
║  accurate and uncorrupted.                                                   ║
║                                                                              ║
║  MODULES IN THIS DIRECTORY:                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  acid_transactions.py → Demonstrates Atomicity (Rollbacks) & Durability.     ║
║  isolation_levels.py  → Demonstrates Concurrency bugs & Serializable locks.  ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import acid_transactions
import isolation_levels


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def print_summary():
    section("DATABASE FUNDAMENTALS — COMPLETE REFERENCE")
    print()
    
    print("  1. ACID PROPERTIES:")
    for row in [
        ("Atomicity",   "'All or nothing'. If a multi-step transaction fails, it rolls back entirely."),
        ("Consistency", "Transactions can only bring the DB from one valid state to another."),
        ("Isolation",   "Concurrent transactions don't interfere with each other."),
        ("Durability",  "Once committed, data survives crashes (written to Write-Ahead Log)."),
    ]:
        print(f"    {row[0]:<12} {row[1]}")

    print("\n  2. ISOLATION PHENOMENA (Bugs):")
    for row in [
        ("Dirty Read",    "Reading data from a transaction that hasn't committed yet (and might rollback)."),
        ("Non-Repeatable","Reading the same row twice in one TX, but the value changed in between."),
        ("Phantom Read",  "Running a count query twice, but new rows appeared in between."),
        ("Lost Update",   "Two users read a value, modify it, and write it back. One overwrites the other."),
    ]:
        print(f"    {row[0]:<14} {row[1]}")

    print("\n  3. ISOLATION LEVELS (Solutions):")
    for row in [
        ("Read Uncommitted", "Fastest. Prevents nothing."),
        ("Read Committed",   "Prevents Dirty Reads. (Postgres Default)"),
        ("Repeatable Read",  "Prevents Dirty + Non-Repeatable Reads. (MySQL Default)"),
        ("Serializable",     "Slowest. Locks rows. Prevents all anomalies."),
    ]:
        print(f"    {row[0]:<17} {row[1]}")

    print()
    print("  Run individual files:")
    print("    python acid_transactions.py")
    print("    python isolation_levels.py")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       DATABASE FUNDAMENTALS — All Concepts")
    print("=" * 65)

    # Let the user run the individual files for deep dives
    print("  (Running cheat sheet summary...)")
    print_summary()
