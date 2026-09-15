"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    DATABASE SHARDING SUITE                                   ║
║                    All Concepts — Main Runner                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WHY SHARD DATABASES?                                                        ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  When a single database server cannot handle the volume of Writes, or the    ║
║  total disk size exceeds hardware limits, we must split the data across      ║
║  multiple physical servers (Horizontal Partitioning).                        ║
║                                                                              ║
║  MODULES IN THIS DIRECTORY:                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  sharding_strategies.py → Range-based vs Hash-based routing & hotspots.      ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

# Import the demo file to run it
import sharding_strategies


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def print_summary():
    section("DATABASE SHARDING — COMPLETE REFERENCE")
    print()
    
    print("  TYPES OF PARTITIONING:")
    for row in [
        ("Vertical",   "Splitting tables. (e.g., put 'Users' table on DB 1, and"),
        ("",           "'Payments' table on DB 2). Easy, but doesn't scale forever."),
        ("Horizontal", "Sharding. Splitting rows. (e.g., Users A-M on DB 1, N-Z on DB 2)."),
    ]:
        print(f"    {row[0]:<12} {row[1]}")

    print("\n  SHARDING STRATEGIES:")
    for row in [
        ("Range",      "Split by continuous values (A-M, Jan-Jun)."),
        ("",           "Pros: Easy range queries. Cons: Massive Hotspots."),
        ("Hash",       "hash(key) % num_shards. Pros: Perfectly even distribution."),
        ("",           "Cons: Adding a new shard requires moving almost all data."),
    ]:
        print(f"    {row[0]:<12} {row[1]}")

    print("\n  THE TWO BIGGEST PROBLEMS WITH SHARDING:")
    for row in [
        ("1. Joins",       "You cannot easily JOIN tables if they live on different shards."),
        ("",               "You must denormalize data or perform joins in application code."),
        ("2. Resharding",  "If Shard 1 gets too full, you have to split it into Shard 1a and 1b."),
        ("",               "This requires careful data migration without downtime. (Consistent Hashing helps)."),
    ]:
        print(f"    {row[0]:<16} {row[1]}")

    print()
    print("  Run individual file:")
    print("    python sharding_strategies.py")
    print()


if __name__ == "__main__":
    # Note: The demo logic runs automatically upon import for sharding_strategies
    print_summary()
