"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                AUTHENTICATION & AUTHORIZATION SUITE                          ║
║                    All Concepts — Main Runner                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  SECURITY AT THE GATES:                                                      ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Authentication (AuthN): Verifying IDENTITY ("Who are you?").                ║
║  Authorization (AuthZ): Verifying PERMISSIONS ("What can you do?").          ║
║                                                                              ║
║  MODULES IN THIS DIRECTORY:                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  authentication.py → Stateful (Redis Sessions) vs Stateless (JWT).           ║
║  authorization.py  → RBAC (Role-Based) vs ABAC (Attribute-Based).            ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import authentication
import authorization


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def print_summary():
    section("AUTHENTICATION & AUTHORIZATION — COMPLETE REFERENCE")
    print()
    
    print("  1. AUTHENTICATION (AuthN):")
    for row in [
        ("Sessions",  "Stateful. Server stores Session ID in DB. Client sends it via Cookie."),
        ("",          "Pros: Easy to revoke. Cons: DB lookup on every request (slow)."),
        ("JWT",       "Stateless. JSON Web Token. Server signs a JSON payload cryptographically."),
        ("",          "Pros: No DB lookup needed! Scales infinitely. Cons: Hard to revoke early."),
        ("OAuth 2.0", "Delegated Auth. (e.g., 'Log in with Google'). You don't get the password,"),
        ("",          "you get a token from Google saying they are who they claim to be."),
    ]:
        print(f"    {row[0]:<12} {row[1]}")

    print("\n  2. AUTHORIZATION (AuthZ):")
    for row in [
        ("RBAC",      "Role-Based. Users have Roles (Admin). Roles have Permissions (Read)."),
        ("",          "Pros: Simple. Cons: Can lead to 'Role Explosion' (too many roles)."),
        ("ABAC",      "Attribute-Based. Rules evaluate User, Resource, and Context attributes."),
        ("",          "Pros: Extremely flexible (e.g., 'Finance can read invoices M-F 9-5')."),
        ("",          "Cons: Complex to build and audit. Often uses Open Policy Agent (OPA)."),
    ]:
        print(f"    {row[0]:<12} {row[1]}")

    print()
    print("  Run individual files:")
    print("    python authentication.py")
    print("    python authorization.py")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       AUTHENTICATION & AUTHORIZATION — All Concepts")
    print("=" * 65)

    # Let the user run the individual files for deep dives
    print("  (Running cheat sheet summary...)")
    print_summary()
