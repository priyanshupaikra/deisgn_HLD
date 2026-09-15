"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                 HEALTH CHECKS & AUTO SCALING SUITE                           ║
║                     All Concepts — Main Runner                               ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  RESILIENCY IN DISTRIBUTED SYSTEMS:                                          ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Systems fail. Spikes happen. A robust system doesn't try to prevent all     ║
║  failures; it handles them gracefully automatically without human intervention.║
║                                                                              ║
║  MODULES IN THIS DIRECTORY:                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  health_checks.py → K8s style Liveness (kill) vs Readiness (stop traffic).  ║
║  auto_scaler.py   → Target Tracking CPU Horizontal Auto Scaling (ASG/HPA).  ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import time
import health_checks
import auto_scaler


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def subsection(title: str):
    print(f"\n  ─── {title} ───")


# ─────────────────────────────────────────────────────────────────────────────
def demo_health_checks():
    section("MODULE 1: LIVENESS & READINESS PROBES")
    
    lb = health_checks.LoadBalancer()
    k8s = health_checks.Orchestrator(lb)
    
    n1 = health_checks.ServerInstance("Svr_A")
    lb.register(n1)
    
    time.sleep(0.6) # Wait for boot
    
    subsection("Simulating Deadlock (Failing Liveness)")
    print("  (Server gets stuck in infinite loop. Cannot respond to HTTP)")
    n1.state = health_checks.ServerState.DEADLOCKED
    
    k8s.run_health_checks() # Notice it kills and restarts the node


# ─────────────────────────────────────────────────────────────────────────────
def demo_auto_scaling():
    section("MODULE 2: AUTO SCALING GROUPS (Target 70% CPU)")
    
    asg = auto_scaler.AutoScalingGroup(target_cpu=70.0, min_size=1, max_size=4)
    
    subsection("Traffic Spike (Scale Out)")
    print("  (Traffic jumps to 280 rps. Single node gets overwhelmed)")
    asg.distribute_traffic(280)
    asg.evaluate_scaling() # Notice it adds 3 nodes
    
    subsection("Traffic Redistributed")
    asg.distribute_traffic(280)
    asg.evaluate_scaling() # Notice average CPU drops back to normal


# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    section("HEALTH & SCALING — COMPLETE REFERENCE")
    print()
    
    print("  1. HEALTH PROBES (KUBERNETES MODEL):")
    for row in [
        ("Liveness",  "Checks if app crashed/deadlocked. Fails? -> Kill and Restart pod."),
        ("Readiness", "Checks if app is ready to serve. Fails? -> Remove from Load Balancer."),
    ]:
        print(f"    {row[0]:<10} {row[1]}")

    print("\n  2. AUTO SCALING (ASG / HPA):")
    for row in [
        ("Scale Out", "Add more nodes to handle spike. (Horizontal scaling)"),
        ("Scale In",  "Remove nodes to save money when traffic drops."),
        ("Deadband",  "Margin around target CPU to prevent rapid flapping (scale up/down)."),
        ("Cooldown",  "Wait time after scaling before evaluating metrics again."),
    ]:
        print(f"    {row[0]:<10} {row[1]}")

    print()
    print("  Run individual files:")
    print("    python health_checks.py")
    print("    python auto_scaler.py")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       HEALTH CHECKS & AUTO SCALING — All Concepts")
    print("=" * 65)

    demo_health_checks()
    demo_auto_scaling()
    print_summary()
