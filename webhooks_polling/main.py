"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    WEBHOOKS VS POLLING SUITE                                 ║
║                    All Concepts — Main Runner                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ASYNCHRONOUS COMMUNICATION:                                                 ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  When Service A asks Service B to do something that takes 5 minutes, how     ║
║  does Service A get the result when it's done?                               ║
║                                                                              ║
║  MODULES IN THIS DIRECTORY:                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  polling.py  → Short Polling (wasteful) and Long Polling (connection heavy). ║
║  webhooks.py → Webhooks (Push-based) and HMAC Signature security.            ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import time
import polling
import webhooks


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


# ─────────────────────────────────────────────────────────────────────────────
def demo_polling():
    section("MODULE 1: SHORT vs LONG POLLING")
    
    server = polling.PaymentGatewayServer()
    client = polling.ClientApp(server)
    
    print("  (Simulating a 3-second backend task)")
    client.execute_short_polling("PAY_123")
    
    print("  (Notice how Short Polling made 6 separate HTTP requests!)")
    print("  (Long Polling will hold the connection open instead)")
    
    client.execute_long_polling("PAY_456")


# ─────────────────────────────────────────────────────────────────────────────
def demo_webhooks():
    section("MODULE 2: WEBHOOKS & HMAC SECURITY")
    
    SECRET = "whsec_super_secret"
    stripe = webhooks.StripeSimulator(SECRET)
    my_api = webhooks.MyBackendAPI(SECRET)
    hacker = webhooks.Hacker()
    
    print("  --- Legitimate Webhook Flow ---")
    stripe.process_payment("PAY_888", 150.0, "https://api.myapp.com/webhooks", my_api)
    time.sleep(2.5) # Wait for Stripe thread
    
    print("  --- Malicious Webhook Attack ---")
    hacker.spoof_payment("PAY_HACKED", 99999.0, my_api)


# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    section("WEBHOOKS VS POLLING — COMPLETE REFERENCE")
    print()
    
    print("  1. COMMUNICATION STRATEGIES:")
    for row in [
        ("Short Polling", "Client asks repeatedly. Very wasteful on CPU and Network."),
        ("Long Polling",  "Client asks, Server holds connection open. Good for low latency,"),
        ("",              "but exhausts concurrent connection limits on servers."),
        ("Webhooks",      "Client gives Server a URL. Server POSTs to URL when done."),
        ("",              "Most efficient for async events (e.g., Stripe, GitHub)."),
    ]:
        print(f"    {row[0]:<14} {row[1]}")

    print("\n  2. WEBHOOK SECURITY (HMAC):")
    for row in [
        ("The Problem",   "Anyone can send a POST request to your webhook endpoint."),
        ("The Solution",  "Sender hashes the payload with a shared Secret Key (HMAC)."),
        ("Verification",  "Receiver re-hashes the payload with their Secret Key. If"),
        ("",              "hashes match, the payload is authentic and untampered."),
    ]:
        print(f"    {row[0]:<14} {row[1]}")

    print()
    print("  Run individual files:")
    print("    python polling.py")
    print("    python webhooks.py")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       WEBHOOKS VS POLLING — All Concepts")
    print("=" * 65)

    demo_polling()
    demo_webhooks()
    print_summary()
