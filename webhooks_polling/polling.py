"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    WEBHOOKS VS POLLING — MODULE 1                            ║
║                       SHORT & LONG POLLING                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS POLLING?
────────────────
Polling is a technique where a client repeatedly requests data from a server 
to check for state changes (e.g., "Is my payment processed yet?").

1. SHORT POLLING:
   - Client sends an HTTP request every X seconds.
   - Server immediately responds with "Pending" or "Completed".
   - BAD: Massive overhead. If payment takes 10 mins, client makes 600 requests.
     99.8% of these requests return empty/pending data. Wastes CPU, bandwidth, DB queries.

2. LONG POLLING:
   - Client sends an HTTP request.
   - Server HOLDS the connection open (does not respond immediately).
   - Server waits until the data is ready, THEN responds.
   - If a timeout occurs (e.g., 30s), server responds "Timeout", and client reconnects.
   - BETTER: Fewer requests, less network overhead. 
   - BAD: Holds thousands of concurrent HTTP connections open on the server (memory heavy).

REAL WORLD USE CASES:
─────────────────────
- Short Polling: Simple UI progress bars.
- Long Polling: SQS (AWS Simple Queue Service), older chat applications before WebSockets.
"""

import time
import threading

class PaymentGatewayServer:
    """
    Simulates a backend server processing a slow payment.
    """
    def __init__(self):
        self.payments = {}

    def start_payment(self, payment_id: str):
        print(f"  [Server] Payment {payment_id} started. It will take 3 seconds...")
        self.payments[payment_id] = "PENDING"
        
        # Simulate background processing
        def process():
            time.sleep(3)
            self.payments[payment_id] = "COMPLETED"
            print(f"  [Server] Payment {payment_id} finished processing internally.")
            
        threading.Thread(target=process, daemon=True).start()

    def short_poll_endpoint(self, payment_id: str) -> str:
        """
        Server returns status immediately, regardless of what it is.
        """
        return self.payments.get(payment_id, "NOT_FOUND")

    def long_poll_endpoint(self, payment_id: str, timeout_seconds: int = 5) -> str:
        """
        Server holds the connection until status changes or timeout occurs.
        """
        start_time = time.time()
        
        # In a real async web server (like FastAPI or Node.js), this wouldn't block
        # a OS thread, it would yield to the event loop. For simplicity, we loop.
        while time.time() - start_time < timeout_seconds:
            status = self.payments.get(payment_id)
            if status == "COMPLETED":
                return status
            time.sleep(0.5) # Wait before checking DB again
            
        return "PENDING" # Timeout reached


class ClientApp:
    def __init__(self, server: PaymentGatewayServer):
        self.server = server

    def execute_short_polling(self, payment_id: str):
        print("\n  --- Starting SHORT POLLING ---")
        self.server.start_payment(payment_id)
        
        attempts = 0
        while True:
            attempts += 1
            print(f"    [Client] Attempt {attempts}: Are you done yet?")
            status = self.server.short_poll_endpoint(payment_id)
            print(f"    [Server -> Client] {status}")
            
            if status == "COMPLETED":
                break
                
            time.sleep(0.5) # Wait 0.5s before annoying the server again
            
        print(f"  --- Short Polling finished in {attempts} requests ---\n")

    def execute_long_polling(self, payment_id: str):
        print("\n  --- Starting LONG POLLING ---")
        self.server.start_payment(payment_id)
        
        attempts = 0
        while True:
            attempts += 1
            print(f"    [Client] Attempt {attempts}: Connecting and waiting...")
            
            # The client blocks here, waiting for the server to respond
            status = self.server.long_poll_endpoint(payment_id, timeout_seconds=5)
            print(f"    [Server -> Client] {status}")
            
            if status == "COMPLETED":
                break
                
        print(f"  --- Long Polling finished in {attempts} requests ---\n")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   SHORT vs LONG POLLING — Demo")
    print("=" * 65)
    
    server = PaymentGatewayServer()
    client = ClientApp(server)
    
    # 1. Short Polling (Lots of wasted requests)
    client.execute_short_polling("PAY_123")
    
    # 2. Long Polling (Way fewer requests, but holds connections)
    client.execute_long_polling("PAY_456")
