"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    WEBHOOKS VS POLLING — MODULE 2                            ║
║                       WEBHOOKS & HMAC SECURITY                               ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS A WEBHOOK?
──────────────────
A Webhook is a "Reverse API". Instead of the Client asking the Server for data,
the Client gives the Server a URL and says, "Call this URL when you have data."

1. The Flow:
   - Client sends request: "Process this $50 payment. When done, POST to 
     https://client.com/webhook/payment-complete"
   - Server responds: "202 Accepted. I'll let you know."
   - (Server processes for 5 seconds)
   - Server sends an HTTP POST request to https://client.com/webhook/payment-complete

2. Benefits:
   - Zero wasted requests. 
   - No open connections sitting idle (like Long Polling).
   - Extremely scalable for asynchronous events (Stripe, GitHub, Slack).

THE SECURITY PROBLEM:
─────────────────────
If your server exposes an endpoint `POST /webhook/payment-complete`, what stops
a malicious hacker from sending a fake POST request saying a $10,000 payment was successful?

Solution: HMAC SIGNATURES (Hash-based Message Authentication Code).

1. When you register for webhooks with Stripe/GitHub, they give you a SECRET KEY.
2. When Stripe sends the webhook, they hash the JSON body using that SECRET KEY,
   and put the resulting hash in a Header (e.g., `Stripe-Signature`).
3. Your server receives the JSON and the Header.
4. Your server hashes the JSON using your copy of the SECRET KEY.
5. If your hash matches the header hash, you know it TRULY came from Stripe, and
   the payload was not tampered with in transit.
"""

import time
import json
import hmac
import hashlib
import threading


# ─────────────────────────────────────────────────────────────────────────────
# 1. THE THIRD-PARTY SERVER (e.g., Stripe, GitHub)
# ─────────────────────────────────────────────────────────────────────────────

class StripeSimulator:
    """Simulates a service that sends webhooks."""
    
    def __init__(self, webhook_secret: str):
        self.webhook_secret = webhook_secret
        
    def process_payment(self, payment_id: str, amount: float, client_webhook_url: str, target_client: 'MyBackendAPI'):
        print(f"  [Stripe] Processing payment {payment_id} for ${amount}...")
        
        def background_task():
            time.sleep(2) # Simulate processing
            
            # Prepare the webhook payload
            payload_dict = {
                "event_type": "payment_success",
                "payment_id": payment_id,
                "amount": amount,
                "status": "succeeded"
            }
            # Convert to string (simulating HTTP body)
            payload_body = json.dumps(payload_dict)
            
            # SECURITY: Generate HMAC signature
            # We hash the payload body using the shared secret key.
            signature = hmac.new(
                self.webhook_secret.encode('utf-8'),
                payload_body.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            print(f"  [Stripe] Finished! Sending POST to {client_webhook_url}")
            print(f"  [Stripe] Added Header: Stripe-Signature: {signature[:10]}...")
            
            # Simulate firing an HTTP POST request to the client
            headers = {"Stripe-Signature": signature}
            target_client.webhook_endpoint(payload_body, headers)
            
        threading.Thread(target=background_task, daemon=True).start()


class Hacker:
    """Simulates a malicious actor trying to fake a successful payment."""
    
    def spoof_payment(self, payment_id: str, amount: float, target_client: 'MyBackendAPI'):
        print(f"\n  [Hacker] Spoofing a successful payment webhook for {payment_id} (${amount})...")
        
        payload_body = json.dumps({
            "event_type": "payment_success",
            "payment_id": payment_id,
            "amount": amount,
            "status": "succeeded"
        })
        
        # The hacker doesn't know the secret key, so they either send no signature,
        # or a fake one generated with a wrong key.
        fake_signature = hmac.new(
            b"wrong_secret",
            payload_body.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        headers = {"Stripe-Signature": fake_signature}
        target_client.webhook_endpoint(payload_body, headers)


# ─────────────────────────────────────────────────────────────────────────────
# 2. YOUR SERVER (Receiving the Webhook)
# ─────────────────────────────────────────────────────────────────────────────

class MyBackendAPI:
    def __init__(self, webhook_secret: str):
        self.webhook_secret = webhook_secret
        
    def webhook_endpoint(self, request_body: str, headers: dict):
        """
        Simulates POST /api/webhooks/stripe
        """
        signature_header = headers.get("Stripe-Signature")
        
        if not signature_header:
            print("    [MyBackend] ❌ 401 Unauthorized: Missing signature header.")
            return

        # 1. Verify the Signature
        # We calculate the expected hash using our copy of the secret.
        expected_signature = hmac.new(
            self.webhook_secret.encode('utf-8'),
            request_body.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        # hmac.compare_digest prevents timing attacks (vs just using ==)
        if not hmac.compare_digest(expected_signature, signature_header):
            print("    [MyBackend] ❌ 401 Unauthorized: Invalid signature! Hacker detected.")
            return
            
        # 2. If valid, process the event
        print("    [MyBackend] ✅ 200 OK: Signature verified. Authentic webhook received.")
        
        event = json.loads(request_body)
        if event["event_type"] == "payment_success":
            print(f"    [MyBackend] Logic: Updating DB. Payment {event['payment_id']} is successful.")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   WEBHOOKS & HMAC SECURITY — Demo")
    print("=" * 65)
    
    # Both Stripe and Your Server share this secret, generated in the Stripe Dashboard
    SHARED_SECRET = "whsec_super_secret_key_123"
    
    stripe = StripeSimulator(SHARED_SECRET)
    my_api = MyBackendAPI(SHARED_SECRET)
    hacker = Hacker()
    
    # 1. Legitimate Webhook Flow
    print("\n  --- Phase 1: Legitimate Webhook Event ---")
    stripe.process_payment(
        payment_id="PAY_888", 
        amount=150.0, 
        client_webhook_url="https://api.myapp.com/webhooks/stripe",
        target_client=my_api
    )
    
    # Wait for background thread to finish
    time.sleep(3)
    
    # 2. Hacker attempts to spoof a payment
    print("\n  --- Phase 2: Malicious Webhook Attack ---")
    hacker.spoof_payment(
        payment_id="PAY_999", 
        amount=10000.0, 
        target_client=my_api
    )
