"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    DISTRIBUTED TRACING — MODULE 2                            ║
║                       MICROSERVICES & PROPAGATION                            ║
╚══════════════════════════════════════════════════════════════════════════════╝

Simulating multiple microservices that communicate over the "network".
Notice how `get_trace_context()` is used to extract the Trace ID and Parent Span ID,
and how we pack them into an HTTP Header to send to the next service.
"""

import time
import random
from tracing_core import Span, get_trace_context, TraceContext, set_trace_context


# ─────────────────────────────────────────────────────────────────────────────
# SIMULATED NETWORK LAYER
# ─────────────────────────────────────────────────────────────────────────────

def http_post(url: str, payload: dict, headers: dict):
    """
    Simulates making an HTTP POST request across the network.
    The most important part is passing the TraceContext via headers!
    """
    # 1. Inject the Trace Context into the Headers (W3C standard is traceparent)
    ctx = get_trace_context()
    if ctx:
        # e.g., "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        # We simplify it here for readability:
        headers["X-Trace-Id"] = ctx.trace_id
        headers["X-Parent-Span-Id"] = ctx.parent_span_id

    # 2. Simulate Network Latency
    time.sleep(0.02)
    
    # 3. Route to the appropriate service
    if "auth" in url:
        return auth_service_handler(payload, headers)
    elif "billing" in url:
        return billing_service_handler(payload, headers)


def extract_trace_context(headers: dict):
    """
    Middleware that runs on every incoming HTTP request to extract tracing headers.
    """
    trace_id = headers.get("X-Trace-Id")
    parent_span_id = headers.get("X-Parent-Span-Id")
    
    if trace_id:
        ctx = TraceContext(trace_id=trace_id, parent_span_id=parent_span_id)
        set_trace_context(ctx)
    else:
        # Start a brand new trace if no headers exist
        set_trace_context(None)


# ─────────────────────────────────────────────────────────────────────────────
# SERVICE 3: DATABASE
# ─────────────────────────────────────────────────────────────────────────────

def db_query_user(user_id: str):
    """Deepest layer. Simulates a slow database query."""
    with Span("Database", "SELECT users", tags={"db.table": "users", "db.query": "SELECT *"}):
        time.sleep(random.uniform(0.05, 0.15)) # 50-150ms
        return True

def db_update_balance(user_id: str, amount: float):
    with Span("Database", "UPDATE balances", tags={"db.table": "balances"}):
        time.sleep(random.uniform(0.1, 0.2)) # 100-200ms
        return True


# ─────────────────────────────────────────────────────────────────────────────
# SERVICE 2: AUTH & BILLING
# ─────────────────────────────────────────────────────────────────────────────

def auth_service_handler(payload: dict, headers: dict):
    extract_trace_context(headers)
    
    with Span("AuthService", "POST /verify"):
        time.sleep(0.01) # CPU processing
        
        # Call Database
        db_query_user(payload["user_id"])
        
        return {"status": 200, "valid": True}


def billing_service_handler(payload: dict, headers: dict):
    extract_trace_context(headers)
    
    with Span("BillingService", "POST /charge", tags={"amount": payload["amount"]}):
        time.sleep(0.02) # CPU processing
        
        # Simulating an N+1 query problem or multiple sequential slow DB calls!
        with Span("BillingService", "ValidateCard"):
            time.sleep(0.1)
            
        db_update_balance(payload["user_id"], payload["amount"])
        
        return {"status": 200, "receipt": "R_123"}


# ─────────────────────────────────────────────────────────────────────────────
# SERVICE 1: API GATEWAY
# ─────────────────────────────────────────────────────────────────────────────

def api_gateway_checkout(user_id: str, amount: float) -> str:
    """
    The entry point. Receives raw request from mobile app (no trace headers yet).
    """
    # Middleware creates the root context
    set_trace_context(None)
    
    with Span("ApiGateway", "POST /checkout", tags={"user_id": user_id}):
        time.sleep(0.01)
        
        # 1. Call Auth Service
        auth_headers = {}
        http_post("http://auth/verify", {"user_id": user_id}, auth_headers)
        
        # 2. Call Billing Service
        billing_headers = {}
        http_post("http://billing/charge", {"user_id": user_id, "amount": amount}, billing_headers)
        
        # Get the Trace ID so we can look it up in Jaeger later
        trace_id = get_trace_context().trace_id
        return trace_id
