"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                   MONITORING & LOGGING — MODULE 1                            ║
║                STRUCTURED LOGGING & CORRELATION IDS                          ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT ARE LOGS?
──────────────
Logs are immutable records of discrete events that happened over time.
They tell you the "Story" of what the system did.

UNSTRUCTURED vs STRUCTURED LOGGING:
───────────────────────────────────
  ❌ Unstructured (Plain Text):
      "2023-10-27 10:00:01 INFO User 123 purchased item 456 for $20.50"
      
      Problem: If you want to graph total revenue, you have to write a complex 
      RegEx to extract "$20.50" from the string. Slow and fragile.

  ✅ Structured (JSON):
      {"timestamp": "2023-10-27T10:00:01Z", "level": "INFO", 
       "msg": "Item purchased", "user_id": 123, "item_id": 456, "amount": 20.50}
      
      Advantage: Can be instantly parsed by log aggregators (Elasticsearch, 
      Splunk, Datadog). You can instantly query: 
      `SELECT SUM(amount) FROM logs WHERE msg="Item purchased"`

WHAT IS A CORRELATION ID?
─────────────────────────
In a microservices architecture, a single user click might trigger requests across
5 different services (API Gateway → Auth → Orders → Inventory → Billing).

If an error happens in Inventory, how do you find the logs for the REST of that 
specific user's flow? 

Solution: A Correlation ID (or Trace ID). 
Generated at the API Gateway and passed in the HTTP Headers (e.g., `X-Correlation-ID`) 
to every downstream service. Every service includes this ID in its logs.
You can then search your Log Aggregator for that ID and see the entire cross-service flow.

"""

import json
import uuid
import time
from datetime import datetime, timezone
import contextvars

# ─────────────────────────────────────────────────────────────────────────────
# 1. CONTEXT VARIABLES (Thread-safe Correlation ID Storage)
# ─────────────────────────────────────────────────────────────────────────────
# In modern Python web frameworks (FastAPI, Starlette), contextvars are used
# to store data that is local to the current async task / request.
_correlation_id_ctx_var = contextvars.ContextVar("correlation_id", default=None)

def get_correlation_id() -> str:
    return _correlation_id_ctx_var.get()

def set_correlation_id(new_id: str):
    _correlation_id_ctx_var.set(new_id)


# ─────────────────────────────────────────────────────────────────────────────
# 2. STRUCTURED LOGGER
# ─────────────────────────────────────────────────────────────────────────────

class StructuredLogger:
    """
    Simulates a production-grade JSON logger (like python-json-logger or structlog).
    Always includes timestamp, level, and the correlation_id if it exists.
    """
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        # In a real app, this would write to stdout/stderr, and a log shipper 
        # (Fluentd/Filebeat) would read it and send it to Elasticsearch/Datadog.
        self._log_storage = []

    def _log(self, level: str, msg: str, **kwargs):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "service": self.service_name,
            "msg": msg,
        }
        
        # Inject correlation ID if present in the current request context
        corr_id = get_correlation_id()
        if corr_id:
            log_entry["correlation_id"] = corr_id
            
        # Add any extra structured fields
        log_entry.update(kwargs)
        
        # In production, print(json.dumps(log_entry))
        formatted_log = json.dumps(log_entry)
        self._log_storage.append(log_entry)
        print(f"  {formatted_log}")

    def info(self, msg: str, **kwargs):
        self._log("INFO", msg, **kwargs)

    def error(self, msg: str, **kwargs):
        self._log("ERROR", msg, **kwargs)

    def debug(self, msg: str, **kwargs):
        self._log("DEBUG", msg, **kwargs)


# ─────────────────────────────────────────────────────────────────────────────
# 3. SIMULATING MICROSERVICES
# ─────────────────────────────────────────────────────────────────────────────

logger_gateway = StructuredLogger("api-gateway")
logger_auth = StructuredLogger("auth-service")
logger_billing = StructuredLogger("billing-service")


def auth_service_check(user_id: str):
    logger_auth.info("Authenticating user", user_id=user_id)
    time.sleep(0.05)
    return True


def billing_service_charge(user_id: str, amount: float):
    logger_billing.info("Attempting charge", user_id=user_id, amount=amount)
    time.sleep(0.1)
    
    if amount > 1000:
        logger_billing.error("Charge failed: Insufficient funds", 
                             user_id=user_id, amount=amount, failure_reason="insufficient_funds")
        return False
        
    logger_billing.info("Charge successful", user_id=user_id, amount=amount)
    return True


def api_gateway_handle_request(user_id: str, amount: float):
    """
    The entry point. Generates the Correlation ID.
    """
    # 1. Generate Correlation ID for this incoming request
    request_id = str(uuid.uuid4())
    set_correlation_id(request_id)
    
    start_time = time.time()
    logger_gateway.info("Incoming checkout request", path="/checkout", method="POST")

    # 2. Call Downstream Services (They will automatically use the correlation ID
    # because it is stored in the context variable).
    is_authed = auth_service_check(user_id)
    if is_authed:
        success = billing_service_charge(user_id, amount)
        
    duration_ms = (time.time() - start_time) * 1000
    
    # 3. Final Log
    if success:
        logger_gateway.info("Request completed", status=200, duration_ms=round(duration_ms, 2))
    else:
        logger_gateway.error("Request failed", status=400, duration_ms=round(duration_ms, 2))


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   STRUCTURED LOGGING & CORRELATION IDs — Demo")
    print("=" * 65)
    print("\n  Watch how the same 'correlation_id' flows through all 3 services.")
    print("  This allows Elasticsearch/Kibana to group these logs together.\n")

    print("  [REQUEST 1: Successful Checkout]")
    api_gateway_handle_request(user_id="U_991", amount=45.00)
    
    print("\n  [REQUEST 2: Failed Checkout (Over Limit)]")
    api_gateway_handle_request(user_id="U_882", amount=5000.00)
