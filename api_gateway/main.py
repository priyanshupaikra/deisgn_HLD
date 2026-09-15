"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                            API GATEWAY                                       ║
║                     Full Pipeline — Main Runner                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WHAT IS AN API GATEWAY?                                                     ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  An API Gateway is the SINGLE ENTRY POINT for all client requests           ║
║  into a microservices system. Every request passes through the gateway       ║
║  before reaching any backend service.                                        ║
║                                                                              ║
║  CLIENT → [API Gateway Pipeline] → [Microservice]                           ║
║                                                                              ║
║  GATEWAY PIPELINE (in order):                                                ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  1. ROUTER         → Match request to backend service & extract params       ║
║  2. AUTH           → Verify identity (API key / JWT) + RBAC authorization   ║
║  3. RATE LIMITER   → Enforce per-client request limits (Token Bucket)        ║
║  4. CIRCUIT BREAKER→ Fail-fast if backend service is down                    ║
║  5. REQ TRANSFORM  → Rewrite URL, inject headers, strip sensitive data       ║
║  6. BACKEND CALL   → Forward to microservice handler                         ║
║  7. RESP TRANSFORM → Add security headers, mask sensitive fields, add CORS   ║
║  8. LOGGER         → Structured log + metrics collection                     ║
║                                                                              ║
║  COMPONENTS:                                                                 ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  router.py          → Path matching, param extraction, service dispatch      ║
║  auth.py            → API Key + JWT auth, RBAC role-based authorization      ║
║  circuit_breaker.py → 3-state breaker (CLOSED/OPEN/HALF_OPEN)               ║
║  transformer.py     → URL rewriting, header injection, response masking      ║
║  logger.py          → Structured logging + metrics (RPS, p95, p99)          ║
║                                                                              ║
║  HOW TO RUN INDIVIDUAL COMPONENTS:                                           ║
║    python router.py                                                          ║
║    python auth.py                                                            ║
║    python circuit_breaker.py                                                 ║
║    python transformer.py                                                     ║
║    python logger.py                                                          ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

# ─── Import all components ────────────────────────────────────────────────────
from router import (
    Request, Response, RequestRouter,
    handle_list_users, handle_get_user, handle_create_user,
    handle_list_products, handle_get_product,
    handle_create_order, handle_get_order, handle_health,
)
from auth import AuthMiddleware, MinimalJWT, JWT_SECRET
from circuit_breaker import CircuitBreakerRegistry
from transformer import RequestTransformer, ResponseTransformer
from logger import GatewayLogger

import threading
from collections import defaultdict


# ─────────────────────────────────────────────────────────────────────────────
# TOKEN BUCKET RATE LIMITER (integrated from rate_limiting concepts)
# ─────────────────────────────────────────────────────────────────────────────
class TokenBucketRateLimiter:
    """
    Per-client Token Bucket rate limiter integrated into the gateway pipeline.

    Each client gets their own bucket:
      capacity    = burst size (max requests in a burst)
      refill_rate = sustained requests per second

    On every request:
      1. Lazy-refill tokens based on time elapsed since last call
      2. If tokens available → consume 1 → ALLOW
      3. If bucket empty → REJECT with 429 Too Many Requests
    """

    def __init__(self, capacity: int = 10, refill_rate: float = 2.0):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.buckets = defaultdict(lambda: [float(capacity), time.time()])
        self.lock = threading.Lock()

    def allow(self, client_id: str) -> tuple[bool, dict]:
        with self.lock:
            now = time.time()
            tokens, last = self.buckets[client_id]
            elapsed = now - last
            tokens = min(self.capacity, tokens + elapsed * self.refill_rate)
            self.buckets[client_id][1] = now

            if tokens >= 1.0:
                self.buckets[client_id][0] = tokens - 1
                return True, {"remaining": int(tokens - 1), "limit": self.capacity}
            else:
                self.buckets[client_id][0] = tokens
                retry_after = round((1 - tokens) / self.refill_rate, 2)
                return False, {"retry_after": retry_after}


# ─────────────────────────────────────────────────────────────────────────────
# FULL API GATEWAY
# ─────────────────────────────────────────────────────────────────────────────
class APIGateway:
    """
    Full API Gateway Pipeline.

    Orchestrates all middleware components in the correct order:
      Router → Auth → Rate Limiter → Circuit Breaker →
      Request Transform → Backend → Response Transform → Logger

    This is the single class that clients interact with.
    All complexity is hidden behind the handle(request) method.
    """

    def __init__(self):
        # ── Component 1: Router ───────────────────────────────────────────
        self.router = RequestRouter()
        self._register_routes()

        # ── Component 2: Auth ─────────────────────────────────────────────
        self.auth = AuthMiddleware(jwt_secret=JWT_SECRET)

        # ── Component 3: Rate Limiter ─────────────────────────────────────
        # 10 burst capacity, 2 requests/second sustained
        self.rate_limiter = TokenBucketRateLimiter(capacity=10, refill_rate=2.0)

        # ── Component 4: Circuit Breakers (one per service) ───────────────
        self.circuit_breakers = CircuitBreakerRegistry(
            failure_threshold=3,
            recovery_timeout=30.0,
        )

        # ── Component 5: Transformers ─────────────────────────────────────
        self.req_transformer = RequestTransformer(
            strip_prefixes=["/api/v1", "/api/v2", "/api/v3"],
        )
        self.resp_transformer = ResponseTransformer(
            cors_origins="*",
        )

        # ── Component 6: Logger & Metrics ─────────────────────────────────
        self.logger = GatewayLogger(enable_console=True)

    def _register_routes(self) -> None:
        """Register all backend service routes with the router."""
        self.router.register("GET",    "/health",              "system",          handle_health)
        self.router.register("GET",    "/users",               "user-service",    handle_list_users)
        self.router.register("POST",   "/users",               "user-service",    handle_create_user)
        self.router.register("GET",    "/users/{user_id}",     "user-service",    handle_get_user)
        self.router.register("GET",    "/products",            "product-service", handle_list_products)
        self.router.register("GET",    "/products/{product_id}","product-service",handle_get_product)
        self.router.register("POST",   "/orders",              "order-service",   handle_create_order)
        self.router.register("GET",    "/orders/{order_id}",   "order-service",   handle_get_order)

    def handle(self, request: Request) -> Response:
        """
        Process a request through the full gateway pipeline.

        PIPELINE STAGES:
        ────────────────
          [0] URL Rewrite  : Strip version prefix BEFORE routing (/api/v1/x → /x)
          [1] Router       : Match path → find service + extract params
          [2] Auth         : Authenticate (API key/JWT) + Authorize (RBAC)
          [3] Rate Limiter : Check per-client token bucket
          [4] Req Transform: Inject headers, strip raw auth (URL already rewritten)
          [5] Circuit Breaker: Check if backend service is healthy
          [6] Backend Call : Forward to microservice handler
          [7] Resp Transform: Add security/CORS headers, mask sensitive data
          [8] Logger       : Record structured log + update metrics

        Any stage can SHORT-CIRCUIT and return an error response directly,
        skipping all subsequent stages.

        :param request: The incoming client request
        :return:        The final response to return to the client
        """
        start_time = time.time()
        original_path = request.path
        service_name = "gateway"
        response = None

        # ── [0] URL REWRITE — strip version prefix BEFORE routing ─────────
        # This ensures /api/v1/users routes to the same handler as /users
        for prefix in self.req_transformer.strip_prefixes:
            if request.path.startswith(prefix):
                stripped = request.path[len(prefix):]
                request.path = stripped if stripped.startswith('/') else '/' + stripped
                break

        # ── [1] ROUTER ────────────────────────────────────────────────────
        route, err = self.router.route(request)
        if err:
            # 404 Not Found — route doesn't exist
            response = err
            self.logger.log(request, response, "gateway", original_path, start_time)
            return response

        service_name = route.service_name

        # ── [2] AUTH ──────────────────────────────────────────────────────
        identity, err = self.auth.process(request)
        if err:
            # 401 Unauthorized or 403 Forbidden
            response = err
            self.logger.log(request, response, service_name, original_path, start_time)
            return response

        # ── [3] RATE LIMITER ──────────────────────────────────────────────
        allowed, rate_info = self.rate_limiter.allow(request.client_id or request.client_ip)
        if not allowed:
            response = Response(429, {
                "error": "Too Many Requests",
                "message": f"Rate limit exceeded. Retry after {rate_info['retry_after']}s",
                "retry_after": rate_info["retry_after"],
            })
            response.headers["Retry-After"] = str(rate_info["retry_after"])
            self.logger.log(request, response, service_name, original_path, start_time)
            return response

        # ── [4] REQUEST TRANSFORMATION (headers only — URL already rewritten) ──
        # Tell transformer not to re-strip prefix (already done in step 0)
        request.headers.update({
            "X-User-ID":        identity.client_id,
            "X-User-Role":      identity.role,
            "X-Forwarded-For":  request.client_ip,
            "X-Gateway":        "PyGateway/1.0",
        })
        # Strip sensitive auth headers before forwarding to backend
        for h in ["Authorization", "X-API-Key", "authorization", "x-api-key"]:
            request.headers.pop(h, None)
        import uuid
        request.request_id = request.request_id or str(uuid.uuid4())
        request.headers["X-Request-ID"] = request.request_id
        print(f"  [ReqTransform] {request.method} {original_path} → {request.path} | "
              f"client={identity.client_id} role={identity.role}")

        # ── [5 & 6] CIRCUIT BREAKER + BACKEND CALL ────────────────────────
        cb = self.circuit_breakers.get(service_name)
        response = cb.call(route.handler, request)

        # ── [7] RESPONSE TRANSFORMATION ───────────────────────────────────
        response = self.resp_transformer.transform(
            response,
            request_id=request.request_id,
            start_time=start_time,
        )

        # Add rate limit headers to successful responses
        if rate_info.get("remaining") is not None:
            response.headers["X-RateLimit-Remaining"] = str(rate_info["remaining"])
            response.headers["X-RateLimit-Limit"] = str(rate_info["limit"])

        # ── [8] LOGGER ─────────────────────────────────────────────────────
        self.logger.log(request, response, service_name, original_path, start_time)

        return response

    def print_status(self) -> None:
        """Print gateway health: registered routes, circuit breaker states, metrics."""
        print(f"\n  ═══ Gateway Status ═══")
        self.router.list_routes()
        self.circuit_breakers.print_all_status()
        self.logger.metrics.print_dashboard()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — Full Gateway Demo
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       API GATEWAY — Full Pipeline Demo")
    print("=" * 65)

    # ── Build the gateway ─────────────────────────────────────────────────
    gw = APIGateway()

    # ── Generate JWT tokens for demo clients ──────────────────────────────
    user_jwt = MinimalJWT.create({
        "user_id": "usr_42",
        "name": "Alice",
        "role": "USER",
        "exp": int(time.time()) + 3600
    }, JWT_SECRET)

    admin_jwt = MinimalJWT.create({
        "user_id": "adm_01",
        "name": "Admin",
        "role": "ADMIN",
        "exp": int(time.time()) + 3600
    }, JWT_SECRET)

    print(f"\n  Generated tokens:")
    print(f"    USER  JWT: {user_jwt[:45]}...")
    print(f"    ADMIN JWT: {admin_jwt[:45]}...")

    # ── Test Scenarios ────────────────────────────────────────────────────
    scenarios = [
        # (description, request)
        ("1. Health check (public — no auth)",
         Request("GET", "/health", client_ip="10.0.0.1")),

        ("2. API v1 versioned URL → rewritten (USER API key)",
         Request("GET", "/api/v1/users", client_ip="10.0.0.2",
                 headers={"X-API-Key": "sk-user-abc"})),

        ("3. Dynamic path param — get specific user (JWT)",
         Request("GET", "/users/42", client_ip="10.0.0.2",
                 headers={"Authorization": f"Bearer {user_jwt}"})),

        ("4. Create order (USER role required — JWT)",
         Request("POST", "/orders", client_ip="10.0.0.3",
                 headers={"Authorization": f"Bearer {user_jwt}"},
                 body={"product_id": "p1", "qty": 2})),

        ("5. Create user (ADMIN only) — ADMIN JWT",
         Request("POST", "/users", client_ip="10.0.0.4",
                 headers={"Authorization": f"Bearer {admin_jwt}"},
                 body={"name": "NewUser", "email": "new@example.com"})),

        ("6. Create user (ADMIN only) — USER JWT (should 403)",
         Request("POST", "/users", client_ip="10.0.0.5",
                 headers={"Authorization": f"Bearer {user_jwt}"},
                 body={"name": "Hacker"})),

        ("7. Invalid API key (should 401)",
         Request("GET", "/users", client_ip="10.0.0.6",
                 headers={"X-API-Key": "sk-invalid-key"})),

        ("8. Unknown route (should 404)",
         Request("GET", "/nonexistent/path", client_ip="10.0.0.7")),

        ("9. Get product (public)",
         Request("GET", "/products/p99", client_ip="10.0.0.8")),

        ("10. List all products (public — no auth needed)",
         Request("GET", "/products", client_ip="10.0.0.9")),
    ]

    print(f"\n{'─'*65}")
    print(f"  PIPELINE EXECUTION LOG")
    print(f"{'─'*65}\n")

    for desc, req in scenarios:
        print(f"\n  ── {desc}")
        resp = gw.handle(req)
        print(f"     → HTTP {resp.status_code} | Body={dict(list(resp.body.items())[:2])}")

    # ── Final Gateway Status ──────────────────────────────────────────────
    gw.print_status()

    # ── Architecture Summary ──────────────────────────────────────────────
    print(f"\n{'═'*65}")
    print(f"  API GATEWAY PIPELINE SUMMARY")
    print(f"{'═'*65}")
    rows = [
        ("Stage",           "Component",        "Purpose"),
        ("─"*14,            "─"*20,             "─"*28),
        ("[1] Router",      "router.py",         "Path matching & param extraction"),
        ("[2] Auth",        "auth.py",           "API Key/JWT verify + RBAC"),
        ("[3] Rate Limit",  "Token Bucket",      "Per-client request throttling"),
        ("[4] Req Transform","transformer.py",   "URL rewrite, header injection"),
        ("[5] Circ. Breaker","circuit_breaker.py","Fast-fail on service failures"),
        ("[6] Backend",     "handler functions", "Microservice call"),
        ("[7] Resp Transform","transformer.py",  "Security headers, field masking"),
        ("[8] Logger",      "logger.py",         "Structured log + RPS/latency"),
    ]
    for row in rows:
        print(f"  {row[0]:<18} {row[1]:<22} {row[2]}")
    print(f"{'═'*65}\n")
