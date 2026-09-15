"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       API GATEWAY — COMPONENT 1                              ║
║                           REQUEST ROUTER                                     ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
The Request Router is the CORE of an API Gateway. It matches incoming HTTP
requests (by path pattern + HTTP method) to registered backend microservices
and resolves path parameters from dynamic routes.

WHAT IS AN API GATEWAY?
────────────────────────
  An API Gateway is a SINGLE ENTRY POINT that sits between clients
  (browsers, mobile apps) and backend microservices.

  Client → [API Gateway] → [User Service]
                         → [Order Service]
                         → [Product Service]
                         → [Payment Service]

  Without a gateway, every client would need to know the address of every
  microservice. The gateway centralizes: routing, auth, rate limiting,
  logging, transformation, etc.

HOW ROUTING WORKS:
──────────────────
  Routes are registered with patterns:
    GET  /users            → User Service (list users)
    GET  /users/{id}       → User Service (get one user)
    POST /orders           → Order Service (create order)
    GET  /products/{id}    → Product Service

  When a request arrives:
    1. Match the URL path against registered route patterns
    2. Extract path parameters (e.g., {id} → "123")
    3. Identify the target backend service
    4. Forward the enriched request

PATH PARAMETER EXTRACTION:
───────────────────────────
  Pattern: /users/{user_id}/orders/{order_id}
  Request: /users/42/orders/99
  Params:  {"user_id": "42", "order_id": "99"}

ROUTE PRIORITY:
───────────────
  More specific routes take precedence over wildcards.
  /users/profile  >  /users/{id}  (exact match before dynamic)

REAL-WORLD EXAMPLES:
────────────────────
  - AWS API Gateway: path-based routing to Lambda functions
  - Kong Gateway: plugin-based routing
  - NGINX: location blocks as a routing mechanism
  - Netflix Zuul / Spring Cloud Gateway

"""

import re
from dataclasses import dataclass, field
from typing import Optional, Callable


@dataclass
class Request:
    """
    Represents an incoming HTTP request flowing through the API Gateway.

    This is the core data structure passed through the entire gateway pipeline
    (router → auth → rate limiter → transformer → backend → response).
    """
    method: str                        # HTTP method: GET, POST, PUT, DELETE, PATCH
    path: str                          # URL path: /users/42/orders
    headers: dict = field(default_factory=dict)  # HTTP headers
    body: dict = field(default_factory=dict)     # Request body (parsed JSON)
    query_params: dict = field(default_factory=dict)  # ?key=value pairs
    path_params: dict = field(default_factory=dict)   # {id} → "42" extracted from path
    client_ip: str = "127.0.0.1"      # Caller's IP address
    client_id: str = ""               # Authenticated client/user ID (filled by auth layer)
    request_id: str = ""              # Unique ID for distributed tracing

    def __repr__(self):
        return f"Request({self.method} {self.path})"


@dataclass
class Response:
    """Represents the HTTP response returned to the client."""
    status_code: int
    body: dict = field(default_factory=dict)
    headers: dict = field(default_factory=dict)

    def __repr__(self):
        return f"Response(HTTP {self.status_code})"


@dataclass
class Route:
    """
    A registered route mapping a method + path pattern to a backend handler.

    Attributes:
        method      : HTTP method this route handles (GET, POST, etc.)
        pattern     : URL pattern, e.g., /users/{user_id}/orders/{order_id}
        service_name: Name of the backend microservice (for logging/routing)
        handler     : Python callable that simulates the backend response
        regex       : Compiled regex derived from the pattern (for matching)
        param_names : List of path param names extracted from pattern
    """
    method: str
    pattern: str
    service_name: str
    handler: Callable[[Request], Response]
    regex: re.Pattern = field(init=False)
    param_names: list = field(init=False)

    def __post_init__(self):
        """Convert the URL pattern into a compiled regex for matching."""
        # Extract parameter names from pattern: /users/{user_id} → ["user_id"]
        self.param_names = re.findall(r'\{(\w+)\}', self.pattern)

        # Replace {param} with a named capture group: (?P<param>[^/]+)
        regex_pattern = re.sub(r'\{(\w+)\}', r'(?P<\1>[^/]+)', self.pattern)

        # Anchor the pattern to match the full path
        self.regex = re.compile(f'^{regex_pattern}$')


class RequestRouter:
    """
    Request Router for the API Gateway.

    Maintains a registry of routes and matches incoming requests to the
    correct backend handler, extracting path parameters in the process.

    Supports:
      - Exact routes: /health, /status
      - Dynamic routes: /users/{id}, /orders/{order_id}/items/{item_id}
      - Method-specific routing (GET vs POST to same path = different handler)
    """

    def __init__(self):
        # Routes are stored in order — first match wins
        # Exact routes should be registered BEFORE dynamic ones
        self.routes: list[Route] = []

    def register(self, method: str, pattern: str, service_name: str,
                 handler: Callable[[Request], Response]) -> None:
        """
        Register a route in the gateway.

        :param method:       HTTP method (GET, POST, PUT, DELETE, PATCH)
        :param pattern:      URL pattern with optional {param} placeholders
        :param service_name: Logical name of the backend service
        :param handler:      Function to call when this route is matched
        """
        route = Route(
            method=method.upper(),
            pattern=pattern,
            service_name=service_name,
            handler=handler,
        )
        self.routes.append(route)
        print(f"  [Router] Registered: {method.upper():6} {pattern:<35} → {service_name}")

    def route(self, request: Request) -> tuple[Optional[Route], Optional[Response]]:
        """
        Match the incoming request to a registered route.

        Algorithm:
          1. Filter routes by HTTP method
          2. For each matching-method route, test its regex against the request path
          3. On match: extract path params and attach to request
          4. Return the matched route (or 404 if no match found)

        :param request: The incoming Request object
        :return:        (matched_route, error_response) — one will be None
        """
        for route in self.routes:
            # Check HTTP method first (cheap comparison)
            if route.method != request.method.upper():
                continue

            # Try to match the path pattern
            match = route.regex.match(request.path)
            if match:
                # Extract path parameters from named capture groups
                request.path_params = match.groupdict()
                return route, None  # Match found!

        # No route matched — return 404
        return None, Response(
            status_code=404,
            body={"error": "Not Found", "path": request.path, "method": request.method}
        )

    def list_routes(self) -> None:
        """Print all registered routes."""
        print(f"\n  {'METHOD':<8} {'PATTERN':<35} SERVICE")
        print(f"  {'─'*8} {'─'*35} {'─'*20}")
        for r in self.routes:
            print(f"  {r.method:<8} {r.pattern:<35} {r.service_name}")


# ─────────────────────────────────────────────────────────────────────────────
# MOCK BACKEND HANDLERS (simulate microservice responses)
# ─────────────────────────────────────────────────────────────────────────────

def handle_list_users(req: Request) -> Response:
    return Response(200, {"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]})

def handle_get_user(req: Request) -> Response:
    user_id = req.path_params.get("user_id", "?")
    return Response(200, {"id": user_id, "name": f"User_{user_id}", "email": f"user{user_id}@example.com"})

def handle_create_user(req: Request) -> Response:
    return Response(201, {"message": "User created", "data": req.body})

def handle_list_products(req: Request) -> Response:
    return Response(200, {"products": [{"id": "p1", "name": "Widget"}, {"id": "p2", "name": "Gadget"}]})

def handle_get_product(req: Request) -> Response:
    pid = req.path_params.get("product_id", "?")
    return Response(200, {"id": pid, "name": f"Product_{pid}", "price": 29.99})

def handle_create_order(req: Request) -> Response:
    return Response(201, {"message": "Order placed", "order_id": "ORD-9999", "data": req.body})

def handle_get_order(req: Request) -> Response:
    oid = req.path_params.get("order_id", "?")
    return Response(200, {"order_id": oid, "status": "processing", "items": []})

def handle_health(req: Request) -> Response:
    return Response(200, {"status": "healthy", "services": ["user", "product", "order"]})


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("   REQUEST ROUTER — API Gateway Component Demo")
    print("=" * 60)

    router = RequestRouter()

    # Register routes — exact routes first, then dynamic
    print("\n  Registering routes:\n")
    router.register("GET",    "/health",                        "system",          handle_health)
    router.register("GET",    "/users",                         "user-service",    handle_list_users)
    router.register("POST",   "/users",                         "user-service",    handle_create_user)
    router.register("GET",    "/users/{user_id}",               "user-service",    handle_get_user)
    router.register("GET",    "/products",                      "product-service", handle_list_products)
    router.register("GET",    "/products/{product_id}",         "product-service", handle_get_product)
    router.register("POST",   "/orders",                        "order-service",   handle_create_order)
    router.register("GET",    "/orders/{order_id}",             "order-service",   handle_get_order)

    router.list_routes()

    # Test incoming requests
    test_requests = [
        Request("GET",  "/health"),
        Request("GET",  "/users"),
        Request("GET",  "/users/42"),
        Request("POST", "/users", body={"name": "Charlie"}),
        Request("GET",  "/products/p99"),
        Request("POST", "/orders", body={"product_id": "p1", "qty": 2}),
        Request("GET",  "/orders/ORD-1234"),
        Request("DELETE", "/unknown/path"),   # Should 404
    ]

    print(f"\n\n  Routing {len(test_requests)} test requests:\n")
    for req in test_requests:
        route, err = router.route(req)
        if err:
            print(f"  {req.method:<6} {req.path:<35} → HTTP {err.status_code} | {err.body['error']}")
        else:
            resp = route.handler(req)
            print(f"  {req.method:<6} {req.path:<35} → HTTP {resp.status_code} | "
                  f"[{route.service_name}] params={req.path_params}")

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY: Router matches path patterns, extracts {params},")
    print("  and dispatches to the correct microservice handler.")
    print("=" * 60)
