"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       API GATEWAY — COMPONENT 4                              ║
║                    REQUEST & RESPONSE TRANSFORMATION                         ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
The Transformer middleware modifies requests BEFORE they reach the backend,
and modifies responses BEFORE they reach the client.

This allows the gateway to:
  - Translate between different API versions
  - Enrich requests with gateway-level metadata
  - Strip or add security headers
  - Mask sensitive fields in responses
  - Aggregate/reshape response data

WHY TRANSFORMATIONS ARE NEEDED:
────────────────────────────────
  1. API Version Compatibility:
     Client sends: GET /api/v1/users
     Backend expects: GET /users
     → Gateway strips the /api/v1 prefix

  2. Header Injection:
     Client sends: {Authorization: Bearer <token>}
     Backend expects: {X-User-ID: "42", X-User-Role: "admin"}
     → Gateway strips the JWT and injects decoded values

  3. Response Sanitization:
     Backend returns: {password_hash: "...", ssn: "...", name: "Alice"}
     Client should get: {name: "Alice"}
     → Gateway strips sensitive fields from the response

  4. CORS Headers:
     Backend is unaware of CORS.
     → Gateway adds: Access-Control-Allow-Origin: * to every response

  5. Protocol Translation:
     External client speaks: REST/JSON
     Internal service speaks: gRPC / XML
     → Gateway translates (not implemented here, but this is the layer)

TYPES OF TRANSFORMATIONS IMPLEMENTED:
──────────────────────────────────────
  REQUEST TRANSFORMATIONS:
    a) URL Rewriting      — Strip version prefix (/api/v1/users → /users)
    b) Header Injection   — Add X-Request-ID, X-User-ID, X-Forwarded-For
    c) Header Stripping   — Remove Authorization before forwarding to backend
    d) Header Renaming    — Map old header names to new ones

  RESPONSE TRANSFORMATIONS:
    a) Security Headers   — Add HSTS, X-Frame-Options, X-Content-Type-Options
    b) CORS Headers       — Add Access-Control-Allow-Origin
    c) Field Masking      — Redact sensitive fields (passwords, tokens, PII)
    d) Field Adding       — Add metadata like response_time, gateway_version
    e) Status Normalization — Map backend HTTP codes to client-friendly codes

REAL-WORLD EXAMPLES:
────────────────────
  - AWS API Gateway: mapping templates for request/response
  - Kong: transformer plugins (request-transformer, response-transformer)
  - NGINX: proxy_set_header, add_header, sub_filter
  - Apigee: JavaScript policies for header/body manipulation

"""

import uuid
import time
from typing import Optional
from router import Request, Response


class RequestTransformer:
    """
    Transforms incoming requests BEFORE forwarding to backend services.

    Responsibilities:
      - Rewrite URL paths (strip prefixes, map versions)
      - Inject gateway-level headers (request ID, user ID, IP)
      - Strip auth headers (backend shouldn't see raw tokens)
      - Rename headers for backend compatibility
    """

    def __init__(self,
                 strip_prefixes: list[str] = None,
                 inject_headers: dict = None,
                 strip_headers: list[str] = None,
                 rename_headers: dict = None):
        """
        :param strip_prefixes:  URL prefixes to remove (e.g., ["/api/v1", "/api/v2"])
        :param inject_headers:  Static headers to always add to every request
        :param strip_headers:   Header names to remove before forwarding
        :param rename_headers:  {old_name: new_name} mapping
        """
        self.strip_prefixes = strip_prefixes or ["/api/v1", "/api/v2", "/api/v3"]
        self.inject_headers = inject_headers or {}
        self.strip_headers = [h.lower() for h in (strip_headers or [
            "authorization",   # Don't forward raw JWT to backend
            "x-api-key",       # Don't forward raw API key to backend
            "cookie",          # Strip session cookies
        ])]
        self.rename_headers = {k.lower(): v for k, v in (rename_headers or {}).items()}

    def transform(self, request: Request, client_id: str = "", role: str = "") -> Request:
        """
        Apply all request transformations in order.

        Transformation pipeline:
          1. Rewrite URL  (strip versioned prefix)
          2. Inject gateway headers (request ID, user ID, source IP)
          3. Strip sensitive headers  (authorization, api-key)
          4. Rename headers for backend compatibility

        :param request:   The original incoming request
        :param client_id: Authenticated client ID (from auth layer)
        :param role:      Authenticated role
        :return:          The mutated request (modified in place + returned)
        """
        original_path = request.path

        # ── Step 1: URL Rewriting — Strip versioned prefix ─────────────────
        for prefix in self.strip_prefixes:
            if request.path.startswith(prefix):
                request.path = request.path[len(prefix):]   # Strip the prefix
                if not request.path.startswith('/'):
                    request.path = '/' + request.path
                break  # Only strip one prefix

        # ── Step 2: Inject Gateway Headers ────────────────────────────────
        # Generate a unique request ID for distributed tracing
        request_id = str(uuid.uuid4())
        request.request_id = request_id

        request.headers.update({
            "X-Request-ID":     request_id,      # Unique per request (for tracing)
            "X-User-ID":        client_id,        # Decoded from JWT/API key
            "X-User-Role":      role,             # Decoded role
            "X-Forwarded-For":  request.client_ip,  # Original client IP
            "X-Gateway":        "PyGateway/1.0",  # Gateway identifier
            **self.inject_headers,                # Any additional static headers
        })

        # ── Step 3: Strip Sensitive Headers ──────────────────────────────
        # Remove headers that should NOT be forwarded to backend services
        stripped = []
        for header in list(request.headers.keys()):
            if header.lower() in self.strip_headers:
                del request.headers[header]
                stripped.append(header)

        # ── Step 4: Rename Headers ────────────────────────────────────────
        for old_name, new_name in self.rename_headers.items():
            for header_key in list(request.headers.keys()):
                if header_key.lower() == old_name:
                    request.headers[new_name] = request.headers.pop(header_key)
                    break

        print(f"  [ReqTransform] {request.method} {original_path} → {request.path} | "
              f"ReqID={request_id[:8]}... | Stripped={stripped}")

        return request


class ResponseTransformer:
    """
    Transforms backend responses BEFORE returning them to the client.

    Responsibilities:
      - Add security headers (HSTS, CSP, X-Frame-Options)
      - Add CORS headers (Access-Control-Allow-Origin)
      - Mask sensitive fields in the response body
      - Inject metadata (request_id, response_time, gateway version)
      - Normalize HTTP status codes
    """

    # Fields that should NEVER be sent to the client — always redacted
    SENSITIVE_FIELDS = {
        "password", "password_hash", "hashed_password",
        "secret", "api_key", "token", "access_token", "refresh_token",
        "ssn", "credit_card", "card_number", "cvv",
        "private_key", "private_data",
    }

    # Security headers added to EVERY response
    SECURITY_HEADERS = {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options":        "DENY",
        "X-XSS-Protection":       "1; mode=block",
        "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
        "Cache-Control":          "no-store",
        "Referrer-Policy":        "no-referrer",
    }

    def __init__(self, cors_origins: str = "*",
                 allowed_fields: Optional[set] = None,
                 extra_headers: dict = None):
        """
        :param cors_origins:   Value for Access-Control-Allow-Origin header
        :param allowed_fields: Whitelist of fields to KEEP (None = keep all except sensitive)
        :param extra_headers:  Additional headers to inject on all responses
        """
        self.cors_origins = cors_origins
        self.allowed_fields = allowed_fields
        self.extra_headers = extra_headers or {}

    def _mask_sensitive(self, body: dict) -> dict:
        """
        Recursively remove sensitive fields from the response body.

        Replaces sensitive field values with "[REDACTED]" instead of deleting
        them — this helps debugging while still protecting sensitive data.
        """
        if not isinstance(body, dict):
            return body
        result = {}
        for key, value in body.items():
            if key.lower() in self.SENSITIVE_FIELDS:
                result[key] = "[REDACTED]"  # Mask the value
            elif isinstance(value, dict):
                result[key] = self._mask_sensitive(value)   # Recurse
            elif isinstance(value, list):
                result[key] = [self._mask_sensitive(v) if isinstance(v, dict) else v
                               for v in value]
            else:
                result[key] = value
        return result

    def transform(self, response: Response, request_id: str = "",
                  start_time: float = 0) -> Response:
        """
        Apply all response transformations.

        Transformation pipeline:
          1. Mask sensitive fields in body
          2. Apply field whitelist (if configured)
          3. Add security headers
          4. Add CORS headers
          5. Inject metadata (request_id, response_time, gateway)

        :param response:    The backend response to transform
        :param request_id:  Request ID for tracing
        :param start_time:  Request start timestamp for latency calculation
        :return:            The mutated response
        """
        # ── Step 1: Mask sensitive data in response body ───────────────────
        if isinstance(response.body, dict):
            response.body = self._mask_sensitive(response.body)

        # ── Step 2: Apply field whitelist ─────────────────────────────────
        if self.allowed_fields and isinstance(response.body, dict):
            response.body = {k: v for k, v in response.body.items()
                             if k in self.allowed_fields}

        # ── Step 3 & 4: Add security + CORS headers ────────────────────────
        response.headers.update(self.SECURITY_HEADERS)
        response.headers["Access-Control-Allow-Origin"] = self.cors_origins
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, PATCH"
        response.headers["Access-Control-Allow-Headers"] = "Authorization, X-API-Key, Content-Type"

        # ── Step 5: Inject gateway metadata ───────────────────────────────
        latency_ms = round((time.time() - start_time) * 1000, 2) if start_time else 0
        response.headers.update({
            "X-Request-ID":      request_id,
            "X-Response-Time":   f"{latency_ms}ms",
            "X-Gateway-Version": "PyGateway/1.0",
            **self.extra_headers,
        })

        return response


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("   REQUEST & RESPONSE TRANSFORMER — API Gateway Demo")
    print("=" * 60)

    req_transformer = RequestTransformer(
        strip_prefixes=["/api/v1", "/api/v2"],
    )
    resp_transformer = ResponseTransformer(cors_origins="https://myapp.com")

    # ── Request Transformation Demo ────────────────────────────────────────
    print("\n  --- REQUEST TRANSFORMATION ---\n")

    test_requests = [
        Request("GET",  "/api/v1/users/42",
                headers={"Authorization": "Bearer jwt.token.here",
                         "Content-Type": "application/json"},
                client_ip="203.0.113.5"),

        Request("POST", "/api/v2/orders",
                headers={"X-API-Key": "sk-user-abc", "Content-Type": "application/json"},
                body={"product_id": "p1", "qty": 3},
                client_ip="198.51.100.7"),
    ]

    for req in test_requests:
        print(f"  Original: {req.method} {req.path} | Headers={list(req.headers.keys())}")
        req_transformer.transform(req, client_id="usr_42", role="USER")
        print(f"  After:    {req.method} {req.path}")
        print(f"  Headers:  {list(req.headers.keys())}")
        print()

    # ── Response Transformation Demo ───────────────────────────────────────
    print("\n  --- RESPONSE TRANSFORMATION ---\n")

    # Simulated backend response WITH sensitive fields
    raw_response = Response(
        status_code=200,
        body={
            "id": "usr_42",
            "name": "Alice",
            "email": "alice@example.com",
            "password_hash": "$2b$12$abc123xyz",    # ← sensitive!
            "api_key": "sk-internal-secret",         # ← sensitive!
            "ssn": "123-45-6789",                    # ← sensitive!
            "role": "USER",
        }
    )

    print(f"  Backend response body: {raw_response.body}\n")

    start = time.time()
    resp_transformer.transform(raw_response, request_id="req-abc-123", start_time=start)

    print(f"  Transformed body:    {raw_response.body}")
    print(f"  Security headers:    {list(ResponseTransformer.SECURITY_HEADERS.keys())}")
    print(f"  Gateway metadata:    X-Request-ID, X-Response-Time, X-Gateway-Version")
    print(f"  CORS:                {raw_response.headers.get('Access-Control-Allow-Origin')}")

    print("\n" + "=" * 60)
    print("  KEY TAKEAWAY: Transformer decouples client API surface")
    print("  from backend service contracts. Enables versioning,")
    print("  security sanitization, and protocol translation.")
    print("=" * 60)
