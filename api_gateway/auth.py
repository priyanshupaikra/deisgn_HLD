"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       API GATEWAY — COMPONENT 2                              ║
║                     AUTHENTICATION & AUTHORIZATION                           ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
The Auth layer is a MIDDLEWARE in the API Gateway pipeline. Every request
passes through it BEFORE reaching the backend service.

TWO SEPARATE CONCERNS:
──────────────────────
  AUTHENTICATION (AuthN):  "Who are you?"
    → Verify the identity of the caller
    → Methods: API Keys, JWT tokens, OAuth2, mTLS

  AUTHORIZATION (AuthZ):  "What are you allowed to do?"
    → Verify the caller has permission for the requested action
    → Methods: Role-Based Access Control (RBAC), scopes, policies

  Both happen in the gateway — backend services trust that requests
  reaching them have already been validated.

HOW API KEY AUTH WORKS:
────────────────────────
  1. Client includes a key in header: X-API-Key: sk-abc123
  2. Gateway looks up the key in its store (Redis/DB)
  3. If valid → attach client identity to request
  4. If invalid → reject with HTTP 401 Unauthorized

HOW JWT AUTH WORKS:
────────────────────
  JWT = JSON Web Token  (format: header.payload.signature)

  1. Client includes: Authorization: Bearer <jwt_token>
  2. Gateway VERIFIES the signature using a shared secret key
     (No DB lookup needed — JWT is self-contained!)
  3. Gateway DECODES the payload: {user_id, role, exp (expiry)}
  4. Checks token is not expired
  5. Attaches claims to request for downstream use

  JWT Structure:
    Header:   {"alg": "HS256", "typ": "JWT"}   ← Base64 encoded
    Payload:  {"user_id": "42", "role": "admin", "exp": 1234567890}
    Signature: HMAC-SHA256(header + "." + payload, secret_key)

HOW RBAC AUTHORIZATION WORKS:
───────────────────────────────
  RBAC = Role Based Access Control

  Each endpoint has a required minimum role:
    GET  /products     → PUBLIC  (anyone can access)
    GET  /users        → USER    (authenticated users only)
    POST /users        → ADMIN   (admin role required)
    DELETE /orders     → ADMIN

  Role hierarchy: ADMIN > USER > PUBLIC
  A USER cannot call admin-only endpoints → HTTP 403 Forbidden

ADVANTAGES OF GATEWAY-LEVEL AUTH:
───────────────────────────────────
  ✅ Centralized — auth logic in one place, not in every microservice
  ✅ Backend services are simpler (no auth code)
  ✅ Easy to rotate keys, add/remove users
  ✅ Audit logging at the gateway level

REAL-WORLD EXAMPLES:
────────────────────
  - AWS API Gateway: Lambda authorizers, Cognito integration
  - Kong: JWT plugin, OAuth2 plugin
  - Apigee: API key management, OAuth2
  - Netflix: Passport (internal auth gateway)

"""

import hmac
import hashlib
import base64
import json
import time
from dataclasses import dataclass
from typing import Optional
from router import Request, Response


# ─────────────────────────────────────────────────────────────────────────────
# Role Definitions (RBAC Hierarchy)
# ─────────────────────────────────────────────────────────────────────────────

# Role hierarchy: higher value = more permissions
ROLE_HIERARCHY = {
    "PUBLIC": 0,   # Unauthenticated / public access
    "USER":   1,   # Authenticated regular user
    "ADMIN":  2,   # Administrator with full access
}


@dataclass
class ClientIdentity:
    """Represents an authenticated client after successful auth."""
    client_id: str
    role: str                   # "PUBLIC", "USER", or "ADMIN"
    name: str = ""
    scopes: list = None         # Fine-grained permission scopes

    def has_role(self, required_role: str) -> bool:
        """Check if this client meets or exceeds the required role level."""
        return ROLE_HIERARCHY.get(self.role, 0) >= ROLE_HIERARCHY.get(required_role, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Simulated Secret Key Store (in production: use Redis or a secrets manager)
# ─────────────────────────────────────────────────────────────────────────────
JWT_SECRET = "super-secret-gateway-signing-key-2026"

# Simulated API Key registry: {api_key → ClientIdentity}
API_KEY_STORE: dict[str, ClientIdentity] = {
    "sk-public-111": ClientIdentity("client_public", "PUBLIC", "Public Client"),
    "sk-user-abc":   ClientIdentity("client_user_1", "USER",   "Alice"),
    "sk-user-def":   ClientIdentity("client_user_2", "USER",   "Bob"),
    "sk-admin-xyz":  ClientIdentity("client_admin",  "ADMIN",  "Admin Service"),
}

# Route-level RBAC: {(method, path_prefix) → minimum required role}
ROUTE_PERMISSIONS: list[tuple[str, str, str]] = [
    ("GET",    "/health",      "PUBLIC"),   # Health check — open to all
    ("GET",    "/products",    "PUBLIC"),   # Product listing — public
    ("GET",    "/users",       "USER"),     # User listing — authenticated only
    ("GET",    "/users/",      "USER"),     # User detail — authenticated only
    ("POST",   "/users",       "ADMIN"),    # Create user — admin only
    ("POST",   "/orders",      "USER"),     # Place order — authenticated
    ("GET",    "/orders",      "USER"),     # Order detail — authenticated
    ("DELETE", "/",            "ADMIN"),    # All DELETE — admin only
]


# ─────────────────────────────────────────────────────────────────────────────
# Minimal JWT Implementation (no external library — educational purposes)
# ─────────────────────────────────────────────────────────────────────────────

class MinimalJWT:
    """
    A minimal HS256 JWT implementation for educational demonstration.
    In production: use PyJWT library (pip install pyjwt).

    JWT = Base64URL(header) + "." + Base64URL(payload) + "." + HMAC-SHA256(...)
    """

    @staticmethod
    def _b64_encode(data: dict) -> str:
        """Encode dict as Base64URL string."""
        return base64.urlsafe_b64encode(
            json.dumps(data, separators=(',', ':')).encode()
        ).rstrip(b'=').decode()

    @staticmethod
    def _b64_decode(s: str) -> dict:
        """Decode Base64URL string back to dict."""
        padding = '=' * (4 - len(s) % 4)
        return json.loads(base64.urlsafe_b64decode(s + padding))

    @staticmethod
    def create(payload: dict, secret: str) -> str:
        """
        Create a signed JWT token.

        :param payload: Dict containing claims (user_id, role, exp, etc.)
        :param secret:  HMAC signing key
        :return:        JWT string: header.payload.signature
        """
        header = {"alg": "HS256", "typ": "JWT"}
        header_b64  = MinimalJWT._b64_encode(header)
        payload_b64 = MinimalJWT._b64_encode(payload)

        # Sign: HMAC-SHA256(header + "." + payload, secret)
        message = f"{header_b64}.{payload_b64}"
        signature = hmac.new(
            secret.encode(),
            message.encode(),
            digestmod=hashlib.sha256
        ).digest()
        sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b'=').decode()

        return f"{header_b64}.{payload_b64}.{sig_b64}"

    @staticmethod
    def verify(token: str, secret: str) -> Optional[dict]:
        """
        Verify JWT signature and return payload if valid.

        Steps:
          1. Split token into header, payload, signature parts
          2. Recompute expected signature from header + payload
          3. Compare with provided signature (constant-time comparison)
          4. If signatures match: decode and return payload
          5. If mismatch: return None (invalid token)

        :return: Decoded payload dict, or None if invalid/tampered
        """
        try:
            parts = token.split('.')
            if len(parts) != 3:
                return None

            header_b64, payload_b64, sig_b64 = parts

            # Recompute the expected signature
            message = f"{header_b64}.{payload_b64}"
            expected_sig = hmac.new(
                secret.encode(),
                message.encode(),
                digestmod=hashlib.sha256
            ).digest()
            expected_b64 = base64.urlsafe_b64encode(expected_sig).rstrip(b'=').decode()

            # IMPORTANT: Use hmac.compare_digest to prevent timing attacks
            # (prevents attackers from guessing signature byte by byte)
            if not hmac.compare_digest(expected_b64, sig_b64):
                return None  # Signature mismatch → token tampered!

            # Decode and return the payload
            return MinimalJWT._b64_decode(payload_b64)

        except Exception:
            return None  # Any error = invalid token


# ─────────────────────────────────────────────────────────────────────────────
# Auth Middleware
# ─────────────────────────────────────────────────────────────────────────────

class AuthMiddleware:
    """
    API Gateway Authentication & Authorization Middleware.

    Pipeline position: Router → [AuthMiddleware] → RateLimiter → Backend

    Supports:
      1. API Key authentication (X-API-Key header)
      2. JWT Bearer token authentication (Authorization: Bearer <token>)
      3. RBAC authorization (role check per route)

    If no auth credentials are provided → treated as PUBLIC role.
    If credentials are invalid → HTTP 401 Unauthorized.
    If role is insufficient → HTTP 403 Forbidden.
    """

    def __init__(self, jwt_secret: str = JWT_SECRET):
        self.jwt_secret = jwt_secret

    def authenticate(self, request: Request) -> tuple[Optional[ClientIdentity], Optional[Response]]:
        """
        Extract and validate the caller's identity from the request headers.

        Order of precedence:
          1. Check for API Key in X-API-Key header
          2. Check for JWT in Authorization: Bearer <token> header
          3. Fall back to PUBLIC identity (no auth = public access)

        :return: (identity, None) on success, (None, error_response) on failure
        """
        headers = {k.lower(): v for k, v in request.headers.items()}

        # ── Strategy 1: API Key Auth ──────────────────────────────────────
        api_key = headers.get("x-api-key", "")
        if api_key:
            identity = API_KEY_STORE.get(api_key)
            if not identity:
                return None, Response(401, {
                    "error": "Unauthorized",
                    "message": "Invalid API key"
                })
            return identity, None  # Valid API key!

        # ── Strategy 2: JWT Bearer Token Auth ─────────────────────────────
        auth_header = headers.get("authorization", "")
        if auth_header.startswith("bearer "):
            token = auth_header[7:]  # Strip "bearer " prefix
            payload = MinimalJWT.verify(token, self.jwt_secret)

            if not payload:
                return None, Response(401, {
                    "error": "Unauthorized",
                    "message": "Invalid or malformed JWT token"
                })

            # Check token expiry
            if payload.get("exp", 0) < time.time():
                return None, Response(401, {
                    "error": "Unauthorized",
                    "message": "JWT token has expired"
                })

            # Build identity from JWT claims
            identity = ClientIdentity(
                client_id=payload.get("user_id", "unknown"),
                role=payload.get("role", "USER"),
                name=payload.get("name", ""),
                scopes=payload.get("scopes", [])
            )
            return identity, None  # Valid JWT!

        # ── Strategy 3: No credentials → PUBLIC access ────────────────────
        return ClientIdentity("anonymous", "PUBLIC", "Anonymous"), None

    def authorize(self, identity: ClientIdentity, request: Request) -> Optional[Response]:
        """
        Check if the authenticated identity has permission for the route.

        Walks the ROUTE_PERMISSIONS list and finds the best-matching rule.
        If the client's role is below the required role → HTTP 403 Forbidden.

        :return: None if authorized, Response(403) if not permitted
        """
        required_role = "PUBLIC"  # Default: publicly accessible

        for method, path_prefix, role in ROUTE_PERMISSIONS:
            if request.method.upper() == method and request.path.startswith(path_prefix):
                required_role = role
                break  # Use first matching rule

        if not identity.has_role(required_role):
            return Response(403, {
                "error": "Forbidden",
                "message": f"Route requires role '{required_role}', "
                           f"but you have role '{identity.role}'"
            })

        return None  # Authorized!

    def process(self, request: Request) -> tuple[Optional[ClientIdentity], Optional[Response]]:
        """
        Full auth pipeline: authenticate then authorize.

        Called by the main gateway pipeline for every request.
        Attaches the client_id to the request on success.

        :return: (identity, None) on success, (None, error_response) on failure
        """
        # Step 1: Verify identity
        identity, err = self.authenticate(request)
        if err:
            return None, err

        # Step 2: Check permissions
        err = self.authorize(identity, request)
        if err:
            return None, err

        # Attach client ID to request (for downstream services + logging)
        request.client_id = identity.client_id
        return identity, None


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("   AUTH MIDDLEWARE — API Gateway Component Demo")
    print("=" * 60)

    auth = AuthMiddleware()

    # Generate some JWT tokens for demo
    user_token = MinimalJWT.create({
        "user_id": "usr_42",
        "name": "Alice",
        "role": "USER",
        "exp": int(time.time()) + 3600  # Expires in 1 hour
    }, JWT_SECRET)

    admin_token = MinimalJWT.create({
        "user_id": "adm_1",
        "name": "Admin",
        "role": "ADMIN",
        "exp": int(time.time()) + 3600
    }, JWT_SECRET)

    expired_token = MinimalJWT.create({
        "user_id": "usr_99",
        "role": "USER",
        "exp": int(time.time()) - 100  # Already expired!
    }, JWT_SECRET)

    print(f"\n  Generated JWT (USER):  {user_token[:50]}...")
    print(f"  Generated JWT (ADMIN): {admin_token[:50]}...")

    # Test scenarios
    scenarios = [
        # (description, request)
        ("Public health check (no auth)",
         Request("GET", "/health")),

        ("API Key (USER) → GET /users",
         Request("GET", "/users", headers={"X-API-Key": "sk-user-abc"})),

        ("API Key (USER) → POST /users (admin-only)",
         Request("POST", "/users", headers={"X-API-Key": "sk-user-abc"})),

        ("API Key (ADMIN) → POST /users",
         Request("POST", "/users", headers={"X-API-Key": "sk-admin-xyz"})),

        ("JWT (USER) → POST /orders",
         Request("POST", "/orders", headers={"Authorization": f"Bearer {user_token}"})),

        ("JWT (ADMIN) → DELETE any route",
         Request("DELETE", "/users/42", headers={"Authorization": f"Bearer {admin_token}"})),

        ("Invalid API key",
         Request("GET", "/users", headers={"X-API-Key": "sk-wrong-key"})),

        ("Expired JWT",
         Request("GET", "/users", headers={"Authorization": f"Bearer {expired_token}"})),

        ("Tampered JWT",
         Request("GET", "/users", headers={"Authorization": "Bearer fake.token.here"})),
    ]

    print(f"\n  {'SCENARIO':<50} {'RESULT'}")
    print(f"  {'─'*50} {'─'*20}")
    for desc, req in scenarios:
        identity, err = auth.process(req)
        if err:
            result = f"HTTP {err.status_code} | {err.body['message']}"
        else:
            result = f"HTTP 200 | identity={identity.client_id} role={identity.role}"
        print(f"  {desc:<50}")
        print(f"  {'':>2}→ {result}\n")

    print("=" * 60)
    print("  KEY TAKEAWAY: Auth happens at the gateway.")
    print("  Backend services receive pre-authenticated requests.")
    print("  API Keys for services, JWT for user sessions.")
    print("=" * 60)
