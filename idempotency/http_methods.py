"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                       IDEMPOTENCY — MODULE 2                                 ║
║                    HTTP METHOD IDEMPOTENCY SEMANTICS                         ║
╚══════════════════════════════════════════════════════════════════════════════╝

CONCEPT:
--------
HTTP defines a CONTRACT for each method regarding idempotency and safety.
Understanding which methods are idempotent and why is fundamental to
correct API design.

HTTP METHOD PROPERTIES (per RFC 7231):
────────────────────────────────────────
  Method   | Idempotent? | Safe? | Has Body? | Typical Use
  ─────────────────────────────────────────────────────────────────────
  GET      | YES         | YES   | No        | Retrieve resource
  HEAD     | YES         | YES   | No        | Retrieve headers only
  OPTIONS  | YES         | YES   | No        | Discover capabilities
  DELETE   | YES         | NO    | Optional  | Remove resource
  PUT      | YES         | NO    | YES       | Replace resource entirely
  PATCH    | NO*         | NO    | YES       | Partial update (can be designed to be)
  POST     | NO          | NO    | YES       | Create / trigger action

  Safe     = Does NOT modify server state (read-only)
  Idempotent = Multiple identical requests = same server state as one request

KEY INSIGHTS:
─────────────
  GET is idempotent AND safe:
    GET /users/42 → {name: "Alice"}
    GET /users/42 → {name: "Alice"}  (same result, no state change)

  DELETE is idempotent but NOT safe:
    DELETE /users/42 → 200 OK (user deleted)
    DELETE /users/42 → 404 Not Found (already gone, but state is the same!)
    Both results in the ABSENCE of user 42 on the server.
    Note: The response code changes, but the SERVER STATE doesn't.

  PUT is idempotent but NOT safe:
    PUT /users/42 {"name": "Bob"} → 200 OK (user updated)
    PUT /users/42 {"name": "Bob"} → 200 OK (same update, same final state)

  POST is NOT idempotent:
    POST /orders {"product": "p1"} → Order #1 created
    POST /orders {"product": "p1"} → Order #2 created (DUPLICATE!)

  PATCH is ambiguous:
    PATCH /balance {"op": "add", "amount": 10} → NOT idempotent (adds $10 each time)
    PATCH /users/42 {"name": "Alice"}           → IS idempotent (same final state)
    Design determines idempotency!

THE PUT vs PATCH DISTINCTION:
───────────────────────────────
  PUT    → Replace the ENTIRE resource (full update)
  PATCH  → Replace ONLY specified fields (partial update)

  PUT /users/42 {"name": "Bob"}     → ALL other fields become null/default!
  PATCH /users/42 {"name": "Bob"}   → Only name changes; other fields preserved

HTTP STATUS CODE GUIDE FOR IDEMPOTENT OPERATIONS:
───────────────────────────────────────────────────
  GET:    200 OK
  PUT:    200 OK (updated) | 201 Created (new) | 204 No Content
  DELETE: 200 OK | 204 No Content | 404 Not Found (already deleted)
  POST:   201 Created | 200 OK (for actions, not resources)

"""

from enum import Enum
from typing import Any, Optional
from dataclasses import dataclass, field


# ─────────────────────────────────────────────────────────────────────────────
# Resource simulation (in-memory "database")
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class User:
    id: int
    name: str
    email: str
    role: str = "user"
    balance: float = 0.0

    def to_dict(self) -> dict:
        return {"id": self.id, "name": self.name,
                "email": self.email, "role": self.role, "balance": self.balance}


@dataclass
class APIResponse:
    status_code: int
    body: Any
    method: str = ""
    path: str = ""

    def __repr__(self):
        return f"HTTP {self.status_code}: {self.body}"


class UserRepository:
    """In-memory user store to simulate a database."""

    def __init__(self):
        self._users: dict[int, User] = {
            1: User(1, "Alice", "alice@example.com", "admin", 500.0),
            2: User(2, "Bob",   "bob@example.com",   "user",  100.0),
        }
        self._next_id = 3
        self.operation_log: list[str] = []   # Audit log of all state changes

    def find(self, user_id: int) -> Optional[User]:
        return self._users.get(user_id)

    def exists(self, user_id: int) -> bool:
        return user_id in self._users

    def create(self, name: str, email: str) -> User:
        user = User(self._next_id, name, email)
        self._users[self._next_id] = user
        self._next_id += 1
        self.operation_log.append(f"CREATE user:{user.id} ({name})")
        return user

    def replace(self, user_id: int, data: dict) -> Optional[User]:
        """Full replacement — PUT semantics."""
        user = User(
            id=user_id,
            name=data.get("name", ""),
            email=data.get("email", ""),
            role=data.get("role", "user"),
            balance=data.get("balance", 0.0),
        )
        self._users[user_id] = user
        self.operation_log.append(f"REPLACE user:{user_id}")
        return user

    def patch(self, user_id: int, changes: dict) -> Optional[User]:
        """Partial update — PATCH semantics."""
        user = self._users.get(user_id)
        if not user:
            return None
        for key, val in changes.items():
            if hasattr(user, key):
                setattr(user, key, val)
        self.operation_log.append(f"PATCH user:{user_id} {changes}")
        return user

    def delete(self, user_id: int) -> bool:
        if user_id in self._users:
            del self._users[user_id]
            self.operation_log.append(f"DELETE user:{user_id}")
            return True
        return False


# ─────────────────────────────────────────────────────────────────────────────
# HTTP Method Handlers — demonstrating idempotency semantics
# ─────────────────────────────────────────────────────────────────────────────

class HTTPMethodDemo:
    """
    Demonstrates correct idempotency semantics for each HTTP method.

    Shows the CONTRACT each method must uphold:
      - GET:    Always safe to call N times
      - PUT:    Calling N times = calling once (full replace, same final state)
      - DELETE: Calling N times = calling once (resource absent, always)
      - POST:   Each call MAY create a new resource (client must use idempotency key)
      - PATCH:  Depends on implementation (can be either)
    """

    def __init__(self, db: UserRepository):
        self.db = db

    # ── GET ──────────────────────────────────────────────────────────────────
    def GET(self, path: str, user_id: int) -> APIResponse:
        """
        GET /users/{id}

        SAFE + IDEMPOTENT:
          - Never modifies state
          - Same result every time (assuming no other writes happen)
          - Safe to retry as many times as needed
        """
        user = self.db.find(user_id)
        if not user:
            return APIResponse(404, {"error": "User not found"}, "GET", path)
        return APIResponse(200, user.to_dict(), "GET", path)

    # ── PUT ──────────────────────────────────────────────────────────────────
    def PUT(self, path: str, user_id: int, body: dict) -> APIResponse:
        """
        PUT /users/{id}

        NOT SAFE, but IDEMPOTENT:
          - Replaces the ENTIRE resource with the provided body
          - Any field NOT in body is reset to default
          - Sending the same body twice → same final state

        WHY IDEMPOTENT:
          PUT /users/1 {name: "Alice", email: "new@ex.com", role: "admin"}
          PUT /users/1 {name: "Alice", email: "new@ex.com", role: "admin"}
          → Both result in the same user record. No harm in retrying.
        """
        if not body.get("name") or not body.get("email"):
            return APIResponse(400, {"error": "name and email required"}, "PUT", path)

        is_new = not self.db.exists(user_id)
        user = self.db.replace(user_id, body)
        code = 201 if is_new else 200
        return APIResponse(code, user.to_dict(), "PUT", path)

    # ── DELETE ───────────────────────────────────────────────────────────────
    def DELETE(self, path: str, user_id: int) -> APIResponse:
        """
        DELETE /users/{id}

        NOT SAFE, but IDEMPOTENT:
          - Removes the resource
          - If already gone: returns 404 (but server STATE is the same!)

        IDEMPOTENCY SUBTLETY:
          First call:  DELETE /users/42 → 200 OK (user deleted)
          Second call: DELETE /users/42 → 404 Not Found

          The response CODE differs, but the server STATE is the same:
          user 42 does NOT exist. This is still considered idempotent
          because the END STATE is identical regardless of how many times
          you call it.

        SOME APIS: return 204 No Content on repeated DELETE for true idempotency
        (same status code every time). Both approaches are acceptable.
        """
        deleted = self.db.delete(user_id)
        if deleted:
            return APIResponse(200, {"message": f"User {user_id} deleted"}, "DELETE", path)
        return APIResponse(404, {"error": f"User {user_id} not found (already deleted?)"}, "DELETE", path)

    # ── POST ─────────────────────────────────────────────────────────────────
    def POST(self, path: str, body: dict, idempotency_key: str = None) -> APIResponse:
        """
        POST /users

        NOT SAFE, NOT IDEMPOTENT (by default):
          - Each call creates a NEW resource
          - Retrying creates duplicates!

        TO MAKE POST IDEMPOTENT:
          Use an Idempotency-Key header (see idempotency_key.py module)
          Client sends: POST /users with Idempotency-Key: uuid-123
          Server deduplicates using the key.

        NOTE:
          POST is used for:
          1. Creating new resources: POST /users
          2. Triggering actions:     POST /payments/charge
          3. RPC-style operations:   POST /send-email
          All are non-idempotent without a deduplication strategy.
        """
        if not body.get("name") or not body.get("email"):
            return APIResponse(400, {"error": "name and email required"}, "POST", path)

        user = self.db.create(body["name"], body["email"])
        return APIResponse(
            201,
            {"message": "User created", "user": user.to_dict()},
            "POST",
            path,
        )

    # ── PATCH ────────────────────────────────────────────────────────────────
    def PATCH(self, path: str, user_id: int, body: dict) -> APIResponse:
        """
        PATCH /users/{id}

        DEPENDS ON IMPLEMENTATION:

        Case 1 — IDEMPOTENT PATCH (setting absolute values):
          PATCH /users/42 {"name": "Alice"} → name = "Alice"
          PATCH /users/42 {"name": "Alice"} → name = "Alice"  (same result)
          ✅ Idempotent

        Case 2 — NON-IDEMPOTENT PATCH (applying deltas):
          PATCH /balance {"op": "add", "amount": 10} → balance += 10
          PATCH /balance {"op": "add", "amount": 10} → balance += 10 again!
          ❌ NOT idempotent

        This implementation does Case 1 (safe, absolute field updates).
        """
        user = self.db.find(user_id)
        if not user:
            return APIResponse(404, {"error": "User not found"}, "PATCH", path)

        # Disallow 'balance' patches via PATCH (requires separate payment flow)
        if "balance" in body:
            return APIResponse(422, {"error": "Use /payments to update balance"}, "PATCH", path)

        updated = self.db.patch(user_id, body)
        return APIResponse(200, updated.to_dict(), "PATCH", path)


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

def show(label: str, resp: APIResponse) -> None:
    print(f"    {label:<45} → HTTP {resp.status_code}: {resp.body}")


if __name__ == "__main__":
    print("=" * 65)
    print("   HTTP METHOD IDEMPOTENCY SEMANTICS — Demo")
    print("=" * 65)

    db = UserRepository()
    api = HTTPMethodDemo(db)

    # ── GET (safe + idempotent) ───────────────────────────────────────────
    print("\n  ─── GET: Safe + Idempotent ───")
    show("GET /users/1 (1st call)", api.GET("/users/1", 1))
    show("GET /users/1 (2nd call)", api.GET("/users/1", 1))
    show("GET /users/1 (3rd call)", api.GET("/users/1", 1))
    print(f"  DB operation log after 3 GETs: {db.operation_log} (empty — no state changes)")

    # ── PUT (idempotent but not safe) ─────────────────────────────────────
    print("\n  ─── PUT: Idempotent (full replace) ───")
    payload = {"name": "Alice Updated", "email": "alice.new@ex.com", "role": "admin", "balance": 500.0}
    show("PUT /users/1 (1st call)", api.PUT("/users/1", 1, payload))
    show("PUT /users/1 (2nd call)", api.PUT("/users/1", 1, payload))
    show("PUT /users/1 (3rd call)", api.PUT("/users/1", 1, payload))
    print(f"  DB log: {db.operation_log[-3:]} (3 REPLACE operations, same final state)")

    # ── DELETE (idempotent, response varies) ──────────────────────────────
    print("\n  ─── DELETE: Idempotent (state same; response code may differ) ───")
    show("DELETE /users/2 (1st call)", api.DELETE("/users/2", 2))
    show("DELETE /users/2 (2nd call)", api.DELETE("/users/2", 2))  # 404 — already gone
    show("DELETE /users/2 (3rd call)", api.DELETE("/users/2", 2))  # Still 404
    print(f"  User 2 exists: {db.exists(2)} (False — correctly gone after just ONE delete)")

    # ── POST (NOT idempotent → duplicates) ───────────────────────────────
    print("\n  ─── POST: NOT Idempotent (creates duplicates!) ───")
    body = {"name": "Carol", "email": "carol@example.com"}
    show("POST /users (1st call)", api.POST("/users", body))
    show("POST /users (2nd call)", api.POST("/users", body))   # DUPLICATE!
    show("POST /users (3rd call)", api.POST("/users", body))   # DUPLICATE!
    print(f"  Users created: {len([l for l in db.operation_log if l.startswith('CREATE')])} "
          f"(3 Carol accounts! — use Idempotency-Key to prevent this)")

    # ── PATCH (idempotent when setting absolute values) ───────────────────
    print("\n  ─── PATCH: Idempotent (absolute field update) ───")
    show("PATCH /users/1 name='Alice'", api.PATCH("/users/1", 1, {"name": "Alice Final"}))
    show("PATCH /users/1 name='Alice'", api.PATCH("/users/1", 1, {"name": "Alice Final"}))
    show("PATCH /users/1 name='Alice'", api.PATCH("/users/1", 1, {"name": "Alice Final"}))
    print(f"  Name after 3 patches: {api.GET('/users/1', 1).body.get('name')}"
          f" (same — idempotent)")

    # ── Comparison table ──────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("  HTTP METHODS IDEMPOTENCY REFERENCE")
    print("=" * 65)
    rows = [
        ("Method", "Idempotent?", "Safe?", "Use Case"),
        ("─"*7,    "─"*12,        "─"*5,   "─"*30),
        ("GET",    "YES",         "YES",   "Read resource"),
        ("HEAD",   "YES",         "YES",   "Check existence/headers"),
        ("OPTIONS","YES",         "YES",   "CORS preflight"),
        ("DELETE", "YES",         "NO",    "Remove resource"),
        ("PUT",    "YES",         "NO",    "Full replace resource"),
        ("PATCH",  "DEPENDS",     "NO",    "Partial update (design carefully)"),
        ("POST",   "NO",          "NO",    "Create/action (use Idempotency-Key)"),
    ]
    for r in rows:
        print(f"  {r[0]:<8} {r[1]:<14} {r[2]:<7} {r[3]}")
    print("=" * 65)
