"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                AUTHENTICATION & AUTHORIZATION — MODULE 1                     ║
║                    AUTHENTICATION: SESSIONS VS JWT                           ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS AUTHENTICATION?
───────────────────────
Authentication (AuthN) is answering the question: "Who are you?"
It usually involves proving identity via a password, biometrics, or an OTP.
Once proven, the server gives the client a "token" so they don't have to log in 
again on every single click.

1. STATEFUL (SESSION) AUTHENTICATION:
   - Server validates password.
   - Server generates a random string (Session ID: "XYZ123").
   - Server SAVES this in a database (e.g., Redis): {"XYZ123": user_id_5}.
   - Server gives the ID to the client as a Cookie.
   - Pros: Very secure. Easy to instantly revoke/logout a user (just delete from Redis).
   - Cons: Server must do a DB lookup on *every single request* to see who "XYZ123" is.

2. STATELESS (JWT - JSON WEB TOKEN) AUTHENTICATION:
   - Server validates password.
   - Server creates a JSON object: {"user_id": 5, "exp": "tomorrow"}.
   - Server cryptographically SIGNS this JSON using a Secret Key.
   - Server gives this token (JWT) to the client.
   - Client sends JWT on next request. Server verifies the signature mathematically.
   - Pros: NO database lookup required! Fast and highly scalable.
   - Cons: Cannot be easily revoked before it expires. If a hacker steals a JWT,
           they have access until the token's 'exp' time runs out.
"""

import time
import base64
import json
import hmac
import hashlib
import uuid


# ─────────────────────────────────────────────────────────────────────────────
# 1. STATEFUL SESSIONS (Redis style)
# ─────────────────────────────────────────────────────────────────────────────

class SessionDatabase:
    def __init__(self):
        # Simulates a Redis store: { session_id: user_id }
        self.sessions = {}

    def login(self, username: str, password: str) -> str:
        if password != "secret":
            raise ValueError("Invalid password")
            
        session_id = uuid.uuid4().hex
        self.sessions[session_id] = username
        print(f"  [Session DB] Created session '{session_id}' for {username}")
        return session_id

    def authenticate_request(self, session_id: str) -> str:
        # 🚨 This requires a database lookup!
        username = self.sessions.get(session_id)
        if not username:
            raise PermissionError("401 Unauthorized: Invalid or expired session")
        return username
        
    def logout(self, session_id: str):
        # Instant revocation!
        if session_id in self.sessions:
            del self.sessions[session_id]
            print(f"  [Session DB] Session '{session_id}' revoked instantly.")


# ─────────────────────────────────────────────────────────────────────────────
# 2. STATELESS JWT (JSON Web Tokens)
# ─────────────────────────────────────────────────────────────────────────────

class JWTHelper:
    def __init__(self, secret_key: str):
        self.secret_key = secret_key

    def _b64_encode(self, data: dict) -> str:
        json_str = json.dumps(data, separators=(',', ':'))
        return base64.urlsafe_b64encode(json_str.encode()).decode().rstrip('=')

    def login(self, username: str, password: str) -> str:
        if password != "secret":
            raise ValueError("Invalid password")
            
        header = {"alg": "HS256", "typ": "JWT"}
        payload = {
            "sub": username,
            "role": "admin",
            "exp": int(time.time()) + 3600 # Expires in 1 hour
        }
        
        b64_header = self._b64_encode(header)
        b64_payload = self._b64_encode(payload)
        
        # Cryptographic Signature
        signature_input = f"{b64_header}.{b64_payload}"
        signature = hmac.new(
            self.secret_key.encode(),
            signature_input.encode(),
            hashlib.sha256
        ).digest()
        
        b64_signature = base64.urlsafe_b64encode(signature).decode().rstrip('=')
        
        # The final JWT
        jwt_token = f"{signature_input}.{b64_signature}"
        print(f"  [JWT Issuer] Created JWT for {username}. Note: Nothing saved in DB!")
        return jwt_token

    def authenticate_request(self, jwt_token: str) -> str:
        """Verifies mathematically, NO database lookup needed."""
        try:
            b64_header, b64_payload, b64_signature = jwt_token.split('.')
            
            # 1. Verify Signature
            signature_input = f"{b64_header}.{b64_payload}"
            expected_sig = hmac.new(
                self.secret_key.encode(),
                signature_input.encode(),
                hashlib.sha256
            ).digest()
            expected_b64_sig = base64.urlsafe_b64encode(expected_sig).decode().rstrip('=')
            
            if not hmac.compare_digest(b64_signature, expected_b64_sig):
                raise PermissionError("401 Unauthorized: Signature tampered with!")
                
            # 2. Verify Expiration
            # Pad the payload back to multiple of 4
            pad = len(b64_payload) % 4
            if pad: b64_payload += '=' * (4 - pad)
            
            payload = json.loads(base64.urlsafe_b64decode(b64_payload))
            if int(time.time()) > payload["exp"]:
                raise PermissionError("401 Unauthorized: Token expired")
                
            return payload["sub"]
            
        except Exception as e:
            raise PermissionError(f"401 Unauthorized: {str(e)}")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   AUTHENTICATION: SESSIONS vs JWT — Demo")
    print("=" * 65)
    
    print("\n  --- 1. STATEFUL SESSIONS ---")
    session_db = SessionDatabase()
    sid = session_db.login("alice", "secret")
    print(f"  App asks: 'Who is this?' -> Session DB says: {session_db.authenticate_request(sid)}")
    
    session_db.logout(sid)
    try:
        session_db.authenticate_request(sid)
    except Exception as e:
        print(f"  Attempting request after logout: {e}")


    print("\n  --- 2. STATELESS JWT ---")
    jwt_helper = JWTHelper(secret_key="super_secret_production_key")
    token = jwt_helper.login("bob", "secret")
    print(f"  JWT looks like: {token[:20]}...{token[-20:]}")
    
    print(f"  App asks: 'Who is this?' -> JWT Math verifies: {jwt_helper.authenticate_request(token)}")
    
    # Tampering test
    tampered_token = token[:-5] + "XXXXX"
    print("\n  [Hacker] Tampering with the token signature...")
    try:
        jwt_helper.authenticate_request(tampered_token)
    except Exception as e:
        print(f"  App rejects tampered token: {e}")
