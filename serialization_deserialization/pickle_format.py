"""
╔══════════════════════════════════════════════════════════════════════════════╗
║             SERIALIZATION & DESERIALIZATION — MODULE 5                       ║
║              Pickle (Python-Specific Object Serialization)                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IT DOES:
─────────────
Serializes arbitrary, complex Python objects (classes, functions, closures,
generators, trained machine learning models) directly into byte streams.
Native to Python only.

USER STORY:
───────────
As a data scientist, I want to save a trained scikit-learn model object to disk
exactly as it is in memory, so I can reload it later in another script without
retraining.

WHY IT FITS:
────────────
• Can serialize custom class instances and graph references with zero boilerplate.
• Preserves Python object types, inheritance, and internal states faithfully.

DOWNSIDES & CRITICAL SECURITY WARNING:
──────────────────────────────────────
• NEVER unpickle data received from untrusted sources or across open networks!
  The pickle format is a bytecode execution virtual machine; deserialization
  can invoke arbitrary operating system commands (Remote Code Execution / RCE).
• Completely non-portable outside Python (cannot be read by Go, Rust, Java, etc.).
"""

import os
import pickle
import hmac
import hashlib
from typing import List


class Model:
    """Simulated Machine Learning Model."""
    def __init__(self, weights: List[float], model_name: str = "LinearRegressor"):
        self.weights = weights
        self.model_name = model_name

    def predict(self, features: List[float]) -> float:
        return sum(w * f for w, f in zip(self.weights, features))

    def __repr__(self) -> str:
        return f"Model(name='{self.model_name}', weights={self.weights})"


# ─── Security Exploit Demonstration ───────────────────────────────────────────

class SimulatedExploitPayload:
    """
    Demonstrates why unpickling untrusted data is dangerous.
    When pickle deserializes an object implementing __reduce__, it executes
    the callable returned by __reduce__ with the given arguments!
    """
    def __reduce__(self):
        # In a real attack, an attacker could supply os.system('rm -rf /')
        # Here we simulate a benign warning trigger
        return (print, ("⚠️ [SECURITY ALERT]: Arbitrary code executed during pickle.loads()!",))


# ─── HMAC Defense Pattern ─────────────────────────────────────────────────────

class SecurePickleSigner:
    """Cryptographically signs pickle payloads using HMAC-SHA256 to prevent tampering."""
    def __init__(self, secret_key: bytes):
        self.secret_key = secret_key

    def dumps_signed(self, obj: object) -> bytes:
        raw_bytes = pickle.dumps(obj)
        signature = hmac.new(self.secret_key, raw_bytes, hashlib.sha256).digest()
        # Wire: [32-byte SHA256 signature] + [raw pickle bytes]
        return signature + raw_bytes

    def loads_signed(self, signed_data: bytes) -> object:
        if len(signed_data) < 32:
            raise ValueError("Payload too short to contain HMAC signature")
        expected_sig = signed_data[:32]
        raw_bytes = signed_data[32:]
        computed_sig = hmac.new(self.secret_key, raw_bytes, hashlib.sha256).digest()
        if not hmac.compare_digest(expected_sig, computed_sig):
            raise SecurityError("HMAC signature verification failed! Untrusted or tampered pickle data.")
        return pickle.loads(raw_bytes)


class SecurityError(Exception):
    pass


def run_demo():
    print(f"\n{'═' * 65}")
    print("  5. Pickle — Python-Specific Object Serialization Demo")
    print(f"{'═' * 65}")

    # Standard User Story Demo
    model_file = "model.pkl"
    model = Model(weights=[0.2, 0.5, 0.3])
    print("\n[Serialization]")
    print(f"Original Model:  {model}")

    with open(model_file, "wb") as f:
        pickle.dump(model, f)
    print(f"Saved model to:  '{model_file}' ({os.path.getsize(model_file)} bytes)")

    print("\n[Deserialization]")
    with open(model_file, "rb") as f:
        loaded_model = pickle.load(f)
    print(f"Loaded Model:    {loaded_model}")
    print(f"Loaded Weights:  {loaded_model.weights}")
    prediction = loaded_model.predict([10.0, 5.0, 2.0])
    print(f"Inference result: predict([10.0, 5.0, 2.0]) = {prediction}")

    # Cleanup temporary file
    if os.path.exists(model_file):
        os.remove(model_file)

    # Security Exploit Demo
    print("\n[SECURITY DEMONSTRATION: Why untrusted pickle is dangerous]")
    exploit_bytes = pickle.dumps(SimulatedExploitPayload())
    print("Simulating an attacker injecting malicious pickle payload over the network...")
    print(f"Malicious bytecode length: {len(exploit_bytes)} bytes")
    print("Victim server calls: pickle.loads(untrusted_bytes)")
    pickle.loads(exploit_bytes)

    # Cryptographic Defense Demo
    print("\n[Defense: HMAC Signed Pickle Tokens]")
    signer = SecurePickleSigner(secret_key=b"super-secret-cluster-key")
    signed_payload = signer.dumps_signed(model)
    print(f"Created signed payload: {len(signed_payload)} bytes (includes 32-byte HMAC)")

    verified_model = signer.loads_signed(signed_payload)
    print(f"Verified & Loaded safely: {verified_model.model_name} ✅")

    # Tamper with the payload
    tampered_payload = bytearray(signed_payload)
    tampered_payload[-1] ^= 0xFF  # Flip a bit
    try:
        signer.loads_signed(bytes(tampered_payload))
    except SecurityError as e:
        print(f"Tamper detected and blocked: {e} 🛡️")


if __name__ == "__main__":
    run_demo()
