"""
╔══════════════════════════════════════════════════════════════════════════════╗
║             SERIALIZATION & DESERIALIZATION — MODULE 1                       ║
║                             JSON (REST APIs)                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IT DOES:
─────────────
Text-based, human-readable data format. Universally supported across every
programming language and web browser natively.

USER STORY:
───────────
As a frontend developer, I want to send and receive data from a REST API in a
format I can read directly in browser dev tools, so that I can debug requests
without special tooling.

WHY IT FITS:
────────────
• No schema needed (schemaless, self-describing key-value structure).
• Works with fetch() / XMLHttpRequest in JavaScript out of the box.
• Direct mapping to standard data structures (objects, arrays, strings, numbers).

DOWNSIDES:
──────────
• Verbose: repeats keys in every single record.
• No native binary, date/time, or custom object types (must encode as ISO strings).
• Slower to parse and serialize compared to compact binary formats at massive scale.
"""

import json
from datetime import datetime, timezone
import uuid
from typing import Any, Dict


class CustomJSONEncoder(json.JSONEncoder):
    """
    Handles types that the default Python json module does not support out of the box:
    - datetime -> ISO 8601 string
    - UUID -> string
    - set -> list
    """
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)


def serialize_user(user_data: Dict[str, Any]) -> str:
    """Serializes a Python dictionary to a JSON formatted string."""
    return json.dumps(user_data, cls=CustomJSONEncoder)


def deserialize_user(json_str: str) -> Dict[str, Any]:
    """Deserializes a JSON string back into a Python dictionary."""
    return json.loads(json_str)


class SimulatedFlaskAPI:
    """
    Simulates a Flask REST API endpoint demonstrating JSON request handling,
    deserialization, business validation, and serialized JSON response.
    """
    def __init__(self):
        self.db = {}

    def post_user(self, request_payload: str) -> tuple[int, str]:
        # Deserialization from incoming HTTP request body
        try:
            data = deserialize_user(request_payload)
        except json.JSONDecodeError as err:
            return 400, json.dumps({"error": "Invalid JSON", "details": str(err)})

        # Validate required fields
        if "name" not in data or "roles" not in data:
            return 422, json.dumps({"error": "Missing required fields: 'name' or 'roles'"})

        user_id = len(self.db) + 1
        record = {
            "id": user_id,
            "name": data["name"],
            "roles": data["roles"],
            "created_at": datetime.now(timezone.utc),
            "token_id": uuid.uuid4()
        }
        self.db[user_id] = record

        # Serialization to outgoing HTTP response body
        response_body = serialize_user({
            "status": "success",
            "data": record
        })
        return 201, response_body


def run_demo():
    print(f"\n{'═' * 65}")
    print("  1. JSON — REST APIs Demonstration")
    print(f"{'═' * 65}")

    # Standard User Story Demo
    user = {"id": 1, "name": "Alice", "roles": ["admin", "editor"]}
    print("\n[Serialization]")
    print(f"Original Object: {user}")
    json_string = json.dumps(user)
    print(f"JSON String:     {json_string}")
    print(f"Payload Size:    {len(json_string.encode('utf-8'))} bytes")

    print("\n[Deserialization]")
    deserialized_data = json.loads(json_string)
    print(f"Deserialized:    {deserialized_data}")
    print(f"Access user['name']: {deserialized_data['name']}")

    # Simulated REST API Flow
    print("\n[Simulated REST API Request/Response Cycle]")
    api = SimulatedFlaskAPI()
    client_payload = '{"name": "Alice", "roles": ["admin", "editor"]}'
    print(f"Incoming HTTP POST payload:\n  {client_payload}")

    status_code, response_payload = api.post_user(client_payload)
    print(f"\nResponse HTTP {status_code}:")
    print(f"  {response_payload}")

    # Inspect payload in DevTools style
    parsed_response = json.loads(response_payload)
    print("\n[Frontend DevTools View]")
    print(f"  Preview Object: id={parsed_response['data']['id']}, "
          f"name='{parsed_response['data']['name']}', "
          f"created_at='{parsed_response['data']['created_at']}'")


if __name__ == "__main__":
    run_demo()
