# System Design Building Blocks (Interview Order)

| #   | Concept                        | 1-line real-world analogy                                       |
| --- | ------------------------------ | --------------------------------------------------------------- |
| 1   | Rate Limiting                  | Security guard allows only a few people per minute.             |
| 2   | Load Balancer                  | Receptionist sends visitors to the least busy counter.          |
| 3   | API Gateway                    | Main entrance that checks tickets and directions.               |
| 4   | Authentication & Authorization | ID card check and access-level check.                           |
| 5   | Caching (Redis)                | Keep frequently used notes on your desk instead of the library. |
| 6   | Database                       | Official scorebook of the tournament.                           |
| 7   | Database Replication           | Photocopy the scorebook to another room.                        |
| 8   | Database Sharding              | Split players by age group into different registers.            |
| 9   | Message Queue (Kafka/RabbitMQ) | Take-a-token counter; requests wait in line.                    |
| 10  | Asynchronous Workers           | Back-office staff process forms later.                          |
| 11  | Search Engine (Elasticsearch)  | Fast index instead of reading every page.                       |
| 12  | CDN                            | Distribute posters to local branches near fans.                 |
| 13  | Monitoring & Logging           | CCTV plus match statistics dashboard.                           |
| 14  | Health Checks & Auto Scaling   | Add more gates when the crowd grows; close broken gates.        |
| 15  | Circuit Breaker                | Stop sending players to a closed practice ground.               |
| 16  | Idempotency                    | Submitting the same form twice still creates one booking.       |
| 17  | Webhooks vs Polling            | Organizer calls you when ready vs you keep asking every minute. |
| 18  | Distributed Tracing            | GPS trail showing where a parcel traveled.                      |
| 19  | Serialization & Deserialization | Packing furniture into flat-pack boxes for shipping and reassembling at destination. |

- link: https://lnkd.in/p/gBq5Jys3

---

# 19. Serialization & Deserialization

**Serialization (Encoding / Marshaling / Pickling)** is the process of converting in-memory objects or data structures into a standardized stream of bytes or text suitable for network transmission or disk storage.

**Deserialization (Decoding / Unmarshaling / Unpickling)** is the inverse process of reconstructing the original in-memory data structures from that byte or text stream.

---

### 1. JSON — REST APIs

**What it does:** Text-based, human-readable data format. Universally supported across every language and browser natively.

**User story:**
> As a frontend developer, I want to send and receive data from a REST API in a format I can read directly in browser dev tools, so that I can debug requests without special tooling.

**Code (Python — Flask API):**
```python
import json

# Serialization
user = {"id": 1, "name": "Alice", "roles": ["admin", "editor"]}
json_string = json.dumps(user)
print(json_string)
# '{"id": 1, "name": "Alice", "roles": ["admin", "editor"]}'

# Deserialization
data = json.loads(json_string)
print(data["name"])  # Alice
```

**Why it fits:** No schema needed, self-describing, works with `fetch()` in JS out of the box.  
**Downside:** Verbose (repeats keys every record), no native binary/date types, slower to parse than binary formats at scale.

---

### 2. Protobuf — gRPC / Microservices

**What it does:** Binary format requiring a predefined schema (`.proto` file). Compiles to strongly-typed classes in many languages. Very compact and fast.

**User story:**
> As a backend engineer building a gRPC service between an order-service and payment-service, I want a strict, versioned contract for messages so both services stay in sync even as they're deployed independently.

**Schema (`order.proto`):**
```protobuf
syntax = "proto3";

message Order {
  int32 id = 1;
  string customer_name = 2;
  float amount = 3;
}
```

**Code (Python, after generating `order_pb2.py`):**
```python
import order_pb2

# Serialize
order = order_pb2.Order(id=101, customer_name="Alice", amount=59.99)
binary_data = order.SerializeToString()
print(binary_data)  # compact binary bytes

# Deserialize
new_order = order_pb2.Order()
new_order.ParseFromString(binary_data)
print(new_order.customer_name)  # Alice
```

**Why it fits:** Schema enforces contracts across services/languages, messages are small and fast to (de)serialize, built-in versioning (field numbers let you add/remove fields safely).  
**Downside:** Not human-readable, requires a build step to generate code.

---

### 3. Avro — Kafka / Data Pipelines

**What it does:** Binary format with schema stored alongside the data (often in a schema registry). Great for evolving schemas over time in streaming pipelines.

**User story:**
> As a data engineer streaming clickstream events through Kafka, I want producers and consumers to evolve their event schema independently (adding new fields) without breaking old consumers.

**Schema (`user.avsc`):**
```json
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
```

**Code (Python, using `fastavro`):**
```python
import fastavro
import io

schema = fastavro.schema.load_schema("user.avsc")

# Serialize
buf = io.BytesIO()
fastavro.writer(buf, schema, [{"id": 1, "name": "Alice", "email": None}])
binary_data = buf.getvalue()

# Deserialize
buf.seek(0)
for record in fastavro.reader(buf):
    print(record)  # {'id': 1, 'name': 'Alice', 'email': None}
```

**Why it fits:** Schema travels with the data (or lives in a registry Kafka consumers can fetch), making it ideal for long-lived pipelines where producers/consumers deploy at different times.  
**Downside:** Tooling is heavier, less common outside the Hadoop/Kafka ecosystem.

---

### 4. MessagePack — Compact Network Communication

**What it does:** Binary version of JSON — same data model (objects, arrays, strings, numbers) but much smaller and faster to parse, with no schema required.

**User story:**
> As a game developer sending frequent player-position updates over a websocket, I want a JSON-like format that's smaller and faster to encode/decode so I can hit my latency budget without designing a formal schema.

**Code (Python, using `msgpack`):**
```python
import msgpack

# Serialize
player = {"x": 12.5, "y": 8.2, "health": 100}
binary_data = msgpack.packb(player)
print(len(binary_data))  # much smaller than json.dumps(player)

# Deserialize
data = msgpack.unpackb(binary_data)
print(data)  # {'x': 12.5, 'y': 8.2, 'health': 100}
```

**Why it fits:** Drop-in replacement for JSON with smaller payloads and faster parsing, no schema/build step needed.  
**Downside:** Not human-readable, less mainstream tooling/debugging support than JSON.

---

### 5. Pickle — Python-Specific Object Serialization

**What it does:** Serializes arbitrary Python objects (including custom classes, functions, even trained ML models) into bytes. Python-only.

**User story:**
> As a data scientist, I want to save a trained scikit-learn model object to disk exactly as it is in memory, so I can reload it later in another script without retraining.

**Code (Python):**
```python
import pickle

class Model:
    def __init__(self, weights):
        self.weights = weights

model = Model(weights=[0.2, 0.5, 0.3])

# Serialize to file
with open("model.pkl", "wb") as f:
    pickle.dump(model, f)

# Deserialize from file
with open("model.pkl", "rb") as f:
    loaded_model = pickle.load(f)
print(loaded_model.weights)  # [0.2, 0.5, 0.3]
```

**Why it fits:** Can serialize almost any Python object with zero extra code.  
**Downside — important:** Never unpickle data from untrusted sources, since deserializing can execute arbitrary code. Also completely non-portable outside Python.

---

### 6. XML — Legacy / Enterprise Integrations

**What it does:** Verbose, tag-based markup format. Supports schemas (XSD), namespaces, and strict validation — common in older enterprise systems (banking, SOAP APIs, government systems).

**User story:**
> As an integration engineer connecting to a 15-year-old banking SOAP API, I need to produce and parse XML in the exact schema the bank's system expects, since that's the only format their gateway accepts.

**Code (Python, using `xml.etree.ElementTree`):**
```python
import xml.etree.ElementTree as ET

# Serialize
order = ET.Element("Order")
ET.SubElement(order, "ID").text = "101"
ET.SubElement(order, "Customer").text = "Alice"
xml_string = ET.tostring(order, encoding="unicode")
print(xml_string)
# <Order><ID>101</ID><Customer>Alice</Customer></Order>

# Deserialize
root = ET.fromstring(xml_string)
print(root.find("Customer").text)  # Alice
```

**Why it fits:** Rich validation via XSD, strong namespace support for complex enterprise documents, widely entrenched in legacy systems.  
**Downside:** Very verbose, slower to parse, mostly being phased out in favor of JSON/Protobuf for new systems.

---

### Quick Comparison

| Format | Human-readable | Schema required | Speed | Typical home |
| :--- | :--- | :--- | :--- | :--- |
| **JSON** | Yes | No | Medium | Web APIs |
| **Protobuf** | No | Yes | Very fast | Internal microservices |
| **Avro** | No | Yes (evolvable) | Fast | Kafka/streaming |
| **MessagePack** | No | No | Fast | Low-latency apps/games |
| **Pickle** | No | No | Fast (Python only) | Python-internal caching/ML |
| **XML** | Yes | Optional (XSD) | Slow | Legacy enterprise systems |

---

### Common Use-Cases

| Format | Common use |
| :--- | :--- |
| **JSON** | REST APIs |
| **Protobuf** | gRPC / microservices |
| **Avro** | Kafka/data pipelines |
| **MessagePack** | Compact network communication |
| **Pickle** | Python-specific object serialization |
| **XML** | Legacy/enterprise integrations |

---

### Running the Code

Navigate to `serialization_deserialization/` and run the all-in-one suite or individual modules:

```bash
# Run the complete test suite & interactive benchmarks
python3 serialization_deserialization/main.py

# Run individual modules
python3 serialization_deserialization/json_format.py
python3 serialization_deserialization/protobuf_format.py
python3 serialization_deserialization/avro_format.py
python3 serialization_deserialization/msgpack_format.py
python3 serialization_deserialization/pickle_format.py
python3 serialization_deserialization/xml_format.py
python3 serialization_deserialization/benchmarks.py
```
