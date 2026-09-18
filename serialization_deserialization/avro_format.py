"""
╔══════════════════════════════════════════════════════════════════════════════╗
║             SERIALIZATION & DESERIALIZATION — MODULE 3                       ║
║                         Avro (Kafka / Data Pipelines)                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IT DOES:
─────────────
Binary serialization format where the schema is stored alongside the data
(or in a central Schema Registry). Designed by Doug Cutting for Apache Hadoop
and Apache Kafka streaming architectures.

USER STORY:
───────────
As a data engineer streaming clickstream events through Kafka, I want producers
and consumers to evolve their event schema independently (adding new fields)
without breaking old consumers.

SCHEMA (user.avsc):
───────────────────
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}

WHY IT FITS:
────────────
• Records contain NO field names and NO field tags — only raw values.
• Schema travels with container files or via Schema Registry ID (5-byte header).
• Exceptional schema evolution support (backward, forward, full compatibility).

DOWNSIDES:
──────────
• Requires the writer's schema to read the data.
• Tooling and ecosystem are heavily tied to Hadoop / Kafka.
"""

import io
import json
import os
import struct
from typing import Any, Dict, List, Optional, Tuple


# ─── Pure Python Avro Binary Encoder & Decoder ────────────────────────────────

def zigzag_encode(n: int) -> bytes:
    """Avro encodes ints/longs with variable-length zigzag encoding."""
    val = (n << 1) ^ (n >> 63)
    out = bytearray()
    while (val & ~0x7F) != 0:
        out.append((val & 0x7F) | 0x80)
        val >>= 7
    out.append(val & 0x7F)
    return bytes(out)


def zigzag_decode(stream: io.BytesIO) -> int:
    """Decodes a variable-length zigzag encoded int/long."""
    val = 0
    shift = 0
    while True:
        b = stream.read(1)
        if not b:
            raise EOFError("Premature end of Avro stream")
        byte = b[0]
        val |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    # Un-zigzag
    return (val >> 1) ^ -(val & 1)


class AvroBinaryEncoder:
    """Serializes Python dicts according to an Avro record schema."""
    @classmethod
    def write_record(cls, buf: io.BytesIO, schema: Dict[str, Any], record: Dict[str, Any]) -> None:
        fields = schema.get("fields", [])
        for field in fields:
            name = field["name"]
            ftype = field["type"]
            val = record.get(name, field.get("default"))
            cls.write_field(buf, ftype, val)

    @classmethod
    def write_field(cls, buf: io.BytesIO, ftype: Any, val: Any) -> None:
        if ftype == "int" or ftype == "long":
            buf.write(zigzag_encode(int(val)))
        elif ftype == "float":
            buf.write(struct.pack("<f", float(val)))
        elif ftype == "double":
            buf.write(struct.pack("<d", float(val)))
        elif ftype == "string":
            str_bytes = str(val).encode("utf-8")
            buf.write(zigzag_encode(len(str_bytes)))
            buf.write(str_bytes)
        elif ftype == "boolean":
            buf.write(b"\x01" if val else b"\x00")
        elif isinstance(ftype, list):
            # Union type, e.g. ["null", "string"]
            if val is None:
                null_index = ftype.index("null")
                buf.write(zigzag_encode(null_index))
            else:
                non_null_type = [t for t in ftype if t != "null"][0]
                type_index = ftype.index(non_null_type)
                buf.write(zigzag_encode(type_index))
                cls.write_field(buf, non_null_type, val)
        else:
            raise NotImplementedError(f"Avro type {ftype} not implemented in demo")


class AvroBinaryDecoder:
    """Deserializes binary bytes according to an Avro record schema."""
    @classmethod
    def read_record(cls, stream: io.BytesIO, writer_schema: Dict[str, Any],
                    reader_schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        target_schema = reader_schema or writer_schema
        raw_values = {}

        # Decode writer fields in exact binary sequence
        for field in writer_schema.get("fields", []):
            name = field["name"]
            ftype = field["type"]
            raw_values[name] = cls.read_field(stream, ftype)

        # Reconcile with reader schema (supports added fields with defaults)
        result = {}
        for field in target_schema.get("fields", []):
            name = field["name"]
            if name in raw_values:
                result[name] = raw_values[name]
            elif "default" in field:
                result[name] = field["default"]
            else:
                raise ValueError(f"Missing required field in reader schema: {name}")

        return result

    @classmethod
    def read_field(cls, stream: io.BytesIO, ftype: Any) -> Any:
        if ftype == "int" or ftype == "long":
            return zigzag_decode(stream)
        elif ftype == "float":
            return struct.unpack("<f", stream.read(4))[0]
        elif ftype == "double":
            return struct.unpack("<d", stream.read(8))[0]
        elif ftype == "string":
            length = zigzag_decode(stream)
            return stream.read(length).decode("utf-8")
        elif ftype == "boolean":
            b = stream.read(1)
            return b != b"\x00"
        elif isinstance(ftype, list):
            # Union
            branch_index = zigzag_decode(stream)
            selected_type = ftype[branch_index]
            if selected_type == "null":
                return None
            return cls.read_field(stream, selected_type)
        else:
            raise NotImplementedError(f"Avro type {ftype} not implemented in demo")


# ─── Kafka Schema Registry Simulator ──────────────────────────────────────────

class SchemaRegistry:
    """Simulates Confluent Schema Registry storing schemas by auto-incrementing ID."""
    def __init__(self):
        self.schemas: Dict[int, Dict[str, Any]] = {}
        self._next_id = 1

    def register(self, schema: Dict[str, Any]) -> int:
        for sid, s in self.schemas.items():
            if s == schema:
                return sid
        sid = self._next_id
        self._next_id += 1
        self.schemas[sid] = schema
        return sid

    def get_schema(self, schema_id: int) -> Dict[str, Any]:
        return self.schemas[schema_id]


def avro_kafka_serialize(record: Dict[str, Any], schema: Dict[str, Any],
                        registry: SchemaRegistry) -> bytes:
    """
    Standard Kafka Avro Wire Format:
    [0x00] (Magic byte - 1 byte) + [Schema ID] (4 bytes big-endian) + [Avro Binary Payload]
    """
    schema_id = registry.register(schema)
    buf = io.BytesIO()
    buf.write(b"\x00")  # Magic byte
    buf.write(struct.pack(">I", schema_id))  # 4-byte Schema ID
    AvroBinaryEncoder.write_record(buf, schema, record)
    return buf.getvalue()


def avro_kafka_deserialize(data: bytes, registry: SchemaRegistry,
                          reader_schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    stream = io.BytesIO(data)
    magic = stream.read(1)
    if magic != b"\x00":
        raise ValueError("Invalid Kafka Avro magic byte")
    schema_id = struct.unpack(">I", stream.read(4))[0]
    writer_schema = registry.get_schema(schema_id)
    return AvroBinaryDecoder.read_record(stream, writer_schema, reader_schema)


def run_demo():
    print(f"\n{'═' * 65}")
    print("  3. Avro — Kafka / Data Pipelines Demonstration")
    print(f"{'═' * 65}")

    schema_path = os.path.join(os.path.dirname(__file__), "user.avsc")
    with open(schema_path, "r") as f:
        schema = json.load(f)

    print(f"Loaded Avro Schema: {schema['name']} with fields:")
    for f in schema["fields"]:
        print(f"  • {f['name']}: {f['type']} (default: {f.get('default')})")

    # Serialize single record
    user_record = {"id": 1, "name": "Alice", "email": None}
    print("\n[Serialization]")
    print(f"Original Record: {user_record}")

    buf = io.BytesIO()
    AvroBinaryEncoder.write_record(buf, schema, user_record)
    binary_data = buf.getvalue()
    print(f"Raw Avro Bytes:  {binary_data}")
    print(f"Hex Dump:        {binary_data.hex(' ')}")
    print(f"Payload Size:    {len(binary_data)} bytes (no field names stored!)")

    # Deserialize
    print("\n[Deserialization]")
    buf.seek(0)
    deserialized = AvroBinaryDecoder.read_record(buf, schema)
    print(f"Deserialized:    {deserialized}")

    # Kafka & Schema Registry Simulation
    print("\n[Kafka Streaming & Schema Registry Simulation]")
    registry = SchemaRegistry()
    kafka_msg = avro_kafka_serialize(user_record, schema, registry)
    print(f"Kafka message size (with 5-byte registry header): {len(kafka_msg)} bytes")
    print(f"Magic Byte: 0x{kafka_msg[0]:02x} | Schema ID: {struct.unpack('>I', kafka_msg[1:5])[0]}")

    # Schema Evolution Demo: Reader adds a new field with default
    v2_reader_schema = {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": ["null", "string"], "default": None},
            {"name": "tier", "type": "string", "default": "STANDARD"}
        ]
    }
    evolved_record = avro_kafka_deserialize(kafka_msg, registry, reader_schema=v2_reader_schema)
    print("\n[Schema Evolution in Kafka Consumer]")
    print(f"Old Producer sent: V1 data (no 'tier' field)")
    print(f"New Consumer read: {evolved_record} ✅ (default applied safely)")


if __name__ == "__main__":
    run_demo()
