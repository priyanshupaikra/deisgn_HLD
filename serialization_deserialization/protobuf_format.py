"""
╔══════════════════════════════════════════════════════════════════════════════╗
║             SERIALIZATION & DESERIALIZATION — MODULE 2                       ║
║                    Protocol Buffers (gRPC / Microservices)                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IT DOES:
─────────────
Binary serialization format requiring a predefined schema (.proto file).
Compiles to strongly-typed classes in Python, Go, Java, C++, Rust, etc.
Extremely compact, fast to serialize/deserialize, and strictly versioned.

USER STORY:
───────────
As a backend engineer building a gRPC service between an order-service and
payment-service, I want a strict, versioned contract for messages so both
services stay in sync even as they're deployed independently.

SCHEMA (order.proto):
─────────────────────
syntax = "proto3";

message Order {
  int32 id = 1;
  string customer_name = 2;
  float amount = 3;
}

WHY IT FITS:
────────────
• Strict schema contracts across multi-language microservices.
• Messages are binary and significantly smaller than JSON/XML.
• Backward & Forward Compatibility: field numbers allow adding/removing fields
  safely without breaking old services.

DOWNSIDES:
──────────
• Not human-readable without schema tooling/reflection.
• Requires a build step (protoc compiler) to generate language code.
"""

import io
import struct
from typing import Optional, Tuple


# ─── Pure Python Protobuf Wire Format Engine ──────────────────────────────────
# Protocol Buffers wire format encodes each field as:
# Key = (field_number << 3) | wire_type
# Wire Types:
#   0: Varint (int32, int64, uint32, bool)
#   1: 64-bit (fixed64, double)
#   2: Length-delimited (string, bytes, embedded messages)
#   5: 32-bit (fixed32, float)

WIRE_TYPE_VARINT = 0
WIRE_TYPE_64BIT = 1
WIRE_TYPE_LENGTH_DELIMITED = 2
WIRE_TYPE_32BIT = 5


def encode_varint(value: int) -> bytes:
    """Encodes an integer into variable-length bytes (varint, 7 bits per byte)."""
    if value < 0:
        # Two's complement for negative numbers (10 bytes in standard protobuf)
        value = (1 << 64) + value
    out = bytearray()
    while True:
        towrite = value & 0x7F
        value >>= 7
        if value:
            out.append(towrite | 0x80)
        else:
            out.append(towrite)
            break
    return bytes(out)


def decode_varint(stream: io.BytesIO) -> int:
    """Decodes a varint from a byte stream."""
    res = 0
    shift = 0
    while True:
        b = stream.read(1)
        if not b:
            raise EOFError("Unexpected end of varint stream")
        byte = b[0]
        res |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            break
        shift += 7
    return res


def encode_tag(field_number: int, wire_type: int) -> bytes:
    return encode_varint((field_number << 3) | wire_type)


def decode_tag(stream: io.BytesIO) -> Tuple[int, int]:
    key = decode_varint(stream)
    field_number = key >> 3
    wire_type = key & 0x07
    return field_number, wire_type


def skip_field(stream: io.BytesIO, wire_type: int) -> None:
    """Skips unknown fields to guarantee backward/forward compatibility."""
    if wire_type == WIRE_TYPE_VARINT:
        decode_varint(stream)
    elif wire_type == WIRE_TYPE_64BIT:
        stream.read(8)
    elif wire_type == WIRE_TYPE_LENGTH_DELIMITED:
        length = decode_varint(stream)
        stream.read(length)
    elif wire_type == WIRE_TYPE_32BIT:
        stream.read(4)
    else:
        raise ValueError(f"Unknown wire type: {wire_type}")


class Order:
    """
    Python representation of the compiled Protobuf Order message (v1):
      field 1: int32 id
      field 2: string customer_name
      field 3: float amount
    """
    def __init__(self, id: int = 0, customer_name: str = "", amount: float = 0.0):
        self.id = id
        self.customer_name = customer_name
        self.amount = float(amount)

    def SerializeToString(self) -> bytes:
        buf = io.BytesIO()

        # Field 1: int32 id (wire type 0)
        if self.id != 0:
            buf.write(encode_tag(1, WIRE_TYPE_VARINT))
            buf.write(encode_varint(self.id))

        # Field 2: string customer_name (wire type 2)
        if self.customer_name:
            str_bytes = self.customer_name.encode("utf-8")
            buf.write(encode_tag(2, WIRE_TYPE_LENGTH_DELIMITED))
            buf.write(encode_varint(len(str_bytes)))
            buf.write(str_bytes)

        # Field 3: float amount (wire type 5 - 32-bit float)
        if self.amount != 0.0:
            buf.write(encode_tag(3, WIRE_TYPE_32BIT))
            buf.write(struct.pack("<f", self.amount))

        return buf.getvalue()

    def ParseFromString(self, data: bytes) -> None:
        self.id = 0
        self.customer_name = ""
        self.amount = 0.0

        stream = io.BytesIO(data)
        while stream.tell() < len(data):
            try:
                field_number, wire_type = decode_tag(stream)
            except EOFError:
                break

            if field_number == 1 and wire_type == WIRE_TYPE_VARINT:
                self.id = decode_varint(stream)
            elif field_number == 2 and wire_type == WIRE_TYPE_LENGTH_DELIMITED:
                length = decode_varint(stream)
                self.customer_name = stream.read(length).decode("utf-8")
            elif field_number == 3 and wire_type == WIRE_TYPE_32BIT:
                self.amount = struct.unpack("<f", stream.read(4))[0]
            else:
                # Forward compatibility: ignore newer fields gracefully!
                skip_field(stream, wire_type)

    def __repr__(self) -> str:
        return f"Order(id={self.id}, customer_name='{self.customer_name}', amount={self.amount:.2f})"


class OrderV2:
    """
    Demonstrates Schema Evolution:
    Adds field 4: string currency
    Adds field 5: bool priority
    """
    def __init__(self, id: int = 0, customer_name: str = "", amount: float = 0.0,
                 currency: str = "USD", priority: bool = False):
        self.id = id
        self.customer_name = customer_name
        self.amount = float(amount)
        self.currency = currency
        self.priority = priority

    def SerializeToString(self) -> bytes:
        buf = io.BytesIO()

        if self.id != 0:
            buf.write(encode_tag(1, WIRE_TYPE_VARINT))
            buf.write(encode_varint(self.id))

        if self.customer_name:
            str_bytes = self.customer_name.encode("utf-8")
            buf.write(encode_tag(2, WIRE_TYPE_LENGTH_DELIMITED))
            buf.write(encode_varint(len(str_bytes)))
            buf.write(str_bytes)

        if self.amount != 0.0:
            buf.write(encode_tag(3, WIRE_TYPE_32BIT))
            buf.write(struct.pack("<f", self.amount))

        if self.currency:
            cur_bytes = self.currency.encode("utf-8")
            buf.write(encode_tag(4, WIRE_TYPE_LENGTH_DELIMITED))
            buf.write(encode_varint(len(cur_bytes)))
            buf.write(cur_bytes)

        if self.priority:
            buf.write(encode_tag(5, WIRE_TYPE_VARINT))
            buf.write(encode_varint(1 if self.priority else 0))

        return buf.getvalue()


def run_demo():
    print(f"\n{'═' * 65}")
    print("  2. Protobuf — gRPC / Microservices Demonstration")
    print(f"{'═' * 65}")

    # Standard User Story Demo
    print("\n[Serialization]")
    order = Order(id=101, customer_name="Alice", amount=59.99)
    print(f"Original Object: {order}")
    binary_data = order.SerializeToString()
    print(f"Binary Bytes:    {binary_data}")
    print(f"Hex Dump:        {binary_data.hex(' ')}")
    print(f"Payload Size:    {len(binary_data)} bytes")

    print("\n[Deserialization]")
    new_order = Order()
    new_order.ParseFromString(binary_data)
    print(f"Deserialized:    {new_order}")
    print(f"Access new_order.customer_name: {new_order.customer_name}")
    print(f"Access new_order.amount:        ${new_order.amount:.2f}")

    # Schema Evolution Demo (Backward & Forward Compatibility)
    print("\n[Schema Evolution & Compatibility Test]")
    print("Order Service deploys v2 with new fields (currency='EUR', priority=True)")
    order_v2 = OrderV2(id=202, customer_name="Bob", amount=129.50, currency="EUR", priority=True)
    v2_bytes = order_v2.SerializeToString()
    print(f"v2 binary payload size: {len(v2_bytes)} bytes")

    print("\nPayment Service is still on v1 (does NOT know about currency or priority)")
    old_service_order = Order()
    old_service_order.ParseFromString(v2_bytes)
    print(f"Payment Service successfully parsed v1 fields: {old_service_order}")
    print("✅ Result: Unknown fields (4 & 5) were safely skipped without errors.")


if __name__ == "__main__":
    run_demo()
