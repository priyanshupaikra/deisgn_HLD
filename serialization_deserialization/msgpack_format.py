"""
╔══════════════════════════════════════════════════════════════════════════════╗
║             SERIALIZATION & DESERIALIZATION — MODULE 4                       ║
║              MessagePack (Compact Network Communication)                     ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IT DOES:
─────────────
Binary version of JSON — same flexible, schemaless data model (objects,
arrays, strings, numbers, booleans), but serialized into much smaller binary
payloads that parse substantially faster. "It's like JSON, but fast and small."

USER STORY:
───────────
As a game developer sending frequent player-position updates over a websocket,
I want a JSON-like format that's smaller and faster to encode/decode so I can
hit my latency budget without designing a formal schema.

WHY IT FITS:
────────────
• Drop-in replacement for JSON: zero schema definitions or code-generation steps.
• Compact binary tags for maps, arrays, strings, and numbers save 30-50% bandwidth.
• Fast memory-aligned parsing ideal for real-time applications and games.

DOWNSIDES:
──────────
• Not human-readable (cannot view raw in browser network tab without decoder).
• Less mainstream ecosystem and debugging tool integration compared to JSON.
"""

import io
import json
import struct
from typing import Any, Dict, List, Tuple


# ─── Pure Python MessagePack Specification Engine ─────────────────────────────
# MessagePack Spec Format Tags:
#   positive fixint: 0x00 - 0x7f
#   fixmap:          0x80 - 0x8f
#   fixarray:        0x90 - 0x9f
#   fixstr:          0xa0 - 0xbf
#   nil:             0xc0
#   false / true:    0xc2 / 0xc3
#   float32:         0xca
#   float64:         0xcb
#   uint8:           0xcc
#   uint16:          0xcd
#   uint32:          0xce
#   int8:            0xd0
#   int16:           0xd1
#   int32:           0xd2
#   str8:            0xd9

def packb(obj: Any) -> bytes:
    """Serializes a Python object to MessagePack bytes."""
    buf = io.BytesIO()
    _pack_value(buf, obj)
    return buf.getvalue()


def _pack_value(buf: io.BytesIO, obj: Any) -> None:
    if obj is None:
        buf.write(b"\xc0")
    elif isinstance(obj, bool):
        buf.write(b"\xc3" if obj else b"\xc2")
    elif isinstance(obj, int):
        if 0 <= obj <= 127:
            buf.write(bytes([obj]))
        elif -32 <= obj < 0:
            buf.write(bytes([0xe0 | (obj + 32)]))
        elif 0 <= obj <= 0xFF:
            buf.write(b"\xcc" + struct.pack(">B", obj))
        elif 0 <= obj <= 0xFFFF:
            buf.write(b"\xcd" + struct.pack(">H", obj))
        elif 0 <= obj <= 0xFFFFFFFF:
            buf.write(b"\xce" + struct.pack(">I", obj))
        elif -128 <= obj < 0:
            buf.write(b"\xd0" + struct.pack(">b", obj))
        elif -32768 <= obj < 0:
            buf.write(b"\xd1" + struct.pack(">h", obj))
        else:
            buf.write(b"\xd2" + struct.pack(">i", obj))
    elif isinstance(obj, float):
        buf.write(b"\xcb" + struct.pack(">d", obj))
    elif isinstance(obj, str):
        utf8 = obj.encode("utf-8")
        length = len(utf8)
        if length <= 31:
            buf.write(bytes([0xa0 | length]))
        elif length <= 0xFF:
            buf.write(b"\xd9" + struct.pack(">B", length))
        else:
            buf.write(b"\xda" + struct.pack(">H", length))
        buf.write(utf8)
    elif isinstance(obj, list):
        length = len(obj)
        if length <= 15:
            buf.write(bytes([0x90 | length]))
        else:
            buf.write(b"\xdc" + struct.pack(">H", length))
        for item in obj:
            _pack_value(buf, item)
    elif isinstance(obj, dict):
        length = len(obj)
        if length <= 15:
            buf.write(bytes([0x80 | length]))
        else:
            buf.write(b"\xde" + struct.pack(">H", length))
        for k, v in obj.items():
            _pack_value(buf, str(k))
            _pack_value(buf, v)
    else:
        raise TypeError(f"Type {type(obj)} is not MessagePack serializable")


def unpackb(data: bytes) -> Any:
    """Deserializes MessagePack bytes back into Python objects."""
    stream = io.BytesIO(data)
    return _unpack_stream(stream)


def _unpack_stream(stream: io.BytesIO) -> Any:
    prefix_byte = stream.read(1)
    if not prefix_byte:
        raise EOFError("Premature end of MessagePack stream")
    b = prefix_byte[0]

    # Positive fixint
    if b <= 0x7F:
        return b
    # Fixmap (up to 15 entries)
    if 0x80 <= b <= 0x8F:
        num_items = b & 0x0F
        res = {}
        for _ in range(num_items):
            k = _unpack_stream(stream)
            v = _unpack_stream(stream)
            res[k] = v
        return res
    # Fixarray (up to 15 elements)
    if 0x90 <= b <= 0x9F:
        num_items = b & 0x0F
        return [_unpack_stream(stream) for _ in range(num_items)]
    # Fixstr (up to 31 bytes)
    if 0xA0 <= b <= 0xBF:
        str_len = b & 0x1F
        return stream.read(str_len).decode("utf-8")
    # Negative fixint
    if b >= 0xE0:
        return b - 256

    if b == 0xC0:
        return None
    if b == 0xC2:
        return False
    if b == 0xC3:
        return True
    if b == 0xCA:
        return struct.unpack(">f", stream.read(4))[0]
    if b == 0xCB:
        return struct.unpack(">d", stream.read(8))[0]
    if b == 0xCC:
        return struct.unpack(">B", stream.read(1))[0]
    if b == 0xCD:
        return struct.unpack(">H", stream.read(2))[0]
    if b == 0xCE:
        return struct.unpack(">I", stream.read(4))[0]
    if b == 0xD0:
        return struct.unpack(">b", stream.read(1))[0]
    if b == 0xD1:
        return struct.unpack(">h", stream.read(2))[0]
    if b == 0xD2:
        return struct.unpack(">i", stream.read(4))[0]
    if b == 0xD9:
        length = struct.unpack(">B", stream.read(1))[0]
        return stream.read(length).decode("utf-8")
    if b == 0xDA:
        length = struct.unpack(">H", stream.read(2))[0]
        return stream.read(length).decode("utf-8")
    if b == 0xDC:
        count = struct.unpack(">H", stream.read(2))[0]
        return [_unpack_stream(stream) for _ in range(count)]
    if b == 0xDE:
        count = struct.unpack(">H", stream.read(2))[0]
        res = {}
        for _ in range(count):
            k = _unpack_stream(stream)
            v = _unpack_stream(stream)
            res[k] = v
        return res

    raise ValueError(f"Unknown MessagePack prefix byte: 0x{b:02x}")


def run_demo():
    print(f"\n{'═' * 65}")
    print("  4. MessagePack — Compact Network Communication Demo")
    print(f"{'═' * 65}")

    # Standard User Story Demo
    player = {"x": 12.5, "y": 8.2, "health": 100}
    print("\n[Serialization]")
    print(f"Original Object:   {player}")

    json_encoded = json.dumps(player).encode("utf-8")
    binary_data = packb(player)

    print(f"JSON Payload:      '{json.dumps(player)}' ({len(json_encoded)} bytes)")
    print(f"MessagePack Bytes: {binary_data} ({len(binary_data)} bytes)")
    print(f"Hex Dump:          {binary_data.hex(' ')}")
    reduction = ((len(json_encoded) - len(binary_data)) / len(json_encoded)) * 100
    print(f"Bandwidth Savings: {reduction:.1f}% reduction compared to JSON")

    print("\n[Deserialization]")
    data = unpackb(binary_data)
    print(f"Deserialized:      {data}")
    print(f"Access player['x']:      {data['x']}")
    print(f"Access player['health']: {data['health']}")

    # Game WebSocket Broadcast Simulation
    print("\n[Game Server WebSocket Simulation: 1,000 Players @ 60 Ticks/sec]")
    ticks_per_sec = 60
    num_players = 1000
    updates_per_sec = num_players * ticks_per_sec

    json_mb_per_sec = (updates_per_sec * len(json_encoded)) / (1024 * 1024)
    msgpack_mb_per_sec = (updates_per_sec * len(binary_data)) / (1024 * 1024)

    print(f"  JSON Bandwidth:        {json_mb_per_sec:.2f} MB/s")
    print(f"  MessagePack Bandwidth: {msgpack_mb_per_sec:.2f} MB/s")
    print(f"  Monthly Network Saved: {(json_mb_per_sec - msgpack_mb_per_sec) * 3600 * 24 * 30 / 1024:.1f} GB/month 🚀")


if __name__ == "__main__":
    run_demo()
