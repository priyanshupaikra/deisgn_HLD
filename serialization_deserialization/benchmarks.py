"""
╔══════════════════════════════════════════════════════════════════════════════╗
║             SERIALIZATION & DESERIALIZATION — BENCHMARKS                     ║
║           Performance, Payload Size & Feature Matrix Comparison              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import io
import json
import pickle
import time
from typing import Any, Dict, List, Tuple
import xml.etree.ElementTree as ET

import json_format
import protobuf_format
import avro_format
import msgpack_format
import xml_format


def run_benchmark(iterations: int = 5000):
    print(f"\n{'═' * 76}")
    print(f"  BENCHMARK: Comparing 6 Serialization Formats ({iterations:,} iterations)")
    print(f"{'═' * 76}")

    # Standard test entity
    test_data = {"id": 101, "customer_name": "Alice", "amount": 59.99}

    # Avro schema
    avro_schema = {
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "customer_name", "type": "string"},
            {"name": "amount", "type": "float"}
        ]
    }

    # Prepare serializers and deserializers
    # 1. JSON
    json_ser = lambda: json.dumps(test_data).encode("utf-8")
    json_sample = json_ser()
    json_deser = lambda b: json.loads(b.decode("utf-8"))

    # 2. Protobuf
    proto_order = protobuf_format.Order(id=101, customer_name="Alice", amount=59.99)
    proto_ser = lambda: proto_order.SerializeToString()
    proto_sample = proto_ser()
    def proto_deser(b):
        o = protobuf_format.Order()
        o.ParseFromString(b)
        return o

    # 3. Avro
    def avro_ser():
        b = io.BytesIO()
        avro_format.AvroBinaryEncoder.write_record(b, avro_schema, test_data)
        return b.getvalue()
    avro_sample = avro_ser()
    avro_deser = lambda b: avro_format.AvroBinaryDecoder.read_record(io.BytesIO(b), avro_schema)

    # 4. MessagePack
    msgpack_ser = lambda: msgpack_format.packb(test_data)
    msgpack_sample = msgpack_ser()
    msgpack_deser = lambda b: msgpack_format.unpackb(b)

    # 5. Pickle
    pickle_ser = lambda: pickle.dumps(test_data)
    pickle_sample = pickle_ser()
    pickle_deser = lambda b: pickle.loads(b)

    # 6. XML
    xml_ser = lambda: xml_format.serialize_order(101, "Alice", 59.99).encode("utf-8")
    xml_sample = xml_ser()
    xml_deser = lambda b: xml_format.deserialize_order(b.decode("utf-8"))

    benchmarks = [
        ("JSON", json_ser, json_deser, json_sample),
        ("Protobuf", proto_ser, proto_deser, proto_sample),
        ("Avro", avro_ser, avro_deser, avro_sample),
        ("MessagePack", msgpack_ser, msgpack_deser, msgpack_sample),
        ("Pickle", pickle_ser, pickle_deser, pickle_sample),
        ("XML", xml_ser, xml_deser, xml_sample),
    ]

    results = []
    base_size = len(json_sample)

    for name, ser_fn, deser_fn, sample_bytes in benchmarks:
        size = len(sample_bytes)
        ratio = (size / base_size) * 100

        # Measure serialization
        t0 = time.perf_counter()
        for _ in range(iterations):
            ser_fn()
        ser_time_ms = (time.perf_counter() - t0) * 1000

        # Measure deserialization
        t0 = time.perf_counter()
        for _ in range(iterations):
            deser_fn(sample_bytes)
        deser_time_ms = (time.perf_counter() - t0) * 1000

        results.append({
            "name": name,
            "size": size,
            "ratio": ratio,
            "ser_time": ser_time_ms,
            "deser_time": deser_time_ms
        })

    # Print Empirical Results Table
    print(f"\n  {'Format':<14} | {'Size':<10} | {'vs JSON':<10} | {'Encode (ms)':<13} | {'Decode (ms)':<13}")
    print(f"  {'-' * 14}-+-{'-' * 10}-+-{'-' * 10}-+-{'-' * 13}-+-{'-' * 13}")
    for r in results:
        print(f"  {r['name']:<14} | {r['size']:>5} bytes | {r['ratio']:>8.1f}%  | {r['ser_time']:>10.2f} ms | {r['deser_time']:>10.2f} ms")

    # Print Quick Comparison Table
    print(f"\n{'═' * 76}")
    print("  QUICK COMPARISON MATRIX")
    print(f"{'═' * 76}")
    comparison_table = [
        ("JSON", "Yes", "No", "Medium", "Web APIs"),
        ("Protobuf", "No", "Yes", "Very fast", "Internal microservices"),
        ("Avro", "No", "Yes (evolvable)", "Fast", "Kafka/streaming"),
        ("MessagePack", "No", "No", "Fast", "Low-latency apps/games"),
        ("Pickle", "No", "No", "Fast (Python only)", "Python-internal caching/ML"),
        ("XML", "Yes", "Optional (XSD)", "Slow", "Legacy enterprise systems"),
    ]
    print(f"\n| {'Format':<11} | {'Human-readable':<14} | {'Schema required':<15} | {'Speed':<18} | {'Typical home':<26} |")
    print(f"| {'-' * 11} | {'-' * 14} | {'-' * 15} | {'-' * 18} | {'-' * 26} |")
    for row in comparison_table:
        print(f"| {row[0]:<11} | {row[1]:<14} | {row[2]:<15} | {row[3]:<18} | {row[4]:<26} |")

    # Print Common Use Table
    print(f"\n{'═' * 76}")
    print("  COMMON USE-CASES TABLE")
    print(f"{'═' * 76}")
    common_use_table = [
        ("JSON", "REST APIs"),
        ("Protobuf", "gRPC / microservices"),
        ("Avro", "Kafka/data pipelines"),
        ("MessagePack", "Compact network communication"),
        ("Pickle", "Python-specific object serialization"),
        ("XML", "Legacy/enterprise integrations"),
    ]
    print(f"\n| {'Format':<11} | {'Common use':<36} |")
    print(f"| {'-' * 11} | {'-' * 36} |")
    for row in common_use_table:
        print(f"| {row[0]:<11} | {row[1]:<36} |")
    print()


if __name__ == "__main__":
    run_benchmark()
