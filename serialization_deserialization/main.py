"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                 SERIALIZATION & DESERIALIZATION SUITE                        ║
║                      All Concepts — Main Runner                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WHAT IS SERIALIZATION & DESERIALIZATION?                                    ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Serialization (Marshaling / Pickling / Encoding):                           ║
║    Converting an in-memory data structure or object graph into a stream of  ║
║    bytes or text suitable for network transmission or disk storage.          ║
║                                                                              ║
║  Deserialization (Unmarshaling / Unpickling / Decoding):                     ║
║    Reconstructing the original in-memory object from the byte/text stream.   ║
║                                                                              ║
║  WHY IT MATTERS IN SYSTEM DESIGN:                                            ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  1. Cross-Language Interop: Python service talks to Go microservice.         ║
║  2. Bandwidth & Latency: Binary formats (Protobuf/Avro) reduce packet sizes  ║
║     by 60-80% compared to JSON/XML, saving millions in egress costs.         ║
║  3. Schema Evolution: Upgrading backend services independently without       ║
║     crashing older consumers during rolling deployments.                     ║
║  4. Security: Untrusted deserialization is an OWASP Top 10 critical risk     ║
║     (e.g., Python Pickle remote code execution vulnerabilities).             ║
║                                                                              ║
║  MODULES IN THIS DIRECTORY:                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  json_format.py     → Text-based, human-readable REST APIs & web browsers    ║
║  protobuf_format.py → High-speed binary schema contracts for gRPC            ║
║  avro_format.py     → Schema-registry binary events for Kafka pipelines      ║
║  msgpack_format.py  → Schemaless binary JSON drop-in for game/WS loops       ║
║  pickle_format.py   → Python-native object graphs & security exploit defense ║
║  xml_format.py      → Tag-based enterprise SOAP & banking architectures      ║
║  benchmarks.py      → Automated benchmark & feature comparison matrix        ║
║                                                                              ║
║  HOW TO RUN INDIVIDUAL MODULES:                                              ║
║    python json_format.py                                                     ║
║    python protobuf_format.py                                                 ║
║    python avro_format.py                                                     ║
║    python msgpack_format.py                                                  ║
║    python pickle_format.py                                                   ║
║    python xml_format.py                                                      ║
║    python benchmarks.py                                                      ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys

# Ensure local imports work regardless of working directory
sys.path.insert(0, os.path.dirname(__file__))

import json_format
import protobuf_format
import avro_format
import msgpack_format
import pickle_format
import xml_format
import benchmarks


def section_banner(num: int, title: str):
    print(f"\n{'━' * 70}")
    print(f"  PART {num}: {title.upper()}")
    print(f"{'━' * 70}")


def main():
    print("=" * 70)
    print("       SERIALIZATION & DESERIALIZATION — COMPREHENSIVE SUITE")
    print("=" * 70)

    # Run each format's demo
    section_banner(1, "JSON (REST APIs)")
    json_format.run_demo()

    section_banner(2, "Protobuf (gRPC / Microservices)")
    protobuf_format.run_demo()

    section_banner(3, "Avro (Kafka / Data Pipelines)")
    avro_format.run_demo()

    section_banner(4, "MessagePack (Compact Network Communication)")
    msgpack_format.run_demo()

    section_banner(5, "Pickle (Python-Specific Object Serialization & Security)")
    pickle_format.run_demo()

    section_banner(6, "XML (Legacy / Enterprise Integrations)")
    xml_format.run_demo()

    section_banner(7, "Comprehensive Benchmarks & Comparisons")
    benchmarks.run_benchmark(iterations=5000)

    print("=" * 70)
    print("  ✅ All Serialization & Deserialization modules completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()
