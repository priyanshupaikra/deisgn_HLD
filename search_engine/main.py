"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                SEARCH ENGINE (Elasticsearch) SUITE                           ║
║                      All Concepts — Main Runner                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  MODULES:                                                                    ║
║    inverted_index.py          → Inverted index, BM25, text analysis         ║
║    elasticsearch_simulation.py→ Query DSL: match/bool/range/aggs            ║
║    search_patterns.py         → Autocomplete, fuzzy, facets, geo, MLT       ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os
import time
import math

sys.path.insert(0, os.path.dirname(__file__))

from inverted_index import InvertedIndex, StandardAnalyzer
from elasticsearch_simulation import ElasticsearchClient
from search_patterns import (
    AutocompleteEngine, FuzzyMatcher,
    FacetedSearch, GeoSearch, MoreLikeThis
)


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


PRODUCTS = [
    {"id": "p1",  "name": "Running Shoes Pro",     "category": "footwear",    "brand": "Nike",     "price": 129.99, "rating": 4.5, "in_stock": True,  "lat": 40.7128, "lon": -74.0060,
     "description": "Professional running shoes for marathon athletes lightweight durable"},
    {"id": "p2",  "name": "Leather Oxford Shoes",  "category": "footwear",    "brand": "Clarks",   "price": 89.99,  "rating": 4.2, "in_stock": True,  "lat": 40.7580, "lon": -73.9855,
     "description": "Classic leather shoes formal business occasions elegant"},
    {"id": "p3",  "name": "Trail Running Boots",   "category": "footwear",    "brand": "Salomon",  "price": 159.99, "rating": 4.7, "in_stock": True,  "lat": 40.6892, "lon": -74.0445,
     "description": "Heavy-duty trail running boots outdoor athletes extreme"},
    {"id": "p4",  "name": "Basketball Sneakers",   "category": "footwear",    "brand": "Nike",     "price": 149.99, "rating": 4.3, "in_stock": False, "lat": 40.7484, "lon": -73.9967,
     "description": "High-performance basketball shoes superior ankle support"},
    {"id": "p5",  "name": "Yoga Mat Premium",      "category": "fitness",     "brand": "Lululemon","price": 79.99,  "rating": 4.8, "in_stock": True,  "lat": 40.7282, "lon": -73.7949,
     "description": "Non-slip yoga mat professional yoga fitness training"},
    {"id": "p6",  "name": "Training Shorts",       "category": "apparel",     "brand": "Adidas",   "price": 39.99,  "rating": 4.1, "in_stock": True,  "lat": 40.6501, "lon": -73.9496,
     "description": "Lightweight training shorts running gym workouts"},
    {"id": "p7",  "name": "Hiking Boots",          "category": "footwear",    "brand": "Merrell",  "price": 199.99, "rating": 4.6, "in_stock": True,  "lat": 40.8448, "lon": -73.8648,
     "description": "Waterproof boots hiking outdoor adventures durable"},
    {"id": "p8",  "name": "Tennis Racket Pro",     "category": "sports",      "brand": "Wilson",   "price": 249.99, "rating": 4.4, "in_stock": True,  "lat": 40.7614, "lon": -73.9776,
     "description": "Professional tennis racket competitive tournament players"},
    {"id": "p9",  "name": "Marathon Running Vest", "category": "apparel",     "brand": "Nike",     "price": 59.99,  "rating": 3.9, "in_stock": True,  "lat": 40.7300, "lon": -73.9950,
     "description": "Hydration vest marathon running ultramarathon athletes"},
    {"id": "p10", "name": "Trail Running Pack",    "category": "accessories",  "brand": "Osprey",   "price": 109.99, "rating": 4.3, "in_stock": True,  "lat": 40.7500, "lon": -74.0100,
     "description": "Lightweight running pack trail athletes backpack"},
]


def demo_inverted_index():
    section("MODULE 1: INVERTED INDEX + BM25 SCORING")
    print("  Core concept: term → posting list → O(1) lookup + relevance scoring")

    index = InvertedIndex(StandardAnalyzer())
    for p in PRODUCTS:
        index.index_document(p["id"], p, searchable_fields=["name", "description"])

    print(f"\n  Index stats: {index.stats()}")

    print("\n  ─── Text Analysis Pipeline ───")
    for text in ["Running Shoes for Athletes!", "Professional Marathon Training"]:
        tokens = index.analyzer.analyze(text)
        print(f"  '{text}' → {tokens}")

    print("\n  ─── BM25 Search Results ───")
    for query in ["running shoes", "outdoor boots", "professional athlete"]:
        results = index.search(query, top_k=3)
        print(f"\n  Query: {query!r}")
        for r in results:
            print(f"    [{r['score']:.4f}] {r['document']['name']}")


def demo_elasticsearch():
    section("MODULE 2: ELASTICSEARCH QUERY DSL")
    print("  match, bool (must+filter), multi_match, term, range, aggregations")

    es = ElasticsearchClient()
    es.create_index("products", {
        "settings": {"number_of_shards": 3, "number_of_replicas": 1},
        "mappings": {"properties": {
            "name":        {"type": "text"},
            "description": {"type": "text"},
            "category":    {"type": "keyword"},
            "brand":       {"type": "keyword"},
            "price":       {"type": "float"},
            "rating":      {"type": "float"},
            "in_stock":    {"type": "boolean"},
        }}
    })
    es.bulk_index("products", PRODUCTS)

    print("\n  ─── bool: match 'running' + filter footwear + price<160 ───")
    resp = es.search("products", {
        "query": {"bool": {
            "must":   [{"match": {"description": "running"}}],
            "filter": [{"term": {"category": "footwear"}},
                       {"range": {"price": {"lt": 160}}}],
        }}
    }, size=5)
    print(f"  Found: {resp.total} | took: {resp.took_ms:.1f}ms")
    for hit in resp.hits:
        print(f"    [{hit.score:.3f}] {hit.source['name']} (${hit.source['price']})")

    print("\n  ─── Aggregations: category counts + avg price + price ranges ───")
    resp2 = es.search("products", {
        "query": {"match_all": {}},
        "aggs": {
            "by_category": {"terms":  {"field": "category"}},
            "avg_price":   {"avg":    {"field": "price"}},
            "price_ranges":{"range":  {
                "field": "price",
                "ranges": [{"to": 100}, {"from": 100, "to": 200}, {"from": 200}]
            }}
        }
    }, size=0)
    print(f"  Total: {resp2.total}")
    for agg_name, agg_result in resp2.aggregations.items():
        print(f"  [{agg_name}]: {agg_result}")


def demo_patterns():
    section("MODULE 3: ADVANCED SEARCH PATTERNS")

    print("\n  ─── Autocomplete ───")
    ac = AutocompleteEngine(min_prefix_len=2)
    for p in PRODUCTS:
        ac.index_text(p["name"], weight=p["rating"])
    for prefix in ["run", "tr", "hik", "ba"]:
        results = ac.suggest(prefix, size=3)
        print(f"  '{prefix}' → {[s['text'] for s in results]}")

    print("\n  ─── Fuzzy Search ───")
    fuzzy = FuzzyMatcher()
    all_names = [p["name"].lower() for p in PRODUCTS]
    for typo in ["runninng shooes", "hicking boot", "baskitball"]:
        results = fuzzy.fuzzy_search(typo, all_names)
        top = results[0]["term"] if results else "no match"
        print(f"  '{typo}' → closest: '{top}' (edit_dist={results[0]['edit_distance'] if results else '∞'})")

    print("\n  ─── Faceted Search ───")
    faceted = FacetedSearch(PRODUCTS)
    result = faceted.search("running", facet_fields=["category", "brand"])
    print(f"  'running': {result['total']} results | facets: {result['facets']}")
    result2 = faceted.search("running", active_filters={"brand": "Nike"}, facet_fields=["category"])
    print(f"  'running' + brand=Nike: {result2['total']} results → {[d['name'] for d in result2['results']]}")

    print("\n  ─── Geo Search (within 8km of Times Square) ───")
    geo = GeoSearch()
    geo.index(PRODUCTS)
    nearby = geo.geo_distance(40.7580, -73.9855, distance_km=8.0)
    for doc in nearby[:4]:
        print(f"  {doc['name']}: {doc['_distance_km']} km")

    print("\n  ─── More Like This ───")
    index = InvertedIndex(StandardAnalyzer())
    for p in PRODUCTS:
        index.index_document(p["id"], p)
    mlt = MoreLikeThis(index)
    similar = mlt.find_similar("p1", top_k=3)
    print(f"  Similar to 'Running Shoes Pro':")
    for r in similar:
        print(f"    [{r['score']:.3f}] {r['document']['name']}")


def print_summary():
    section("SEARCH ENGINE — COMPLETE REFERENCE")
    print()
    print("  CORE CONCEPT:")
    print("  Inverted Index:  term → [doc_ids]  (O(1) lookup vs O(n) SQL LIKE scan)")
    print("  Text Analysis:   tokenize → lowercase → stop words → stemming")
    print("  BM25 Scoring:    TF(saturated) × IDF × doc_length_normalized")

    print("\n  QUERY TYPES:")
    for q, desc, use in [
        ("match",       "Full-text (analyzed)",           "Text fields (name, desc)"),
        ("term",        "Exact keyword (not analyzed)",   "Status, category, enum"),
        ("range",       "Numeric/date range",              "Price, date filters"),
        ("bool",        "must/should/must_not/filter",    "Complex compound queries"),
        ("multi_match", "Multi-field + boost (^)",        "title^3 + description"),
    ]:
        print(f"    {q:<14} {desc:<32} → {use}")

    print("\n  QUERY vs FILTER CONTEXT:")
    print("  Query context  → computes relevance score (use for full-text search)")
    print("  Filter context → YES/NO, no score, CACHED (use for structured data)")

    print("\n  AGGREGATIONS:")
    for a, desc in [
        ("terms",    "GROUP BY field → buckets with doc counts (faceted navigation)"),
        ("avg/max",  "Metric statistics on numeric fields"),
        ("range",    "Price/date range buckets"),
        ("date_hist","Time-series charts (daily, monthly)"),
    ]:
        print(f"    {a:<12} → {desc}")

    print("\n  ADVANCED PATTERNS:")
    for p, impl, use in [
        ("Autocomplete",  "Edge N-grams / Completion FST",      "Search suggestions"),
        ("Fuzzy",         "Levenshtein edit distance + AUTO",   "Typo correction"),
        ("Faceted",       "Terms agg + post_filter",            "Drill-down filters"),
        ("Geo",           "Haversine + BKD tree / Geohash",     "Location queries"),
        ("More Like This","TF-IDF interesting terms",           "Recommendations"),
    ]:
        print(f"    {p:<16} {impl:<36} → {use}")

    print("\n  ARCHITECTURE:")
    print("  Index → N shards (parallel) × R replicas (fault tolerance)")
    print("  shard = hash(doc_id) % N  |  replica serves reads")
    print()
    print("  Run individual files:")
    for f in ["inverted_index.py", "elasticsearch_simulation.py", "search_patterns.py"]:
        print(f"    python {f}")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       SEARCH ENGINE (Elasticsearch) — All Concepts")
    print("=" * 65)

    demo_inverted_index()
    demo_elasticsearch()
    demo_patterns()
    print_summary()
