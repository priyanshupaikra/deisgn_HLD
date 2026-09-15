"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    SEARCH ENGINE (Elasticsearch) — MODULE 2                  ║
║                    ELASTICSEARCH QUERY DSL SIMULATION                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

ELASTICSEARCH QUERY DSL:
─────────────────────────
ES queries are expressed as JSON — the "Domain Specific Language" (DSL).
Every search request is a JSON body sent to:
  GET /index_name/_search
  {
    "query": { ... },
    "aggs":  { ... },
    "sort":  [ ... ],
    "from":  0,
    "size":  10
  }

QUERY CONTEXT vs FILTER CONTEXT:
──────────────────────────────────
  QUERY CONTEXT  → "How well does this document match?" → RELEVANCE SCORE computed
  FILTER CONTEXT → "Does this document match yes/no?" → NO SCORE, CACHED, FASTER

  Rule of thumb:
    Use QUERY for full-text search (title, description, body)
    Use FILTER for structured data (status=active, price<100, date range)

  {
    "query": {
      "bool": {
        "must":   [{"match": {"title": "laptop"}}],    ← QUERY (scored)
        "filter": [{"term": {"status": "active"}},     ← FILTER (not scored, cached)
                   {"range": {"price": {"lte": 1000}}}] ← FILTER
      }
    }
  }

KEY QUERY TYPES:
─────────────────
  TERM QUERY:     Exact match, no analysis. For keywords, IDs, enums.
                  {"term": {"status": "published"}}

  MATCH QUERY:    Full-text analyzed search. For text fields.
                  {"match": {"description": "running shoes"}}

  MATCH PHRASE:   All terms must appear in ORDER and ADJACENT.
                  {"match_phrase": {"title": "New York"}}
                  Matches "New York City" but NOT "York New"

  RANGE QUERY:    Numeric/date ranges.
                  {"range": {"price": {"gte": 10, "lte": 100}}}

  BOOL QUERY:     Combines multiple queries. The most important compound query.
                  must:     All must match (AND) → contributes to score
                  should:   Any should match (OR) → boosts score if matches
                  must_not: Must NOT match → no score contribution
                  filter:   Must match (AND) → no score, cached

  MULTI_MATCH:    Search across multiple fields with field boosting.
                  {"multi_match": {"query": "laptop", "fields": ["title^3", "description"]}}
                  title^3 = title field is 3x more important than description

AGGREGATIONS:
──────────────
  Aggregations are analytics on search results (like SQL GROUP BY + COUNT).

  TYPES:
    Bucket aggregations:  Group documents into buckets
      terms:              Group by field value (faceted navigation)
      range:              Group by numeric ranges (price: 0-100, 100-500, 500+)
      date_histogram:     Group by time periods (monthly sales chart)

    Metric aggregations:  Compute metrics within buckets
      avg, min, max, sum: Statistics on numeric fields
      cardinality:        Count of unique values (like COUNT DISTINCT)

  Example:
    "How many products in each category, and what's the average price per category?"
    → terms agg on "category" + nested avg agg on "price"

ELASTICSEARCH SPECIAL FEATURES:
─────────────────────────────────
  NESTED DOCUMENTS:  Arrays of objects that can be queried independently
  PARENT-CHILD:      Related documents in the same index (no reindexing on update)
  PERCOLATOR:        Reverse search — store queries, match against incoming docs
  VECTOR SEARCH:     kNN (k-Nearest Neighbours) for ML embeddings similarity
  SCRIPTED FIELDS:   Compute custom scores using Painless scripting language

"""

import time
import math
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional
from inverted_index import InvertedIndex, StandardAnalyzer


# ─────────────────────────────────────────────────────────────────────────────
# Document Model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ESDocument:
    """Represents a document stored in Elasticsearch."""
    id: str
    source: dict           # The actual document data (_source)
    index_name: str = ""
    version: int = 1


@dataclass
class SearchHit:
    """A single search result (hit)."""
    doc_id: str
    score: float
    source: dict
    index: str
    highlights: dict = field(default_factory=dict)

    def __repr__(self):
        return f"Hit(id={self.doc_id}, score={self.score:.4f}, name={self.source.get('name', '')})"


@dataclass
class SearchResponse:
    """Full search response (mirrors ES _search response structure)."""
    total: int
    hits: list[SearchHit]
    took_ms: float
    aggregations: dict = field(default_factory=dict)
    max_score: float = 0.0

    def __repr__(self):
        return (f"SearchResponse(total={self.total}, hits={len(self.hits)}, "
                f"took={self.took_ms:.1f}ms)")


# ─────────────────────────────────────────────────────────────────────────────
# Elasticsearch Index (simulated)
# ─────────────────────────────────────────────────────────────────────────────

class ESIndex:
    """
    Simulates a single Elasticsearch index.

    An ES index is:
      - A collection of documents stored in JSON format
      - Backed by a Lucene inverted index for text search
      - Spread across N PRIMARY SHARDS (for parallelism)
      - Each shard replicated R times (REPLICA SHARDS for fault tolerance)
      - Defined by a MAPPING (schema) that specifies field types

    MAPPING FIELD TYPES:
      text:     Full-text analyzed (use for search: descriptions, titles)
      keyword:  Exact match, not analyzed (use for: status, category, IDs)
      integer:  Numeric integer
      float:    Numeric float
      date:     Date/datetime
      boolean:  true/false
      nested:   Array of objects (queryable independently)
      geo_point: Latitude/longitude coordinates
    """

    def __init__(self, name: str, mapping: dict = None,
                 num_shards: int = 3, num_replicas: int = 1):
        self.name = name
        self.mapping = mapping or {}     # Field type definitions
        self.num_shards = num_shards
        self.num_replicas = num_replicas

        self._documents: dict[str, ESDocument] = {}
        self._text_index = InvertedIndex(StandardAnalyzer())
        self._keyword_fields: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
        # keyword index: {field → {value → [doc_ids]}}

    def index(self, doc_id: str, source: dict) -> dict:
        """
        Index (store + analyze) a document.
        Equivalent to: PUT /index_name/_doc/doc_id  { ...source... }
        """
        doc = ESDocument(id=doc_id, source=source, index_name=self.name)

        # Handle version conflict (optimistic concurrency)
        existing = self._documents.get(doc_id)
        if existing:
            doc.version = existing.version + 1

        self._documents[doc_id] = doc

        # Full-text index for text fields
        text_fields = [
            f for f, t in self.mapping.items()
            if t.get("type") == "text"
        ]
        if text_fields:
            self._text_index.index_document(doc_id, source, text_fields)
        else:
            # Default: index all string fields as text
            self._text_index.index_document(doc_id, source)

        # Keyword index for exact matching + aggregations
        for field_name, field_def in self.mapping.items():
            if field_def.get("type") in ("keyword", "integer", "float", "boolean"):
                value = source.get(field_name)
                if value is not None:
                    key = str(value)
                    if doc_id not in self._keyword_fields[field_name][key]:
                        self._keyword_fields[field_name][key].append(doc_id)

        return {"_id": doc_id, "_version": doc.version, "result": "created" if doc.version == 1 else "updated"}

    def get(self, doc_id: str) -> Optional[ESDocument]:
        """GET /index/_doc/doc_id"""
        return self._documents.get(doc_id)

    def delete(self, doc_id: str) -> bool:
        """DELETE /index/_doc/doc_id"""
        if doc_id in self._documents:
            del self._documents[doc_id]
            return True
        return False

    def count(self) -> int:
        return len(self._documents)


# ─────────────────────────────────────────────────────────────────────────────
# Query Executor
# ─────────────────────────────────────────────────────────────────────────────

class QueryExecutor:
    """
    Executes Elasticsearch-style query DSL against an ESIndex.

    Implements the core query types:
      match, term, range, bool (must/should/must_not/filter)
      plus aggregations (terms, avg, range buckets)
    """

    def __init__(self, index: ESIndex):
        self.index = index

    def execute(self, query_dsl: dict, from_: int = 0,
                size: int = 10, sort: list = None) -> SearchResponse:
        """
        Main entry point: execute a query DSL dict.

        :param query_dsl:  {"query": {...}, "aggs": {...}}
        :param from_:      Pagination offset (default 0)
        :param size:       Page size (default 10)
        :param sort:       Sort fields [{"price": "asc"}]
        """
        start = time.time()

        query = query_dsl.get("query", {"match_all": {}})
        aggs_def = query_dsl.get("aggs", {})

        # Execute the query
        scores = self._execute_query(query)

        # Sort: by score (default) or by sort fields
        docs_with_scores = [
            (doc_id, score, self.index._documents[doc_id].source)
            for doc_id, score in scores.items()
            if doc_id in self.index._documents
        ]

        if sort:
            for sort_clause in reversed(sort):
                for field, order in sort_clause.items():
                    reverse = (order == "desc")
                    docs_with_scores.sort(
                        key=lambda x: x[2].get(field, 0),
                        reverse=reverse
                    )
        else:
            docs_with_scores.sort(key=lambda x: x[1], reverse=True)

        total = len(docs_with_scores)
        page = docs_with_scores[from_: from_ + size]

        hits = [
            SearchHit(doc_id=doc_id, score=round(score, 4),
                      source=source, index=self.index.name)
            for doc_id, score, source in page
        ]

        # Aggregations
        all_matching_sources = [src for _, _, src in docs_with_scores]
        aggregations = self._execute_aggs(aggs_def, all_matching_sources)

        took_ms = (time.time() - start) * 1000
        max_score = max((h.score for h in hits), default=0.0)

        return SearchResponse(
            total=total,
            hits=hits,
            took_ms=round(took_ms, 2),
            aggregations=aggregations,
            max_score=max_score,
        )

    # ── Query handlers ────────────────────────────────────────────────────

    def _execute_query(self, query: dict) -> dict[str, float]:
        """Route to the correct query handler. Returns {doc_id → score}."""
        if "match_all" in query:
            return self._match_all()
        if "match" in query:
            return self._match(query["match"])
        if "term" in query:
            return self._term(query["term"])
        if "terms" in query:
            return self._terms_query(query["terms"])
        if "range" in query:
            return self._range(query["range"])
        if "bool" in query:
            return self._bool(query["bool"])
        if "multi_match" in query:
            return self._multi_match(query["multi_match"])
        if "match_phrase" in query:
            return self._match_phrase(query["match_phrase"])
        return {}

    def _match_all(self) -> dict[str, float]:
        """match_all: every document scores 1.0."""
        return {doc_id: 1.0 for doc_id in self.index._documents}

    def _match(self, clause: dict) -> dict[str, float]:
        """
        match query: full-text search on a single field.
        Analyzes the query string and scores with BM25.
        """
        field, query_text = next(iter(clause.items()))
        if isinstance(query_text, dict):
            query_text = query_text.get("query", "")
        results = self.index._text_index.search(str(query_text), top_k=1000)
        return {r["doc_id"]: r["score"] for r in results}

    def _match_phrase(self, clause: dict) -> dict[str, float]:
        """
        match_phrase: all terms must appear in order.
        Approximated here: all terms must match (AND), with order check skipped.
        Real ES checks positions using the posting list.
        """
        field, query_text = next(iter(clause.items()))
        results = self.index._text_index.search(str(query_text), top_k=1000, operator="AND")
        return {r["doc_id"]: r["score"] for r in results}

    def _term(self, clause: dict) -> dict[str, float]:
        """
        term query: EXACT match on a keyword/numeric field. No analysis.
        In ES: "term" queries do NOT analyze — use for keywords, IDs, status values.
        """
        field, value = next(iter(clause.items()))
        key = str(value)
        doc_ids = self.index._keyword_fields[field].get(key, [])
        return {doc_id: 1.0 for doc_id in doc_ids}

    def _terms_query(self, clause: dict) -> dict[str, float]:
        """terms query: match any of the provided values (like SQL IN)."""
        field, values = next(iter(clause.items()))
        result = {}
        for v in values:
            doc_ids = self.index._keyword_fields[field].get(str(v), [])
            for doc_id in doc_ids:
                result[doc_id] = 1.0
        return result

    def _range(self, clause: dict) -> dict[str, float]:
        """
        range query: numeric or date range filter.
        Supported: gte (>=), gt (>), lte (<=), lt (<)
        """
        field, conditions = next(iter(clause.items()))
        matching = {}
        for doc_id, doc in self.index._documents.items():
            value = doc.source.get(field)
            if value is None:
                continue
            try:
                v = float(value)
                match = True
                if "gte" in conditions and not v >= conditions["gte"]:
                    match = False
                if "gt"  in conditions and not v >  conditions["gt"]:
                    match = False
                if "lte" in conditions and not v <= conditions["lte"]:
                    match = False
                if "lt"  in conditions and not v <  conditions["lt"]:
                    match = False
                if match:
                    matching[doc_id] = 0.0  # Filter: no score
            except (TypeError, ValueError):
                pass
        return matching

    def _bool(self, clause: dict) -> dict[str, float]:
        """
        bool query: combines must/should/must_not/filter.

        must:     All must match → contributes to score (AND)
        should:   At least one should match → boosts score (OR)
        must_not: Must not match → excludes from results
        filter:   Must match, but no score contribution → CACHED

        Final score = sum of must scores + sum of matching should scores
        """
        scores: dict[str, float] = {}

        # MUST: all must match (AND)
        musts = clause.get("must", [])
        if not isinstance(musts, list):
            musts = [musts]

        must_results = [self._execute_query(q) for q in musts]
        if must_results:
            # Intersection: only docs present in ALL must results
            candidate_docs = set.intersection(*[set(r.keys()) for r in must_results])
            for doc_id in candidate_docs:
                scores[doc_id] = sum(r.get(doc_id, 0) for r in must_results)
        else:
            scores = self._match_all()

        # FILTER: must match, no score, results cached
        filters = clause.get("filter", [])
        if not isinstance(filters, list):
            filters = [filters]
        for f in filters:
            filter_results = self._execute_query(f)
            scores = {doc_id: s for doc_id, s in scores.items()
                      if doc_id in filter_results}

        # MUST_NOT: exclude matches
        must_nots = clause.get("must_not", [])
        if not isinstance(must_nots, list):
            must_nots = [must_nots]
        for q in must_nots:
            excluded = self._execute_query(q)
            scores = {d: s for d, s in scores.items() if d not in excluded}

        # SHOULD: boost score for matches; if no must, at least one required
        shoulds = clause.get("should", [])
        if not isinstance(shoulds, list):
            shoulds = [shoulds]
        should_results = [self._execute_query(q) for q in shoulds]

        if should_results:
            if not musts and not filters:
                # No must/filter: should acts as OR (at least one must match)
                all_should_docs = set.union(*[set(r.keys()) for r in should_results])
                scores = {d: 0.0 for d in all_should_docs if d in scores or not scores}
                if not musts:
                    scores = {d: 0.0 for d in all_should_docs}

            # Add should score boosts
            for doc_id in list(scores.keys()):
                for sr in should_results:
                    scores[doc_id] = scores.get(doc_id, 0) + sr.get(doc_id, 0)

        return scores

    def _multi_match(self, clause: dict) -> dict[str, float]:
        """
        multi_match: search across multiple fields.
        Fields can have boost: "title^3" = title is 3x more important.
        """
        query_text = clause["query"]
        fields = clause.get("fields", ["*"])
        combined: dict[str, float] = defaultdict(float)

        for field_spec in fields:
            # Parse boost: "title^3" → field="title", boost=3
            if "^" in field_spec:
                field, boost = field_spec.split("^")
                boost = float(boost)
            else:
                field, boost = field_spec, 1.0

            results = self.index._text_index.search(query_text, top_k=1000)
            for r in results:
                combined[r["doc_id"]] += r["score"] * boost

        return dict(combined)

    # ── Aggregations ──────────────────────────────────────────────────────

    def _execute_aggs(self, aggs_def: dict, documents: list[dict]) -> dict:
        """Execute aggregations on the matched documents."""
        result = {}
        for agg_name, agg_def in aggs_def.items():
            if "terms" in agg_def:
                result[agg_name] = self._terms_agg(agg_def["terms"], documents)
            elif "avg" in agg_def:
                result[agg_name] = self._metric_agg("avg", agg_def["avg"], documents)
            elif "max" in agg_def:
                result[agg_name] = self._metric_agg("max", agg_def["max"], documents)
            elif "min" in agg_def:
                result[agg_name] = self._metric_agg("min", agg_def["min"], documents)
            elif "sum" in agg_def:
                result[agg_name] = self._metric_agg("sum", agg_def["sum"], documents)
            elif "range" in agg_def:
                result[agg_name] = self._range_agg(agg_def["range"], documents)
        return result

    def _terms_agg(self, agg_def: dict, documents: list[dict]) -> dict:
        """
        Terms aggregation: group by field value + count (like SQL GROUP BY + COUNT).
        Used for faceted navigation (filter by category, brand, etc.)
        """
        field = agg_def["field"]
        size = agg_def.get("size", 10)
        counts: dict[str, int] = defaultdict(int)
        for doc in documents:
            v = doc.get(field)
            if v is not None:
                counts[str(v)] += 1
        buckets = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:size]
        return {"buckets": [{"key": k, "doc_count": c} for k, c in buckets]}

    def _metric_agg(self, metric: str, agg_def: dict, documents: list[dict]) -> dict:
        """avg/min/max/sum aggregation on a numeric field."""
        field = agg_def["field"]
        values = [doc[field] for doc in documents if field in doc
                  and isinstance(doc[field], (int, float))]
        if not values:
            return {"value": None}
        funcs = {"avg": lambda v: sum(v)/len(v), "max": max, "min": min, "sum": sum}
        return {"value": round(funcs[metric](values), 2)}

    def _range_agg(self, agg_def: dict, documents: list[dict]) -> dict:
        """Range aggregation: bucket documents into numeric ranges."""
        field = agg_def["field"]
        ranges = agg_def.get("ranges", [])
        buckets = []
        for r in ranges:
            lo = r.get("from", float("-inf"))
            hi = r.get("to", float("inf"))
            count = sum(
                1 for doc in documents
                if field in doc and lo <= float(doc.get(field, 0)) < hi
            )
            key = f"{lo if lo != float('-inf') else '*'}-{hi if hi != float('inf') else '*'}"
            buckets.append({"key": key, "from": lo, "to": hi, "doc_count": count})
        return {"buckets": buckets}


# ─────────────────────────────────────────────────────────────────────────────
# Elasticsearch Client (simulated)
# ─────────────────────────────────────────────────────────────────────────────

class ElasticsearchClient:
    """
    Simulated Elasticsearch client.
    Mirrors the official Python elasticsearch-py client API.
    """

    def __init__(self):
        self._indices: dict[str, ESIndex] = {}

    def create_index(self, index: str, body: dict = None) -> dict:
        """PUT /index_name"""
        mapping = (body or {}).get("mappings", {}).get("properties", {})
        settings = (body or {}).get("settings", {})
        num_shards = settings.get("number_of_shards", 3)
        num_replicas = settings.get("number_of_replicas", 1)
        self._indices[index] = ESIndex(index, mapping, num_shards, num_replicas)
        print(f"  [ES] Created index '{index}' "
              f"({num_shards} shards, {num_replicas} replicas)")
        return {"acknowledged": True, "index": index}

    def index(self, index: str, id: str, body: dict) -> dict:
        """PUT /index/_doc/id"""
        if index not in self._indices:
            self.create_index(index)
        return self._indices[index].index(id, body)

    def bulk_index(self, index: str, documents: list[dict], id_field: str = "id") -> int:
        """Bulk index (POST /_bulk) — efficient batch indexing."""
        if index not in self._indices:
            self.create_index(index)
        idx = self._indices[index]
        for doc in documents:
            doc_id = str(doc.get(id_field, str(uuid.uuid4())))
            idx.index(doc_id, doc)
        print(f"  [ES] Bulk indexed {len(documents)} docs into '{index}'")
        return len(documents)

    def search(self, index: str, body: dict,
               from_: int = 0, size: int = 10,
               sort: list = None) -> SearchResponse:
        """GET /index/_search"""
        if index not in self._indices:
            return SearchResponse(0, [], 0.0)
        executor = QueryExecutor(self._indices[index])
        return executor.execute(body, from_=from_, size=size, sort=sort)

    def get(self, index: str, id: str) -> Optional[dict]:
        """GET /index/_doc/id"""
        idx = self._indices.get(index)
        if not idx:
            return None
        doc = idx.get(id)
        return {"_id": id, "_source": doc.source} if doc else None

    def delete(self, index: str, id: str) -> bool:
        idx = self._indices.get(index)
        return idx.delete(id) if idx else False

    def count(self, index: str) -> int:
        return self._indices[index].count() if index in self._indices else 0


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

PRODUCTS = [
    {"id": "p1", "name": "Running Shoes Pro", "category": "footwear",
     "brand": "Nike", "price": 129.99, "rating": 4.5, "in_stock": True,
     "description": "Professional running shoes for marathon athletes"},
    {"id": "p2", "name": "Leather Oxford Shoes", "category": "footwear",
     "brand": "Clarks", "price": 89.99, "rating": 4.2, "in_stock": True,
     "description": "Classic leather shoes for formal business occasions"},
    {"id": "p3", "name": "Trail Running Boots", "category": "footwear",
     "brand": "Salomon", "price": 159.99, "rating": 4.7, "in_stock": True,
     "description": "Heavy-duty trail running boots for outdoor athletes"},
    {"id": "p4", "name": "Basketball Sneakers", "category": "footwear",
     "brand": "Nike", "price": 149.99, "rating": 4.3, "in_stock": False,
     "description": "High-performance basketball shoes with ankle support"},
    {"id": "p5", "name": "Yoga Mat Premium", "category": "fitness",
     "brand": "Lululemon", "price": 79.99, "rating": 4.8, "in_stock": True,
     "description": "Non-slip yoga mat for professional yoga fitness training"},
    {"id": "p6", "name": "Training Shorts", "category": "apparel",
     "brand": "Adidas", "price": 39.99, "rating": 4.1, "in_stock": True,
     "description": "Lightweight training shorts for running gym workouts"},
    {"id": "p7", "name": "Hiking Boots", "category": "footwear",
     "brand": "Merrell", "price": 199.99, "rating": 4.6, "in_stock": True,
     "description": "Waterproof boots for hiking and outdoor adventures"},
    {"id": "p8", "name": "Tennis Racket", "category": "sports",
     "brand": "Wilson", "price": 249.99, "rating": 4.4, "in_stock": True,
     "description": "Professional tennis racket for competitive players"},
    {"id": "p9", "name": "Compression Socks", "category": "apparel",
     "brand": "Nike", "price": 19.99, "rating": 3.9, "in_stock": True,
     "description": "Compression socks for running and athletic performance"},
    {"id": "p10","name": "Foam Running Insoles", "category": "footwear",
     "brand": "Superfeet", "price": 44.99, "rating": 4.3, "in_stock": True,
     "description": "Orthopedic foam insoles for running shoes comfort"},
]


def show_results(label: str, response: SearchResponse, top_n: int = 4):
    print(f"\n  QUERY: {label}")
    print(f"  Found: {response.total} docs | took: {response.took_ms:.1f}ms")
    for hit in response.hits[:top_n]:
        print(f"    [{hit.score:.3f}] {hit.source['name']} "
              f"(${hit.source['price']}, ⭐{hit.source['rating']})")
    if response.aggregations:
        print(f"  Aggregations: {response.aggregations}")


if __name__ == "__main__":
    es = ElasticsearchClient()

    # Create index with mapping
    es.create_index("products", {
        "settings": {"number_of_shards": 3, "number_of_replicas": 1},
        "mappings": {
            "properties": {
                "name":        {"type": "text"},
                "description": {"type": "text"},
                "category":    {"type": "keyword"},
                "brand":       {"type": "keyword"},
                "price":       {"type": "float"},
                "rating":      {"type": "float"},
                "in_stock":    {"type": "boolean"},
            }
        }
    })
    es.bulk_index("products", PRODUCTS)

    print("\n" + "=" * 65)
    print("   ELASTICSEARCH QUERY DSL — Demo")
    print("=" * 65)

    # ── match query ────────────────────────────────────────────────────────
    show_results("match: 'running shoes'",
        es.search("products", {"query": {"match": {"description": "running shoes"}}}, size=4))

    # ── bool + filter ──────────────────────────────────────────────────────
    show_results("bool: match 'running' + filter category=footwear + filter price<150",
        es.search("products", {
            "query": {
                "bool": {
                    "must":   [{"match": {"description": "running"}}],
                    "filter": [
                        {"term":  {"category": "footwear"}},
                        {"range": {"price": {"lt": 150}}},
                    ]
                }
            }
        }, size=5))

    # ── multi_match with boosting ──────────────────────────────────────────
    show_results("multi_match: 'running' on name^3 + description",
        es.search("products", {
            "query": {"multi_match": {
                "query": "running",
                "fields": ["name^3", "description"],
            }}
        }, size=4))

    # ── term + sort ────────────────────────────────────────────────────────
    show_results("term: brand=Nike, sorted by price asc",
        es.search("products",
                  {"query": {"term": {"brand": "Nike"}}},
                  sort=[{"price": "asc"}], size=5))

    # ── aggregations (faceted navigation) ─────────────────────────────────
    show_results("match_all + aggs: categories, avg_price, price_ranges",
        es.search("products", {
            "query": {"match_all": {}},
            "aggs": {
                "by_category": {"terms": {"field": "category", "size": 5}},
                "avg_price":   {"avg":   {"field": "price"}},
                "price_ranges":{"range": {
                    "field": "price",
                    "ranges": [
                        {"to": 50},
                        {"from": 50, "to": 150},
                        {"from": 150},
                    ]
                }},
            }
        }, size=0))

    print("\n" + "=" * 65)
    print("  ES QUERY DSL SUMMARY")
    print("=" * 65)
    rows = [
        ("match",       "Full-text (analyzed)",  "Text search"),
        ("term",        "Exact keyword match",   "Status, category, ID"),
        ("range",       "Numeric/date ranges",   "Price, date filter"),
        ("bool",        "Combine: must/should/filter", "Complex conditions"),
        ("multi_match", "Search N fields + boost","Title + description"),
        ("terms agg",   "GROUP BY + COUNT",       "Facets, navigation"),
        ("avg/max agg", "Metrics on matched docs","Statistics"),
    ]
    for r in rows:
        print(f"  {r[0]:<14} {r[1]:<28} → {r[2]}")
    print("=" * 65)
