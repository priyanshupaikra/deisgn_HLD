"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    SEARCH ENGINE (Elasticsearch) — MODULE 3                  ║
║                        ADVANCED SEARCH PATTERNS                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

PATTERNS COVERED:
──────────────────
  1. AUTOCOMPLETE / TYPEAHEAD  → Suggest completions as user types
  2. FUZZY SEARCH              → Match despite spelling mistakes
  3. FACETED SEARCH            → Filter + drill down by attributes
  4. GEO SEARCH                → Find documents within a distance/bounding box
  5. MORE LIKE THIS            → Find similar documents (content-based recommendations)

PATTERN 1: AUTOCOMPLETE / TYPEAHEAD
─────────────────────────────────────
  USER TYPES → server returns suggestions instantly (< 50ms)

  ES APPROACHES:
    a) Edge N-gram tokenizer (best for large text)
       Index time: "laptop" → ["l","la","lap","lapt","lapto","laptop"]
       Query: prefix "lap" → matches "laptop" immediately
       Pro: very fast  Con: large index size

    b) Completion suggester (for UI suggestions)
       Special "completion" field type with optimized FST (Finite State Transducer)
       GET /products/_search {"suggest": {"name_suggest": {"prefix": "run", "completion": {...}}}}
       Pro: fastest  Con: must store data in special format

    c) Prefix query (simple but slower)
       {"prefix": {"name": "run"}}
       Pro: simple  Con: not recommended for high traffic

PATTERN 2: FUZZY SEARCH
────────────────────────
  Handle typos: "shoees" → "shoes", "athltes" → "athletes"

  Based on LEVENSHTEIN EDIT DISTANCE:
    Edit distance = minimum number of single-character edits (insert, delete, replace)
    "shoe" → "shooe" = 1 edit (insert 'o')
    "shoe" → "sheo"  = 1 edit (swap 'o' and 'e')
    "shoe" → "shoe"  = 0 edits (exact match)

  ES fuzzy matching:
    fuzziness: "AUTO" → edit distance 0 for 1-2 chars, 1 for 3-5 chars, 2 for 6+ chars
    {"match": {"name": {"query": "shooes", "fuzziness": "AUTO"}}}

PATTERN 3: FACETED SEARCH
───────────────────────────
  Facets = dynamic filters shown to users based on search results
  Example (Amazon-style):
    Search "laptop" → results + facets:
      Brand: [Apple (45), Dell (32), HP (28)]
      Price: [$0-500 (30), $500-1000 (50), $1000+ (25)]
      Rating: [4+ stars (70), 3+ stars (95)]
    User clicks "Apple" → re-search with brand=Apple filter

  ES Implementation:
    1. Search with query
    2. Add aggregations for each facet field
    3. UI renders clickable facet counts
    4. User click → add filter + re-search

  POST-FILTER vs. FILTER:
    filter: affects both results AND aggregation counts
    post_filter: affects only results, NOT aggregations
    (post_filter allows you to show ALL counts even when filtered)

PATTERN 4: GEO SEARCH
──────────────────────
  Find documents near a location or within a bounding box.

  ES geo_distance query:
    {"geo_distance": {"distance": "10km", "location": {"lat": 40.71, "lon": -74.00}}}

  ES geo_bounding_box query:
    {"geo_bounding_box": {"location": {
        "top_left":     {"lat": 41.0, "lon": -74.5},
        "bottom_right": {"lat": 40.0, "lon": -73.5}
    }}}

  USE CASES:
    Restaurant finder: "pizza within 2km"
    Uber: "drivers within 5km of pickup"
    Real estate: "homes in this map bounding box"

  HOW IT WORKS:
    ES uses GEOHASH or BKD trees (3D range trees) to efficiently query
    geographic data. Geohash tiles the earth into a grid of cells.

PATTERN 5: MORE LIKE THIS (MLT)
────────────────────────────────
  Find documents similar to a given document.
  Used for content-based recommendations.

  ALGORITHM:
    1. Extract "interesting" terms from source document
       (high TF-IDF terms — frequent in this doc but rare overall)
    2. Build a query using those terms
    3. Run query against the index
    4. Result = similar documents

  ES query:
    {"more_like_this": {
        "fields": ["description", "tags"],
        "like": [{"_index": "products", "_id": "p1"}],
        "min_term_freq": 1,
        "max_query_terms": 12
    }}

  USE CASES:
    "Related products" on product pages
    "Similar articles" on news sites
    "Related questions" on Stack Overflow

"""

import math
import time
import heapq
from collections import defaultdict
from typing import Any, Optional
from inverted_index import InvertedIndex, StandardAnalyzer


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 1: AUTOCOMPLETE / TYPEAHEAD
# ─────────────────────────────────────────────────────────────────────────────

class AutocompleteEngine:
    """
    Autocomplete / Typeahead using edge N-gram approach.

    EDGE N-GRAM:
      For each word, generate all prefixes:
        "laptop" → ["l", "la", "lap", "lapt", "lapto", "laptop"]
      Store all these prefixes in the index.
      When user types "lap" → exact match → returns "laptop"

    ALTERNATIVE: Trie (Prefix Tree)
      A trie stores strings where common prefixes share nodes.
      O(L) lookup where L = length of prefix.
      Memory-efficient for large suggestion sets.

    COMPLETION SUGGESTER (ES built-in):
      Uses Finite State Transducer (FST) — a compact automaton
      that maps prefixes to completions.
      FST is more memory-efficient than tries for large datasets.
    """

    def __init__(self, min_prefix_len: int = 2, max_prefix_len: int = 20):
        self.min_prefix_len = min_prefix_len
        self.max_prefix_len = max_prefix_len
        # {edge_ngram → [(weight, suggestion_text, original_doc)]}
        self._edge_ngrams: dict[str, list[tuple]] = defaultdict(list)
        self._indexed_count = 0

    def index_text(self, text: str, weight: float = 1.0, doc: dict = None) -> None:
        """
        Index a text for autocomplete.
        Generates edge n-grams for each word and the full string.
        """
        text_lower = text.lower().strip()

        # Index full phrase prefixes
        self._add_ngrams(text_lower, weight, text, doc)

        # Index each word individually (for mid-string matching)
        for word in text_lower.split():
            if len(word) >= self.min_prefix_len:
                self._add_ngrams(word, weight * 0.8, text, doc)

        self._indexed_count += 1

    def _add_ngrams(self, text: str, weight: float, original: str, doc: dict) -> None:
        """Generate edge N-grams from the start of text."""
        for end in range(self.min_prefix_len,
                          min(len(text) + 1, self.max_prefix_len + 1)):
            ngram = text[:end]
            entry = (weight, original, doc)
            if entry not in self._edge_ngrams[ngram]:
                self._edge_ngrams[ngram].append(entry)

    def suggest(self, prefix: str, size: int = 5,
                context: dict = None) -> list[dict]:
        """
        Get autocomplete suggestions for a prefix.
        Returns top-N suggestions sorted by weight (popularity).

        :param prefix:  User-typed prefix
        :param size:    Max suggestions to return
        :param context: Optional filter (e.g., category filter for contextual suggest)
        """
        prefix_lower = prefix.lower().strip()
        if len(prefix_lower) < self.min_prefix_len:
            return []

        candidates = self._edge_ngrams.get(prefix_lower, [])

        # Apply context filter if provided
        if context and candidates:
            candidates = [
                (w, text, doc) for w, text, doc in candidates
                if doc and all(doc.get(k) == v for k, v in context.items())
            ]

        # Sort by weight descending, deduplicate by text
        seen = set()
        results = []
        for weight, text, doc in sorted(candidates, key=lambda x: x[0], reverse=True):
            if text not in seen:
                seen.add(text)
                results.append({
                    "text": text,
                    "weight": weight,
                    "doc": doc,
                })
            if len(results) >= size:
                break

        return results


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 2: FUZZY SEARCH
# ─────────────────────────────────────────────────────────────────────────────

class FuzzyMatcher:
    """
    Fuzzy search using Levenshtein Edit Distance.

    LEVENSHTEIN EDIT DISTANCE:
      Minimum number of single-character edits to transform string A → string B.
      Operations:
        Insert:  "cat" → "cats" (edit=1)
        Delete:  "cats" → "cat" (edit=1)
        Replace: "cat" → "bat"  (edit=1)

    TRANSPOSITION (Damerau-Levenshtein):
      Adjacent swap: "teh" → "the" (edit=1)
      ES uses Damerau-Levenshtein by default.

    ES FUZZINESS:
      "AUTO"     → edit distance 0 for 1-2 chars, 1 for 3-5 chars, 2 for 6+
      fuzziness=1 → allow 1 edit (typo tolerance for short words)
      fuzziness=2 → allow 2 edits (more tolerant but more false positives)
      prefix_length=2 → first N characters must match exactly
                         (prevents very different suggestions, improves performance)
    """

    def edit_distance(self, a: str, b: str) -> int:
        """
        Compute Levenshtein edit distance using dynamic programming.
        Time: O(m×n), Space: O(m×n)
        where m, n = lengths of strings a, b.
        """
        m, n = len(a), len(b)
        # dp[i][j] = edit distance between a[:i] and b[:j]
        dp = [[0] * (n + 1) for _ in range(m + 1)]

        # Base cases: transform empty string to a[:i] needs i insertions
        for i in range(m + 1):
            dp[i][0] = i
        for j in range(n + 1):
            dp[0][j] = j

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if a[i-1] == b[j-1]:
                    dp[i][j] = dp[i-1][j-1]       # No edit needed
                else:
                    dp[i][j] = 1 + min(
                        dp[i-1][j],    # Delete from a
                        dp[i][j-1],    # Insert into a
                        dp[i-1][j-1],  # Replace
                    )
        return dp[m][n]

    def auto_fuzziness(self, query_term: str) -> int:
        """ES AUTO fuzziness rule: 0 for 1-2, 1 for 3-5, 2 for 6+ chars."""
        length = len(query_term)
        if length <= 2:
            return 0
        elif length <= 5:
            return 1
        else:
            return 2

    def fuzzy_search(self, query: str, candidates: list[str],
                     fuzziness: int = None,
                     prefix_length: int = 1) -> list[dict]:
        """
        Find candidates within fuzziness edit distance of query.

        :param query:          Search term (possibly misspelled)
        :param candidates:     All terms in the index
        :param fuzziness:      Max allowed edit distance (None = AUTO)
        :param prefix_length:  First N chars must match exactly
        """
        q = query.lower()
        max_edits = fuzziness if fuzziness is not None else self.auto_fuzziness(q)

        results = []
        for candidate in candidates:
            c = candidate.lower()

            # Prefix check: first prefix_length chars must match
            if prefix_length and not c.startswith(q[:prefix_length]):
                continue

            dist = self.edit_distance(q, c)
            if dist <= max_edits:
                results.append({
                    "term":          candidate,
                    "edit_distance": dist,
                    "score":         1.0 / (dist + 1),  # Closer = higher score
                })

        return sorted(results, key=lambda x: x["edit_distance"])


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 3: FACETED SEARCH
# ─────────────────────────────────────────────────────────────────────────────

class FacetedSearch:
    """
    Faceted Search — dynamic drill-down filtering with aggregation counts.

    CONCEPT:
      Search results come with FACET COUNTS for each filterable attribute.
      User can click a facet value to add it as a filter.
      After filtering, all other facet counts update to reflect new result set.

    EXAMPLE: Search "running shoes" on an e-commerce site
      Results: 42 products
      Facets:
        Category: [Footwear (35), Apparel (7)]
        Brand:    [Nike (15), Adidas (12), Salomon (8), other]
        Price:    [Under $50 (5), $50-$100 (18), $100-$200 (15), Over $200 (4)]
        Rating:   [4+ stars (30), 3+ stars (42)]
      User clicks "Nike" → Re-search + filter brand=Nike → 15 results, facets update

    SELECTED FACETS (active filters):
      Shown as "chips" / "breadcrumbs" at top of results.
      User can remove individual filters.
    """

    def __init__(self, documents: list[dict]):
        self.documents = documents

    def search(self, query: str, active_filters: dict = None,
               facet_fields: list[str] = None,
               text_fields: list[str] = None) -> dict:
        """
        Execute faceted search.

        :param query:          Full-text search string
        :param active_filters: Currently selected filters {field → value}
        :param facet_fields:   Which fields to compute facet counts for
        :param text_fields:    Which fields to search in
        """
        active_filters = active_filters or {}
        facet_fields = facet_fields or []
        text_fields = text_fields or ["name", "description"]

        # 1. Full-text search filter
        if query:
            query_lower = query.lower()
            results = [
                doc for doc in self.documents
                if any(query_lower in str(doc.get(f, "")).lower()
                       for f in text_fields)
            ]
        else:
            results = list(self.documents)

        # 2. Apply active filters
        for field, value in active_filters.items():
            results = [
                doc for doc in results
                if str(doc.get(field, "")).lower() == str(value).lower()
            ]

        # 3. Compute facets on the filtered results
        facets: dict[str, dict] = {}
        for field in facet_fields:
            counts: dict[str, int] = defaultdict(int)
            for doc in results:
                v = doc.get(field)
                if v is not None:
                    counts[str(v)] += 1
            # Sort by count descending
            facets[field] = dict(
                sorted(counts.items(), key=lambda x: x[1], reverse=True)
            )

        # 4. Price range buckets (example of range facets)
        if "price" in [f for f in facet_fields]:
            price_ranges = [
                ("Under $50", 0, 50),
                ("$50-$100",  50, 100),
                ("$100-$200", 100, 200),
                ("Over $200", 200, float("inf")),
            ]
            price_facets = {}
            for label, lo, hi in price_ranges:
                count = sum(1 for doc in results
                            if lo <= doc.get("price", 0) < hi)
                if count > 0:
                    price_facets[label] = count
            facets["price_ranges"] = price_facets

        return {
            "total":          len(results),
            "results":        results,
            "facets":         facets,
            "active_filters": active_filters,
        }


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 4: GEO SEARCH
# ─────────────────────────────────────────────────────────────────────────────

class GeoSearch:
    """
    Geo Search — find documents within a distance or bounding box.

    Uses Haversine formula for accurate Earth-surface distances.

    HAVERSINE FORMULA:
      Computes the great-circle distance between two points on a sphere.
      More accurate than flat-Earth Pythagorean distance for large distances.

    IN ELASTICSEARCH:
      Field type: geo_point (stores lat/lon)
      Query types:
        geo_distance:     Documents within N km of a point
        geo_bounding_box: Documents within a rectangle (map viewport)
        geo_polygon:      Documents within an arbitrary polygon
        geo_shape:        Complex geometry (multipolygon, linestring, etc.)

    GEOHASH:
      Encodes lat/lon as a string of characters.
      Longer geohash = more precise location.
      Nearby locations share long common prefixes.
      "u09tvw" ≈ "u09tvq" (same neighborhood in London)
    """

    EARTH_RADIUS_KM = 6371.0

    def __init__(self):
        self._documents: list[dict] = []

    def index(self, documents: list[dict]) -> None:
        """Index documents with lat/lon fields."""
        self._documents = [
            doc for doc in documents
            if "lat" in doc and "lon" in doc
        ]

    def haversine_distance(self, lat1: float, lon1: float,
                            lat2: float, lon2: float) -> float:
        """
        Haversine formula: accurate great-circle distance in km.
        Used by Elasticsearch's geo_distance query.
        """
        R = self.EARTH_RADIUS_KM
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat/2) ** 2 +
             math.cos(math.radians(lat1)) *
             math.cos(math.radians(lat2)) *
             math.sin(dlon/2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def geo_distance(self, lat: float, lon: float,
                     distance_km: float, sort_by_distance: bool = True) -> list[dict]:
        """
        Find all documents within distance_km of (lat, lon).
        Equivalent to: {"geo_distance": {"distance": "Xkm", "location": {...}}}
        """
        results = []
        for doc in self._documents:
            dist = self.haversine_distance(lat, lon, doc["lat"], doc["lon"])
            if dist <= distance_km:
                results.append({**doc, "_distance_km": round(dist, 2)})

        if sort_by_distance:
            results.sort(key=lambda x: x["_distance_km"])
        return results

    def geo_bounding_box(self, top_left: dict, bottom_right: dict) -> list[dict]:
        """
        Find documents within a bounding box (map viewport).
        top_left = {"lat": max_lat, "lon": min_lon}
        bottom_right = {"lat": min_lat, "lon": max_lon}
        """
        min_lat = bottom_right["lat"]
        max_lat = top_left["lat"]
        min_lon = top_left["lon"]
        max_lon = bottom_right["lon"]
        return [
            doc for doc in self._documents
            if min_lat <= doc["lat"] <= max_lat and min_lon <= doc["lon"] <= max_lon
        ]


# ─────────────────────────────────────────────────────────────────────────────
# PATTERN 5: MORE LIKE THIS
# ─────────────────────────────────────────────────────────────────────────────

class MoreLikeThis:
    """
    More Like This (MLT) — content-based similarity search.

    ALGORITHM:
      1. Get the source document
      2. Extract important terms using TF-IDF:
         - High TF in source doc (frequent here)
         - High IDF globally (rare in corpus = distinctive)
         → These are the "interesting" terms
      3. Build a query with those interesting terms
      4. Search the index with that query
      5. Return top results (excluding the source document itself)

    USE CASES:
      - "Related products" widget on product detail pages
      - "Similar articles" on news/blog sites
      - "Related questions" on Q&A platforms (Stack Overflow)
      - "You might also like" in streaming platforms

    ES PARAMETERS:
      min_term_freq:   Minimum occurrences in source doc to be "interesting"
      max_query_terms: Max number of terms to use for the query
      min_doc_freq:    Term must appear in at least N docs (filter noise)
      max_doc_freq:    Term must appear in at most N docs (filter too-common terms)
    """

    def __init__(self, index: InvertedIndex):
        self.index = index

    def find_similar(self, doc_id: str, top_k: int = 5,
                     max_terms: int = 10) -> list[dict]:
        """
        Find documents similar to doc_id.

        :param doc_id:    Source document to find similarities for
        :param top_k:     How many similar docs to return
        :param max_terms: Max interesting terms to use for similarity query
        """
        source_doc = self.index._documents.get(doc_id)
        if not source_doc:
            return []

        # 1. Extract all terms from source document
        source_text = " ".join(str(v) for v in source_doc.values() if isinstance(v, str))
        source_terms = self.index.analyzer.analyze(source_text)

        if not source_terms:
            return []

        # 2. Compute TF-IDF for each term in source doc
        N = self.index._total_docs
        term_tfidf: list[tuple[float, str]] = []
        term_freq_in_doc: dict[str, int] = defaultdict(int)

        for term in source_terms:
            term_freq_in_doc[term] += 1

        for term, tf in term_freq_in_doc.items():
            posting = self.index._index.get(term)
            if not posting:
                continue
            df = posting.doc_frequency
            idf = math.log((N - df + 0.5) / (df + 0.5) + 1) if N > 0 else 0
            tfidf = tf * idf
            term_tfidf.append((tfidf, term))

        # 3. Sort by TF-IDF descending, take top max_terms "interesting" terms
        term_tfidf.sort(reverse=True)
        interesting_terms = [term for _, term in term_tfidf[:max_terms]]

        if not interesting_terms:
            return []

        # 4. Build query from interesting terms and search
        query_string = " ".join(interesting_terms)
        results = self.index.search(query_string, top_k=top_k + 1)

        # 5. Exclude source document from results
        return [r for r in results if r["doc_id"] != doc_id][:top_k]


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

PRODUCTS = [
    {"id": "p1", "name": "Running Shoes Pro",    "category": "footwear", "brand": "Nike",  "price": 129.99, "lat": 40.7128, "lon": -74.0060},
    {"id": "p2", "name": "Leather Oxford Shoes", "category": "footwear", "brand": "Clarks","price": 89.99,  "lat": 40.7580, "lon": -73.9855},
    {"id": "p3", "name": "Trail Running Boots",  "category": "footwear", "brand": "Salomon","price": 159.99, "lat": 40.6892, "lon": -74.0445},
    {"id": "p4", "name": "Basketball Sneakers",  "category": "footwear", "brand": "Nike",  "price": 149.99, "lat": 40.7484, "lon": -73.9967},
    {"id": "p5", "name": "Yoga Mat Premium",     "category": "fitness",  "brand": "Lululemon","price": 79.99,"lat": 40.7282, "lon": -73.7949},
    {"id": "p6", "name": "Training Shorts",      "category": "apparel",  "brand": "Adidas","price": 39.99,  "lat": 40.6501, "lon": -73.9496},
    {"id": "p7", "name": "Hiking Boots",         "category": "footwear", "brand": "Merrell","price": 199.99,"lat": 40.8448, "lon": -73.8648},
    {"id": "p8", "name": "Tennis Racket Pro",    "category": "sports",   "brand": "Wilson","price": 249.99, "lat": 40.7614, "lon": -73.9776},
    {"id": "p9", "name": "Marathon Running Vest","category": "apparel",  "brand": "Nike",  "price": 59.99,  "lat": 40.7300, "lon": -73.9950},
    {"id": "p10","name": "Trail Running Pack",   "category": "accessories","brand":"Osprey","price": 109.99,"lat": 40.7500, "lon": -74.0100},
]


if __name__ == "__main__":
    print("=" * 65)
    print("   ADVANCED SEARCH PATTERNS — Demo")
    print("=" * 65)

    # ── PATTERN 1: AUTOCOMPLETE ────────────────────────────────────────────
    print("\n\n  ═══ PATTERN 1: AUTOCOMPLETE / TYPEAHEAD ═══\n")
    ac = AutocompleteEngine(min_prefix_len=2)
    for p in PRODUCTS:
        ac.index_text(p["name"], weight=p.get("price", 1) / 100, doc=p)

    test_prefixes = ["run", "tr", "ba", "hik", "yog"]
    for prefix in test_prefixes:
        suggestions = ac.suggest(prefix, size=3)
        names = [s["text"] for s in suggestions]
        print(f"  Type '{prefix}' → {names}")

    print("\n  Contextual autocomplete (category=footwear only):")
    for prefix in ["tr", "run"]:
        suggestions = ac.suggest(prefix, size=3, context={"category": "footwear"})
        names = [s["text"] for s in suggestions]
        print(f"  Type '{prefix}' (footwear only) → {names}")

    # ── PATTERN 2: FUZZY SEARCH ────────────────────────────────────────────
    print("\n\n  ═══ PATTERN 2: FUZZY SEARCH (Typo Tolerance) ═══\n")
    fuzzy = FuzzyMatcher()
    all_product_names = [p["name"].lower() for p in PRODUCTS]

    typo_queries = [
        ("runninng shoes", None),    # Double n
        ("hicking boots",  None),    # hicking → hiking
        ("baskrtball",     None),    # Transposition
        ("tenis racket",   None),    # Single n
        ("yga mat",        None),    # Missing 'o'
    ]

    for query, fuzz in typo_queries:
        results = fuzzy.fuzzy_search(query, all_product_names, fuzziness=fuzz)
        print(f"  Query: {query!r}")
        for r in results[:2]:
            print(f"    edit_dist={r['edit_distance']} → {r['term']!r}")

    # ── PATTERN 3: FACETED SEARCH ──────────────────────────────────────────
    print("\n\n  ═══ PATTERN 3: FACETED SEARCH ═══\n")
    faceted = FacetedSearch(PRODUCTS)

    result = faceted.search(
        query="running",
        active_filters={},
        facet_fields=["category", "brand", "price"],
    )
    print(f"  Search 'running': {result['total']} results")
    print(f"  Facets: {result['facets']}")

    print("\n  After clicking 'Nike' facet:")
    result2 = faceted.search(
        query="running",
        active_filters={"brand": "Nike"},
        facet_fields=["category", "brand", "price"],
    )
    print(f"  Search 'running' + brand=Nike: {result2['total']} results")
    for doc in result2["results"]:
        print(f"    {doc['name']} (${doc['price']})")
    print(f"  Facets after filter: {result2['facets']}")

    # ── PATTERN 4: GEO SEARCH ─────────────────────────────────────────────
    print("\n\n  ═══ PATTERN 4: GEO SEARCH ═══\n")
    geo = GeoSearch()
    geo.index(PRODUCTS)

    # Central Manhattan: 40.7580, -73.9855 (Times Square)
    center_lat, center_lon = 40.7580, -73.9855
    radius_km = 8.0

    nearby = geo.geo_distance(center_lat, center_lon, distance_km=radius_km)
    print(f"  Products within {radius_km}km of Times Square:")
    for doc in nearby:
        print(f"    {doc['name']}: {doc['_distance_km']} km")

    bbox_results = geo.geo_bounding_box(
        top_left={"lat": 40.80, "lon": -74.05},
        bottom_right={"lat": 40.70, "lon": -73.90},
    )
    print(f"\n  Products in Manhattan bounding box: {len(bbox_results)}")
    for doc in bbox_results:
        print(f"    {doc['name']} (lat={doc['lat']}, lon={doc['lon']})")

    # ── PATTERN 5: MORE LIKE THIS ──────────────────────────────────────────
    print("\n\n  ═══ PATTERN 5: MORE LIKE THIS ═══\n")
    index = InvertedIndex(StandardAnalyzer())
    for p in PRODUCTS:
        index.index_document(p["id"], p)

    mlt = MoreLikeThis(index)
    source_doc = PRODUCTS[0]
    print(f"  Source: '{source_doc['name']}'")
    similar = mlt.find_similar("p1", top_k=4, max_terms=8)
    print(f"  Similar documents:")
    for r in similar:
        print(f"    [{r['score']:.3f}] {r['document']['name']}")

    print("\n" + "=" * 65)
    print("  ADVANCED PATTERNS SUMMARY")
    print("=" * 65)
    for name, impl, use in [
        ("Autocomplete",  "Edge N-grams / Completion FST",    "Search box suggestions"),
        ("Fuzzy Search",  "Levenshtein edit distance",         "Typo tolerance"),
        ("Faceted Search","Terms agg + active filter",         "Drill-down navigation"),
        ("Geo Search",    "Haversine / BKD tree / Geohash",    "Location-based queries"),
        ("More Like This","TF-IDF interesting terms query",    "Content recommendations"),
    ]:
        print(f"  {name:<16} {impl:<34} → {use}")
    print("=" * 65)
