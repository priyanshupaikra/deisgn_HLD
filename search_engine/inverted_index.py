"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    SEARCH ENGINE (Elasticsearch) — MODULE 1                  ║
║                    INVERTED INDEX & TEXT ANALYSIS                            ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS A SEARCH ENGINE?
─────────────────────────
A search engine enables FULL-TEXT SEARCH — finding documents that contain
specific words or phrases, ranked by relevance.

SQL LIKE vs. FULL-TEXT SEARCH:
───────────────────────────────
  SQL:  SELECT * FROM products WHERE description LIKE '%running shoes%'
        → Scans EVERY ROW (O(n)) → slow at scale → no relevance ranking
        → Can't handle "run shoe", misspellings, synonyms

  Elasticsearch:
        GET /products/_search {"query": {"match": {"description": "running shoes"}}}
        → Uses INVERTED INDEX (O(1) lookup) → millisecond results at 100M docs
        → Handles synonyms, stemming, fuzzy matching, relevance scoring

THE INVERTED INDEX — THE CORE DATA STRUCTURE:
──────────────────────────────────────────────
  A regular index: document_id → content
    Doc 1: "running shoes for athletes"
    Doc 2: "leather shoes for fashion"

  An INVERTED INDEX: term → list of documents containing that term
    "running"  → [Doc 1]
    "shoes"    → [Doc 1, Doc 2]
    "athletes" → [Doc 1]
    "leather"  → [Doc 2]
    "fashion"  → [Doc 2]

  QUERY "shoes":
    → Lookup "shoes" in index → [Doc 1, Doc 2] → O(1) lookup!
    vs. SQL LIKE: scan ALL documents → O(n)

  This is why Elasticsearch handles BILLIONS of docs with sub-second queries.

TEXT ANALYSIS PIPELINE:
─────────────────────────
  Before indexing, raw text goes through an ANALYZER:
    "The Quick Brown Fox JUMPED over lazy dogs!"
         │
    [CHARACTER FILTER]   → normalize: "The Quick Brown Fox JUMPED over lazy dogs"
         │
    [TOKENIZER]          → split:     ["The", "Quick", "Brown", "Fox", "JUMPED", "over", "lazy", "dogs"]
         │
    [TOKEN FILTERS]      → lowercase: ["the", "quick", "brown", "fox", "jumped", "over", "lazy", "dogs"]
                         → stop words: ["quick", "brown", "fox", "jumped", "lazy", "dogs"]  (remove "the","over")
                         → stemming:   ["quick", "brown", "fox", "jump", "lazi", "dog"]  (e.g., Porter stemmer)
         │
    TERMS STORED IN INDEX: ["quick", "brown", "fox", "jump", "lazi", "dog"]

  This is why "jumping" matches "jumped" (stemmed to "jump")
  and "dogs" matches "dog" (stemmed).

RELEVANCE SCORING — TF-IDF and BM25:
──────────────────────────────────────
  How does ES decide which document is MORE RELEVANT for a query?

  TF-IDF (classic):
    TF  = Term Frequency = how often term appears in this doc
          (more occurrences = more relevant)
    IDF = Inverse Document Frequency = how rare the term is across all docs
          (rare terms = more distinctive = more relevant)
    Score = TF × IDF

    Example: "shoes" appears in 90% of docs → low IDF → common term → low score boost
             "orthopedic" appears in 1% of docs → high IDF → rare term → high score boost

  BM25 (Elasticsearch default since v5):
    Improved TF-IDF that:
    - Saturates TF (100 occurrences ≈ 50 occurrences, not 2×)
    - Normalizes for document length (short doc with term = more relevant)
    Score = IDF × TF(k+1) / (TF + k(1 - b + b × docLen/avgDocLen))
    k=1.2 (TF saturation), b=0.75 (length normalization)

ELASTICSEARCH ARCHITECTURE:
─────────────────────────────
  CLUSTER  → A group of ES nodes working together
  NODE     → A single ES server
  INDEX    → A collection of documents (like a database table)
  SHARD    → A piece of an index (index is split into N shards for parallelism)
  REPLICA  → Copy of a shard (for fault tolerance + read throughput)
  DOCUMENT → A JSON object stored in the index
  FIELD    → A key-value pair in a document (like a column)
  MAPPING  → Schema definition for an index (field names + types)

SHARD ROUTING:
───────────────
  shard_num = hash(document_id) % num_primary_shards
  Documents are evenly distributed across shards automatically.
  Search queries are BROADCAST to all shards → results merged.

"""

import re
import math
import time
import uuid
from collections import defaultdict
from typing import Any, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Text Analyzer
# ─────────────────────────────────────────────────────────────────────────────

class StandardAnalyzer:
    """
    Standard text analyzer — mimics Elasticsearch's 'standard' analyzer.

    Pipeline:
      Input text → Character filter → Tokenize → Lowercase → Remove stop words → Stem

    In ES, analyzers are configurable:
      GET /my_index/_analyze {"analyzer": "english", "text": "running dogs"}
    """

    STOP_WORDS = {
        "a", "an", "the", "is", "are", "was", "were", "be", "been",
        "being", "have", "has", "had", "do", "does", "did", "will",
        "would", "could", "should", "may", "might", "shall", "can",
        "to", "of", "in", "for", "on", "with", "at", "by", "from",
        "up", "about", "into", "through", "over", "and", "but", "or",
        "so", "yet", "both", "either", "neither", "not", "no", "nor",
    }

    def analyze(self, text: str, stemming: bool = True) -> list[str]:
        """
        Analyze text into terms for indexing/querying.

        :param text:     Raw input text
        :param stemming: Apply Porter-style stemming
        :return:         List of analyzed tokens (terms)
        """
        if not text:
            return []

        # 1. Character filter: remove punctuation, normalize whitespace
        text = re.sub(r"[^\w\s]", " ", text.lower())

        # 2. Tokenize: split on whitespace
        tokens = text.split()

        # 3. Remove stop words
        tokens = [t for t in tokens if t not in self.STOP_WORDS and len(t) > 1]

        # 4. Stemming: simplified Porter-style suffix removal
        if stemming:
            tokens = [self._stem(t) for t in tokens]

        return tokens

    def _stem(self, word: str) -> str:
        """
        Simplified English stemmer.
        Full Porter Stemmer has 5 phases. This implements common cases.
        Elasticsearch uses the full Porter2 (Snowball) stemmer.
        """
        # Common suffix removal (order matters)
        suffixes = [
            ("ational", "ate"), ("tional", "tion"), ("enci", "ence"),
            ("anci", "ance"), ("izing", "ize"), ("ising", "ise"),
            ("iness", "i"),    ("ness", ""),     ("ment", ""),
            ("ation", "ate"), ("ator", "ate"),   ("alism", "al"),
            ("ness", ""),     ("ful", ""),       ("ous", ""),
            ("ive", ""),      ("ize", ""),       ("ise", ""),
            ("ing", ""),      ("tion", ""),      ("ers", "er"),
            ("ies", "y"),     ("ves", "f"),      ("ied", "y"),
            ("ing", ""),      ("eed", "ee"),     ("ed", ""),
            ("ings", ""),     ("ness", ""),      ("ly", ""),
            ("es", ""),       ("s", ""),
        ]
        for suffix, replacement in suffixes:
            if word.endswith(suffix) and len(word) - len(suffix) >= 3:
                return word[: len(word) - len(suffix)] + replacement
        return word


# ─────────────────────────────────────────────────────────────────────────────
# Inverted Index
# ─────────────────────────────────────────────────────────────────────────────

class PostingList:
    """
    Posting list for a single term — the core data structure of an inverted index.

    STORES:
      - doc_id: Which document contains this term
      - tf:     How many times the term appears in that document (term frequency)
      - positions: Exact positions of the term (for phrase queries: "hello world")

    COMPRESSED in production:
      ES stores posting lists in a highly compressed binary format.
      DocID deltas are encoded (store difference between consecutive IDs, not full IDs)
      → much smaller on disk, faster to read.
    """

    def __init__(self):
        # {doc_id → {count, positions}}
        self.postings: dict[str, dict] = {}
        self.doc_frequency: int = 0  # Number of docs containing this term

    def add(self, doc_id: str, position: int) -> None:
        """Add an occurrence of this term in a document at a given position."""
        if doc_id not in self.postings:
            self.postings[doc_id] = {"count": 0, "positions": []}
            self.doc_frequency += 1
        self.postings[doc_id]["count"] += 1
        self.postings[doc_id]["positions"].append(position)

    def term_freq(self, doc_id: str) -> int:
        return self.postings.get(doc_id, {}).get("count", 0)

    def documents(self) -> set[str]:
        return set(self.postings.keys())


class InvertedIndex:
    """
    Inverted Index — the core data structure powering search engines.

    STRUCTURE:
      {
        "shoe":     PostingList([doc1: tf=3, doc2: tf=1, ...]),
        "run":      PostingList([doc1: tf=2, doc3: tf=5, ...]),
        "athletic": PostingList([doc3: tf=1, ...]),
      }

    INDEX OPERATIONS:
      INDEX (add): tokenize document → for each token → add to posting list
      SEARCH:      tokenize query → lookup each token → intersect/union posting lists

    MERGE STRATEGIES (for distributed search):
      Each shard has its own inverted index.
      On search: query sent to all shards → each scores locally → merge + sort results.
    """

    def __init__(self, analyzer: StandardAnalyzer = None):
        self.analyzer = analyzer or StandardAnalyzer()
        # {term → PostingList}
        self._index: dict[str, PostingList] = defaultdict(PostingList)
        # {doc_id → {field → raw text}}
        self._documents: dict[str, dict] = {}
        # {doc_id → total term count (for length normalization)}
        self._doc_lengths: dict[str, int] = {}
        self._total_docs = 0
        self._avg_doc_length = 0.0

    def index_document(self, doc_id: str, document: dict,
                        searchable_fields: list[str] = None) -> None:
        """
        Index a document.

        :param doc_id:           Unique document identifier
        :param document:         The document (dict of fields)
        :param searchable_fields: Which fields to index (None = all string fields)
        """
        self._documents[doc_id] = document
        self._total_docs += 1

        # Collect all text from searchable fields
        all_terms = []
        fields = searchable_fields or [k for k, v in document.items()
                                        if isinstance(v, str)]
        for field in fields:
            text = str(document.get(field, ""))
            terms = self.analyzer.analyze(text)
            for position, term in enumerate(terms):
                self._index[term].add(doc_id, position)
                all_terms.append(term)

        # Update document length stats (for BM25 normalization)
        self._doc_lengths[doc_id] = len(all_terms)
        self._avg_doc_length = (
            sum(self._doc_lengths.values()) / len(self._doc_lengths)
        )

    def search(self, query: str, top_k: int = 10,
               operator: str = "OR") -> list[dict]:
        """
        Full-text search with BM25 relevance scoring.

        :param query:    Search query string
        :param top_k:    Return top-K results
        :param operator: "OR" (any term) or "AND" (all terms must match)
        :return:         List of {doc_id, score, document} dicts

        BM25 Parameters:
          k1 = 1.2  (term frequency saturation)
          b  = 0.75 (document length normalization)
        """
        query_terms = self.analyzer.analyze(query)
        if not query_terms:
            return []

        k1, b = 1.2, 0.75
        scores: dict[str, float] = defaultdict(float)

        # Get candidate docs (union or intersection based on operator)
        if operator == "AND":
            candidate_docs = None
            for term in query_terms:
                docs = self._index[term].documents() if term in self._index else set()
                candidate_docs = docs if candidate_docs is None else candidate_docs & docs
            candidate_docs = candidate_docs or set()
        else:  # OR
            candidate_docs = set()
            for term in query_terms:
                if term in self._index:
                    candidate_docs |= self._index[term].documents()

        # Score each candidate document using BM25
        for doc_id in candidate_docs:
            for term in query_terms:
                if term not in self._index:
                    continue
                posting = self._index[term]
                tf = posting.term_freq(doc_id)
                if tf == 0:
                    continue

                # IDF: log((N - df + 0.5) / (df + 0.5) + 1)
                N = self._total_docs
                df = posting.doc_frequency
                idf = math.log((N - df + 0.5) / (df + 0.5) + 1)

                # TF normalization (BM25)
                doc_len = self._doc_lengths.get(doc_id, 1)
                tf_norm = (tf * (k1 + 1)) / (
                    tf + k1 * (1 - b + b * doc_len / max(self._avg_doc_length, 1))
                )

                scores[doc_id] += idf * tf_norm

        # Sort by score descending
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [
            {
                "doc_id": doc_id,
                "score":  round(score, 4),
                "document": self._documents[doc_id],
            }
            for doc_id, score in ranked[:top_k]
        ]

    def suggest_completions(self, prefix: str, max_suggestions: int = 5) -> list[str]:
        """
        Prefix-based term suggestions (like autocomplete on the index terms).
        In ES: completion suggester or prefix query.
        """
        prefix_terms = prefix.lower().strip()
        suggestions = [
            term for term in self._index.keys()
            if term.startswith(prefix_terms)
        ]
        return sorted(suggestions)[:max_suggestions]

    def stats(self) -> dict:
        return {
            "total_docs":       self._total_docs,
            "unique_terms":     len(self._index),
            "avg_doc_length":   round(self._avg_doc_length, 1),
        }


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_PRODUCTS = [
    {"id": "p1", "name": "Running Shoes Pro",
     "description": "Professional running shoes for marathon athletes. Lightweight and durable."},
    {"id": "p2", "name": "Leather Oxford Shoes",
     "description": "Classic leather shoes for formal business occasions. Elegant design."},
    {"id": "p3", "name": "Trail Running Boots",
     "description": "Heavy-duty trail running boots for extreme outdoor athletes."},
    {"id": "p4", "name": "Basketball Sneakers",
     "description": "High-performance basketball shoes with superior ankle support."},
    {"id": "p5", "name": "Yoga Mat Premium",
     "description": "Non-slip yoga mat for professional yoga and fitness training."},
    {"id": "p6", "name": "Athletic Training Shorts",
     "description": "Lightweight training shorts for running, yoga, and gym workouts."},
    {"id": "p7", "name": "Waterproof Hiking Boots",
     "description": "Durable waterproof boots for hiking and outdoor adventures."},
    {"id": "p8", "name": "Tennis Racket Pro",
     "description": "Professional tennis racket for competitive tournament players."},
]


if __name__ == "__main__":
    print("=" * 65)
    print("   INVERTED INDEX & TEXT ANALYSIS — Demo")
    print("=" * 65)

    analyzer = StandardAnalyzer()
    index = InvertedIndex(analyzer)

    # ── Build the index ────────────────────────────────────────────────────
    print("\n  ─── Indexing 8 product documents ───\n")
    for p in SAMPLE_PRODUCTS:
        index.index_document(p["id"], p,
                             searchable_fields=["name", "description"])
    print(f"  Index stats: {index.stats()}")

    # ── Text analysis demo ────────────────────────────────────────────────
    print("\n\n  ─── Text Analysis Pipeline ───\n")
    texts = [
        "Running shoes for Athletes!",
        "Professional Basketball Players",
        "Hiking and Outdoor Adventures",
    ]
    for text in texts:
        tokens = analyzer.analyze(text)
        print(f"  Input:  {text!r}")
        print(f"  Output: {tokens}\n")

    # ── Search queries ────────────────────────────────────────────────────
    print("\n  ─── Full-Text Search with BM25 Scoring ───")
    queries = [
        ("running shoes",      "OR"),
        ("professional athlete", "OR"),
        ("outdoor boots",      "OR"),
        ("running AND athlete", "AND"),
    ]
    for query, op in queries:
        results = index.search(query, top_k=3, operator=op)
        print(f"\n  QUERY: {query!r} ({op})")
        for r in results:
            print(f"    [{r['score']:.4f}] {r['document']['name']}")

    # ── Inverted index peek ────────────────────────────────────────────────
    print("\n\n  ─── Inverted Index Structure ───\n")
    for term in ["run", "shoe", "athlet", "boot"]:
        if term in index._index:
            pl = index._index[term]
            print(f"  Term '{term}': df={pl.doc_frequency} docs → "
                  f"{[(d, pl.term_freq(d)) for d in list(pl.documents())[:3]]}")

    # ── Autocomplete suggestions ───────────────────────────────────────────
    print("\n\n  ─── Autocomplete (prefix suggestions) ───\n")
    for prefix in ["run", "ath", "boot", "prof"]:
        suggestions = index.suggest_completions(prefix, max_suggestions=4)
        print(f"  Prefix '{prefix}' → {suggestions}")

    print("\n" + "=" * 65)
    print("  INVERTED INDEX KEY CONCEPTS:")
    print("  Inverted Index  → term → [doc_ids] (O(1) lookup vs O(n) scan)")
    print("  Text Analysis   → tokenize → lowercase → stop words → stem")
    print("  BM25 Scoring    → TF(saturated) × IDF × length-normalized")
    print("  Shard           → each shard has its own inverted index")
    print("  Replica         → copy of shard for fault tolerance + read scale")
    print("=" * 65)
