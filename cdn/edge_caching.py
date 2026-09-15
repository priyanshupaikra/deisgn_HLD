"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     CONTENT DELIVERY NETWORK (CDN) — MODULE 1                ║
║                     EDGE SERVERS & PULL/PUSH CACHING                         ║
╚══════════════════════════════════════════════════════════════════════════════╝

WHAT IS A CDN?
──────────────
A Content Delivery Network (CDN) is a geographically distributed group of servers
that work together to provide fast delivery of Internet content.
Instead of every user hitting your main server in New York, users in Tokyo
download images from a CDN server in Tokyo.

KEY COMPONENTS:
───────────────
  1. ORIGIN SERVER: The original source of truth (your main backend).
  2. EDGE SERVERS (PoPs - Points of Presence): Servers deployed globally
     that cache content close to the users.

WHY USE A CDN?
──────────────
  - Latency Reduction: Shorter physical distance = faster loading times.
  - Bandwidth Savings: CDN absorbs 80-90% of traffic, saving origin bandwidth costs.
  - High Availability: Can handle traffic spikes (DDoS protection).
  - Security: Provides WAF (Web Application Firewall), rate limiting at the edge.

PULL vs PUSH CDN:
─────────────────
  PULL CDN (Most Common):
    - Origin does nothing initially.
    - When User A requests a file, Edge Server checks its cache (MISS).
    - Edge pulls it from Origin, caches it, and serves it.
    - When User B requests it, Edge serves from cache (HIT).
    - Use for: General web traffic (images, CSS, JS, HTML).

  PUSH CDN:
    - You (the developer) proactively upload (push) files to the CDN.
    - Content is guaranteed to be there when requested.
    - Use for: Large files, software updates, video streaming, where the first
      user experiencing a cache miss would be unacceptable.

CACHE CONTROL & EVICTION:
─────────────────────────
  - TTL (Time To Live): How long content stays at the edge (e.g., Cache-Control: max-age=3600).
  - Purge/Invalidation: API call to immediately remove an item from all edge caches.
  - Cache Buster: Appending a version to the URL (style_v2.css) to force a new fetch.

"""

import time
from typing import Optional, Dict, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# 1. ORIGIN SERVER
# ─────────────────────────────────────────────────────────────────────────────

class OriginServer:
    """
    The main backend server that holds the single source of truth for all files.
    Usually located in one specific geographic region (e.g., us-east-1).
    """

    def __init__(self):
        # Database of files: {path: (content, content_type)}
        self._storage: Dict[str, Tuple[str, str]] = {
            "/assets/logo.png": ("[BINARY_LOGO_DATA]", "image/png"),
            "/assets/style.css": ("body { color: red; }", "text/css"),
            "/js/app.js": ("console.log('v1');", "application/javascript"),
            "/videos/promo.mp4": ("[HUGE_VIDEO_DATA]", "video/mp4"),
        }
        self.total_requests = 0
        self.bandwidth_served_bytes = 0

    def fetch(self, path: str) -> Optional[Tuple[str, str]]:
        """Simulate a request to the origin (takes time due to distance/processing)."""
        self.total_requests += 1
        time.sleep(0.05)  # Simulate network latency / DB lookup

        content = self._storage.get(path)
        if content:
            self.bandwidth_served_bytes += len(content[0])
            print(f"    [ORIGIN] Served {path}")
            return content
        else:
            print(f"    [ORIGIN] 404 Not Found: {path}")
            return None

    def update_file(self, path: str, content: str, content_type: str) -> None:
        """Update a file at the origin."""
        self._storage[path] = (content, content_type)
        print(f"    [ORIGIN] File updated: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# 2. EDGE SERVER (PULL CDN)
# ─────────────────────────────────────────────────────────────────────────────

class EdgeServer:
    """
    A CDN Edge Server located in a specific geographic region (e.g., Tokyo).
    Uses an LRU (Least Recently Used) cache with TTL expiration.
    """

    def __init__(self, region: str, origin: OriginServer, cache_capacity: int = 100):
        self.region = region
        self.origin = origin
        self.capacity = cache_capacity
        
        # Cache: {path: {"content": str, "type": str, "expires_at": float}}
        self._cache: Dict[str, dict] = {}
        # Simple list to track access order (most recent at end)
        self._lru_order: list[str] = []

        # Stats
        self.hits = 0
        self.misses = 0
        self.total_requests = 0

    def request(self, path: str, current_time: float = None) -> Tuple[int, str]:
        """
        Handle a user request at the edge.
        Returns: (HTTP_STATUS_CODE, CONTENT)
        """
        self.total_requests += 1
        now = current_time or time.time()

        # 1. Check Cache
        if path in self._cache:
            entry = self._cache[path]
            
            # Check TTL (Time To Live)
            if now < entry["expires_at"]:
                # HIT (Valid)
                self.hits += 1
                self._mark_used(path)
                print(f"  [{self.region} EDGE] HIT  → {path}")
                return 200, entry["content"]
            else:
                # HIT (Expired / Stale) → Treat as MISS
                print(f"  [{self.region} EDGE] STALE→ {path} (expired)")
                del self._cache[path]
                self._lru_order.remove(path)

        # 2. MISS → Pull from Origin
        self.misses += 1
        print(f"  [{self.region} EDGE] MISS → {path} (Pulling from Origin)")
        response = self.origin.fetch(path)

        if response:
            content, content_type = response
            
            # 3. Store in Cache (Assuming 1-hour TTL for static assets)
            ttl = 3600
            self._cache_store(path, content, content_type, now + ttl)
            
            return 200, content
        else:
            return 404, "Not Found"

    def _cache_store(self, path: str, content: str, content_type: str, expires_at: float) -> None:
        """Store item in cache, evicting LRU if capacity reached."""
        if len(self._cache) >= self.capacity:
            # Evict least recently used (first item in list)
            lru_key = self._lru_order.pop(0)
            del self._cache[lru_key]
            print(f"  [{self.region} EDGE] EVICT→ {lru_key} (capacity reached)")

        self._cache[path] = {
            "content": content,
            "type": content_type,
            "expires_at": expires_at
        }
        self._lru_order.append(path)

    def _mark_used(self, path: str) -> None:
        """Update LRU tracking."""
        if path in self._lru_order:
            self._lru_order.remove(path)
            self._lru_order.append(path)

    def purge(self, path: str) -> None:
        """Cache Invalidation: Forcibly remove an item from the edge cache."""
        if path in self._cache:
            del self._cache[path]
            self._lru_order.remove(path)
            print(f"  [{self.region} EDGE] PURGE→ {path}")

    def get_hit_ratio(self) -> float:
        if self.total_requests == 0: return 0.0
        return (self.hits / self.total_requests) * 100


# ─────────────────────────────────────────────────────────────────────────────
# 3. PUSH CDN
# ─────────────────────────────────────────────────────────────────────────────

class PushCDN:
    """
    In a Push CDN, the developer proactively uploads files to the CDN edges
    before users request them. Good for large media files where a "miss" penalty
    is too high.
    """
    
    def __init__(self, edge_servers: list[EdgeServer]):
        self.edge_servers = edge_servers

    def push_asset(self, path: str, content: str, content_type: str, ttl: int = 3600) -> None:
        """Push a file to all edge servers proactively."""
        print(f"\n  [PUSH CDN] Distributing '{path}' to all {len(self.edge_servers)} edge servers...")
        now = time.time()
        for edge in self.edge_servers:
            # Bypass origin entirely, inject straight into edge cache
            edge._cache_store(path, content, content_type, expires_at=now + ttl)
            print(f"    → Pushed to {edge.region} Edge")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / DRIVER CODE
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   PULL vs PUSH CDN & EDGE CACHING — Demo")
    print("=" * 65)

    # Setup infrastructure
    origin = OriginServer()
    tokyo_edge = EdgeServer("Tokyo", origin, cache_capacity=2)
    frankfurt_edge = EdgeServer("Frankfurt", origin, cache_capacity=2)
    
    # ── PULL CDN DEMO ──────────────────────────────────────────────────────
    print("\n  ─── PULL CDN: First request (Cache MISS) ───")
    status, data = tokyo_edge.request("/assets/logo.png")
    
    print("\n  ─── PULL CDN: Subsequent requests (Cache HIT) ───")
    tokyo_edge.request("/assets/logo.png")
    tokyo_edge.request("/assets/logo.png")

    print("\n  ─── GEOGRAPHIC ISOLATION (Frankfurt has its own cache) ───")
    # Tokyo has it cached, but Frankfurt doesn't yet
    frankfurt_edge.request("/assets/logo.png")
    frankfurt_edge.request("/assets/logo.png")

    # ── CACHE EVICTION (LRU) ───────────────────────────────────────────────
    print("\n  ─── LRU CACHE EVICTION (Capacity=2) ───")
    tokyo_edge.request("/assets/style.css") # Cache now has logo.png, style.css
    print("  (Tokyo Cache full. Next new file will evict the oldest: logo.png)")
    
    # Request a new file → forces eviction
    tokyo_edge.request("/js/app.js")
    
    # Now requesting the evicted file causes a MISS again
    tokyo_edge.request("/assets/logo.png")

    # ── CACHE INVALIDATION ─────────────────────────────────────────────────
    print("\n  ─── CACHE INVALIDATION / PURGE ───")
    # Developer updates a file at the origin
    origin.update_file("/assets/style.css", "body { color: blue; }", "text/css")
    
    # If users request now, they get stale data from Tokyo
    print("  User requests before purge:")
    tokyo_edge.request("/assets/style.css") # HIT (Stale data)
    
    # Developer issues a Purge command
    print("  Developer issues PURGE API call:")
    tokyo_edge.purge("/assets/style.css")
    
    # Next request pulls the fresh data
    print("  User requests after purge:")
    tokyo_edge.request("/assets/style.css") # MISS (Pulls fresh)

    # ── PUSH CDN DEMO ──────────────────────────────────────────────────────
    print("\n  ─── PUSH CDN: Proactive Distribution ───")
    push_cdn = PushCDN([tokyo_edge, frankfurt_edge])
    
    # Dev pushes a large video file
    push_cdn.push_asset("/videos/new_release.mp4", "[4K_VIDEO_DATA]", "video/mp4")
    
    # Users request it - instant HIT, no origin penalty
    print("  User requests video:")
    tokyo_edge.request("/videos/new_release.mp4")
    frankfurt_edge.request("/videos/new_release.mp4")

    # ── STATS ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("  PERFORMANCE SUMMARY")
    print("=" * 65)
    print(f"  Origin Server Total Requests: {origin.total_requests} (Would be {tokyo_edge.total_requests + frankfurt_edge.total_requests} without CDN!)")
    print(f"  Tokyo Edge Hit Ratio:     {tokyo_edge.get_hit_ratio():.1f}%")
    print(f"  Frankfurt Edge Hit Ratio: {frankfurt_edge.get_hit_ratio():.1f}%")
    print("=" * 65)
