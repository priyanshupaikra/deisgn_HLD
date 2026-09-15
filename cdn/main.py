"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     CONTENT DELIVERY NETWORK (CDN) SUITE                     ║
║                           All Concepts — Main Runner                         ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  WHAT IS A CDN?                                                              ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  A global network of servers (Edge Nodes / PoPs) that cache static content   ║
║  closer to the user to reduce latency, bandwidth costs, and server load.     ║
║                                                                              ║
║  MODULES:                                                                    ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  edge_caching.py → Pull vs Push CDN, LRU cache eviction, TTL, Purge.        ║
║  geo_routing.py  → GeoDNS routing using Haversine distance & failover.       ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from edge_caching import OriginServer, EdgeServer, PushCDN
from geo_routing import Location, CDNEndpoint, GeoDNSRouter


def section(title: str):
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def subsection(title: str):
    print(f"\n  ─── {title} ───")


# ─────────────────────────────────────────────────────────────────────────────
def demo_geo_routing():
    section("MODULE 1: GEO-DNS ROUTING & ANYCAST")
    print("  How users find the closest Edge Server via DNS + Haversine distance.")

    router = GeoDNSRouter()
    
    ep_tokyo = CDNEndpoint(Location("Tokyo (Edge)", 35.6762, 139.6503), "1.1.1.1")
    ep_ny = CDNEndpoint(Location("New York (Edge)", 40.7128, -74.0060), "1.1.1.2")
    
    router.register_endpoint(ep_tokyo)
    router.register_endpoint(ep_ny)
    
    subsection("Normal Geo-Routing")
    user_seoul = Location("Seoul (User)", 37.5665, 126.9780)
    router.resolve(user_seoul)  # Expected: Tokyo
    
    user_toronto = Location("Toronto (User)", 43.6532, -79.3832)
    router.resolve(user_toronto) # Expected: New York

    subsection("Failover (Tokyo node goes down)")
    ep_tokyo.is_healthy = False
    router.resolve(user_seoul)  # Expected: Fails over to NY


# ─────────────────────────────────────────────────────────────────────────────
def demo_edge_caching():
    section("MODULE 2: PULL vs PUSH CDN & CACHE EVICTION")
    print("  How edge nodes cache data and communicate with the origin.")

    origin = OriginServer()
    tokyo_edge = EdgeServer("Tokyo", origin, cache_capacity=2)
    ny_edge = EdgeServer("New York", origin, cache_capacity=2)
    
    subsection("PULL CDN: Cache Miss -> Origin Pull -> Cache Hit")
    # First request: misses, fetches from origin
    tokyo_edge.request("/assets/style.css")
    # Second request: hits the edge cache instantly
    tokyo_edge.request("/assets/style.css")
    
    subsection("Geographic Isolation")
    # NY edge hasn't cached it yet, so it takes a miss
    ny_edge.request("/assets/style.css")

    subsection("Cache Eviction (LRU)")
    print(f"  Tokyo Cache Capacity: {tokyo_edge.capacity}")
    print("  (Adding 2 new items will force 'style.css' out of cache)")
    tokyo_edge.request("/assets/logo.png")  # Fills capacity
    tokyo_edge.request("/js/app.js")        # Evicts style.css
    
    tokyo_edge.request("/assets/style.css") # MISS! It was evicted

    subsection("Cache Invalidation (Purge)")
    origin.update_file("/js/app.js", "console.log('v2');", "application/javascript")
    tokyo_edge.purge("/js/app.js")          # API call to clear stale cache
    tokyo_edge.request("/js/app.js")        # MISS! Gets v2 from origin

    subsection("PUSH CDN: Proactive Caching")
    push = PushCDN([tokyo_edge, ny_edge])
    push.push_asset("/video/movie.mp4", "[BINARY_VIDEO]", "video/mp4")
    tokyo_edge.request("/video/movie.mp4")  # Instant HIT!


# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    section("CDN (CONTENT DELIVERY NETWORK) — COMPLETE REFERENCE")
    print()
    print("  KEY CONCEPTS:")
    for row in [
        ("Origin Server", "The main backend DB/Storage holding original files"),
        ("Edge Server",   "Globally distributed proxy nodes caching content"),
        ("PoP",           "Point of Presence (a CDN datacenter location)"),
    ]:
        print(f"    {row[0]:<15} → {row[1]}")

    print("\n  PULL vs PUSH CDN:")
    for row in [
        ("Pull CDN", "Edge fetches on demand (first user gets cache miss) [Most Common]"),
        ("Push CDN", "Dev uploads directly to Edge proactively (for large files/media)"),
    ]:
        print(f"    {row[0]:<15} → {row[1]}")

    print("\n  ROUTING STRATEGIES:")
    for row in [
        ("GeoDNS",       "DNS resolves URL to closest Edge IP based on user location"),
        ("Anycast BGP",  "All Edges share same IP; network routers route to closest"),
    ]:
        print(f"    {row[0]:<15} → {row[1]}")

    print("\n  CACHE MANAGEMENT:")
    for row in [
        ("TTL",               "Time-To-Live (Cache-Control: max-age). Expires stale data."),
        ("LRU Eviction",      "Drops least recently used file when Edge runs out of space"),
        ("Cache Purge",       "API call to instantly delete a file globally (Invalidation)"),
        ("Cache Busting",     "Appending version to URL (app_v2.js) to bypass stale cache"),
    ]:
        print(f"    {row[0]:<15} → {row[1]}")

    print()
    print("  Run individual files:")
    for f in ["edge_caching.py", "geo_routing.py"]:
        print(f"    python {f}")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 65)
    print("       CONTENT DELIVERY NETWORK (CDN) — All Concepts")
    print("=" * 65)

    demo_geo_routing()
    demo_edge_caching()
    print_summary()
