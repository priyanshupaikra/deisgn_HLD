"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     CONTENT DELIVERY NETWORK (CDN) — MODULE 2                ║
║                           GEO-ROUTING & ANYCAST                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

HOW DOES A USER FIND THE CLOSEST EDGE SERVER?
─────────────────────────────────────────────
When a user types "cdn.example.com", how does the network know to send the Tokyo
user to the Tokyo server, and the NY user to the NY server?

There are two primary ways CDNs route traffic:

1. DNS GEO-ROUTING (GeoDNS):
   - The DNS server looks at the IP address of the user making the DNS request.
   - It determines the user's geographic location (using a GeoIP database).
   - It replies with the IP address of the edge server closest to that location.
   - Pros: Simple, customizable routing rules.
   - Cons: Relies on accurate GeoIP databases; DNS caching can cause stale routes.

2. ANYCAST ROUTING (BGP Anycast):
   - Multiple edge servers around the world broadcast the EXACT SAME IP ADDRESS.
   - When the user sends a packet to that IP, the Internet's core routers (using BGP)
     automatically forward the packet to the physically closest server on the network.
   - Pros: Extremely fast, automatic failover (if Tokyo goes down, packets naturally
     flow to the next closest node like Seoul), mitigates DDoS attacks globally.
   - Cons: Harder to set up, requires BGP control.

This file simulates DNS Geo-Routing using Haversine distance calculations.
"""

import math
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# DATA MODELS
# ─────────────────────────────────────────────────────────────────────────────

class Location:
    def __init__(self, name: str, lat: float, lon: float):
        self.name = name
        self.lat = lat
        self.lon = lon
        
    def distance_to(self, other: 'Location') -> float:
        """Haversine formula to calculate distance between two coordinates in km."""
        R = 6371.0 # Earth radius in kilometers
        dlat = math.radians(other.lat - self.lat)
        dlon = math.radians(other.lon - self.lon)
        a = (math.sin(dlat/2)**2 + 
             math.cos(math.radians(self.lat)) * math.cos(math.radians(other.lat)) * 
             math.sin(dlon/2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c


class CDNEndpoint:
    def __init__(self, location: Location, ip_address: str, is_healthy: bool = True):
        self.location = location
        self.ip_address = ip_address
        self.is_healthy = is_healthy


# ─────────────────────────────────────────────────────────────────────────────
# GEO-DNS ROUTER
# ─────────────────────────────────────────────────────────────────────────────

class GeoDNSRouter:
    """
    Simulates a DNS server that routes traffic based on geographic proximity.
    """
    
    def __init__(self):
        self.endpoints: list[CDNEndpoint] = []
        
    def register_endpoint(self, endpoint: CDNEndpoint) -> None:
        self.endpoints.append(endpoint)
        print(f"[DNS] Registered Edge at {endpoint.location.name} ({endpoint.ip_address})")

    def resolve(self, user_location: Location) -> Optional[CDNEndpoint]:
        """
        Finds the closest *healthy* edge server to the user.
        """
        print(f"\n[DNS Query] Resolving cdn.example.com for user in {user_location.name}...")
        
        closest_endpoint = None
        min_distance = float('inf')
        
        for endpoint in self.endpoints:
            # 1. Skip unhealthy nodes (automatic failover)
            if not endpoint.is_healthy:
                print(f"  - {endpoint.location.name} is down, skipping.")
                continue
                
            # 2. Calculate distance
            dist = user_location.distance_to(endpoint.location)
            print(f"  - Distance to {endpoint.location.name}: {dist:.1f} km")
            
            # 3. Track closest
            if dist < min_distance:
                min_distance = dist
                closest_endpoint = endpoint
                
        if closest_endpoint:
            print(f"[DNS Answer] → Routing to {closest_endpoint.location.name} ({closest_endpoint.ip_address})")
            return closest_endpoint
            
        print("[DNS Answer] → No healthy endpoints available!")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# DEMO
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("   GEO-ROUTING (GeoDNS) — Demo")
    print("=" * 65)

    router = GeoDNSRouter()
    
    # 1. Setup global CDN PoPs (Points of Presence)
    tokyo = CDNEndpoint(Location("Tokyo, JP", 35.6762, 139.6503), "198.51.100.1")
    ny = CDNEndpoint(Location("New York, US", 40.7128, -74.0060), "198.51.100.2")
    frankfurt = CDNEndpoint(Location("Frankfurt, DE", 50.1109, 8.6821), "198.51.100.3")
    sydney = CDNEndpoint(Location("Sydney, AU", -33.8688, 151.2093), "198.51.100.4")
    
    for ep in [tokyo, ny, frankfurt, sydney]:
        router.register_endpoint(ep)
        
    # 2. Users making requests from around the world
    users = [
        Location("Seoul, KR", 37.5665, 126.9780),      # Should route to Tokyo
        Location("London, UK", 51.5074, -0.1278),      # Should route to Frankfurt
        Location("Toronto, CA", 43.6532, -79.3832),    # Should route to New York
        Location("Melbourne, AU", -37.8136, 144.9631)  # Should route to Sydney
    ]
    
    for user in users:
        router.resolve(user)
        
    # 3. Simulate an Outage (Failover Demo)
    print("\n" + "=" * 65)
    print("   FAILOVER SCENARIO: Frankfurt datacenter goes offline")
    print("=" * 65)
    
    frankfurt.is_healthy = False
    
    # User in London requests again
    london_user = Location("London, UK", 51.5074, -0.1278)
    router.resolve(london_user) # Should automatically failover to NY!
