import requests
import time
import math
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import get_logger
from config.settings import Settings

logger = get_logger(__name__)

@dataclass
class GeoLocation:
    """Geographic location with coordinates and metadata."""
    address: str
    latitude: float
    longitude: float
    city: str = ""
    state: str = ""
    zip_code: str = ""
    country: str = "US"
    formatted_address: str = ""
    confidence: float = 0.0

@dataclass
class RouteOptimization:
    """Route optimization result."""
    waypoints: List[GeoLocation]
    total_distance: float
    total_time: float
    optimized_order: List[int]
    savings: Dict[str, float]

class GeoProcessor:
    """
    Geographic processing for route data.
    Handles geocoding, distance calculations, and route optimization.
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.geocoding_cache = {}
        self.distance_cache = {}
        
        # Rate limiting for API calls
        self.last_api_call = 0
        self.min_api_interval = 0.1  # 10 requests per second max
    
    def geocode_address(self, address: str, city: str = "", 
                       state: str = "", zip_code: str = "") -> Optional[GeoLocation]:
        """
        Geocode an address to get coordinates.
        Uses multiple geocoding services with fallback.
        """
        if not address:
            return None
        
        # Create full address string
        full_address = self._create_full_address(address, city, state, zip_code)
        
        # Check cache first
        if full_address in self.geocoding_cache:
            logger.debug(f"Using cached geocoding for: {full_address}")
            return self.geocoding_cache[full_address]
        
        # Try geocoding services in order of preference
        result = None
        
        # Try OpenStreetMap Nominatim (free, no API key required)
        result = self._geocode_nominatim(full_address)
        
        if not result:
            # Try Google Maps (requires API key)
            result = self._geocode_google(full_address)
        
        if not result:
            # Try MapBox (requires API key)
            result = self._geocode_mapbox(full_address)
        
        if result:
            self.geocoding_cache[full_address] = result
            logger.info(f"Geocoded: {full_address} -> {result.latitude}, {result.longitude}")
        else:
            logger.warning(f"Failed to geocode: {full_address}")
        
        return result
    
    def _create_full_address(self, address: str, city: str = "", 
                            state: str = "", zip_code: str = "") -> str:
        """Create a full address string for geocoding."""
        parts = [address]
        
        if city:
            parts.append(city)
        if state:
            parts.append(state)
        if zip_code:
            parts.append(zip_code)
        
        return ", ".join(parts)
    
    def _geocode_nominatim(self, address: str) -> Optional[GeoLocation]:
        """
        Geocode using OpenStreetMap Nominatim service.
        """
        try:
            self._rate_limit()
            
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': address,
                'format': 'json',
                'limit': 1,
                'countrycodes': 'us',
                'addressdetails': 1
            }
            
            headers = {
                'User-Agent': 'RouteDataPipeline/1.0'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data:
                    result = data[0]
                    
                    # Extract address components
                    address_parts = result.get('address', {})
                    
                    return GeoLocation(
                        address=address,
                        latitude=float(result['lat']),
                        longitude=float(result['lon']),
                        city=address_parts.get('city', ''),
                        state=address_parts.get('state', ''),
                        zip_code=address_parts.get('postcode', ''),
                        formatted_address=result.get('display_name', ''),
                        confidence=float(result.get('importance', 0))
                    )
            
        except Exception as e:
            logger.error(f"Nominatim geocoding error: {e}")
        
        return None
    
    def _geocode_google(self, address: str) -> Optional[GeoLocation]:
        """
        Geocode using Google Maps API.
        """
        api_key = self.settings.get_google_maps_api_key()
        if not api_key:
            return None
        
        try:
            self._rate_limit()
            
            url = "https://maps.googleapis.com/maps/api/geocode/json"
            params = {
                'address': address,
                'key': api_key,
                'region': 'us'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data['status'] == 'OK' and data['results']:
                    result = data['results'][0]
                    location = result['geometry']['location']
                    
                    # Extract address components
                    components = result.get('address_components', [])
                    city = self._extract_component(components, 'locality')
                    state = self._extract_component(components, 'administrative_area_level_1')
                    zip_code = self._extract_component(components, 'postal_code')
                    
                    return GeoLocation(
                        address=address,
                        latitude=location['lat'],
                        longitude=location['lng'],
                        city=city,
                        state=state,
                        zip_code=zip_code,
                        formatted_address=result.get('formatted_address', ''),
                        confidence=1.0  # Google results are generally high confidence
                    )
            
        except Exception as e:
            logger.error(f"Google Maps geocoding error: {e}")
        
        return None
    
    def _geocode_mapbox(self, address: str) -> Optional[GeoLocation]:
        """
        Geocode using MapBox API.
        """
        api_key = self.settings.get_mapbox_api_key()
        if not api_key:
            return None
        
        try:
            self._rate_limit()
            
            url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json"
            params = {
                'access_token': api_key,
                'country': 'us',
                'limit': 1
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data['features']:
                    feature = data['features'][0]
                    coordinates = feature['geometry']['coordinates']
                    
                    # Extract address components
                    context = feature.get('context', [])
                    city = self._extract_mapbox_component(context, 'place')
                    state = self._extract_mapbox_component(context, 'region')
                    zip_code = self._extract_mapbox_component(context, 'postcode')
                    
                    return GeoLocation(
                        address=address,
                        latitude=coordinates[1],
                        longitude=coordinates[0],
                        city=city,
                        state=state,
                        zip_code=zip_code,
                        formatted_address=feature.get('place_name', ''),
                        confidence=feature.get('relevance', 0)
                    )
            
        except Exception as e:
            logger.error(f"MapBox geocoding error: {e}")
        
        return None
    
    def _extract_component(self, components: List[Dict], component_type: str) -> str:
        """Extract address component from Google Maps API response."""
        for component in components:
            if component_type in component.get('types', []):
                return component.get('short_name', '')
        return ''
    
    def _extract_mapbox_component(self, context: List[Dict], component_type: str) -> str:
        """Extract address component from MapBox API response."""
        for item in context:
            if item.get('id', '').startswith(component_type):
                return item.get('text', '')
        return ''
    
    def _rate_limit(self):
        """Implement rate limiting for API calls."""
        now = time.time()
        elapsed = now - self.last_api_call
        
        if elapsed < self.min_api_interval:
            time.sleep(self.min_api_interval - elapsed)
        
        self.last_api_call = time.time()
    
    def calculate_distance(self, lat1: float, lon1: float, 
                          lat2: float, lon2: float) -> float:
        """
        Calculate distance between two points using Haversine formula.
        Returns distance in miles.
        """
        try:
            # Convert to radians
            lat1_rad = math.radians(lat1)
            lon1_rad = math.radians(lon1)
            lat2_rad = math.radians(lat2)
            lon2_rad = math.radians(lon2)
            
            # Haversine formula
            dlat = lat2_rad - lat1_rad
            dlon = lon2_rad - lon1_rad
            
            a = (math.sin(dlat/2)**2 + 
                 math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2)
            c = 2 * math.asin(math.sqrt(a))
            
            # Radius of earth in miles
            distance = c * 3956
            return round(distance, 2)
            
        except Exception as e:
            logger.error(f"Distance calculation error: {e}")
            return 0.0
    
    def calculate_route_distance(self, waypoints: List[GeoLocation]) -> float:
        """
        Calculate total distance for a route with multiple waypoints.
        """
        if len(waypoints) < 2:
            return 0.0
        
        total_distance = 0.0
        
        for i in range(len(waypoints) - 1):
            current = waypoints[i]
            next_point = waypoints[i + 1]
            
            distance = self.calculate_distance(
                current.latitude, current.longitude,
                next_point.latitude, next_point.longitude
            )
            total_distance += distance
        
        return round(total_distance, 2)
    
    def get_route_directions(self, start: GeoLocation, 
                           end: GeoLocation) -> Optional[Dict[str, Any]]:
        """
        Get turn-by-turn directions between two points.
        """
        # Try different routing services
        directions = self._get_osrm_directions(start, end)
        
        if not directions:
            directions = self._get_google_directions(start, end)
        
        return directions
    
    def _get_osrm_directions(self, start: GeoLocation, 
                           end: GeoLocation) -> Optional[Dict[str, Any]]:
        """
        Get directions using OSRM (Open Source Routing Machine).
        """
        try:
            self._rate_limit()
            
            url = f"http://router.project-osrm.org/route/v1/driving/{start.longitude},{start.latitude};{end.longitude},{end.latitude}"
            params = {
                'overview': 'false',
                'geometries': 'geojson',
                'steps': 'true'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 'Ok' and data.get('routes'):
                    route = data['routes'][0]
                    
                    return {
                        'distance': route['distance'] * 0.000621371,  # Convert meters to miles
                        'duration': route['duration'] / 60,  # Convert seconds to minutes
                        'steps': route.get('legs', [{}])[0].get('steps', [])
                    }
            
        except Exception as e:
            logger.error(f"OSRM directions error: {e}")
        
        return None
    
    def _get_google_directions(self, start: GeoLocation, 
                             end: GeoLocation) -> Optional[Dict[str, Any]]:
        """
        Get directions using Google Maps API.
        """
        api_key = self.settings.get_google_maps_api_key()
        if not api_key:
            return None
        
        try:
            self._rate_limit()
            
            url = "https://maps.googleapis.com/maps/api/directions/json"
            params = {
                'origin': f"{start.latitude},{start.longitude}",
                'destination': f"{end.latitude},{end.longitude}",
                'key': api_key,
                'units': 'imperial'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'OK' and data.get('routes'):
                    route = data['routes'][0]
                    leg = route['legs'][0]
                    
                    return {
                        'distance': leg['distance']['value'] * 0.000621371,  # Convert meters to miles
                        'duration': leg['duration']['value'] / 60,  # Convert seconds to minutes
                        'steps': leg.get('steps', [])
                    }
            
        except Exception as e:
            logger.error(f"Google directions error: {e}")
        
        return None
    
    def optimize_route(self, waypoints: List[GeoLocation], 
                      start_point: Optional[GeoLocation] = None) -> RouteOptimization:
        """
        Optimize the order of waypoints for minimum distance.
        Uses a simple nearest neighbor algorithm for now.
        """
        if len(waypoints) < 2:
            return RouteOptimization(
                waypoints=waypoints,
                total_distance=0.0,
                total_time=0.0,
                optimized_order=[],
                savings={}
            )
        
        # Calculate original route distance
        original_distance = self.calculate_route_distance(waypoints)
        
        # Use nearest neighbor algorithm for optimization
        optimized_order = self._nearest_neighbor_optimization(waypoints, start_point)
        optimized_waypoints = [waypoints[i] for i in optimized_order]
        optimized_distance = self.calculate_route_distance(optimized_waypoints)
        
        # Calculate savings
        distance_savings = original_distance - optimized_distance
        time_savings = distance_savings / 55  # Assume 55 mph average speed
        
        return RouteOptimization(
            waypoints=optimized_waypoints,
            total_distance=optimized_distance,
            total_time=optimized_distance / 55,  # Estimate time
            optimized_order=optimized_order,
            savings={
                'distance_miles': distance_savings,
                'time_hours': time_savings,
                'distance_percent': (distance_savings / original_distance * 100) if original_distance > 0 else 0
            }
        )
    
    def _nearest_neighbor_optimization(self, waypoints: List[GeoLocation], 
                                     start_point: Optional[GeoLocation] = None) -> List[int]:
        """
        Simple nearest neighbor algorithm for route optimization.
        """
        if not waypoints:
            return []
        
        unvisited = set(range(len(waypoints)))
        order = []
        
        # Start from specified point or first waypoint
        if start_point:
            # Find closest waypoint to start point
            min_distance = float('inf')
            current_idx = 0
            
            for i, waypoint in enumerate(waypoints):
                distance = self.calculate_distance(
                    start_point.latitude, start_point.longitude,
                    waypoint.latitude, waypoint.longitude
                )
                if distance < min_distance:
                    min_distance = distance
                    current_idx = i
        else:
            current_idx = 0
        
        # Visit each waypoint in nearest neighbor order
        while unvisited:
            order.append(current_idx)
            unvisited.remove(current_idx)
            
            if not unvisited:
                break
            
            # Find nearest unvisited waypoint
            min_distance = float('inf')
            next_idx = None
            
            current_waypoint = waypoints[current_idx]
            
            for idx in unvisited:
                waypoint = waypoints[idx]
                distance = self.calculate_distance(
                    current_waypoint.latitude, current_waypoint.longitude,
                    waypoint.latitude, waypoint.longitude
                )
                
                if distance < min_distance:
                    min_distance = distance
                    next_idx = idx
            
            current_idx = next_idx
        
        return order
    
    def get_geographic_center(self, locations: List[GeoLocation]) -> Optional[GeoLocation]:
        """
        Calculate the geographic center of a list of locations.
        """
        if not locations:
            return None
        
        # Calculate average latitude and longitude
        total_lat = sum(loc.latitude for loc in locations)
        total_lon = sum(loc.longitude for loc in locations)
        
        center_lat = total_lat / len(locations)
        center_lon = total_lon / len(locations)
        
        return GeoLocation(
            address="Geographic Center",
            latitude=center_lat,
            longitude=center_lon
        )
    
    def find_locations_within_radius(self, center: GeoLocation, 
                                   locations: List[GeoLocation], 
                                   radius_miles: float) -> List[GeoLocation]:
        """
        Find locations within a specified radius of a center point.
        """
        nearby_locations = []
        
        for location in locations:
            distance = self.calculate_distance(
                center.latitude, center.longitude,
                location.latitude, location.longitude
            )
            
            if distance <= radius_miles:
                nearby_locations.append(location)
        
        return nearby_locations
    
    def geocode_batch(self, addresses: List[Dict[str, str]]) -> List[Optional[GeoLocation]]:
        """
        Geocode multiple addresses in batch.
        """
        logger.info(f"Geocoding {len(addresses)} addresses")
        
        results = []
        
        for i, addr_data in enumerate(addresses):
            if i % 10 == 0:
                logger.info(f"Geocoded {i}/{len(addresses)} addresses")
            
            result = self.geocode_address(
                addr_data.get('address', ''),
                addr_data.get('city', ''),
                addr_data.get('state', ''),
                addr_data.get('zip_code', '')
            )
            results.append(result)
            
            # Rate limiting
            time.sleep(0.1)
        
        logger.info(f"Geocoding complete: {sum(1 for r in results if r)} successful")
        return results
    
    def clear_cache(self):
        """Clear geocoding and distance caches."""
        self.geocoding_cache.clear()
        self.distance_cache.clear()
        logger.info("Cleared geographic processing caches")