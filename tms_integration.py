import logging
import requests
import pytz
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import re
import urllib3

from config import Config

# Disable SSL warnings for OpenRouteService API calls
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class TMSIntegration:
    """Integration with TMS API for truck location data with enhanced speed handling"""

    def __init__(self, config: Config):
        self.config = config
        self.geocache = {}
        self.zip_cache = config.get_cache_settings().get("zip_cache", {})

    def load_truck_list(self, retry_count: int = 3) -> List[Dict[str, Any]]:
        """Load truck list from TMS API with speed data and robust error handling"""
        params = {
            "api_key": self.config.TMS_API_KEY,
            "api_hash": self.config.TMS_API_HASH,
            "active_only": "true"  # Only get active equipment to avoid old data
        }

        for attempt in range(retry_count):
            try:
                logger.info(
                    f"Loading truck list from TMS API: {self.config.TMS_API_URL} (attempt {attempt + 1}/{retry_count})")
                logger.debug(f"TMS API params: {params}")

                # Try different API call approaches
                response_methods = [
                    lambda: requests.get(
                        self.config.TMS_API_URL, params=params, timeout=30),
                    lambda: requests.get(
                        self.config.TMS_API_URL, timeout=30),  # Without params
                    lambda: requests.post(
                        self.config.TMS_API_URL,
                        json=params,
                        timeout=30),
                    # POST method
                ]

                last_error = None
                for method_idx, method in enumerate(response_methods):
                    try:
                        r = method()
                        if r.status_code == 200:
                            logger.debug(
                                f"TMS API success with method {method_idx + 1}, status: {r.status_code}")
                            break
                        else:
                            logger.warning(
                                f"TMS API method {method_idx + 1} returned status: {r.status_code}")
                            last_error = f"HTTP {r.status_code}"
                            continue
                    except Exception as method_error:
                        logger.debug(
                            f"TMS API method {method_idx + 1} failed: {method_error}")
                        last_error = str(method_error)
                        continue
                else:
                    # All methods failed
                    if attempt < retry_count - 1:
                        # 5, 10, 15 second backoff
                        wait_time = (attempt + 1) * 5
                        logger.warning(
                            f"TMS API attempt {attempt + 1} failed, retrying in {wait_time}s. Last error: {last_error}")
                        import time
                        time.sleep(wait_time)
                        continue
                    else:
                        raise requests.RequestException(
                            f"All TMS API methods failed. Last error: {last_error}")

                # If we get here, one of the methods succeeded
                r.raise_for_status()
                logger.debug(f"TMS API response status: {r.status_code}")

                all_trucks = []
                skipped = 0
                skipped_old_data = 0

                # Handle both old and new API response formats
                response_data = r.json()
                # New API uses "data" field, old API used "equipment" or "locations"
                trucks_data = response_data.get(
                    "data", response_data.get(
                        "equipment", response_data.get(
                            "locations", [])))
                logger.debug(
                    f"TMS API returned {len(trucks_data)} truck records")

                for truck in trucks_data:
                    address = truck.get("address", "Unknown")
                    # Handle both old and new API field names
                    update_time_str = truck.get("last_update_time") or truck.get("update_time")
                    lat = truck.get("latitude") or truck.get("lat")
                    lng = truck.get("longitude") or truck.get("lng")
                    source = truck.get("source", "")
                    speed = truck.get("speed")  # Extract speed from raw data

                    # Filter by source - accept all valid sources
                    valid_sources = ["samsara", "clubeld",
                                     "ada_eld", "skybitz", "intangles"]
                    if source.lower() not in valid_sources:
                        skipped += 1
                        continue

                    # Require coordinates
                    if not lat or not lng:
                        skipped += 1
                        continue

                    # Check for extremely stale data - reject anything over 24
                    # hours old regardless of address
                    if update_time_str:
                        try:
                            update_dt = datetime.strptime(
                                update_time_str.replace("EST", "").replace("EDT", ""),
                                "%m-%d-%Y %H:%M:%S "
                            ).replace(tzinfo=pytz.timezone("America/New_York"))

                            age_hours = (datetime.now(
                                pytz.utc) - update_dt.astimezone(pytz.utc)).total_seconds() / 3600

                            # Reject data older than 8 hours completely (more
                            # aggressive)
                            if age_hours > 8:
                                logger.debug(
                                    f"Skipping truck {truck.get('vin', 'unknown')} - GPS data {age_hours:.1f} hours old")
                                skipped += 1
                                skipped_old_data += 1
                                continue

                            # Also check for unknown addresses with data older
                            # than 6 hours
                            if (not address or address.strip().lower()
                                    == "unknown") and age_hours > 6:
                                logger.debug(
                                    f"Skipping truck {truck.get('vin', 'unknown')} - unknown location and {age_hours:.1f} hours old")
                                skipped += 1
                                continue

                        except Exception:
                            logger.debug(
                                f"Skipping truck {truck.get('vin', 'unknown')} - invalid timestamp: {update_time_str}")
                            skipped += 1
                            continue

                    # Process and normalize speed data
                    processed_truck = truck.copy()
                    processed_truck['speed'] = self._normalize_speed(speed)
                    
                    # Ensure standard field names for compatibility
                    processed_truck['lat'] = lat
                    processed_truck['lng'] = lng  
                    processed_truck['update_time'] = update_time_str

                    all_trucks.append(processed_truck)

                logger.info(
                    f"✅ Loaded {len(all_trucks)} trucks from TMS. Skipped: {skipped} (including {skipped_old_data} with data >24h old)")
                return all_trucks

            except Exception as e:
                logger.error(
                    f"❌ Failed to load trucks from TMS (attempt {attempt + 1}/{retry_count}): {e}")
                if attempt == retry_count - 1:
                    # Last attempt failed
                    logger.error(
                        f"❌ All {retry_count} TMS API attempts failed. Returning empty list.")
                    return []
                else:
                    # Wait before next retry
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"Waiting {wait_time}s before retry...")
                    import time
                    time.sleep(wait_time)

        # Should not reach here, but just in case
        return []

    def load_individual_truck(self, vin: str) -> Optional[Dict[str, Any]]:
        """Load single truck data when bulk API fails"""
        logger.info(f"Attempting individual truck lookup for VIN {vin[-4:]}")

        # Try alternative endpoints or methods for single truck lookup
        endpoints = [
            f"{self.config.TMS_API_URL}?vin={vin}",
            f"{self.config.TMS_API_URL.replace('/api/tms_get_locations', '/api/truck')}?vin={vin}",
            f"{self.config.TMS_API_URL.replace('/api/tms_get_locations', '/api/vehicle')}?vin={vin}"]

        for endpoint in endpoints:
            try:
                response = requests.get(endpoint, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and data:
                        truck = data[0]
                    elif isinstance(data, dict):
                        truck = data
                    else:
                        continue

                    # Validate truck data
                    if truck.get('vin', '').upper() == vin.upper(
                    ) and truck.get('lat') and truck.get('lng'):
                        logger.info(
                            f"✅ Individual truck lookup successful for VIN {vin[-4:]}")
                        return truck

            except Exception as e:
                logger.debug(
                    f"Individual truck lookup failed for {endpoint}: {e}")
                continue

        logger.warning(f"❌ Individual truck lookup failed for VIN {vin[-4:]}")
        return None

    def _is_truck_online(self, update_time_str: str) -> bool:
        """Determine if a truck should be considered online based on update time"""
        if not update_time_str:
            return False

        try:
            # Parse the update time
            update_dt = datetime.strptime(
                update_time_str.replace("EST", "").replace("EDT", ""),
                "%m-%d-%Y %H:%M:%S "
            ).replace(tzinfo=pytz.timezone("America/New_York"))

            # Convert to UTC for comparison
            update_utc = update_dt.astimezone(pytz.utc)
            now_utc = datetime.now(pytz.utc)

            # Consider truck online if updated within last 4 hours (more
            # aggressive)
            time_diff = now_utc - update_utc
            return time_diff <= timedelta(hours=4)

        except Exception as e:
            logger.debug(
                f"Could not parse update time '{update_time_str}': {e}")
            return False

    def _normalize_speed(self, speed_value: Any) -> float:
        """Normalize speed value from TMS API to a consistent float"""
        if speed_value is None:
            return 0.0

        try:
            # Handle different speed formats
            if isinstance(speed_value, (int, float)):
                return float(speed_value)

            if isinstance(speed_value, str):
                # Remove common speed unit suffixes
                speed_clean = speed_value.lower().replace(
                    'mph',
                    '').replace(
                    'kmh',
                    '').replace(
                    'kph',
                    '').strip()
                if speed_clean:
                    return float(speed_clean)

            return 0.0

        except (ValueError, TypeError):
            logger.debug(f"Could not parse speed value: {speed_value}")
            return 0.0

    def _format_speed(self, speed: float) -> str:
        """Format speed for display"""
        if speed == 0.0:
            return "0 mph"
        elif speed < 1.0:
            return f"{speed:.1f} mph"
        else:
            return f"{int(speed)} mph"

    def find_truck_by_vin(
            self, trucks: List[Dict[str, Any]], vin: str) -> Optional[Dict[str, Any]]:
        """Find truck by VIN in the truck list"""
        vin_upper = vin.upper()
        for truck in trucks:
            if truck.get("vin", "").upper() == vin_upper:
                return truck
        return None

    def check_vin_status(self, vin: str) -> Dict[str, Any]:
        """Check why a VIN might not be available in filtered truck list"""
        params = {
            "api_key": self.config.TMS_API_KEY,
            "api_hash": self.config.TMS_API_HASH,
            "active_only": "true"  # Only check active equipment
        }

        try:
            r = requests.get(
                self.config.TMS_API_URL,
                params=params,
                timeout=30)
            r.raise_for_status()

            vin_upper = vin.upper()

            # Handle both equipment and locations endpoints
            response_data = r.json()
            trucks_data = response_data.get(
                "equipment", response_data.get(
                    "locations", []))

            for truck in trucks_data:
                if truck.get("vin", "").upper() == vin_upper:
                    update_time_str = truck.get("update_time", "")

                    if not update_time_str:
                        return {
                            "found": True,
                            "filtered": True,
                            "reason": "no_timestamp",
                            "message": "GPS data has no timestamp"
                        }

                    try:
                        update_dt = datetime.strptime(
                            update_time_str.replace("EST", "").replace("EDT", ""),
                            "%m-%d-%Y %H:%M:%S "
                        ).replace(tzinfo=pytz.timezone("America/New_York"))

                        age_hours = (datetime.now(
                            pytz.utc) - update_dt.astimezone(pytz.utc)).total_seconds() / 3600

                        if age_hours > 8:
                            if age_hours > 24:
                                days = int(age_hours / 24)
                                return {
                                    "found": True,
                                    "filtered": True,
                                    "reason": "too_old",
                                    "age_hours": age_hours,
                                    "message": f"GPS data is {days} days old - filtered out as too outdated"}
                            else:
                                return {
                                    "found": True,
                                    "filtered": True,
                                    "reason": "too_old",
                                    "age_hours": age_hours,
                                    "message": f"GPS data is {age_hours:.1f} hours old - filtered out for freshness"
                                }

                        # Check coordinates
                        if not truck.get("lat") or not truck.get("lng"):
                            return {
                                "found": True,
                                "filtered": True,
                                "reason": "no_coordinates",
                                "message": "No GPS coordinates available"
                            }

                        # Truck exists and should be in filtered list
                        return {
                            "found": True,
                            "filtered": False,
                            "message": "Truck should be available - check filtering logic"}

                    except Exception:
                        return {
                            "found": True,
                            "filtered": True,
                            "reason": "invalid_timestamp",
                            "message": f"Invalid GPS timestamp: {update_time_str}"}

            # VIN not found in raw data
            return {
                "found": False,
                "filtered": False,
                "message": "VIN not found in TMS system"
            }

        except Exception as e:
            logger.error(f"Error checking VIN status: {e}")
            return {
                "found": False,
                "filtered": False,
                "message": f"Error checking TMS data: {str(e)}"
            }

    def find_truck_by_name(
            self, trucks: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
        """Find truck by name in the truck list"""
        name_lower = name.lower()
        for truck in trucks:
            truck_name = truck.get("name", "").lower()
            if name_lower in truck_name or truck_name in name_lower:
                return truck
        return None

    def geocode(self, address: str) -> Optional[List[float]]:
        """Geocode address using OpenRouteService"""
        if not address or not address.strip():
            return None

        # Check if daily quota exceeded
        if hasattr(
                self,
                '_ors_daily_quota_exceeded') and self._ors_daily_quota_exceeded:
            # Check if 24 hours have passed
            if hasattr(self, '_ors_quota_exceeded_time') and time.time(
            ) - self._ors_quota_exceeded_time > 86400:
                logger.info(
                    "ORS daily quota reset after 24 hours - re-enabling geocoding")
                self._ors_daily_quota_exceeded = False
            else:
                logger.debug(
                    "ORS daily quota exceeded - skipping geocoding request")
                return None

        # Clean address
        cleaned = " ".join(address.strip().lower().split())
        cleaned = re.sub(r"\s+", " ", cleaned)

        # Check cache first
        if cleaned in self.geocache:
            logger.debug(f"🔍 Using geocode cache for: {cleaned}")
            return self.geocache[cleaned]

        # Check ZIP cache
        if cleaned.isdigit() and cleaned in self.zip_cache:
            logger.info(f"📮 Using ZIP cache for {cleaned}")
            coords = self.zip_cache[cleaned]
            self.geocache[cleaned] = coords
            return coords

        # Apply replacements for common abbreviations
        replacements = {
            "slc": "salt lake city",
            "nyc": "new york",
            "la": "los angeles",
            "sf": "san francisco"
        }
        for k, v in replacements.items():
            cleaned = cleaned.replace(k, v)

        # Use OpenRouteService for geocoding
        url = "https://api.openrouteservice.org/geocode/search"
        params = {
            "api_key": self.config.ORS_API_KEY,
            "text": cleaned,
            "boundary.country": "US",
            "size": 1
        }

        try:
            logger.debug(f"🌍 Geocoding address: {cleaned}")

            # Add aggressive delay to prevent rate limiting
            import time
            if hasattr(self, '_last_geocode_time'):
                elapsed = time.time() - self._last_geocode_time
                min_delay = getattr(
                    self.config,
                    'ORS_REQUEST_DELAY',
                    3.0)  # 3 seconds minimum
                if elapsed < min_delay:
                    time.sleep(min_delay - elapsed)

            r = requests.get(url, params=params, timeout=10, verify=False)
            self._last_geocode_time = time.time()

            if r.status_code == 403:
                logger.error(
                    "ORS geocoding - DAILY QUOTA EXCEEDED (403) - disabling ORS geocoding")
                # Set instance flag to stop making requests
                self._ors_daily_quota_exceeded = True
                self._ors_quota_exceeded_time = time.time()
                return None

            if r.status_code == 429:
                logger.warning(
                    "ORS geocoding rate limited (429) - aggressive backoff")
                time.sleep(15)  # 15 second backoff
                return None

            r.raise_for_status()

            features = r.json().get("features", [])
            if not features:
                logger.warning(f"⚠️ No geocoding results for: {cleaned}")
                return None

            coords = features[0]["geometry"]["coordinates"]  # [lng, lat]
            self.geocache[cleaned] = coords
            logger.debug(f"✅ Geocoded '{cleaned}' to {coords}")
            return coords

        except Exception as e:
            logger.warning(f"❌ Geocoding failed for '{cleaned}': {e}")
            return None

    def get_route(
            self, origin: List[float], destination: List[float]) -> Optional[Dict[str, Any]]:
        """Get route information between two points using OpenRouteService"""
        if not origin or not destination:
            return None

        url = "https://api.openrouteservice.org/v2/directions/driving-car"
        headers = {"Authorization": self.config.ORS_API_KEY}
        body = {"coordinates": [origin, destination]}

        try:
            logger.debug(f"🛣️ Getting route from {origin} to {destination}")

            # Add aggressive delay to prevent rate limiting
            import time
            if hasattr(self, '_last_route_time'):
                elapsed = time.time() - self._last_route_time
                min_delay = getattr(
                    self.config,
                    'ORS_REQUEST_DELAY',
                    3.0)  # 3 seconds minimum
                if elapsed < min_delay:
                    time.sleep(min_delay - elapsed)

            r = requests.post(
                url,
                headers=headers,
                json=body,
                timeout=15,
                verify=False)
            self._last_route_time = time.time()

            if r.status_code == 429:
                logger.warning(
                    "ORS routing rate limited (429) - aggressive backoff")
                time.sleep(15)  # 15 second backoff
                return None

            r.raise_for_status()

            data = r.json()
            route_summary = data["routes"][0]["summary"]

            # Calculate route information
            duration_seconds = route_summary["duration"]
            distance_meters = route_summary["distance"]

            route_info = {
                "duration": timedelta(seconds=duration_seconds),
                "distance_miles": round(distance_meters / 1609.34, 1),
                "distance_km": round(distance_meters / 1000, 1),
                "map_url": f"https://www.openstreetmap.org/directions?engine=fossgis_osrm_car&route={origin[1]}%2C{origin[0]}%3B{destination[1]}%2C{destination[0]}"
            }

            logger.debug(
                f"✅ Route calculated: {route_info['distance_miles']} miles, {route_info['duration']}")
            return route_info

        except Exception as e:
            logger.warning(f"❌ Route calculation failed: {e}")
            return None

    def calculate_eta(
            self,
            current_location: List[float],
            destination: List[float]) -> Optional[datetime]:
        """Calculate ETA to destination"""
        route = self.get_route(current_location, destination)
        if route:
            return datetime.utcnow() + route["duration"]
        return None

    def format_truck_info(self, truck: Dict[str, Any]) -> Dict[str, Any]:
        """Format truck information for display with proper speed handling"""
        # Get and normalize speed
        raw_speed = truck.get("speed", 0)
        normalized_speed = self._normalize_speed(raw_speed)

        # Determine if truck should be considered online based on update time
        update_time_str = truck.get("update_time", "")
        is_online = self._is_truck_online(update_time_str)

        # Use online status if data is fresh, otherwise use API status
        api_status = truck.get("status", "unknown").title()
        if is_online and api_status.lower() == "offline":
            status = "Online"
        else:
            status = api_status

        return {
            "name": truck.get("name", "Unknown"),
            "vin": truck.get("vin", ""),
            "status": status,
            "location": truck.get("address", "Unknown"),
            "coordinates": [truck.get("lng"), truck.get("lat")] if truck.get("lng") and truck.get("lat") else None,
            "latitude": truck.get("lat"),
            "longitude": truck.get("lng"),
            "update_time": truck.get("update_time", ""),
            "source": truck.get("source", ""),
            "speed": normalized_speed,  # Normalized float value
            # Formatted string for display
            "speed_display": self._format_speed(normalized_speed),
            "heading": truck.get("heading"),
            "raw_speed": raw_speed,  # Keep original for debugging
            "is_online": is_online
        }

    def get_truck_speed_info(self, truck: Dict[str, Any]) -> Dict[str, Any]:
        """Get detailed speed information for a truck"""
        raw_speed = truck.get("speed", 0)
        normalized_speed = self._normalize_speed(raw_speed)

        # Determine movement status based on speed
        if normalized_speed == 0:
            movement_status = "Stopped"
        elif normalized_speed < 5:
            movement_status = "Idle"
        elif normalized_speed < 25:
            movement_status = "City Driving"
        elif normalized_speed < 55:
            movement_status = "Highway"
        else:
            movement_status = "High Speed"

        return {
            "speed_mph": normalized_speed,
            "speed_display": self._format_speed(normalized_speed),
            "movement_status": movement_status,
            "raw_speed": raw_speed,
            "is_moving": normalized_speed > 0.5
        }

    def validate_coordinates(self, lat: float, lng: float) -> bool:
        """Validate coordinate values"""
        try:
            lat_f = float(lat)
            lng_f = float(lng)

            # Check if coordinates are within reasonable bounds
            if -90 <= lat_f <= 90 and -180 <= lng_f <= 180:
                # Check if coordinates are not at origin (0,0) which often
                # indicates bad data
                if not (abs(lat_f) < 0.001 and abs(lng_f) < 0.001):
                    return True

            return False
        except (ValueError, TypeError):
            return False

    def search_trucks_by_pattern(
            self, trucks: List[Dict[str, Any]], pattern: str) -> List[Dict[str, Any]]:
        """Search trucks by name or VIN pattern"""
        pattern_lower = pattern.lower()
        matches = []

        for truck in trucks:
            truck_name = truck.get("name", "").lower()
            truck_vin = truck.get("vin", "").lower()

            if (pattern_lower in truck_name or
                pattern_lower in truck_vin or
                    truck_name.startswith(pattern_lower)):
                matches.append(truck)

        return matches

    def get_trucks_near_location(self,
                                 trucks: List[Dict[str,
                                                   Any]],
                                 target_coords: List[float],
                                 radius_miles: float = 50) -> List[Dict[str,
                                                                        Any]]:
        """Get trucks within a certain radius of a location"""
        if not target_coords:
            return []

        nearby_trucks = []

        for truck in trucks:
            truck_coords = [truck.get("lng"), truck.get("lat")]
            if not all(truck_coords):
                continue

            # Simple distance calculation (not precise but good enough for
            # filtering)
            lat_diff = abs(float(truck_coords[1]) - float(target_coords[1]))
            lng_diff = abs(float(truck_coords[0]) - float(target_coords[0]))

            # Rough approximation: 1 degree ≈ 69 miles
            distance_approx = ((lat_diff ** 2 + lng_diff ** 2) ** 0.5) * 69

            if distance_approx <= radius_miles:
                truck_info = self.format_truck_info(truck)
                truck_info["approximate_distance"] = round(distance_approx, 1)
                nearby_trucks.append(truck_info)

        return sorted(
            nearby_trucks,
            key=lambda x: x.get(
                "approximate_distance",
                999))

    def get_fleet_speed_summary(
            self, trucks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get speed summary for the entire fleet"""
        if not trucks:
            return {}

        speeds = []
        moving_count = 0
        stopped_count = 0

        for truck in trucks:
            speed = self._normalize_speed(truck.get("speed", 0))
            speeds.append(speed)

            if speed > 0.5:
                moving_count += 1
            else:
                stopped_count += 1

        if speeds:
            avg_speed = sum(speeds) / len(speeds)
            max_speed = max(speeds)

            return {
                "total_trucks": len(trucks),
                "moving": moving_count,
                "stopped": stopped_count,
                "average_speed": round(avg_speed, 1),
                "max_speed": round(max_speed, 1),
                "average_speed_display": self._format_speed(avg_speed),
                "max_speed_display": self._format_speed(max_speed)
            }

        return {"total_trucks": 0}


def test_tms_integration(config: Config):
    """Test TMS integration with speed data"""
    try:
        print("🧪 Testing TMS integration with speed handling...")

        # Create integration instance
        tms_integration = TMSIntegration(config)

        # Test loading trucks
        trucks = tms_integration.load_truck_list()

        if trucks:
            print(f"✅ Successfully loaded {len(trucks)} trucks from TMS")

            # Show sample truck info with speed
            if len(trucks) > 0:
                sample_truck = tms_integration.format_truck_info(trucks[0])
                speed_info = tms_integration.get_truck_speed_info(trucks[0])
                print(f"📋 Sample truck: {sample_truck['name']}")
                print(f"   Location: {sample_truck['location']}")
                print(
                    f"   Speed: {speed_info['speed_display']} ({speed_info['movement_status']})")
                print(f"   Raw speed from TMS: {speed_info['raw_speed']}")

            # Test fleet speed summary
            fleet_summary = tms_integration.get_fleet_speed_summary(trucks)
            if fleet_summary:
                print(f"🚚 Fleet Summary:")
                print(f"   Total trucks: {fleet_summary['total_trucks']}")
                print(f"   Moving: {fleet_summary.get('moving', 0)}")
                print(f"   Stopped: {fleet_summary.get('stopped', 0)}")
                print(
                    f"   Average speed: {fleet_summary.get('average_speed_display', 'N/A')}")
                print(
                    f"   Max speed: {fleet_summary.get('max_speed_display', 'N/A')}")

            # Test geocoding
            test_address = "Salt Lake City, UT"
            coords = tms_integration.geocode(test_address)
            if coords:
                print(
                    f"✅ Geocoding test successful: {test_address} -> {coords}")
            else:
                print(f"❌ Geocoding test failed for: {test_address}")

            # Test route calculation if we have coordinates
            if coords and len(trucks) > 0:
                truck_coords = [trucks[0].get("lng"), trucks[0].get("lat")]
                if all(truck_coords):
                    route = tms_integration.get_route(truck_coords, coords)
                    if route:
                        print(
                            f"✅ Route calculation test successful: {route['distance_miles']} miles")
                    else:
                        print("❌ Route calculation test failed")

            print("✅ TMS integration test completed successfully")
            return True
        else:
            print("❌ No trucks loaded from TMS")
            return False

    except Exception as e:
        print(f"❌ TMS integration test failed: {str(e)}")
        return False
