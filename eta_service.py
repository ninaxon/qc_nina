# eta_service.py
import requests
import logging
import time
import pytz
from datetime import datetime, timedelta
from dateutil import parser as dtp
from typing import Optional, Tuple, Dict, Any
import urllib3

# Disable SSL warnings for OpenRouteService API
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class ETAService:
    """ETA calculation service using OpenRouteService with circuit breaker"""

    def __init__(self, ors_api_key: str):
        self.key = ors_api_key
        self.geocache = {}  # Simple geocoding cache

        # Circuit breaker for external API calls
        self.circuit_breaker_failures = 0
        self.circuit_breaker_threshold = 3  # Open circuit after 3 failures
        self.circuit_breaker_timeout = 300  # 5 minutes
        self.circuit_open_until = None

    def _route(self, src_lat: float, src_lon: float,
               dst_lat: float, dst_lon: float) -> Tuple[int, int]:
        """
        Calculate route between two points
        Returns: (miles, seconds)
        """
        # Check circuit breaker
        if self.circuit_open_until:
            if time.time() < self.circuit_open_until:
                raise Exception(
                    "Circuit breaker is open - external API temporarily unavailable")
            else:
                # Reset circuit breaker after timeout
                self.circuit_open_until = None
                self.circuit_breaker_failures = 0
                logger.info(
                    "Circuit breaker reset - attempting to reconnect to external API")

        url = "https://api.openrouteservice.org/v2/directions/driving-car"
        payload = {"coordinates": [[src_lon, src_lat], [dst_lon, dst_lat]]}
        headers = {
            "Authorization": self.key,
            "Content-Type": "application/json"
        }

        try:
            # Add aggressive delay to prevent rate limiting
            import time
            if hasattr(self, '_last_request_time'):
                elapsed = time.time() - self._last_request_time
                min_delay = 3.0  # 3 second minimum delay
                if elapsed < min_delay:
                    time.sleep(min_delay - elapsed)

            r = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=20,
                verify=False)
            self._last_request_time = time.time()

            if r.status_code == 429:
                logger.warning(
                    "ORS ETA service rate limited (429) - aggressive backoff")
                time.sleep(15)  # 15 second backoff
                raise Exception("Rate limited")

            r.raise_for_status()

            feat = r.json()["routes"][0]
            sec = int(feat["summary"]["duration"])
            km = float(feat["summary"]["distance"]) / 1000.0
            miles = round(km * 0.621371)

            # Success - reset circuit breaker
            self.circuit_breaker_failures = 0

            return miles, sec
        except Exception as e:
            # Handle circuit breaker on failures
            self.circuit_breaker_failures += 1
            if self.circuit_breaker_failures >= self.circuit_breaker_threshold:
                self.circuit_open_until = time.time() + self.circuit_breaker_timeout
                logger.error(
                    f"Circuit breaker opened after {self.circuit_breaker_failures} failures - API unavailable for {self.circuit_breaker_timeout}s")

            logger.error(f"❌ Route calculation failed: {e}")
            raise

    def geocode(self, address: str) -> Optional[Tuple[float, float]]:
        """
        Geocode address to (lat, lon)
        Returns: (latitude, longitude) or None
        """
        if not address or not address.strip():
            return None

        # Clean and normalize address
        cleaned = address.strip().lower()

        # Check cache first
        if cleaned in self.geocache:
            logger.debug(f"Using geocode cache for: {cleaned}")
            return self.geocache[cleaned]

        # Check circuit breaker
        if self.circuit_open_until:
            if time.time() < self.circuit_open_until:
                logger.warning(
                    "❌ Geocoding failed: Circuit breaker is open - external API temporarily unavailable")
                return None
            else:
                # Reset circuit breaker after timeout
                self.circuit_open_until = None
                self.circuit_breaker_failures = 0
                logger.info(
                    "Circuit breaker reset - attempting to reconnect to external API")

        # Use OpenRouteService for geocoding
        url = "https://api.openrouteservice.org/geocode/search"
        params = {
            "api_key": self.key,
            "text": cleaned,
            "boundary.country": "US",
            "size": 1
        }

        try:
            logger.debug(f"Geocoding address: {cleaned}")

            # Add aggressive delay to prevent rate limiting
            import time
            if hasattr(self, '_last_geocode_time'):
                elapsed = time.time() - self._last_geocode_time
                min_delay = 3.0  # 3 second minimum delay
                if elapsed < min_delay:
                    time.sleep(min_delay - elapsed)

            r = requests.get(url, params=params, timeout=10, verify=False)
            self._last_geocode_time = time.time()

            if r.status_code == 429:
                logger.warning(
                    "ORS geocoding rate limited (429) - aggressive backoff")
                time.sleep(15)  # 15 second backoff
                return None

            r.raise_for_status()

            features = r.json().get("features", [])
            if not features:
                logger.warning(f"No geocoding results for: {cleaned}")
                return None

            coords = features[0]["geometry"]["coordinates"]  # [lng, lat]
            result = (coords[1], coords[0])  # Return as (lat, lng)

            # Cache the result
            self.geocache[cleaned] = result
            logger.debug(f"Geocoded '{cleaned}' to {result}")

            # Success - reset circuit breaker failures
            self.circuit_breaker_failures = 0

            return result

        except Exception as e:
            # Handle circuit breaker on failures
            self.circuit_breaker_failures += 1
            if self.circuit_breaker_failures >= self.circuit_breaker_threshold:
                self.circuit_open_until = time.time() + self.circuit_breaker_timeout
                logger.error(
                    f"Circuit breaker opened after {self.circuit_breaker_failures} failures - API unavailable for {self.circuit_breaker_timeout}s")

            logger.warning(f"❌ Geocoding failed for '{cleaned}': {e}")
            return None

    def eta_from_now(self, src_lat: float, src_lon: float,
                     address: str) -> Optional[Dict[str, Any]]:
        """
        Calculate ETA from current location to destination address
        Returns: {miles: int, seconds: int, eta_utc: datetime} or None
        """
        try:
            # Geocode destination
            dst_coords = self.geocode(address)
            if not dst_coords:
                logger.warning(f"Could not geocode destination: {address}")
                return None

            dst_lat, dst_lon = dst_coords

            # Calculate route
            miles, sec = self._route(src_lat, src_lon, dst_lat, dst_lon)

            # Calculate ETA
            eta_utc = datetime.utcnow() + timedelta(seconds=sec)

            return {
                "miles": miles,
                "seconds": sec,
                "eta_utc": eta_utc
            }

        except Exception as e:
            logger.error(f"ETA calculation failed: {e}")
            return None

    @staticmethod
    def is_late(eta_utc: datetime, appt_str: str,
                grace_min: int = 10) -> Tuple[bool, int]:
        """
        Check if ETA is late compared to appointment
        Returns: (is_late: bool, minutes_difference: int)
        """
        if not appt_str:
            return (False, 0)

        try:
            # Parse appointment time
            appt = dtp.parse(appt_str)

            # Ensure both times are timezone-aware or both are naive
            if eta_utc.tzinfo is None and appt.tzinfo is not None:
                eta_utc = eta_utc.replace(tzinfo=appt.tzinfo)
            elif eta_utc.tzinfo is not None and appt.tzinfo is None:
                # Assume appointment is in UTC if no timezone specified
                appt = appt.replace(tzinfo=eta_utc.tzinfo)

            # Calculate difference in minutes
            diff = int((eta_utc - appt).total_seconds() / 60)

            # Consider late if beyond grace period
            is_late = diff > grace_min

            return (is_late, diff)

        except Exception as e:
            logger.error(f"Error parsing appointment time '{appt_str}': {e}")
            return (False, 0)

    def format_eta_info(self,
                        eta_info: Dict[str,
                                       Any],
                        appointment: Optional[str] = None,
                        grace_minutes: int = 10) -> Dict[str,
                                                         str]:
        """
        Format ETA information for display
        Returns: dict with formatted strings
        """
        if not eta_info:
            return {}

        try:
            # Format basic info
            miles = eta_info.get("miles", 0)
            seconds = eta_info.get("seconds", 0)
            eta_utc = eta_info.get("eta_utc")

            # Format duration
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60

            if hours > 0:
                duration_str = f"{hours}h {minutes}m"
            else:
                duration_str = f"{minutes}m"

            # Format ETA time (convert to NY timezone)
            if eta_utc:
                # Convert UTC to NY timezone
                ny_tz = pytz.timezone('America/New_York')
                eta_ny = eta_utc.replace(tzinfo=pytz.utc).astimezone(ny_tz)
                eta_str = eta_ny.strftime("%I:%M %p ET")
            else:
                eta_str = "Unknown"

            result = {
                "distance": f"{miles} miles",
                "duration": duration_str,
                "eta": eta_str,
                "status": "On Time",
                "status_emoji": "✅"
            }

            # Check against appointment if provided
            if appointment and eta_utc:
                is_late, diff_min = self.is_late(
                    eta_utc, appointment, grace_minutes)

                if is_late:
                    result["status"] = f"Running Late ({diff_min} min)"
                    result["status_emoji"] = "⚠️"
                elif diff_min > 0:
                    result["status"] = f"Cutting Close ({diff_min} min ahead)"
                    result["status_emoji"] = "🟡"
                else:
                    early_min = abs(diff_min)
                    result["status"] = f"Early ({early_min} min ahead)"
                    result["status_emoji"] = "✅"

            return result

        except Exception as e:
            logger.error(f"Error formatting ETA info: {e}")
            return {"error": str(e)}


def test_eta_service(ors_api_key: str) -> bool:
    """Test ETA service functionality"""
    try:
        print("🧪 Testing ETA Service...")

        eta_service = ETAService(ors_api_key)

        # Test geocoding
        test_address = "Salt Lake City, UT"
        coords = eta_service.geocode(test_address)

        if coords:
            print(f"✅ Geocoding test: {test_address} -> {coords}")

            # Test ETA calculation from a known location
            # Using approximate coordinates for Dallas, TX as source
            dallas_lat, dallas_lon = 32.7767, -96.7970

            eta_info = eta_service.eta_from_now(
                dallas_lat, dallas_lon, test_address)

            if eta_info:
                print(f"✅ ETA calculation: Dallas to {test_address}")
                print(f"   Distance: {eta_info['miles']} miles")
                print(
                    f"   Duration: {eta_info['seconds']//3600}h {(eta_info['seconds']%3600)//60}m")
                print(
                    f"   ETA: {eta_info['eta_utc'].strftime('%I:%M %p UTC')}")

                # Test appointment comparison
                test_appointment = (
                    eta_info['eta_utc'] +
                    timedelta(
                        hours=1)).strftime("%I:%M %p")
                is_late, diff = eta_service.is_late(
                    eta_info['eta_utc'], test_appointment, 10)
                print(
                    f"✅ Appointment comparison: Late={is_late}, Diff={diff} min")

                # Test formatting
                formatted = eta_service.format_eta_info(
                    eta_info, test_appointment)
                print(
                    f"✅ Formatted output: {formatted.get('status', 'Unknown')}")

                return True
            else:
                print("❌ ETA calculation failed")
                return False
        else:
            print(f"❌ Geocoding failed for {test_address}")
            return False

    except Exception as e:
        print(f"❌ ETA service test failed: {e}")
        return False
