#!/usr/bin/env python3
"""
Google Maps Geocoding Service
Fallback geocoding service for when ORS quota is exceeded
"""

import logging
import asyncio
import aiohttp
from typing import Optional
import json

logger = logging.getLogger(__name__)


class GoogleMapsGeocoder:
    """Google Maps reverse geocoding service as fallback to ORS"""

    def __init__(self, config):
        self.config = config
        self.api_key = getattr(config, 'GOOGLE_MAPS_API_KEY', None)
        self.base_url = "https://maps.googleapis.com/maps/api/geocode/json"
        self.timeout = getattr(config, 'GOOGLE_MAPS_TIMEOUT_SECS', 5)

        if not self.api_key:
            logger.warning(
                "Google Maps API key not configured - service will be disabled")
        else:
            logger.info("Google Maps geocoder initialized successfully")

    async def reverse_geocode(self, lat: float, lon: float) -> Optional[str]:
        """
        Reverse geocode coordinates to readable address using Google Maps API
        """
        if not self.api_key:
            logger.debug("Google Maps API key not configured")
            return None

        try:
            params = {
                'latlng': f"{lat},{lon}",
                'key': self.api_key,
                'result_type': 'street_address|route|locality',  # Prioritize detailed addresses
                'language': 'en'
            }

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.get(self.base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        if data.get('status') == 'OK' and data.get('results'):
                            # Get the most detailed address available
                            result = data['results'][0]
                            address = result.get('formatted_address', '')

                            if address:
                                # Clean up the address (remove country if it's
                                # US)
                                if address.endswith(', USA'):
                                    address = address[:-5]

                                logger.debug(
                                    f"Google Maps geocoded {lat}, {lon} -> {address}")
                                return address
                        else:
                            logger.debug(
                                f"No Google Maps results for {lat}, {lon}: {data.get('status')}")
                            return None

                    elif response.status == 429:
                        logger.warning(f"Google Maps rate limited (429)")
                        return None

                    elif response.status == 403:
                        logger.error(
                            f"Google Maps quota exceeded or API key invalid (403)")
                        return None

                    else:
                        logger.warning(
                            f"Google Maps API returned status {response.status}")
                        return None

        except asyncio.TimeoutError:
            logger.warning(f"Google Maps API timeout for {lat}, {lon}")
            return None

        except Exception as e:
            logger.error(f"Google Maps API error for {lat}, {lon}: {e}")
            return None

    def is_configured(self) -> bool:
        """Check if Google Maps API is properly configured"""
        return bool(self.api_key)

# Test function


async def test_google_maps_geocoder(api_key: str):
    """Test Google Maps geocoding functionality"""
    print("🧪 Testing Google Maps Geocoder...")

    class MockConfig:
        GOOGLE_MAPS_API_KEY = api_key
        GOOGLE_MAPS_TIMEOUT_SECS = 5

    geocoder = GoogleMapsGeocoder(MockConfig())

    # Test coordinates (New York City)
    test_lat, test_lon = 40.7128, -74.0060

    try:
        address = await geocoder.reverse_geocode(test_lat, test_lon)
        if address:
            print(f"✅ Google Maps geocoding successful: {address}")
            return True
        else:
            print("❌ Google Maps geocoding returned no results")
            return False
    except Exception as e:
        print(f"❌ Google Maps geocoding failed: {e}")
        return False

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        api_key = sys.argv[1]
        asyncio.run(test_google_maps_geocoder(api_key))
    else:
        print("Usage: python google_maps_geocoder.py YOUR_GOOGLE_MAPS_API_KEY")
