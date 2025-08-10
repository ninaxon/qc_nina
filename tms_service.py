"""
TMS Service - Typed client with tz-aware UTC timestamps and rate limiting.
Provides FleetPoint data contract compliance.
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import List, Optional, Dict, Any
from zoneinfo import ZoneInfo

import aiohttp
from data_contracts import FleetPoint
from config import Config


logger = logging.getLogger(__name__)


class TMSService:
    """Typed TMS client with rate limiting and tz-aware timestamps"""
    
    def __init__(self, config: Config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self._rate_limiter = asyncio.Semaphore(config.TMS_MAX_REQUESTS_PER_MINUTE)
        self._last_request_time = 0.0
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _rate_limited_request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """Make rate-limited HTTP request with exponential backoff"""
        async with self._rate_limiter:
            # Enforce minimum delay between requests
            now = time.time()
            elapsed = now - self._last_request_time
            if elapsed < self.config.TMS_REQUEST_DELAY:
                await asyncio.sleep(self.config.TMS_REQUEST_DELAY - elapsed)
            
            retries = 0
            while retries < self.config.MAX_RETRY_ATTEMPTS:
                try:
                    async with self.session.request(method, url, **kwargs) as response:
                        self._last_request_time = time.time()
                        
                        if response.status == 429:
                            # Rate limited - exponential backoff
                            delay = (2 ** retries) * self.config.RETRY_DELAY_SECONDS
                            logger.warning(f"Rate limited by TMS, backing off {delay}s")
                            await asyncio.sleep(delay)
                            retries += 1
                            continue
                            
                        response.raise_for_status()
                        return await response.json()
                        
                except aiohttp.ClientError as e:
                    retries += 1
                    if retries >= self.config.MAX_RETRY_ATTEMPTS:
                        logger.error(f"TMS request failed after {retries} attempts: {e}")
                        raise
                    
                    # Exponential backoff for client errors
                    delay = (2 ** retries) * self.config.RETRY_DELAY_SECONDS
                    logger.warning(f"TMS request failed, retrying in {delay}s: {e}")
                    await asyncio.sleep(delay)
            
            raise Exception(f"TMS request failed after {self.config.MAX_RETRY_ATTEMPTS} attempts")
    
    async def fetch_fleet_locations(self) -> List[FleetPoint]:
        """Fetch all fleet locations and return as FleetPoint objects"""
        if not self.session:
            raise RuntimeError("TMSService not initialized - use async context manager")
        
        try:
            # Use the same API structure as the working TMS integration
            params = {
                "api_key": self.config.TMS_API_KEY,
                "api_hash": self.config.TMS_API_HASH
            }
            
            response_data = await self._rate_limited_request(
                'GET', 
                self.config.TMS_API_URL,
                params=params
            )
            
            fleet_points = []
            for truck_data in response_data.get('locations', []):
                fleet_point = self._convert_to_fleet_point(truck_data)
                if fleet_point:
                    fleet_points.append(fleet_point)
            
            logger.info(f"Fetched {len(fleet_points)} fleet locations from TMS")
            return fleet_points
            
        except Exception as e:
            logger.error(f"Error fetching fleet locations: {e}")
            return []
    
    def _convert_to_fleet_point(self, truck_data: Dict[str, Any]) -> Optional[FleetPoint]:
        """Convert TMS truck data to FleetPoint with proper timezone handling"""
        try:
            # Use the same field mapping as working TMS integration
            vin = str(truck_data.get('vin', '')).strip().upper()
            if not vin:
                return None
            
            # Filter by source (like working integration)
            source = truck_data.get("source", "")
            if source.lower() != "samsara":
                return None  # Skip non-samsara data
            
            # Parse timestamp and ensure UTC with staleness detection
            updated_at_utc = None
            if truck_data.get('update_time'):
                try:
                    # Parse timestamp (TMS uses MM-dd-yyyy HH:mm:ss format)
                    timestamp_str = truck_data['update_time']
                    if isinstance(timestamp_str, str):
                        # Handle TMS format: '08-09-2025 04:29:40 EST'
                        try:
                            # First try direct datetime parsing
                            if 'EST' in timestamp_str or 'EDT' in timestamp_str:
                                # Strip timezone name and parse as NY time
                                dt_str = timestamp_str.replace(' EST', '').replace(' EDT', '')
                                parsed_time = datetime.strptime(dt_str, '%m-%d-%Y %H:%M:%S')
                                # Convert from NY time to UTC
                                parsed_time = parsed_time.replace(tzinfo=ZoneInfo('America/New_York'))
                                parsed_time = parsed_time.astimezone(ZoneInfo('UTC'))
                            else:
                                # Fallback to ISO format
                                parsed_time = datetime.fromisoformat(
                                    timestamp_str.replace('Z', '+00:00')
                                )
                        except ValueError:
                            # Try ISO format as fallback
                            parsed_time = datetime.fromisoformat(
                                timestamp_str.replace('Z', '+00:00')
                            )
                        # Ensure it's UTC
                        if parsed_time.tzinfo is None:
                            parsed_time = parsed_time.replace(tzinfo=ZoneInfo('UTC'))
                        else:
                            parsed_time = parsed_time.astimezone(ZoneInfo('UTC'))
                        
                        # Check if data is stale (older than max allowed age)
                        max_age_hours = getattr(self.config, 'MAX_LOCATION_AGE_HOURS', 12)
                        now_utc = datetime.now(ZoneInfo('UTC'))
                        age_hours = (now_utc - parsed_time).total_seconds() / 3600
                        
                        if age_hours > max_age_hours:
                            logger.info(f"Correcting stale data for VIN {vin}: {age_hours:.1f}h old (max: {max_age_hours}h), using current time")
                            updated_at_utc = now_utc  # Use current time for stale data
                        else:
                            updated_at_utc = parsed_time
                            logger.debug(f"Fresh data for VIN {vin}: {age_hours:.1f}h old")
                            
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid timestamp for VIN {vin}: {e}")
                    updated_at_utc = datetime.now(ZoneInfo('UTC'))  # Fallback to current time
            else:
                # No timestamp from TMS, use current time
                updated_at_utc = datetime.now(ZoneInfo('UTC'))
            
            # Normalize speed to mph (handle string input)
            speed_raw = truck_data.get('speed', 0)
            try:
                speed = float(speed_raw) if speed_raw else 0.0
            except (ValueError, TypeError):
                speed = 0.0
            
            status = self._normalize_status(truck_data.get('status', ''), speed)
            
            return FleetPoint(
                vin=vin,
                driver_name=str(truck_data.get('driver_name', '')).strip() or None,
                location_str=str(truck_data.get('address', '')).strip() or None,
                lat=truck_data.get('lat'),  # Use 'lat' not 'latitude'
                lon=truck_data.get('lng'),  # Use 'lng' not 'longitude'
                status=status,
                updated_at_utc=updated_at_utc,
                source="samsara"  # Use source from TMS data
            )
            
        except Exception as e:
            logger.error(f"Error converting truck data to FleetPoint: {e}")
            return None
    
    def _normalize_status(self, raw_status: str, speed: float) -> str:
        """Normalize status with speed info"""
        speed_mph = round(speed) if speed else 0
        
        if speed_mph == 0:
            return "Idle"
        elif speed_mph < 5:
            return f"Moving Slowly ({speed_mph} mph)"
        else:
            return f"Moving ({speed_mph} mph)"