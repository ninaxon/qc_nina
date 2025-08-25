"""
Reverse Geocoding Service with ORS integration and caching
Performance-optimized with background refresh and fallbacks
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple, List
from zoneinfo import ZoneInfo

import aiohttp
from config import Config
from location_renderer import update_reverse_geocode_cache, _format_coordinates


logger = logging.getLogger(__name__)


class ReverseGeocodeService:
    """ORS-powered reverse geocoding with caching and fallbacks"""
    
    def __init__(self, config: Config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.timeout = getattr(config, 'REVERSE_GEOCODE_TIMEOUT_SECS', 5)
        self.ors_api_key = config.ORS_API_KEY
        
        # Rate limiting
        self._last_request_time = 0.0
        self._request_count = 0
        self._minute_start = time.time()
        
        # Background processing queue
        self._geocode_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._background_task: Optional[asyncio.Task] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        # Start background geocoding task
        self._background_task = asyncio.create_task(self._background_geocoder())
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Stop background task
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
        
        if self.session:
            await self.session.close()
    
    async def _rate_limit(self) -> None:
        """Enforce ORS rate limits"""
        now = time.time()
        
        # Reset counter every minute
        if now - self._minute_start > 60:
            self._request_count = 0
            self._minute_start = now
        
        # Check if we're over the limit
        if self._request_count >= self.config.ORS_MAX_REQUESTS_PER_MINUTE:
            sleep_time = 60 - (now - self._minute_start)
            if sleep_time > 0:
                logger.warning(f"ORS rate limit reached, sleeping {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)
                self._request_count = 0
                self._minute_start = time.time()
        
        # Enforce minimum delay between requests
        elapsed = now - self._last_request_time
        if elapsed < self.config.ORS_REQUEST_DELAY:
            await asyncio.sleep(self.config.ORS_REQUEST_DELAY - elapsed)
        
        self._last_request_time = time.time()
        self._request_count += 1
    
    async def reverse_geocode(self, lat: float, lon: float) -> Optional[str]:
        """
        Reverse geocode coordinates to readable address using ORS
        Returns None on failure - caller should use fallback
        """
        if not self.session or not self.ors_api_key:
            logger.warning("ORS not configured for reverse geocoding")
            return None
        
        try:
            await self._rate_limit()
            
            # ORS reverse geocoding endpoint
            url = "https://api.openrouteservice.org/geocode/reverse"
            
            params = {
                'api_key': self.ors_api_key,
                'point.lon': lon,
                'point.lat': lat,
                'size': 1,  # Only need one result
                'layers': 'address,street,locality',  # Prefer detailed addresses
                'sources': 'openaddresses,osm'  # Use multiple sources
            }
            
            async with self.session.get(
                url, 
                params=params,
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            ) as response:
                
                if response.status == 403:
                    logger.error("ORS returned 403 (DAILY QUOTA EXCEEDED) - disabling ORS for 24 hours")
                    # Set a flag to disable ORS requests for the rest of the day
                    self._daily_quota_exceeded = True
                    self._quota_exceeded_time = time.time()
                    return None
                
                if response.status == 429:
                    logger.warning("ORS returned 429 (rate limited) - implementing aggressive backoff")
                    # Much more aggressive backoff for 429 errors
                    backoff_delay = min(10.0 * (2.0 ** (self._request_count % 4)), 120.0)  # 10s, 20s, 40s, 80s, max 120s
                    logger.warning(f"Backing off for {backoff_delay:.1f}s due to ORS rate limit")
                    await asyncio.sleep(backoff_delay)
                    return None
                
                if response.status != 200:
                    logger.warning(f"ORS returned status {response.status}")
                    return None
                
                data = await response.json()
                features = data.get('features', [])
                
                if not features:
                    logger.debug(f"No reverse geocoding results for {lat}, {lon}")
                    return None
                
                # Extract address from first result
                properties = features[0].get('properties', {})
                
                # Try different address formats in priority order
                address = self._extract_best_address(properties)
                
                if address:
                    logger.debug(f"Reverse geocoded {lat}, {lon} -> {address}")
                    return address
                
                return None
                
        except asyncio.TimeoutError:
            logger.warning(f"ORS reverse geocode timeout for {lat}, {lon}")
            return None
        except Exception as e:
            logger.error(f"ORS reverse geocode error for {lat}, {lon}: {e}")
            return None
    
    def _extract_best_address(self, properties: Dict) -> Optional[str]:
        """Extract the best address string from ORS response properties"""
        # Try full label first (most complete)
        if properties.get('label'):
            return properties['label'].strip()
        
        # Build address from components
        address_parts = []
        
        # Street number and name
        house_number = properties.get('housenumber', '')
        street = properties.get('street', '')
        if house_number and street:
            address_parts.append(f"{house_number} {street}")
        elif street:
            address_parts.append(street)
        
        # City/locality
        locality = (properties.get('locality') or 
                   properties.get('city') or 
                   properties.get('town') or
                   properties.get('village'))
        if locality:
            address_parts.append(locality)
        
        # State/region
        region = (properties.get('region') or 
                 properties.get('state') or
                 properties.get('macroregion'))
        if region:
            address_parts.append(region)
        
        # Postal code
        postalcode = properties.get('postalcode')
        if postalcode:
            address_parts.append(postalcode)
        
        if address_parts:
            return ', '.join(address_parts)
        
        # Fallback to any available name
        name = (properties.get('name') or 
               properties.get('region') or
               properties.get('country'))
        
        return name.strip() if name else None
    
    def enqueue_background_geocode(self, lat: float, lon: float) -> None:
        """Enqueue coordinates for background reverse geocoding"""
        try:
            # Format coordinates to avoid duplicate requests
            lat_str, lon_str = _format_coordinates(lat, lon, 5)
            coordinate_item = (lat_str, lon_str, lat, lon)
            
            # Try to add to queue (non-blocking)
            self._geocode_queue.put_nowait(coordinate_item)
            logger.debug(f"Enqueued background geocoding for {lat_str}, {lon_str}")
            
        except asyncio.QueueFull:
            logger.warning("Background geocoding queue is full, dropping request")
    
    async def _background_geocoder(self) -> None:
        """Background task that processes the geocoding queue"""
        logger.info("Started background reverse geocoder")
        
        while True:
            try:
                # Wait for items in the queue
                lat_str, lon_str, lat, lon = await self._geocode_queue.get()
                
                # Perform reverse geocoding
                address = await self.reverse_geocode(lat, lon)
                
                if address:
                    # Update the cache
                    update_reverse_geocode_cache(lat, lon, address, decimals=5)
                    logger.info(f"Background geocoded {lat_str}, {lon_str} -> {address}")
                else:
                    logger.debug(f"Background geocoding failed for {lat_str}, {lon_str}")
                
                # Mark task as done
                self._geocode_queue.task_done()
                
                # Small delay between background requests
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info("Background reverse geocoder cancelled")
                break
            except Exception as e:
                logger.error(f"Error in background reverse geocoder: {e}")
                # Continue processing despite errors
                await asyncio.sleep(5)
    
    async def warm_cache_for_fleet(self, fleet_points) -> int:
        """Warm the cache for a list of fleet points during 5-min refresh"""
        warmed_count = 0
        
        for point in fleet_points:
            if point.lat and point.lon:
                # Check if we already have this cached
                lat_str, lon_str = _format_coordinates(point.lat, point.lon, 5)
                from location_renderer import _get_cached_address
                
                cache_key = f"{lat_str},{lon_str}"
                cache_ttl = getattr(self.config, 'LOCATION_ADDR_CACHE_TTL_SECS', 86400)
                
                if not _get_cached_address(cache_key, cache_ttl):
                    # Not cached - do reverse geocoding now (during silent refresh)
                    address = await self.reverse_geocode(point.lat, point.lon)
                    if address:
                        update_reverse_geocode_cache(point.lat, point.lon, address, decimals=5)
                        warmed_count += 1
                        logger.debug(f"Warmed cache: {cache_key} -> {address}")
                    
                    # Rate limiting - small delay between requests
                    await asyncio.sleep(0.5)
        
        if warmed_count > 0:
            logger.info(f"Warmed reverse geocode cache for {warmed_count} locations")
        
        return warmed_count
    
    def get_stats(self) -> Dict[str, int]:
        """Get service statistics"""
        return {
            'queue_size': self._geocode_queue.qsize(),
            'requests_this_minute': self._request_count,
            'background_running': self._background_task is not None and not self._background_task.done()
        }