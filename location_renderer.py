"""
Location Update Renderer - Clean separation of addresses and coordinates
Implements exact format specification with reverse geocoding cache
"""
import re
import html
import logging
from datetime import datetime
from typing import Optional, Dict, Tuple
from zoneinfo import ZoneInfo

from config import Config


logger = logging.getLogger(__name__)


# Global cache for address lookups - in production use Redis
_address_cache: Dict[str, Tuple[str, datetime]] = {}


def is_latlon_like(s: str) -> bool:
    """
    Detect if a string is actually coordinates masquerading as an address
    Rejects patterns like: "40.72734708, -111.94709302" or "40.7273,-111.9471"
    """
    if not s or not isinstance(s, str):
        return False
    
    s_clean = s.strip()
    
    # Pattern 1: Two decimal numbers separated by comma/space
    # Examples: "40.72734708, -111.94709302", "40.7273,-111.9471"
    coord_pattern = r'^-?\d+\.\d+\s*,\s*-?\d+\.\d+$'
    if re.match(coord_pattern, s_clean):
        return True
    
    # Pattern 2: Just decimal numbers with minimal text
    # Examples: "40.72734708", "-111.94709302" 
    single_coord = r'^-?\d{2,3}\.\d{4,}$'
    if re.match(single_coord, s_clean):
        return True
    
    # Pattern 3: Coordinates with parentheses or brackets
    # Examples: "(40.7273, -111.9471)", "[40.7273,-111.9471]"
    bracket_coords = r'^[\(\[]?\s*-?\d+\.\d+\s*,\s*-?\d+\.\d+\s*[\)\]]?$'
    if re.match(bracket_coords, s_clean):
        return True
    
    # Pattern 4: Multiple coordinate pairs (common TMS error)
    # Examples: "40.7273, -111.9471, 40.72734708, -111.94709302"
    if s_clean.count(',') >= 3:  # More than one coordinate pair
        # Check if it's mostly numbers, commas, spaces, and decimals
        clean_chars = re.sub(r'[-\d\.\s,]', '', s_clean)
        if len(clean_chars) == 0:  # Only coordinate-like characters
            return True
    
    return False


def _clamp_coordinates(lat: float, lon: float) -> Tuple[float, float]:
    """Clamp coordinates to valid ranges"""
    lat_clamped = max(-90.0, min(90.0, lat))
    lon_clamped = max(-180.0, min(180.0, lon))
    return lat_clamped, lon_clamped


def _format_coordinates(lat: float, lon: float, decimals: int = 5) -> Tuple[str, str]:
    """Format coordinates to specified decimal places"""
    lat_clamped, lon_clamped = _clamp_coordinates(lat, lon)
    format_str = f"{{:.{decimals}f}}"
    return format_str.format(lat_clamped), format_str.format(lon_clamped)


def _get_cached_address(cache_key: str, ttl_seconds: int) -> Optional[str]:
    """Get cached address if not expired"""
    if cache_key not in _address_cache:
        return None
    
    address, cached_at = _address_cache[cache_key]
    age = (datetime.utcnow() - cached_at).total_seconds()
    
    if age > ttl_seconds:
        # Expired - remove from cache
        del _address_cache[cache_key]
        return None
    
    return address


def _cache_address(cache_key: str, address: str) -> None:
    """Cache address with current timestamp"""
    _address_cache[cache_key] = (address, datetime.utcnow())


def _get_fallback_location(lat: float, lon: float) -> str:
    """Generate fallback location string when reverse geocoding fails"""
    # Simple geographic fallback based on coordinates
    lat_abs, lon_abs = abs(lat), abs(lon)
    
    # US coordinate ranges (very rough)
    if 24 <= lat_abs <= 50 and 65 <= lon_abs <= 125:
        if lat > 40:
            region = "Northern US"
        elif lat > 35:
            region = "Central US" 
        else:
            region = "Southern US"
    elif lat_abs < 24 and lon_abs < 180:
        region = "Tropical region"
    elif lat_abs > 50:
        region = "Northern region"
    else:
        region = "Remote area"
    
    return f"Near {region}"


def _render_timezone_aware_time(utc_dt: Optional[datetime]) -> str:
    """Render UTC datetime in America/New_York with correct EDT/EST"""
    if not utc_dt:
        return "Unknown"
    
    # Ensure UTC timezone
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=ZoneInfo('UTC'))
    elif utc_dt.tzinfo != ZoneInfo('UTC'):
        utc_dt = utc_dt.astimezone(ZoneInfo('UTC'))
    
    # Convert to NY timezone
    ny_tz = ZoneInfo('America/New_York')
    ny_time = utc_dt.astimezone(ny_tz)
    
    # Get timezone name (EDT or EST based on DST)
    tz_name = ny_time.strftime('%Z')
    
    # Format with proper timezone label
    return ny_time.strftime(f'%Y-%m-%d %H:%M:%S {tz_name}')


def render_location_update(
    driver: str,
    status: str,
    lat: float,
    lon: float,
    speed_mph: float,
    updated_at_utc: Optional[datetime],
    location_str: Optional[str],
    map_source: str  # e.g., "samsara" | "TMS Auto-Update"
) -> str:
    """
    Render location update message with clean address/coordinate separation
    
    Returns HTML-formatted message following exact specification:
    - Readable address in 📍 Location line
    - Coordinates shown separately in 🗺️ Coordinates line
    - No coordinate duplicates anywhere
    """
    
    # Import config here to avoid circular imports
    try:
        from config import Config
        config = Config()
        coord_decimals = getattr(config, 'RENDER_COORD_DECIMALS', 5)
        cache_ttl = getattr(config, 'LOCATION_ADDR_CACHE_TTL_SECS', 86400)
    except:
        coord_decimals = 5
        cache_ttl = 86400
    
    # Sanitize inputs
    driver = (driver or "Unknown Driver").strip()
    status = (status or "Unknown").strip()
    
    # Clamp coordinates
    lat, lon = _clamp_coordinates(lat, lon)
    
    # Convert speed to integer mph
    speed_mph_int = max(0, int(round(speed_mph))) if speed_mph else 0
    
    # Determine readable location using priority order
    readable_location = "Unknown"
    
    # P1: Use location_str if it exists and is not lat/lon-like
    if location_str and location_str.strip():
        location_clean = location_str.strip()
        if not is_latlon_like(location_clean):
            # Good location string - use it
            readable_location = location_clean
        else:
            logger.debug(f"Rejecting lat/lon-like location_str: {location_clean}")
            
            # P2: Try cached reverse geocoding result
            lat_str, lon_str = _format_coordinates(lat, lon, coord_decimals)
            cache_key = f"{lat_str},{lon_str}"
            
            cached_addr = _get_cached_address(cache_key, cache_ttl)
            if cached_addr:
                readable_location = cached_addr
            else:
                # P3: Use fallback (reverse geocoding would happen in background)
                readable_location = _get_fallback_location(lat, lon)
                logger.info(f"Using fallback location for {cache_key}: {readable_location}")
    else:
        # No location_str provided - try cache then fallback
        lat_str, lon_str = _format_coordinates(lat, lon, coord_decimals)
        cache_key = f"{lat_str},{lon_str}"
        
        cached_addr = _get_cached_address(cache_key, cache_ttl)
        if cached_addr:
            readable_location = cached_addr
        else:
            readable_location = _get_fallback_location(lat, lon)
    
    # Truncate location to reasonable length for UI
    if len(readable_location) > 80:
        readable_location = readable_location[:77] + "..."
    
    # Format coordinates (always shown when lat/lon exist)
    lat_str, lon_str = _format_coordinates(lat, lon, coord_decimals)
    
    # Format timestamp in America/New_York timezone
    time_str = _render_timezone_aware_time(updated_at_utc)
    
    # HTML escape all user/sheet data to prevent XSS
    driver_safe = html.escape(driver)
    status_safe = html.escape(status)
    location_safe = html.escape(readable_location)
    
    # Build message following exact specification
    message = f"""🚛 <b>Location Update</b>

👤 <b>Driver:</b> {driver_safe}
🛑 <b>Status:</b> {status_safe}
📍 <b>Location:</b> {location_safe}
🏃 <b>Speed:</b> {speed_mph_int} mph
📡 <b>Updated:</b> {time_str}

🗺️ <b>Coordinates:</b> {lat_str}, {lon_str}
🔗 <b>Map:</b> https://maps.google.com/?q={lat_str},{lon_str}"""
    
    # Ensure message is under Telegram's 4096 character limit
    if len(message) > 4096:
        # Truncate location further if needed
        excess = len(message) - 4090  # Leave some buffer
        if len(location_safe) > excess + 20:
            location_truncated = location_safe[:-(excess + 3)] + "..."
            message = message.replace(location_safe, location_truncated)
    
    return message


def update_reverse_geocode_cache(lat: float, lon: float, address: str, decimals: int = 5) -> None:
    """Update reverse geocode cache - called during background refresh"""
    lat_str, lon_str = _format_coordinates(lat, lon, decimals)
    cache_key = f"{lat_str},{lon_str}"
    _cache_address(cache_key, address)
    logger.debug(f"Updated address cache: {cache_key} -> {address}")


def get_cache_stats() -> Dict[str, int]:
    """Get cache statistics for monitoring"""
    return {
        'cached_addresses': len(_address_cache),
        'cache_size_bytes': sum(len(k) + len(v[0]) for k, v in _address_cache.items())
    }