"""
Data contracts and type system for the Asset Tracking Bot.
Provides tz-aware UTC timestamps and consistent data structures.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Iterable
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class FleetPoint:
    """Core data contract for fleet location points with tz-aware UTC timestamps"""
    vin: str
    driver_name: Optional[str]
    location_str: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    status: Optional[str]               # "Idle" | "Moving" | ...
    updated_at_utc: Optional[datetime]  # tz-aware UTC
    source: str                         # "samsara" | "TMS Auto-Update" | ...

    def __post_init__(self):
        # Ensure VIN is normalized
        if self.vin:
            object.__setattr__(self, 'vin', self.vin.strip().upper())

        # Ensure timestamp is tz-aware UTC
        if self.updated_at_utc and self.updated_at_utc.tzinfo is None:
            # Naive datetime - assume UTC
            object.__setattr__(
                self,
                'updated_at_utc',
                self.updated_at_utc.replace(
                    tzinfo=ZoneInfo('UTC')))

    def to_ny_time(self) -> Optional[datetime]:
        """Convert UTC timestamp to America/New_York timezone"""
        if not self.updated_at_utc:
            return None
        return self.updated_at_utc.astimezone(ZoneInfo('America/New_York'))

    def speed_mph(self) -> int:
        """Extract integer speed in mph from status or return 0"""
        if not self.status:
            return 0

        # Try to extract speed from various status formats
        import re
        speed_match = re.search(r'(\d+)\s*mph', self.status.lower())
        if speed_match:
            return int(speed_match.group(1))

        # Default to 0 for stationary states
        if any(word in self.status.lower()
               for word in ['idle', 'stopped', 'parked']):
            return 0

        return 0
