#!/usr/bin/env python3
"""
Simplified Cargo Theft Risk Detection Module
No external dependencies version - uses built-in math only
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import pytz

logger = logging.getLogger(__name__)


class RiskLevel(Enum):
    """Risk level enumeration"""
    LOW = "LOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class RiskZone:
    """Risk zone definition using simple coordinate bounds"""
    name: str
    risk_level: RiskLevel
    min_lat: float
    max_lat: float
    min_lng: float
    max_lng: float
    description: str
    alerts_enabled: bool = True


@dataclass
class RiskAlert:
    """Risk alert data structure"""
    driver_name: str
    vin: str
    location: Tuple[float, float]  # (lat, lng)
    address: str
    risk_level: RiskLevel
    zone_name: str
    timestamp: datetime
    duration_stopped: timedelta
    speed: float
    google_maps_link: str
    alert_id: str
    driver_phone: str = "Not available"
    load_details: dict = None  # QC Panel load information


class CargoTheftRiskDetector:
    """
    Simplified Cargo Theft Risk Detection System
    Uses rectangular zones instead of complex polygons
    """

    def __init__(self, config, tms_integration=None):
        self.config = config
        self.tms_integration = tms_integration
        self.google_integration = None  # Will be set separately

        # Risk zones storage
        self.risk_zones: List[RiskZone] = []

        # Tracking state for drivers
        self.driver_states: Dict[str, Dict[str, Any]] = {}

        # Alert tracking to prevent spam
        self.recent_alerts: Dict[str, datetime] = {}
        self.alert_cooldown = timedelta(
            minutes=getattr(
                config, 'ALERT_COOLDOWN_MINUTES', 30))

        # Risk detection settings
        self.min_stop_duration = timedelta(
            minutes=getattr(
                config, 'MIN_STOP_DURATION_MINUTES', 10))
        self.max_speed_threshold = getattr(
            config, 'MAX_SPEED_THRESHOLD_MPH', 2.0)

        # Home location settings
        self.home_radius_miles = getattr(config, 'HOME_RADIUS_MILES', 2.0)
        self.home_locations_cache = {}

        # Initialize risk zones
        self._initialize_risk_zones()

        logger.info(
            f"Simplified cargo theft risk detector initialized with {len(self.risk_zones)} zones")

    def _initialize_risk_zones(self):
        """Initialize risk zones using rectangular coordinates"""

        # ========================================
        # CRITICAL ZONES (Red/Dark Red on map)
        # ========================================

        # Los Angeles Metro - Massive red zone in Southern California
        self.risk_zones.append(
            RiskZone(
                name="Los Angeles Metro Complex",
                risk_level=RiskLevel.CRITICAL,
                min_lat=33.4,
                max_lat=34.4,
                min_lng=-118.8,
                max_lng=-117.1,
                description="Massive Southern California cargo theft epicenter - highest risk nationwide"))

        # Dallas-Fort Worth - Large red zone in North Texas
        self.risk_zones.append(RiskZone(
            name="Dallas-Fort Worth Metroplex",
            risk_level=RiskLevel.CRITICAL,
            min_lat=32.3, max_lat=33.3,
            min_lng=-97.5, max_lng=-96.3,
            description="North Texas major cargo theft corridor"
        ))

        # Memphis, TN - Prominent red zone in Tennessee
        self.risk_zones.append(
            RiskZone(
                name="Memphis Cargo Hub",
                risk_level=RiskLevel.CRITICAL,
                min_lat=34.8,
                max_lat=35.4,
                min_lng=-90.4,
                max_lng=-89.6,
                description="Tennessee cargo theft epicenter - major distribution hub"))

        # Miami-South Florida - Red zone in Southeast Florida
        self.risk_zones.append(RiskZone(
            name="South Florida Corridor",
            risk_level=RiskLevel.CRITICAL,
            min_lat=25.2, max_lat=26.2,
            min_lng=-80.8, max_lng=-80.0,
            description="Miami-Dade cargo theft hotspot"
        ))

        # New York Metro - Red zone covering NYC/NJ area
        self.risk_zones.append(RiskZone(
            name="New York Metro Area",
            risk_level=RiskLevel.CRITICAL,
            min_lat=40.2, max_lat=41.2,
            min_lng=-74.8, max_lng=-73.4,
            description="NYC-New Jersey cargo theft corridor"
        ))

        # Chicago Metro - Red zone in Northern Illinois
        self.risk_zones.append(RiskZone(
            name="Chicago Metro",
            risk_level=RiskLevel.CRITICAL,
            min_lat=41.4, max_lat=42.2,
            min_lng=-88.2, max_lng=-87.2,
            description="Illinois cargo theft hub - Great Lakes corridor"
        ))

        # ========================================
        # HIGH RISK ZONES (Orange/Yellow on map)
        # ========================================

        # Atlanta Metro - Orange zone in Georgia
        self.risk_zones.append(RiskZone(
            name="Atlanta Metro",
            risk_level=RiskLevel.HIGH,
            min_lat=33.2, max_lat=34.1,
            min_lng=-85.0, max_lng=-83.8,
            description="Georgia transportation and logistics hub"
        ))

        # Houston Metro - Orange zone in Southeast Texas
        self.risk_zones.append(RiskZone(
            name="Houston Metro",
            risk_level=RiskLevel.HIGH,
            min_lat=29.3, max_lat=30.2,
            min_lng=-95.9, max_lng=-94.8,
            description="Texas Gulf Coast port and logistics corridor"
        ))

        # Phoenix Metro - Orange zone in Central Arizona
        self.risk_zones.append(RiskZone(
            name="Phoenix Metro",
            risk_level=RiskLevel.HIGH,
            min_lat=33.1, max_lat=33.9,
            min_lng=-112.8, max_lng=-111.4,
            description="Arizona distribution and logistics hub"
        ))

        # San Antonio, TX - Yellow/Orange zone in South Central Texas
        self.risk_zones.append(RiskZone(
            name="San Antonio Corridor",
            risk_level=RiskLevel.HIGH,
            min_lat=29.2, max_lat=29.8,
            min_lng=-98.8, max_lng=-98.2,
            description="South Central Texas logistics corridor"
        ))

        # ========================================
        # MODERATE RISK ZONES (Blue/Light zones)
        # ========================================

        # Central Arkansas - I-40 Corridor
        self.risk_zones.append(RiskZone(
            name="Central Arkansas I-40",
            risk_level=RiskLevel.MODERATE,
            min_lat=34.4, max_lat=35.2,
            min_lng=-92.8, max_lng=-91.2,
            description="Arkansas I-40 transportation corridor"
        ))

        # Louisville, KY - Moderate zone in Kentucky
        self.risk_zones.append(RiskZone(
            name="Louisville Metro",
            risk_level=RiskLevel.MODERATE,
            min_lat=37.9, max_lat=38.5,
            min_lng=-85.9, max_lng=-85.3,
            description="Kentucky logistics and distribution hub"
        ))

        # Nashville, TN - Moderate zone in Middle Tennessee
        self.risk_zones.append(RiskZone(
            name="Nashville Metro",
            risk_level=RiskLevel.MODERATE,
            min_lat=35.8, max_lat=36.4,
            min_lng=-87.2, max_lng=-86.4,
            description="Middle Tennessee transportation hub"
        ))

        # Denver Metro - Moderate zone in Colorado
        self.risk_zones.append(RiskZone(
            name="Denver Metro",
            risk_level=RiskLevel.MODERATE,
            min_lat=39.4, max_lat=40.1,
            min_lng=-105.3, max_lng=-104.6,
            description="Colorado Rocky Mountain corridor hub"
        ))

        # Charlotte, NC - Moderate zone in North Carolina
        self.risk_zones.append(RiskZone(
            name="Charlotte Metro",
            risk_level=RiskLevel.MODERATE,
            min_lat=34.9, max_lat=35.5,
            min_lng=-81.2, max_lng=-80.5,
            description="North Carolina Piedmont transportation hub"
        ))

        logger.info(
            f"Initialized {len(self.risk_zones)} simplified cargo theft risk zones")
        logger.info(
            f"Zone breakdown: {sum(1 for z in self.risk_zones if z.risk_level == RiskLevel.CRITICAL)} Critical, "
            f"{sum(1 for z in self.risk_zones if z.risk_level == RiskLevel.HIGH)} High, "
            f"{sum(1 for z in self.risk_zones if z.risk_level == RiskLevel.MODERATE)} Moderate")

    def check_location_risk(self,
                            lat: float,
                            lng: float) -> Tuple[RiskLevel,
                                                 Optional[RiskZone]]:
        """Check if a location is in a high-risk zone using rectangular bounds"""
        for zone in self.risk_zones:
            if (zone.min_lat <= lat <= zone.max_lat and
                    zone.min_lng <= lng <= zone.max_lng):
                return zone.risk_level, zone

        return RiskLevel.LOW, None

    # Home location logic removed - only tracking on-load drivers in risk zones

    def _get_driver_contact_info(self, vin: str) -> Tuple[str, str]:
        """Get driver contact info from Google Sheets assets worksheet"""
        try:
            if self.google_integration and hasattr(
                    self.google_integration,
                    'get_driver_contact_info_by_vin'):
                # Use the enhanced method that looks up by VIN in assets
                # worksheet
                driver_name, phone = self.google_integration.get_driver_contact_info_by_vin(
                    vin)
                return driver_name or "Unknown Driver", phone or "Not available"
        except Exception as e:
            logger.error(f"Error getting contact info for VIN {vin}: {e}")

        return "Unknown Driver", "Not available"

    def _has_active_load(self, vin: str, driver_name: str = None) -> Tuple[bool, dict]:
        """
        Check if driver has an active load from QC Panel
        QC Panel is keyed by driver names only (no VINs)
        
        Args:
            vin: Vehicle VIN (used for logging only)
            driver_name: Driver name for QC Panel lookup
            
        Returns:
            Tuple of (has_load, load_details)
        """
        try:
            if not self.google_integration or not hasattr(self.google_integration, 'get_active_load_map'):
                logger.debug("Google integration or active load method not available")
                return False, {}
                
            # QC Panel only has driver names, so check by driver name directly
            if driver_name:
                active_loads = self.google_integration.get_active_load_map()
                
                # Check exact match first
                if driver_name in active_loads:
                    logger.debug(f"Found active load for driver {driver_name}: {active_loads[driver_name].get('load_id', 'N/A')}")
                    return True, active_loads[driver_name]
                
                # Try normalized driver name matching
                normalized_driver = driver_name.lower().strip()
                for load_driver, load_details in active_loads.items():
                    if load_driver.lower().strip() == normalized_driver:
                        logger.debug(f"Found active load for driver {driver_name} (normalized match): {load_details.get('load_id', 'N/A')}")
                        return True, load_details
                    
            logger.debug(f"No active load found for driver {driver_name} (VIN: {vin})")
            return False, {}
            
        except Exception as e:
            logger.error(f"Error checking active load for driver {driver_name} (VIN: {vin}): {e}")
            return False, {}

    # _is_near_home method removed - only tracking on-load drivers

    def update_driver_state(
            self, driver_data: Dict[str, Any]) -> Optional[RiskAlert]:
        """
        Update driver state and check for risk alerts with home location filtering

        Args:
            driver_data: Dict with keys: driver_name, vin, lat, lng, speed, address, city, state

        Returns:
            RiskAlert if alert conditions are met, None otherwise
        """
        vin = driver_data.get('vin', '')
        if not vin:
            return None

        lat = driver_data.get('lat')
        lng = driver_data.get('lng')
        speed = self._normalize_speed(driver_data.get('speed', 0))

        if not lat or not lng:
            return None

        # Use timezone-aware datetime consistently
        current_time = datetime.now(pytz.UTC)

        # Only track on-load drivers - no home location filtering needed
        # Check current risk level
        risk_level, risk_zone = self.check_location_risk(lat, lng)

        # Initialize driver state if not exists
        if vin not in self.driver_states:
            self.driver_states[vin] = {
                'last_location': (lat, lng),
                'last_speed': speed,
                'last_update': current_time,
                'stop_start_time': None,
                'current_risk_zone': None,
                'is_stopped': False
            }

        driver_state = self.driver_states[vin]

        # Determine if driver is stopped
        is_currently_stopped = speed < self.max_speed_threshold

        # Update stop tracking
        if is_currently_stopped:
            if not driver_state['is_stopped']:
                # Just stopped
                driver_state['stop_start_time'] = current_time
                driver_state['is_stopped'] = True
                logger.debug(f"Driver {vin} stopped at {current_time}")

            # Check if stopped long enough and in high-risk zone
            if (
                    driver_state['stop_start_time'] and risk_level in [
                        RiskLevel.HIGH,
                        RiskLevel.CRITICAL] and current_time -
                    driver_state['stop_start_time'] >= self.min_stop_duration):

                # NEW: Check if driver has active load before alerting
                driver_name = driver_data.get('driver_name', '')
                has_load, load_details = self._has_active_load(vin, driver_name)
                
                if not has_load:
                    logger.debug(f"Driver {vin} ({driver_name}) stopped in risk zone {risk_zone.name} but has no active load - skipping alert")
                    # Update driver state but don't alert
                    driver_state.update({
                        'last_location': (lat, lng),
                        'last_speed': speed,
                        'last_update': current_time,
                        'current_risk_zone': risk_zone.name
                    })
                    return None

                # Check alert cooldown
                alert_key = f"{vin}_{risk_zone.name}"
                if (alert_key not in self.recent_alerts or current_time -
                        self.recent_alerts[alert_key] >= self.alert_cooldown):

                    # Generate alert with load details
                    alert = self._create_risk_alert(
                        driver_data,
                        risk_level,
                        risk_zone,
                        current_time -
                        driver_state['stop_start_time'],
                        load_details)

                    # Update alert tracking
                    self.recent_alerts[alert_key] = current_time

                    logger.warning(
                        f"CARGO THEFT RISK ALERT: {vin} with active load {load_details.get('load_id', 'N/A')} stopped in {risk_zone.name}")

                    # Update driver state
                    driver_state.update({
                        'last_location': (lat, lng),
                        'last_speed': speed,
                        'last_update': current_time,
                        'current_risk_zone': risk_zone.name
                    })

                    return alert

        else:
            # Driver is moving
            if driver_state['is_stopped']:
                logger.debug(f"Driver {vin} resumed movement")

            driver_state.update({
                'is_stopped': False,
                'stop_start_time': None,
                'current_risk_zone': None
            })

        # Update driver state
        driver_state.update({
            'last_location': (lat, lng),
            'last_speed': speed,
            'last_update': current_time
        })

        return None

    def _create_risk_alert(self,
                           driver_data: Dict[str,
                                             Any],
                           risk_level: RiskLevel,
                           risk_zone: RiskZone,
                           stop_duration: timedelta,
                           load_details: dict = None) -> RiskAlert:
        """Create a risk alert object with contact info"""
        lat = driver_data.get('lat')
        lng = driver_data.get('lng')
        vin = driver_data.get('vin', '')

        # Get driver contact information
        driver_name, driver_phone = self._get_driver_contact_info(vin)
        if driver_name == "Unknown":
            driver_name = driver_data.get('driver_name', 'Unknown')

        return RiskAlert(
            driver_name=driver_name,
            vin=vin,
            location=(lat, lng),
            address=driver_data.get('address', 'Unknown'),
            risk_level=risk_level,
            zone_name=risk_zone.name,
            timestamp=datetime.now(pytz.timezone('America/New_York')),
            duration_stopped=stop_duration,
            speed=self._normalize_speed(driver_data.get('speed', 0)),
            google_maps_link=f"https://maps.google.com/?q={lat},{lng}",
            alert_id=f"{vin}_{int(datetime.now().timestamp())}",
            driver_phone=driver_phone,
            load_details=load_details or {}
        )

    def _normalize_speed(self, speed_value: Any) -> float:
        """Normalize speed value to float"""
        if speed_value is None:
            return 0.0

        try:
            if isinstance(speed_value, (int, float)):
                return float(speed_value)

            if isinstance(speed_value, str):
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
            return 0.0

    def format_risk_alert_message(self, alert: RiskAlert) -> str:
        """Format COMPACT risk alert for Telegram message"""
        # Risk level emoji mapping
        risk_emojis = {
            RiskLevel.LOW: "🟢",
            RiskLevel.MODERATE: "🟡",
            RiskLevel.HIGH: "🟠",
            RiskLevel.CRITICAL: "🔴"
        }

        # Format stop duration
        hours = int(alert.duration_stopped.total_seconds() // 3600)
        minutes = int((alert.duration_stopped.total_seconds() % 3600) // 60)

        if hours > 0:
            duration_str = f"{hours}h {minutes}m"
        else:
            duration_str = f"{minutes}m"

        # COMPACT FORMAT - Only essential information with load details
        load_info = ""
        if alert.load_details:
            load_id = alert.load_details.get('load_id', 'N/A')
            del_address = alert.load_details.get('del_address', 'N/A')
            del_status = alert.load_details.get('del_status', 'N/A')
            load_info = (
                f"📦 **Load ID:** {load_id}\n"
                f"🚛 **Status:** {del_status}\n"
                f"📍 **Destination:** {del_address}\n"
            )
        
        message = (
            f"🚨 **CARGO THEFT ALERT** {risk_emojis[alert.risk_level]}\n\n"
            f"👤 **Driver:** {alert.driver_name}\n"
            f"📞 **Phone:** {alert.driver_phone}\n"
            f"📍 **Location:** {alert.address}\n"
            f"🎯 **Risk Zone:** {alert.zone_name}\n"
            f"⏱️ **Stopped:** {duration_str}\n"
            f"📡 **Time:** {alert.timestamp.strftime('%I:%M %p')} EDT\n"
            f"{load_info}"
            f"\n🔗 [View on Map]({alert.google_maps_link})\n\n"
            f"**Actions:** Call driver • Verify load • Monitor movement"
        )

        return message

    def get_zone_statistics(self) -> Dict[str, Any]:
        """Get statistics about risk zones and current driver states"""
        stats = {
            'total_zones': len(self.risk_zones),
            'zones_by_risk': {level.value: 0 for level in RiskLevel},
            'active_drivers': len(self.driver_states),
            'stopped_drivers': 0,
            'drivers_in_risk_zones': 0,
            'recent_alerts': len(self.recent_alerts),
            'home_locations_cached': len(self.home_locations_cache)
        }

        # Count zones by risk level
        for zone in self.risk_zones:
            stats['zones_by_risk'][zone.risk_level.value] += 1

        # Count driver states
        for driver_state in self.driver_states.values():
            if driver_state['is_stopped']:
                stats['stopped_drivers'] += 1
            if driver_state['current_risk_zone']:
                stats['drivers_in_risk_zones'] += 1

        return stats

    def cleanup_old_states(self, hours: int = 24):
        """Clean up old driver states and alerts"""
        cutoff_time = datetime.now(pytz.timezone(
            'America/New_York')) - timedelta(hours=hours)

        # Clean up driver states
        vins_to_remove = []
        for vin, state in self.driver_states.items():
            if state['last_update'] < cutoff_time:
                vins_to_remove.append(vin)

        for vin in vins_to_remove:
            del self.driver_states[vin]

        # Clean up old alerts
        alerts_to_remove = []
        for alert_key, alert_time in self.recent_alerts.items():
            if alert_time < cutoff_time:
                alerts_to_remove.append(alert_key)

        for alert_key in alerts_to_remove:
            del self.recent_alerts[alert_key]

        logger.info(
            f"Cleaned up {len(vins_to_remove)} old driver states and {len(alerts_to_remove)} old alerts")


def test_simplified_risk_detection():
    """Test the simplified cargo theft risk detection system"""
    print("Testing Simplified Cargo Theft Risk Detection System...")

    # Mock config for testing
    class MockConfig:
        ALERT_COOLDOWN_MINUTES = 30
        MIN_STOP_DURATION_MINUTES = 10
        MAX_SPEED_THRESHOLD_MPH = 2.0
        HOME_RADIUS_MILES = 2.0

    config = MockConfig()
    detector = CargoTheftRiskDetector(config)

    # Test locations
    test_locations = [
        # Los Angeles - CRITICAL
        {'lat': 34.0522,
         'lng': -118.2437,
         'expected': RiskLevel.CRITICAL,
         'name': 'Los Angeles'},
        # Dallas - CRITICAL
        {'lat': 32.7767,
         'lng': -96.7970,
         'expected': RiskLevel.CRITICAL,
         'name': 'Dallas'},
        # Atlanta - HIGH
        {'lat': 33.7490,
         'lng': -84.3880,
         'expected': RiskLevel.HIGH,
         'name': 'Atlanta'},
        # Denver - MODERATE
        {'lat': 39.7392,
         'lng': -104.9903,
         'expected': RiskLevel.MODERATE,
         'name': 'Denver'},
        # Rural Montana - LOW
        {'lat': 47.0527,
         'lng': -109.6333,
         'expected': RiskLevel.LOW,
         'name': 'Rural Montana'}
    ]

    print(f"✅ Initialized detector with {len(detector.risk_zones)} zones")

    for i, location in enumerate(test_locations):
        risk_level, zone = detector.check_location_risk(
            location['lat'], location['lng'])
        zone_name = zone.name if zone else "No Zone"
        expected = location['expected'].value
        actual = risk_level.value
        status = "✅" if actual == expected else "❌"

        print(
            f"Test {i+1} {status}: {location['name']} -> Expected: {expected}, Got: {actual} ({zone_name})")

    print("\n✅ Simplified risk detection system test completed")
    return True


if __name__ == "__main__":
    test_simplified_risk_detection()
