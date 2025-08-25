import os
import logging
from typing import Dict, Any
from pathlib import Path

# Try to load dotenv, but make it optional
try:
    from dotenv import load_dotenv
    load_dotenv()
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

logger = logging.getLogger(__name__)

class Config:
    """Enhanced configuration with dual-mode scheduling system and cargo theft risk detection"""
    
    def __init__(self):
        # Try to load from .env if available
        if DOTENV_AVAILABLE:
            load_dotenv()
        
        # Core Telegram Configuration (REQUIRED)
        self.TELEGRAM_BOT_TOKEN = self._get_env_or_error("TELEGRAM_BOT_TOKEN")
        self.OWNER_TELEGRAM_ID = self._get_optional_int("OWNER_TELEGRAM_ID")
        
        # Google Sheets Configuration (REQUIRED)
        self.SHEETS_SERVICE_ACCOUNT_FILE = self._get_env_or_error("SHEETS_SERVICE_ACCOUNT_FILE")
        self.SPREADSHEET_ID = self._get_env_or_error("SPREADSHEET_ID")
        self.SPREADSHEET_ASSETS = self._get_optional("SPREADSHEET_ASSETS", "assets")
        self.SPREADSHEET_GROUPS = self._get_optional("SPREADSHEET_GROUPS", "groups")
        
        # TMS API Configuration (REQUIRED)
        self.TMS_API_URL = self._get_env_or_error("TMS_API_URL")
        self.TMS_API_KEY = self._get_env_or_error("TMS_API_KEY")
        self.TMS_API_HASH = self._get_env_or_error("TMS_API_HASH")
        
        # OpenRouteService Configuration (REQUIRED)
        self.ORS_API_KEY = self._get_env_or_error("ORS_API_KEY")
        
        # =====================================================
        # NEW: QC PANEL INTEGRATION CONFIGURATION
        # =====================================================
        self.QC_PANEL_SPREADSHEET_ID = self._get_optional("QC_PANEL_SPREADSHEET_ID", "")
        self.QC_ACTIVE_TABS = self._get_optional("QC_ACTIVE_TABS", "BIDH S,BIDH D,CPWP S,CPWP D,SSOY,OTMV")
        self.RISK_MONITOR_DEL_STATUSES = self._get_optional("RISK_MONITOR_DEL_STATUSES", "IN TRANSIT,WILL BE LATE,AT SHIPPER")

        # =====================================================
        # RATE LIMITING CONFIGURATION
        # =====================================================
        self.SHEETS_RATE_LIMIT_ENABLED = self._get_optional_bool("SHEETS_RATE_LIMIT_ENABLED", True)
        self.SHEETS_MAX_REQUESTS_PER_MINUTE = self._get_optional_int("SHEETS_MAX_REQUESTS_PER_MINUTE", 180)
        self.SHEETS_CACHE_DEFAULT_TTL = self._get_optional_int("SHEETS_CACHE_DEFAULT_TTL", 300)  # 5 minutes
        self.SHEETS_CACHE_LONG_TTL = self._get_optional_int("SHEETS_CACHE_LONG_TTL", 1800)  # 30 minutes
        self.SHEETS_EXPONENTIAL_BACKOFF_BASE = self._get_optional_float("SHEETS_EXPONENTIAL_BACKOFF_BASE", 1.0)
        self.SHEETS_EXPONENTIAL_BACKOFF_MAX = self._get_optional_float("SHEETS_EXPONENTIAL_BACKOFF_MAX", 60.0)
        self.SHEETS_CIRCUIT_BREAKER_THRESHOLD = self._get_optional_int("SHEETS_CIRCUIT_BREAKER_THRESHOLD", 10)
        self.SHEETS_CIRCUIT_BREAKER_TIMEOUT = self._get_optional_int("SHEETS_CIRCUIT_BREAKER_TIMEOUT", 300)  # 5 minutes
        
        # =====================================================
        # GOOGLE SHEETS COLUMN MAPPING (A,B,C notation)
        # =====================================================
        self.USE_COLUMN_MAPPING = self._get_optional_bool("USE_COLUMN_MAPPING", True)
        
        # Assets worksheet columns - customize if your sheet has different layout
        self.ASSETS_DRIVER_NAME_COL = self._get_optional("ASSETS_DRIVER_NAME_COL", "D")  # Column D
        self.ASSETS_VIN_COL = self._get_optional("ASSETS_VIN_COL", "E")  # Column E
        self.ASSETS_LOCATION_COL = self._get_optional("ASSETS_LOCATION_COL", "F")  # Column F
        self.ASSETS_LATITUDE_COL = self._get_optional("ASSETS_LATITUDE_COL", "G")  # Column G
        self.ASSETS_LONGITUDE_COL = self._get_optional("ASSETS_LONGITUDE_COL", "H")  # Column H
        self.ASSETS_PHONE_COL = self._get_optional("ASSETS_PHONE_COL", "L")  # Column L

        # =====================================================
        # OPENROUTESERVICE RATE LIMITING
        # =====================================================
        # OpenRouteService actual limits: Directions=40/min, Geocoding=100/min, Reverse=100/min
        # Using conservative limits to avoid hitting quotas
        self.ORS_MAX_REQUESTS_PER_MINUTE = self._get_optional_int("ORS_MAX_REQUESTS_PER_MINUTE", 30)  # Conservative for mixed endpoints
        self.ORS_REQUEST_DELAY = self._get_optional_float("ORS_REQUEST_DELAY", 2.0)  # 2 second minimum delay between requests
        self.ORS_ENABLE_CACHING = self._get_optional_bool("ORS_ENABLE_CACHING", True)
        self.ORS_CACHE_TTL = self._get_optional_int("ORS_CACHE_TTL", 3600)  # 1 hour cache

        # ETA / Alerting Configuration
        self.ETA_GRACE_MINUTES = self._get_optional_int("ETA_GRACE_MINUTES", 10)
        self.SEND_QC_LATE_ALERTS = self._get_optional_bool("SEND_QC_LATE_ALERTS", True)
        self.RISK_REQUIRE_LATE = self._get_optional_bool("RISK_REQUIRE_LATE", False)
        self.MGMT_CHAT_ID = self._get_optional("MGMT_CHAT_ID", "")
        
        # =====================================================
        # DUAL-MODE SCHEDULING CONFIGURATION
        # =====================================================
        
        # Group Location Messages (Hourly - Sends actual messages to groups)
        self.GROUP_LOCATION_INTERVAL = self._get_optional_int("GROUP_LOCATION_INTERVAL", 3600)  # 1 hour
        
        # Silent Data Refresh (8-minute - Updates TMS data to assets sheet)  
        self.LIVE_TRACKING_INTERVAL = self._get_optional_int("LIVE_TRACKING_INTERVAL", 480)  # 8 minutes
        
        # Legacy support (maps to LIVE_TRACKING_INTERVAL for backward compatibility)
        self.LIVE_UPDATE_INTERVAL = self.LIVE_TRACKING_INTERVAL
        
        # Group Auto-Update Settings
        self.ENABLE_GROUP_LOCATION_UPDATES = self._get_optional_bool("ENABLE_GROUP_LOCATION_UPDATES", True)
        self.ENABLE_LIVE_ETA_TRACKING = self._get_optional_bool("ENABLE_LIVE_ETA_TRACKING", True)
        self.AUTO_START_LOCATION_UPDATES = self._get_optional_bool("AUTO_START_LOCATION_UPDATES", True)
        
        # Session Management
        self.MAX_LIVE_SESSIONS = self._get_optional_int("MAX_LIVE_SESSIONS", 100)
        self.MAX_GROUP_SESSIONS = self._get_optional_int("MAX_GROUP_SESSIONS", 50)
        self.SESSION_TIMEOUT_HOURS = self._get_optional_int("SESSION_TIMEOUT_HOURS", 24)
        
        # =====================================================
        # CARGO THEFT RISK DETECTION CONFIGURATION
        # =====================================================
        
        # Risk Detection Configuration
        self.ENABLE_RISK_MONITORING = self._get_optional_bool("ENABLE_RISK_MONITORING", True)
        self.RISK_CHECK_INTERVAL = self._get_optional_int("RISK_CHECK_INTERVAL", 300)  # 5 minutes
        self.ASSETS_UPDATE_INTERVAL = self._get_optional_int("ASSETS_UPDATE_INTERVAL", 3600)  # 1 hour
        self.QC_TEAM_CHAT_ID = self._get_optional_int("QC_TEAM_CHAT_ID")
        self.MANAGEMENT_CHAT_ID = self._get_optional_int("MANAGEMENT_CHAT_ID")
        
        # Risk Detection Thresholds  
        self.MIN_STOP_DURATION_MINUTES = self._get_optional_int("MIN_STOP_DURATION_MINUTES", 10)
        self.MAX_SPEED_THRESHOLD_MPH = self._get_optional_float("MAX_SPEED_THRESHOLD_MPH", 2.0)
        self.ALERT_COOLDOWN_MINUTES = self._get_optional_int("ALERT_COOLDOWN_MINUTES", 30)
        
        # =====================================================
        # STORAGE & BACKEND CONFIGURATION
        # =====================================================
        
        # Storage Configuration (with defaults)
        self.USE_SQLITE_BACKEND = self._get_optional_bool("USE_SQLITE_BACKEND", False)
        self.SQLITE_DB_PATH = self._get_optional("SQLITE_DB_PATH", "bot_data.db")
        self.GSERVICE_ACCOUNT_JSON = self._get_optional("GSERVICE_ACCOUNT_JSON", "service_account.json")
        
        # =====================================================
        # PERFORMANCE & RATE LIMITING
        # =====================================================
        
        # Performance Settings
        self.MAX_CONCURRENT_JOBS = self._get_optional_int("MAX_CONCURRENT_JOBS", 50)
        self.JOB_QUEUE_MAX_SIZE = self._get_optional_int("JOB_QUEUE_MAX_SIZE", 1000)
        
        # API Rate Limiting
        self.TMS_REQUEST_DELAY = self._get_optional_float("TMS_REQUEST_DELAY", 1.0)
        self.TMS_MAX_REQUESTS_PER_MINUTE = self._get_optional_int("TMS_MAX_REQUESTS_PER_MINUTE", 60)
        self.ORS_REQUEST_DELAY = self._get_optional_float("ORS_REQUEST_DELAY", 0.5)
        self.ORS_MAX_REQUESTS_PER_MINUTE = self._get_optional_int("ORS_MAX_REQUESTS_PER_MINUTE", 120)
        
        # Error Handling & Retry Logic
        self.MAX_RETRY_ATTEMPTS = self._get_optional_int("MAX_RETRY_ATTEMPTS", 3)
        self.RETRY_DELAY_SECONDS = self._get_optional_int("RETRY_DELAY_SECONDS", 5)
        self.RETRY_EXPONENTIAL_BACKOFF = self._get_optional_bool("RETRY_EXPONENTIAL_BACKOFF", True)
        
        # =====================================================
        # FEATURE FLAGS
        # =====================================================
        
        # Core Features
        self.ENABLE_PM_TRACKING = self._get_optional_bool("ENABLE_PM_TRACKING", True)
        self.ENABLE_GROUP_AUTO_UPDATES = self._get_optional_bool("ENABLE_GROUP_AUTO_UPDATES", True)
        self.ENABLE_LIVE_TRACKING = self._get_optional_bool("ENABLE_LIVE_TRACKING", True)
        
        # Advanced Features
        self.ENABLE_ROUTE_OPTIMIZATION = self._get_optional_bool("ENABLE_ROUTE_OPTIMIZATION", True)
        self.ENABLE_TRAFFIC_DATA = self._get_optional_bool("ENABLE_TRAFFIC_DATA", True)
        self.ENABLE_WEATHER_INTEGRATION = self._get_optional_bool("ENABLE_WEATHER_INTEGRATION", False)
        
        # =====================================================
        # NOTIFICATION & MESSAGING
        # =====================================================
        
        # Notification Settings
        self.SEND_ERROR_NOTIFICATIONS = self._get_optional_bool("SEND_ERROR_NOTIFICATIONS", True)
        self.SEND_STATUS_UPDATES = self._get_optional_bool("SEND_STATUS_UPDATES", True)
        self.NOTIFICATION_COOLDOWN_MINUTES = self._get_optional_int("NOTIFICATION_COOLDOWN_MINUTES", 15)
        
        # Message Formatting
        self.USE_MARKDOWN_MESSAGES = self._get_optional_bool("USE_MARKDOWN_MESSAGES", True)
        self.ENABLE_MAP_PREVIEWS = self._get_optional_bool("ENABLE_MAP_PREVIEWS", True)
        self.MAX_MESSAGE_LENGTH = self._get_optional_int("MAX_MESSAGE_LENGTH", 4096)
        
        # Admin Contact
        self.ADMIN_USERNAME = self._get_optional("ADMIN_USERNAME", "")
        self.ADMIN_CONTACT_URL = self._get_optional("ADMIN_CONTACT_URL", "")
        
        # =====================================================
        # LOCATION RENDERING CONFIGURATION
        # =====================================================
        
        # Address caching and reverse geocoding
        self.LOCATION_ADDR_CACHE_TTL_SECS = self._get_optional_int("LOCATION_ADDR_CACHE_TTL_SECS", 86400)  # 24 hours
        self.RENDER_COORD_DECIMALS = self._get_optional_int("RENDER_COORD_DECIMALS", 5)  # ~1.1m precision
        self.REVERSE_GEOCODE_TIMEOUT_SECS = self._get_optional_int("REVERSE_GEOCODE_TIMEOUT_SECS", 5)
        
        # Feature flag for location rendering
        self.ENABLE_NEW_LOCATION_RENDERER = self._get_optional_bool("ENABLE_NEW_LOCATION_RENDERER", True)
        
        # =====================================================
        # CACHING & DATA MANAGEMENT
        # =====================================================
        
        # Caching Configuration
        self.ENABLE_CACHING = self._get_optional_bool("ENABLE_CACHING", True)
        self.CACHE_TTL_MINUTES = self._get_optional_int("CACHE_TTL_MINUTES", 5)
        self.GEOCODING_CACHE_SIZE = self._get_optional_int("GEOCODING_CACHE_SIZE", 1000)
        self.ROUTE_CACHE_SIZE = self._get_optional_int("ROUTE_CACHE_SIZE", 500)
        self.TRUCK_DATA_CACHE_TTL = self._get_optional_int("TRUCK_DATA_CACHE_TTL", 120)  # 2 minutes
        
        # Data Quality Settings
        self.MAX_LOCATION_AGE_HOURS = self._get_optional_int("MAX_LOCATION_AGE_HOURS", 12)
        self.MIN_COORDINATE_PRECISION = self._get_optional_float("MIN_COORDINATE_PRECISION", 0.001)
        self.MAX_ROUTE_DISTANCE_MILES = self._get_optional_int("MAX_ROUTE_DISTANCE_MILES", 2000)
        
        # =====================================================
        # LOGGING CONFIGURATION
        # =====================================================
        
        # Logging Settings
        self.LOG_LEVEL = self._get_optional("LOG_LEVEL", "INFO")
        self.LOG_FILE_MAX_MB = self._get_optional_int("LOG_FILE_MAX_MB", 10)
        self.LOG_BACKUP_COUNT = self._get_optional_int("LOG_BACKUP_COUNT", 5)
        self.ENABLE_DEBUG_LOGGING = self._get_optional_bool("ENABLE_DEBUG_LOGGING", False)
        
        # Detailed Logging Options
        self.LOG_TMS_REQUESTS = self._get_optional_bool("LOG_TMS_REQUESTS", False)
        self.LOG_ORS_REQUESTS = self._get_optional_bool("LOG_ORS_REQUESTS", False)
        self.LOG_USER_INTERACTIONS = self._get_optional_bool("LOG_USER_INTERACTIONS", True)
        self.LOG_JOB_EXECUTION = self._get_optional_bool("LOG_JOB_EXECUTION", True)
        
        # Dashboard Logging Settings
        self.ENABLE_DASHBOARD_LOGGING = self._get_optional_bool("ENABLE_DASHBOARD_LOGGING", True)
        self.LOG_FLUSH_INTERVAL = self._get_optional_int("LOG_FLUSH_INTERVAL", 300)  # 5 minutes
        self.SHEETS_BATCH_SIZE = self._get_optional_int("SHEETS_BATCH_SIZE", 50)

        # Worksheet Names for Logging
        self.SPREADSHEET_DASHBOARD = self._get_optional("SPREADSHEET_DASHBOARD", "dashboard_logs")
        self.SPREADSHEET_FLEET_STATUS = self._get_optional("SPREADSHEET_FLEET_STATUS", "fleet_status")

         # =====================================================
        # ENHANCED SCHEDULER CONFIGURATION (NEW)
        # =====================================================
        
        # Jitter & Rate Limiting Settings
        self.MAX_CONCURRENT_TELEGRAM_SENDS = self._get_optional_int("MAX_CONCURRENT_TELEGRAM_SENDS", 12)
        self.SCHEDULER_JITTER_MAX_SECONDS = self._get_optional_int("SCHEDULER_JITTER_MAX_SECONDS", 15)
        self.SCHEDULER_RETRY_MAX_ATTEMPTS = self._get_optional_int("SCHEDULER_RETRY_MAX_ATTEMPTS", 3)
        
        # Distribution Settings for Large Fleets
        self.ENABLE_SCHEDULER_JITTER = self._get_optional_bool("ENABLE_SCHEDULER_JITTER", True)
        self.SCHEDULER_DISTRIBUTION_WINDOW = self._get_optional_int("SCHEDULER_DISTRIBUTION_WINDOW", 900)  # 
        # =====================================================
        # VALIDATION & INITIALIZATION
        # =====================================================
        
        # Validate critical settings
        self._validate_critical_settings()
        
        # Log configuration summary
        self._log_configuration_summary()
        
        logger.info("Enhanced dual-mode configuration loaded successfully")
    
    def _get_env_or_error(self, key: str) -> str:
        """Get required environment variable or raise detailed error"""
        value = os.getenv(key)
        if not value:
            missing_vars = self._get_missing_required_vars()
            error_msg = f"""
❌ Configuration Error: Required environment variable '{key}' is not set.

🔧 Quick Fix Options:

1. Create .env file in current directory:
   echo '{key}=your_value_here' >> .env

2. Set environment variable:
   export {key}=your_value_here

3. Missing required variables: {missing_vars}

4. Example .env file structure:
   TELEGRAM_BOT_TOKEN=123456789:your_bot_token
   SPREADSHEET_ID=1234567890abcdef
   SHEETS_SERVICE_ACCOUNT_FILE=service_account.json
   TMS_API_URL=https://api.yourtms.com
   TMS_API_KEY=your_tms_key
   TMS_API_HASH=your_tms_hash
   ORS_API_KEY=your_openrouteservice_key
   
   # QC Panel Integration
   QC_PANEL_SPREADSHEET_ID=your_qc_panel_sheet_id
   QC_ACTIVE_TABS=BIDH S,BIDH D,CPWP S,CPWP D,SSOY,OTMV
   MGMT_CHAT_ID=-1234567890
   
   # Optional: Customize intervals
   GROUP_LOCATION_INTERVAL=3600  # 1 hour (sends messages to groups)
   LIVE_TRACKING_INTERVAL=300    # 5 minutes (silent data refresh only)
   
   # Optional: Risk Detection
   ENABLE_RISK_MONITORING=true
   QC_TEAM_CHAT_ID=-1234567890
   
📁 Current directory: {Path.cwd()}
📁 Looking for .env file: {Path('.env').exists()}
"""
            raise ValueError(error_msg)
        return value
    
    def _get_missing_required_vars(self) -> list:
        """Get list of missing required environment variables"""
        required_vars = [
            "TELEGRAM_BOT_TOKEN",
            "SHEETS_SERVICE_ACCOUNT_FILE", 
            "SPREADSHEET_ID",
            "TMS_API_URL",
            "TMS_API_KEY", 
            "TMS_API_HASH",
            "ORS_API_KEY"
        ]
        
        missing = []
        for var in required_vars:
            if not os.getenv(var):
                missing.append(var)
        
        return missing
    
    def _get_optional(self, key: str, default: str = "") -> str:
        """Get optional environment variable with default"""
        return os.getenv(key, default)
    
    def _get_optional_int(self, key: str, default: int = 0) -> int:
        """Get optional integer environment variable with default"""
        try:
            value = os.getenv(key)
            return int(value) if value else default
        except ValueError:
            logger.warning(f"Invalid integer value for {key}, using default: {default}")
            return default
    
    def _get_optional_float(self, key: str, default: float = 0.0) -> float:
        """Get optional float environment variable with default"""
        try:
            value = os.getenv(key)
            return float(value) if value else default
        except ValueError:
            logger.warning(f"Invalid float value for {key}, using default: {default}")
            return default
    
    def _get_optional_bool(self, key: str, default: bool = False) -> bool:
        """Get optional boolean environment variable with default"""
        value = os.getenv(key, "").lower()
        if value in ("true", "1", "yes", "on"):
            return True
        elif value in ("false", "0", "no", "off"):
            return False
        else:
            return default
    
    def _validate_critical_settings(self):
        """Validate critical configuration values"""
        warnings = []
        errors = []
        
        # Check if service account file exists
        if not Path(self.SHEETS_SERVICE_ACCOUNT_FILE).exists():
            warnings.append(f"Service account file not found: {self.SHEETS_SERVICE_ACCOUNT_FILE}")
        
        # Validate intervals
        if self.GROUP_LOCATION_INTERVAL < 300:  # 5 minutes minimum
            warnings.append("GROUP_LOCATION_INTERVAL should be at least 300 seconds (5 minutes)")
        
        if self.LIVE_TRACKING_INTERVAL < 60:  # 1 minute minimum
            warnings.append("LIVE_TRACKING_INTERVAL should be at least 60 seconds for API rate limits")
        
        # Validate session limits
        if self.MAX_LIVE_SESSIONS < 1:
            errors.append("MAX_LIVE_SESSIONS must be at least 1")
        
        if self.MAX_GROUP_SESSIONS < 1:
            errors.append("MAX_GROUP_SESSIONS must be at least 1")
        
        # Validate API rate limits
        if self.TMS_MAX_REQUESTS_PER_MINUTE < 1:
            warnings.append("TMS_MAX_REQUESTS_PER_MINUTE should be at least 1")
        
        if self.ORS_MAX_REQUESTS_PER_MINUTE < 1:
            warnings.append("ORS_MAX_REQUESTS_PER_MINUTE should be at least 1")
        
        # Log warnings
        for warning in warnings:
            logger.warning(f"Configuration Warning: {warning}")
        
        # Raise errors
        if errors:
            error_msg = "Configuration Errors:\n" + "\n".join(f"• {error}" for error in errors)
            raise ValueError(error_msg)
    
    def _log_configuration_summary(self):
        """Log a summary of key configuration settings"""
        if self.ENABLE_DEBUG_LOGGING:
            logger.info("=== CONFIGURATION SUMMARY ===")
            logger.info(f"Group Location Messages: {self.GROUP_LOCATION_INTERVAL}s ({self.GROUP_LOCATION_INTERVAL//60} min)")
            logger.info(f"Silent Data Refresh: {self.LIVE_TRACKING_INTERVAL}s ({self.LIVE_TRACKING_INTERVAL//60} min)")
            logger.info(f"Auto-start location updates: {self.AUTO_START_LOCATION_UPDATES}")
            logger.info(f"Max live sessions: {self.MAX_LIVE_SESSIONS}")
            logger.info(f"Max group sessions: {self.MAX_GROUP_SESSIONS}")
            logger.info(f"Risk monitoring enabled: {self.ENABLE_RISK_MONITORING}")
            logger.info(f"QC team chat ID: {'Configured' if self.QC_TEAM_CHAT_ID else 'Not configured'}")
            logger.info(f"QC Panel spreadsheet: {'Configured' if self.QC_PANEL_SPREADSHEET_ID else 'Not configured'}")
            logger.info(f"Features enabled: PM={self.ENABLE_PM_TRACKING}, Groups={self.ENABLE_GROUP_AUTO_UPDATES}, Live={self.ENABLE_LIVE_TRACKING}")
            logger.info("==============================")
    
    # =====================================================
    # UTILITY METHODS
    # =====================================================
    
    def get_cache_settings(self) -> Dict[str, Any]:
        """Get cache configuration"""
        return {
            "enabled": self.ENABLE_CACHING,
            "ttl_minutes": self.CACHE_TTL_MINUTES,
            "geocoding_cache_size": self.GEOCODING_CACHE_SIZE,
            "route_cache_size": self.ROUTE_CACHE_SIZE,
            "truck_data_cache_ttl": self.TRUCK_DATA_CACHE_TTL,
            "zip_cache": {}  # Add this for compatibility
        }
    
    def get_scheduling_config(self) -> Dict[str, Any]:
        """Get dual-mode scheduling configuration"""
        return {
            "group_location_interval": self.GROUP_LOCATION_INTERVAL,
            "live_tracking_interval": self.LIVE_TRACKING_INTERVAL,
            "auto_start_enabled": self.AUTO_START_LOCATION_UPDATES,
            "max_live_sessions": self.MAX_LIVE_SESSIONS,
            "max_group_sessions": self.MAX_GROUP_SESSIONS,
            "session_timeout_hours": self.SESSION_TIMEOUT_HOURS
        }
    
    def get_rate_limit_config(self) -> Dict[str, Any]:
        """Get API rate limiting configuration"""
        return {
            "tms": {
                "delay": self.TMS_REQUEST_DELAY,
                "max_per_minute": self.TMS_MAX_REQUESTS_PER_MINUTE
            },
            "ors": {
                "delay": self.ORS_REQUEST_DELAY,
                "max_per_minute": self.ORS_MAX_REQUESTS_PER_MINUTE
            },
            "retry": {
                "max_attempts": self.MAX_RETRY_ATTEMPTS,
                "delay_seconds": self.RETRY_DELAY_SECONDS,
                "exponential_backoff": self.RETRY_EXPONENTIAL_BACKOFF
            }
        }
    
    def is_feature_enabled(self, feature: str) -> bool:
        """Check if a feature is enabled"""
        feature_map = {
            "live_tracking": self.ENABLE_LIVE_TRACKING,
            "group_auto_updates": self.ENABLE_GROUP_AUTO_UPDATES,
            "group_location_updates": self.ENABLE_GROUP_LOCATION_UPDATES,
            "live_eta_tracking": self.ENABLE_LIVE_ETA_TRACKING,
            "pm_tracking": self.ENABLE_PM_TRACKING,
            "caching": self.ENABLE_CACHING,
            "route_optimization": self.ENABLE_ROUTE_OPTIMIZATION,
            "traffic_data": self.ENABLE_TRAFFIC_DATA,
            "weather_integration": self.ENABLE_WEATHER_INTEGRATION,
            "risk_monitoring": self.ENABLE_RISK_MONITORING
        }
        return feature_map.get(feature, False)
    
    def get_update_interval(self, mode: str) -> int:
        """Get update interval for specific mode"""
        intervals = {
            "group_location": self.GROUP_LOCATION_INTERVAL,
            "live_eta": self.LIVE_TRACKING_INTERVAL,
            "live_tracking": self.LIVE_TRACKING_INTERVAL,  # Legacy support
            "risk_check": self.RISK_CHECK_INTERVAL,
            "assets_update": self.ASSETS_UPDATE_INTERVAL,
            "default": self.LIVE_TRACKING_INTERVAL
        }
        return intervals.get(mode, intervals["default"])
    
    def should_auto_start_updates(self) -> bool:
        """Check if location updates should auto-start for groups"""
        return (
            self.AUTO_START_LOCATION_UPDATES and
            self.ENABLE_GROUP_LOCATION_UPDATES and
            self.ENABLE_GROUP_AUTO_UPDATES
        )
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return {
            "level": self.LOG_LEVEL,
            "max_file_mb": self.LOG_FILE_MAX_MB,
            "backup_count": self.LOG_BACKUP_COUNT,
            "debug_enabled": self.ENABLE_DEBUG_LOGGING,
            "log_requests": {
                "tms": self.LOG_TMS_REQUESTS,
                "ors": self.LOG_ORS_REQUESTS,
                "users": self.LOG_USER_INTERACTIONS,
                "jobs": self.LOG_JOB_EXECUTION
            }
        }
    
    def __str__(self) -> str:
        """String representation (safe, no secrets)"""
        return (
            f"Config("
            f"group_updates={self.GROUP_LOCATION_INTERVAL}s, "
            f"live_tracking={self.LIVE_TRACKING_INTERVAL}s, "
            f"risk_monitoring={self.ENABLE_RISK_MONITORING}, "
            f"qc_panel={'Configured' if self.QC_PANEL_SPREADSHEET_ID else 'Not configured'}, "
            f"features={self.ENABLE_LIVE_TRACKING}/{self.ENABLE_GROUP_AUTO_UPDATES}"
            f")"
        )
    
    def __repr__(self) -> str:
        """Detailed representation for debugging"""
        return (
            f"Config(group_location={self.GROUP_LOCATION_INTERVAL}s, "
            f"live_eta={self.LIVE_TRACKING_INTERVAL}s, "
            f"risk_check={self.RISK_CHECK_INTERVAL}s, "
            f"assets_update={self.ASSETS_UPDATE_INTERVAL}s, "
            f"qc_panel={bool(self.QC_PANEL_SPREADSHEET_ID)}, "
            f"max_sessions={self.MAX_LIVE_SESSIONS}, "
            f"auto_start={self.AUTO_START_LOCATION_UPDATES})"
        )