import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any

try:
    import gspread
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    gspread = None

from config import Config

logger = logging.getLogger(__name__)

# Import column mapping utilities
try:
    from sheets_column_mapper import SheetsColumnMapper, AssetsColumnMapper
    from column_mapping_config import WorksheetType, initialize_column_mapper
    COLUMN_MAPPING_AVAILABLE = True
except ImportError:
    logger.warning("Column mapping utilities not available")
    COLUMN_MAPPING_AVAILABLE = False

# Import rate limiting wrapper
try:
    from rate_limiting_wrapper import RateLimitedSheetsWrapper
    RATE_LIMITING_AVAILABLE = True
except ImportError:
    logger.warning("Rate limiting wrapper not available")
    RATE_LIMITING_AVAILABLE = False
    RateLimitedSheetsWrapper = None

class GoogleSheetsIntegration:
    """Enhanced Google Sheets integration with QC PANEL → assets sync"""

    def __init__(self, config: Config):
        self.config = config
        self.gc = None
        self.spreadsheet = None
        self.assets_worksheet = None
        self.groups_worksheet = None
        self.dashboard_logs_worksheet = None
        self.fleet_status_worksheet = None

        # QC Panel integration
        self.qc_panel_spreadsheet = None

        # Caching
        self.last_fetch_time = None
        self.cached_driver_names = []
        self.cache_duration = timedelta(minutes=5)
        self._active_cache = {}
        self._active_cache_ts = None
        
        # Groups records cache to reduce header duplication issues
        self._groups_records_cache = None
        self._groups_records_cache_ts = None
        self._groups_records_cache_duration = timedelta(seconds=30)  # Short cache to reduce repeated calls

        # Rate limiting
        self.rate_limiter = None
        if RATE_LIMITING_AVAILABLE:
            self.rate_limiter = RateLimitedSheetsWrapper(config)
            logger.info("Rate limiting enabled for Google Sheets operations")

        # Column mapping - enabled for robust column access
        self.use_column_mapping = COLUMN_MAPPING_AVAILABLE and getattr(config, 'USE_COLUMN_MAPPING', True)
        self.assets_mapper = None
        
        if self.use_column_mapping and COLUMN_MAPPING_AVAILABLE:
            try:
                initialize_column_mapper(config)
                self.assets_mapper = AssetsColumnMapper(config=config)
                logger.info("Column mapping enabled for robust column access")
                logger.info(f"Assets columns: Driver={config.ASSETS_DRIVER_NAME_COL}, VIN={config.ASSETS_VIN_COL}")
            except Exception as e:
                logger.error(f"Failed to initialize column mapping: {e}")
                self.use_column_mapping = False
                logger.info("Falling back to header-based access with rate limiting")
        else:
            logger.info("Column mapping not available - using header-based access with rate limiting")

        # Logging settings
        self.enable_dashboard_logging = getattr(config, 'ENABLE_DASHBOARD_LOGGING', True)

        # Initialize connection
        self._initialize_connection()

        # Initialize comprehensive sheets model after connection
        try:
            from sheets_model import SheetsModelManager
            self.sheets_model = SheetsModelManager(self, config)
            logger.info("Comprehensive sheets model initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize sheets model: {e}")
            self.sheets_model = None

    def _initialize_connection(self):
        """Initialize Google Sheets connection with enhanced error handling"""
        if not GSPREAD_AVAILABLE:
            raise ImportError("gspread is not installed. Install with: pip install gspread")

        try:
            # Use service account file from config
            service_account_file = self.config.SHEETS_SERVICE_ACCOUNT_FILE

            if not os.path.exists(service_account_file):
                raise FileNotFoundError(f"Service account file not found: {service_account_file}")

            logger.info(f"Using service account file: {service_account_file}")

            # Initialize gspread client
            self.gc = gspread.service_account(filename=service_account_file)

            # Open the main spreadsheet
            self.spreadsheet = self.gc.open_by_key(self.config.SPREADSHEET_ID)
            logger.info(f"Successfully connected to Google Sheets: {self.config.SPREADSHEET_ID}")

            # Initialize QC Panel spreadsheet if configured
            if self.config.QC_PANEL_SPREADSHEET_ID:
                try:
                    self.qc_panel_spreadsheet = self.gc.open_by_key(self.config.QC_PANEL_SPREADSHEET_ID)
                    logger.info(f"Connected to QC Panel spreadsheet: {self.config.QC_PANEL_SPREADSHEET_ID}")
                except Exception as e:
                    logger.warning(f"Could not connect to QC Panel spreadsheet: {e}")
                    self.qc_panel_spreadsheet = None

            # Initialize all worksheets
            self._initialize_worksheets()
            
            # Apply rate limiting to worksheets if available
            if self.rate_limiter:
                self._apply_rate_limiting()

        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets connection: {e}")
            raise

    def _initialize_worksheets(self):
        """Initialize all required worksheets"""
        try:
            # Assets worksheet (main data)
            self.assets_worksheet = self.spreadsheet.worksheet(self.config.SPREADSHEET_ASSETS)
            logger.info(f"Connected to assets worksheet: {self.config.SPREADSHEET_ASSETS}")

            # Groups worksheet
            try:
                self.groups_worksheet = self.spreadsheet.worksheet(self.config.SPREADSHEET_GROUPS)
                logger.info(f"Connected to groups worksheet: {self.config.SPREADSHEET_GROUPS}")
            except gspread.exceptions.WorksheetNotFound:
                self.groups_worksheet = self._create_groups_worksheet()
                logger.info(f"Created groups worksheet: {self.config.SPREADSHEET_GROUPS}")

            # Initialize logging worksheets if enabled
            if self.enable_dashboard_logging:
                self._initialize_dashboard_logs_worksheet()
                self._initialize_fleet_status_worksheet()

        except Exception as e:
            logger.error(f"Failed to initialize worksheets: {e}")
            raise

    def _apply_rate_limiting(self):
        """Apply rate limiting to all worksheets"""
        try:
            if self.assets_worksheet:
                self.assets_worksheet = self.rate_limiter.wrap_worksheet(self.assets_worksheet)
                logger.debug("Applied rate limiting to assets worksheet")

            if self.groups_worksheet:
                self.groups_worksheet = self.rate_limiter.wrap_worksheet(self.groups_worksheet)
                logger.debug("Applied rate limiting to groups worksheet")

            if self.fleet_status_worksheet:
                self.fleet_status_worksheet = self.rate_limiter.wrap_worksheet(self.fleet_status_worksheet)
                logger.debug("Applied rate limiting to fleet_status worksheet")

            if self.dashboard_logs_worksheet:
                self.dashboard_logs_worksheet = self.rate_limiter.wrap_worksheet(self.dashboard_logs_worksheet)
                logger.debug("Applied rate limiting to dashboard_logs worksheet")

            # Also wrap QC Panel worksheets if available
            if self.qc_panel_spreadsheet:
                # We'll wrap individual worksheets as they're accessed
                logger.debug("QC Panel spreadsheet available for rate limiting")

            logger.info("Rate limiting successfully applied to all worksheets")
        except Exception as e:
            logger.error(f"Failed to apply rate limiting: {e}")
            # Continue without rate limiting rather than failing completely
            self.rate_limiter = None

    def _create_groups_worksheet(self):
        """Create groups worksheet with proper headers"""
        try:
            worksheet = self.spreadsheet.add_worksheet(
                title=self.config.SPREADSHEET_GROUPS,
                rows=1000,
                cols=10
            )

            # Set headers
            headers = [
                "group_id", "group_title", "vin", "driver_name",
                "status", "last_updated", "error_count", "created_at",
                "updated_at", "notes"
            ]
            worksheet.update('A1', [headers])

            logger.info(f"Created groups worksheet with headers: {headers}")
            return worksheet

        except Exception as e:
            logger.error(f"Failed to create groups worksheet: {e}")
            raise

    def _initialize_dashboard_logs_worksheet(self):
        """Initialize or create dashboard logs worksheet"""
        try:
            worksheet_name = getattr(self.config, 'SPREADSHEET_DASHBOARD', 'dashboard_logs')

            try:
                self.dashboard_logs_worksheet = self.spreadsheet.worksheet(worksheet_name)
                logger.info(f"Dashboard logs worksheet '{worksheet_name}' already exists")
            except gspread.exceptions.WorksheetNotFound:
                self.dashboard_logs_worksheet = self.spreadsheet.add_worksheet(
                    title=worksheet_name, rows=2000, cols=12
                )

                # Set headers for dashboard logs
                headers = [
                    "timestamp", "event_type", "user_id", "chat_id", "command",
                    "vin", "driver_name", "success", "error_message", "duration_ms",
                    "session_data", "notes"
                ]
                self.dashboard_logs_worksheet.update('A1', [headers])
                logger.info(f"Created dashboard logs worksheet '{worksheet_name}'")

        except Exception as e:
            logger.error(f"Failed to initialize dashboard logs worksheet: {e}")

    def _initialize_fleet_status_worksheet(self):
        """Initialize or create fleet status worksheet"""
        try:
            worksheet_name = getattr(self.config, 'SPREADSHEET_FLEET_STATUS', 'fleet_status')

            try:
                self.fleet_status_worksheet = self.spreadsheet.worksheet(worksheet_name)
                logger.info(f"Fleet status worksheet '{worksheet_name}' already exists")
            except gspread.exceptions.WorksheetNotFound:
                self.fleet_status_worksheet = self.spreadsheet.add_worksheet(
                    title=worksheet_name, rows=1000, cols=20
                )

                # Set headers for fleet status - match actual worksheet headers
                headers = [
                    "vin", "driver_name", "last_updated", "latitude", "longitude", "address",
                    "speed_mph", "status", "movement_status", "risk_level", "group_chat_id", "last_contact"
                ]
                self.fleet_status_worksheet.update('A1', [headers])
                logger.info(f"Created fleet status worksheet '{worksheet_name}'")

        except Exception as e:
            logger.error(f"Failed to initialize fleet status worksheet: {e}")

    # =====================================================
    # QC PANEL → ASSETS SYNC IMPLEMENTATION
    # =====================================================

    def _open_qc_panel(self):
        """Open QC Panel spreadsheet"""
        if not self.qc_panel_spreadsheet:
            if self.config.QC_PANEL_SPREADSHEET_ID:
                self.qc_panel_spreadsheet = self.gc.open_by_key(self.config.QC_PANEL_SPREADSHEET_ID)
            else:
                raise ValueError("QC_PANEL_SPREADSHEET_ID not configured")
        return self.qc_panel_spreadsheet

    @staticmethod
    def _norm(s):
        """Normalize string value"""
        return str(s or "").strip()

    @staticmethod
    def _norm_vin(s):
        """Normalize VIN value"""
        return str(s or "").strip().upper()

    @staticmethod
    def _norm_driver(s):
        """Normalize driver name so that variations like 'John Doe / Jane' are handled consistently"""
        if s is None:
            return ""
        # Take text before any '/' or ',' delimiters, common in dual-driver entries
        name = str(s).split("/")[0].split(",")[0]
        return name.strip().lower()

    def _get_groups_records_safe(self):
        """
        Get groups records safely, bypassing gspread header duplication issues with caching
        """
        # Check cache first
        now = datetime.now()
        if (self._groups_records_cache is not None and 
            self._groups_records_cache_ts is not None and
            now - self._groups_records_cache_ts < self._groups_records_cache_duration):
            logger.debug("Using cached groups records")
            return self._groups_records_cache
        
        try:
            # Try the normal method first
            records = self.groups_worksheet.get_all_records()
            # Update cache
            self._groups_records_cache = records
            self._groups_records_cache_ts = now
            logger.debug(f"Cached {len(records)} groups records")
            return records
        except Exception as e:
            if "header row in the worksheet is not unique" in str(e):
                logger.warning("Working around header duplication issue in groups worksheet")
                # Fallback to manual record creation
                try:
                    all_data = self.groups_worksheet.get_all_values()
                    if len(all_data) < 2:
                        records = []
                    else:
                        headers = [h.strip() for h in all_data[0] if h.strip()]  # Remove empty headers
                        data_rows = all_data[1:]

                        records = []
                        for row in data_rows:
                            # Pad row if it's shorter than headers
                            padded_row = row + [''] * (len(headers) - len(row))
                            # Only use non-empty headers
                            record = dict(zip(headers, padded_row[:len(headers)]))
                            records.append(record)

                    logger.info(f"Successfully retrieved {len(records)} records using fallback method")
                    # Update cache
                    self._groups_records_cache = records
                    self._groups_records_cache_ts = now
                    return records
                except Exception as fallback_e:
                    logger.error(f"Fallback method also failed: {fallback_e}")
                    # Return cached data if available, even if stale
                    if self._groups_records_cache is not None:
                        logger.warning("Returning stale cached data due to errors")
                        return self._groups_records_cache
                    return []
            else:
                logger.error(f"Error getting groups records: {e}")
                # Return cached data if available, even if stale
                if self._groups_records_cache is not None:
                    logger.warning("Returning stale cached data due to errors")
                    return self._groups_records_cache
                return []
    
    def _invalidate_groups_cache(self):
        """Invalidate the groups records cache"""
        self._groups_records_cache = None
        self._groups_records_cache_ts = None
        logger.debug("Groups records cache invalidated")

    def col_to_a1(self, n: int) -> str:
        """Convert column number to A1 notation"""
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    def _get_rate_limited_worksheet(self, spreadsheet, worksheet_name: str):
        """Get a worksheet with rate limiting applied"""
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            if self.rate_limiter:
                worksheet = self.rate_limiter.wrap_worksheet(worksheet)
            return worksheet
        except Exception as e:
            logger.error(f"Failed to get worksheet {worksheet_name}: {e}")
            return None

    def get_active_load_map(self) -> dict:
        """
        Build dict of active loads keyed by VIN and by driver (fallback).
        Include only rows where STS OF DEL (col S) is one of the monitored statuses.
        Exact QC PANEL columns:
          D '#' (Load id), T 'PU APT', U 'PU ADDRESS', V 'DEL APT', W 'DEL ADDRESS'
          R 'STS OF PU', S 'STS OF DEL'
        """
        if not self.config.QC_PANEL_SPREADSHEET_ID:
            logger.warning("QC Panel spreadsheet not configured")
            return {}

        try:
            sh = self._open_qc_panel()
            tabs = [t.strip() for t in (self.config.QC_ACTIVE_TABS or "").split(",") if t.strip()]
            
            # Fix: Ensure RISK_MONITOR_DEL_STATUSES is a string, not boolean
            risk_statuses = self.config.RISK_MONITOR_DEL_STATUSES
            if isinstance(risk_statuses, bool) or risk_statuses is None:
                risk_statuses = "IN TRANSIT,WILL BE LATE,AT SHIPPER"  # Default values
                logger.warning(f"RISK_MONITOR_DEL_STATUSES is not a string, using defaults: {risk_statuses}")
            
            watch = {x.strip().upper() for x in str(risk_statuses).split(",") if x.strip()}
            out = {}

            for tab in tabs:
                try:
                    ws = self._get_rate_limited_worksheet(sh, tab)
                    if not ws:
                        continue
                    # Use safe method for QC Panel worksheets
                    try:
                        rows = ws.get_all_records()  # assumes header row 1
                    except Exception as e:
                        if "header row in the worksheet is not unique" in str(e):
                            logger.warning(f"Working around header duplication issue in QC Panel tab '{tab}'")
                            try:
                                all_data = ws.get_all_values()
                                if len(all_data) < 2:
                                    continue
                                headers = [h.strip() for h in all_data[0] if h.strip()]
                                data_rows = all_data[1:]
                                rows = []
                                for row in data_rows:
                                    padded_row = row + [''] * (len(headers) - len(row))
                                    record = dict(zip(headers, padded_row[:len(headers)]))
                                    rows.append(record)
                                logger.info(f"Successfully retrieved {len(rows)} QC Panel records using fallback method for tab '{tab}'")
                            except Exception as fallback_e:
                                logger.error(f"Fallback method failed for QC Panel tab '{tab}': {fallback_e}")
                                continue
                        else:
                            logger.error(f"Error getting QC Panel records from tab '{tab}': {e}")
                            continue

                    for r in rows:
                        vin     = self._norm_vin(r.get("VIN", ""))
                        driver  = self._norm_driver(r.get("DRIVER", ""))
                        del_sts = self._norm(r.get("STS OF DEL", "")).upper()

                        if del_sts not in watch:
                            continue

                        payload = {
                            "driver_name": self._norm(r.get("DRIVER", "")),
                            "load_id":     self._norm(r.get("#", "")),                 # D
                            "pu_address":  self._norm(r.get("PU ADDRESS", "")),        # U
                            "pu_appt":     self._norm(r.get("PU APT", "")),            # T
                            "del_address": self._norm(r.get("DEL ADDRESS", "")),       # W
                            "del_appt":    self._norm(r.get("DEL APT", "")),           # V
                            "pu_status":   self._norm(r.get("STS OF PU", "")),         # R
                            "del_status":  del_sts,                                     # S
                            "in_transit":  True,
                            "is_late":     del_sts == "WILL BE LATE",
                        }
                        if vin:    out[vin]    = payload
                        if driver: out[driver] = payload
                except Exception as e:
                    logger.error(f"Error processing QC Panel tab '{tab}': {e}")
                    continue

            logger.debug(f"Found {len(out)} active loads from QC Panel")
            return out
        except Exception as e:
            logger.error(f"Error getting active load map: {e}")
            return {}

    def get_active_load_status_for_vin(self, vin: str) -> dict | None:
        """Get active load status for specific VIN with enhanced caching"""
        now = datetime.utcnow()
        cache_ts = getattr(self, "_active_cache_ts", None)
        
        # Enhanced cache duration - 3 minutes instead of 90 seconds to reduce API calls
        cache_duration_seconds = 180
        
        if not cache_ts or (now - cache_ts).seconds > cache_duration_seconds:
            logger.debug(f"Refreshing active load cache (last update: {cache_ts})")
            try:
                self._active_cache = self.get_active_load_map()
                self._active_cache_ts = now
                logger.info(f"Active load cache refreshed with {len(self._active_cache)} entries")
            except Exception as e:
                logger.error(f"Failed to refresh active load cache: {e}")
                # Return stale data if available
                if hasattr(self, '_active_cache') and self._active_cache:
                    logger.warning("Using stale cache data due to refresh failure")
                    return self._active_cache.get(self._norm_vin(vin))
                return None
        else:
            logger.debug("Using cached active load data")
            
        return self._active_cache.get(self._norm_vin(vin))

    def sync_active_loads_to_assets(self) -> int:
        """
        Write Load id + PU/DEL fields into assets (columns O:S) when matched by VIN/Driver.
        Target headers in assets: 'Load id','PU address','PU appt','DEL address','DEL appt'
        """
        if not self.config.QC_PANEL_SPREADSHEET_ID:
            logger.debug("QC Panel not configured, skipping sync")
            return 0

        try:
            active = self.get_active_load_map()
            if not active:
                logger.debug("No active loads found, skipping sync")
                return 0

            ws = self.assets_worksheet
            data = self._get_assets_records_safe()
            
            # Use column mapping for robust column access
            if self.use_column_mapping and self.assets_mapper:
                # Get column indices from column mapping
                c = {
                    "VIN": self.assets_mapper.get_column_index("vin") + 1,  # +1 for 1-based indexing
                    "Driver": self.assets_mapper.get_column_index("driver_name") + 1,
                    "Load": self.assets_mapper.get_column_index("load_id") + 1,
                    "PU_ADDR": self.assets_mapper.get_column_index("pu_address") + 1,
                    "PU_APPT": self.assets_mapper.get_column_index("pu_appt") + 1,
                    "DEL_ADDR": self.assets_mapper.get_column_index("del_address") + 1,
                    "DEL_APPT": self.assets_mapper.get_column_index("del_appt") + 1,
                }
                logger.debug(f"Using column mapping for sync: {c}")
            else:
                # Fallback to header-based column lookup
                headers = ws.row_values(1)
                def col(name):
                    return headers.index(name) + 1 if name in headers else None

                c = {
                    "VIN": col("VIN"),
                    "Driver": col("Driver Name"),
                    "Load": col("Load id"),
                    "PU_ADDR": col("PU address"),
                    "PU_APPT": col("PU appt"),
                    "DEL_ADDR": col("DEL address"),
                    "DEL_APPT": col("DEL appt"),
                }

                # Check if required columns exist
                missing_cols = [name for name, col_idx in c.items() if col_idx is None]
                if missing_cols:
                    logger.warning(f"Missing columns in assets sheet: {missing_cols}")
                    # Add missing columns if needed
                    self._ensure_assets_columns(headers, missing_cols)
                    # Refresh headers and column mapping
                    headers = ws.row_values(1)
                    c = {
                        "VIN": col("VIN"),
                        "Driver": col("Driver Name"),
                        "Load": col("Load id"),
                        "PU_ADDR": col("PU address"),
                        "PU_APPT": col("PU appt"),
                        "DEL_ADDR": col("DEL address"),
                        "DEL_APPT": col("DEL appt"),
                    }

            updates = []
            for i, rec in enumerate(data, start=2):
                # Use column mapping for robust field access
                if self.use_column_mapping and self.assets_mapper:
                    vin = self._norm_vin(rec.get("vin", ""))
                    drv = self._norm_driver(rec.get("driver_name", ""))
                else:
                    vin = self._norm_vin(rec.get("VIN", ""))
                    drv = self._norm_driver(rec.get("Driver Name", ""))

                # Prioritize driver name matching since QC Panel is keyed by driver names
                src = active.get(drv) or active.get(vin)
                if not src:
                    continue

                def rng(ci):
                    return f"{self.col_to_a1(ci)}{i}"

                if c["Load"]:     updates.append({"range": rng(c["Load"]),     "values": [[src["load_id"]]]})
                if c["PU_ADDR"]:  updates.append({"range": rng(c["PU_ADDR"]),  "values": [[src["pu_address"]]]})
                if c["PU_APPT"]:  updates.append({"range": rng(c["PU_APPT"]),  "values": [[src["pu_appt"]]]})
                if c["DEL_ADDR"]: updates.append({"range": rng(c["DEL_ADDR"]), "values": [[src["del_address"]]]})
                if c["DEL_APPT"]: updates.append({"range": rng(c["DEL_APPT"]), "values": [[src["del_appt"]]]})

            if updates:
                ws.batch_update(updates)
                logger.info(f"Synced {len(updates)} load data updates to assets sheet")

            return len(updates)
        except Exception as e:
            logger.error(f"Error syncing active loads to assets: {e}")
            return 0

    def _ensure_assets_columns(self, current_headers: List[str], missing_cols: List[str]):
        """Ensure required columns exist in assets worksheet"""
        try:
            col_mapping = {
                "Load": "Load id",
                "PU_ADDR": "PU address",
                "PU_APPT": "PU appt",
                "DEL_ADDR": "DEL address",
                "DEL_APPT": "DEL appt"
            }

            new_headers = current_headers.copy()
            for col_key in missing_cols:
                if col_key in col_mapping:
                    new_headers.append(col_mapping[col_key])

            # Update header row
            self.assets_worksheet.update('1:1', [new_headers])
            logger.info(f"Added missing columns to assets sheet: {[col_mapping.get(c, c) for c in missing_cols]}")
        except Exception as e:
            logger.error(f"Error adding columns to assets sheet: {e}")

    # Routing helper (VIN -> destination chats)
    def resolve_destinations(self, vin: str) -> list[int]:
        """
        Returns [qc_group_chat_id(s) from groups sheet for VIN] + [management].
        """
        try:
            chat_ids = list(self.lookup_group_ids_by_vin(vin) or [])
            mgmt = getattr(self.config, "MGMT_CHAT_ID", None)
            if mgmt:
                # Handle both single ID and comma-separated list
                if isinstance(mgmt, str) and ',' in mgmt:
                    mgmt_ids = [int(x.strip()) for x in mgmt.split(',') if x.strip()]
                    chat_ids.extend(mgmt_ids)
                elif mgmt:
                    try:
                        chat_ids.append(int(mgmt))
                    except ValueError:
                        logger.warning(f"Invalid MGMT_CHAT_ID format: {mgmt}")

            # Add QC team chat if configured
            if self.config.QC_TEAM_CHAT_ID:
                chat_ids.append(self.config.QC_TEAM_CHAT_ID)

            # De-duplicate
            return list(dict.fromkeys(chat_ids))
        except Exception as e:
            logger.error(f"Error resolving destinations for VIN {vin}: {e}")
            return []

    def lookup_group_ids_by_vin(self, vin: str) -> List[int]:
        """Look up group chat IDs associated with a VIN"""
        try:
            if not self.groups_worksheet:
                return []

            records = self._get_groups_records_safe()
            group_ids = []

            for record in records:
                if (record.get('vin', '').upper().strip() == vin.upper().strip() and
                    record.get('status', '').upper() == 'ACTIVE'):
                    group_id = record.get('group_id')
                    if group_id:
                        try:
                            group_ids.append(int(group_id))
                        except ValueError:
                            continue

            return group_ids
        except Exception as e:
            logger.error(f"Error looking up group IDs for VIN {vin}: {e}")
            return []

    # =====================================================
    # EXISTING METHODS (unchanged)
    # =====================================================

    def get_all_driver_names(self) -> List[str]:
        """Get all driver names from assets worksheet with enhanced caching"""
        try:
            # Enhanced cache duration - 10 minutes for driver names as they don't change frequently
            enhanced_cache_duration = timedelta(minutes=10)
            
            # Use cache if available and fresh
            if (self.last_fetch_time and
                datetime.now() - self.last_fetch_time < enhanced_cache_duration and
                self.cached_driver_names):
                logger.debug(f"Using cached driver names ({len(self.cached_driver_names)} entries)")
                return self.cached_driver_names

            # Get all records from assets worksheet - use safe method
            records = self._get_assets_records_safe()

            driver_names = []
            seen_names = set()

            for record in records:
                # Use column mapping for robust access
                if self.use_column_mapping and self.assets_mapper:
                    driver_name = record.get('driver_name', '')
                else:
                    # Fallback to header-based access
                    driver_name = record.get('driver_name', '')
                
                driver_name = str(driver_name).strip()

                # Skip empty or obviously invalid names
                if (driver_name and
                    driver_name not in seen_names and
                    driver_name.lower() != 'driver name' and  # Skip header
                    len(driver_name) > 2 and  # Skip too short names
                    not driver_name.startswith('#')):  # Skip error values

                    driver_names.append(driver_name)
                    seen_names.add(driver_name)

            # Update cache
            self.cached_driver_names = driver_names
            self.last_fetch_time = datetime.now()

            logger.info(f"Loaded {len(driver_names)} driver names from assets worksheet")
            return driver_names

        except Exception as e:
            logger.error(f"Error getting driver names: {e}")
            return []

    def find_vin_by_driver_name(self, driver_name: str) -> Optional[str]:
        """Find VIN by driver name using improved DriverNameMatcher"""
        try:
            # Use the improved DriverNameMatcher
            from driver_name_matcher import DriverNameMatcher
            
            # Create matcher instance (it will build its own cache)
            matcher = DriverNameMatcher(self)
            
            # Use the improved matching logic
            vin = matcher.find_vin_for_driver(driver_name)
            
            if vin:
                logger.info(f"Driver match found: '{driver_name}' -> VIN: {vin}")
                return vin.upper()
            else:
                logger.warning(f"No match found for driver name: '{driver_name}'")
                return None

        except Exception as e:
            logger.error(f"Error finding VIN by driver name '{driver_name}': {e}")
            return None

    def find_similar_driver_names(self, search_name: str) -> List[str]:
        """Find similar driver names for suggestions using improved DriverNameMatcher"""
        try:
            # Use the improved DriverNameMatcher
            from driver_name_matcher import DriverNameMatcher
            
            # Create matcher instance
            matcher = DriverNameMatcher(self)
            
            # Get all driver names from the cache
            all_driver_names = list(matcher.driver_vin_cache.keys())
            
            # Filter out partial names (keep only full names for suggestions)
            full_names = []
            for name in all_driver_names:
                # Skip partial names (single words that are likely first/last names)
                if len(name.split()) >= 2:
                    full_names.append(name)
            
            # Use fuzzy matching to find similar names
            from fuzzywuzzy import process
            
            if full_names:
                # Find top 10 matches
                matches = process.extract(
                    search_name, 
                    full_names, 
                    limit=10,
                    scorer=lambda s1, s2: max(
                        # Try different scoring methods
                        process.fuzz.ratio(s1.lower(), s2.lower()),
                        process.fuzz.partial_ratio(s1.lower(), s2.lower()),
                        process.fuzz.token_sort_ratio(s1.lower(), s2.lower())
                    )
                )
                
                # Filter by minimum confidence and return names
                suggestions = []
                for name, score in matches:
                    if score >= 60:  # Minimum 60% confidence
                        suggestions.append(name)
                
                logger.info(f"Found {len(suggestions)} similar names for '{search_name}': {suggestions[:5]}")
                return suggestions[:10]  # Return top 10
            
            return []

        except Exception as e:
            logger.error(f"Error finding similar driver names for '{search_name}': {e}")
            return []

    def get_driver_contact_info_by_vin(self, vin: str) -> Tuple[Optional[str], Optional[str]]:
        """Get driver name and phone by VIN - HARDCODED column indices for reliability"""
        try:
            # HARDCODED COLUMN INDICES (to prevent future confusion):
            # Column 4 (index 3): Driver Name
            # Column 5 (index 4): VIN  
            # Column 12 (index 11): Phone
            DRIVER_NAME_COL = 3
            VIN_COL = 4
            PHONE_COL = 11
            
            # Get raw data directly from worksheet
            assets_worksheet = self.assets_worksheet
            all_data = assets_worksheet.get_all_values()
            
            if len(all_data) < 2:
                return None, None
                
            vin_upper = vin.upper().strip()
            
            # Skip header row, search data rows
            for row_data in all_data[1:]:
                if len(row_data) > max(DRIVER_NAME_COL, VIN_COL, PHONE_COL):
                    # Check VIN match using hardcoded column index
                    row_vin = str(row_data[VIN_COL]).upper().strip() if len(row_data) > VIN_COL else ''
                    
                    if row_vin == vin_upper:
                        # Get driver name using hardcoded column index
                        driver_name = str(row_data[DRIVER_NAME_COL]).strip() if len(row_data) > DRIVER_NAME_COL else ''
                        
                        # Handle multiple driver names (data quality fix)
                        if driver_name and ' / ' in driver_name:
                            # Take the first driver name when multiple names are present
                            driver_name = driver_name.split(' / ')[0].strip()
                            logger.debug(f"Multiple drivers found for VIN {vin}, using first: '{driver_name}'")
                        
                        # Get phone using hardcoded column index
                        phone = str(row_data[PHONE_COL]).strip() if len(row_data) > PHONE_COL else ''
                        
                        logger.debug(f"Contact info for VIN {vin}: Driver: '{driver_name}', Phone: '{phone}'")
                        return driver_name if driver_name else None, phone if phone else None
            
            logger.debug(f"No contact info found for VIN: {vin}")
            return None, None

        except Exception as e:
            logger.error(f"Error getting contact info for VIN {vin}: {e}")
            return None, None

    def get_driver_name_by_vin(self, vin: str) -> Optional[str]:
        """Get driver name by VIN - HARDCODED column indices for reliability"""
        try:
            # HARDCODED COLUMN INDICES (to prevent future confusion):
            # Column 4 (index 3): Driver Name
            # Column 5 (index 4): VIN
            DRIVER_NAME_COL = 3
            VIN_COL = 4
            
            # Get raw data directly from worksheet
            assets_worksheet = self.assets_worksheet
            all_data = assets_worksheet.get_all_values()
            
            if len(all_data) < 2:
                return None
                
            vin_upper = vin.upper().strip()
            
            # Skip header row, search data rows
            for row_data in all_data[1:]:
                if len(row_data) > max(DRIVER_NAME_COL, VIN_COL):
                    # Check VIN match using hardcoded column index
                    row_vin = str(row_data[VIN_COL]).upper().strip() if len(row_data) > VIN_COL else ''
                    
                    if row_vin == vin_upper:
                        # Get driver name using hardcoded column index
                        driver_name = str(row_data[DRIVER_NAME_COL]).strip() if len(row_data) > DRIVER_NAME_COL else ''
                        
                        # Handle multiple driver names (data quality fix)
                        if driver_name and ' / ' in driver_name:
                            # Take the first driver name when multiple names are present
                            driver_name = driver_name.split(' / ')[0].strip()
                            logger.debug(f"Multiple drivers found for VIN {vin}, using first: '{driver_name}'")
                        
                        if driver_name:
                            logger.debug(f"Driver name for VIN {vin}: '{driver_name}'")
                            return driver_name

            logger.debug(f"No driver name found for VIN: {vin}")
            return None

        except Exception as e:
            logger.error(f"Error getting driver name for VIN {vin}: {e}")
            return None

    def get_driver_contact_info(self, driver_name: str) -> Tuple[Optional[str], Optional[str]]:
        """Get driver contact info by name - FIXED to search in Driver Name column"""
        try:
            records = self._get_assets_records_safe()
            driver_name_lower = driver_name.lower().strip()

            for record in records:
                # Search in Driver Name column
                sheet_driver_name = str(record.get('driver_name', '')).lower().strip()

                if sheet_driver_name == driver_name_lower:
                    # Return the actual driver name (with proper casing) and phone
                    actual_name = str(record.get('driver_name', '')).strip()
                    phone = str(record.get('phone', '')).strip()

                    logger.info(f"Contact info for driver '{driver_name}': Phone: '{phone}'")
                    return actual_name, phone if phone else None

            logger.warning(f"No contact info found for driver: {driver_name}")
            return None, None

        except Exception as e:
            logger.error(f"Error getting contact info for driver '{driver_name}': {e}")
            return None, None

    # Group management functions
    def get_group_vin(self, group_id: int) -> Optional[str]:
        """Get VIN for a group from groups worksheet"""
        try:
            if not self.groups_worksheet:
                logger.error("Groups worksheet not initialized")
                return None

            records = self._get_groups_records_safe()

            for record in records:
                if int(record.get('group_id', 0)) == group_id:
                    vin = str(record.get('vin', '')).strip()
                    if vin:
                        logger.info(f"Found VIN for group {group_id}: {vin}")
                        return vin.upper()

            logger.info(f"No VIN found for group {group_id}")
            return None

        except Exception as e:
            logger.error(f"Error getting group VIN for {group_id}: {e}")
            return None

    def save_group_vin(self, group_id: int, group_title: str, vin: str, driver_name: Optional[str] = None) -> bool:
        """Save VIN for a group to groups worksheet"""
        try:
            if not self.groups_worksheet:
                logger.error("Groups worksheet not initialized")
                return False

            # Check if group already exists
            records = self._get_groups_records_safe()
            existing_row = None

            for i, record in enumerate(records):
                if int(record.get('group_id', 0)) == group_id:
                    existing_row = i + 2  # +2 because sheets are 1-indexed and we skip header
                    break

            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Get driver name if not provided
            if not driver_name and vin:
                driver_name = self.get_driver_name_by_vin(vin)

            # Row data matching the actual headers: ['group_id', 'group_title', 'vin', 'status', 'last_updated', 'error_count']
            row_data = [
                group_id,
                group_title,
                vin.upper(),
                'ACTIVE',  # status
                current_time,  # last_updated
                0,  # error_count
            ]

            if existing_row:
                # Update existing row
                self.groups_worksheet.update(f'A{existing_row}', [row_data])
                logger.info(f"Updated group {group_id} with VIN {vin}")
            else:
                # Add new row
                self.groups_worksheet.append_row(row_data)
                logger.info(f"Added new group {group_id} with VIN {vin}")

            # Invalidate cache since groups data changed
            self._invalidate_groups_cache()
            return True

        except Exception as e:
            logger.error(f"Error saving group VIN for {group_id}: {e}")
            return False

    # Enhanced logging functions

    def log_dashboard_event(self, event_type: str, user_id: int, chat_id: int,
                           command: str, vin: Optional[str] = None,
                           driver_name: Optional[str] = None, success: bool = True,
                           error_message: Optional[str] = None, duration_ms: int = 0,
                           session_data: Optional[str] = None) -> bool:
        """Log dashboard events to dashboard logs worksheet for user analytics"""
        if not self.enable_dashboard_logging or not self.dashboard_logs_worksheet:
            return False

        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            row_data = [
                current_time,
                event_type,
                user_id,
                chat_id,
                command,
                vin or '',
                driver_name or '',
                'SUCCESS' if success else 'FAILED',
                error_message or '',
                duration_ms,
                session_data or '',  # session_data for context
                f'Event logged at {current_time}'
            ]

            self.dashboard_logs_worksheet.append_row(row_data)
            logger.debug(f"Logged dashboard event: {event_type} - {command} for user {user_id}")
            return True

        except Exception as e:
            logger.error(f"Error logging dashboard event: {e}")
            return False

    def log_command_execution(self, user_id: int, chat_id: int, command: str,
                             success: bool = True, error_message: Optional[str] = None,
                             vin: Optional[str] = None, driver_name: Optional[str] = None,
                             duration_ms: int = 0, extra_info: Optional[str] = None) -> bool:
        """Convenience method for logging command executions"""
        return self.log_dashboard_event(
            event_type="COMMAND_EXECUTION",
            user_id=user_id,
            chat_id=chat_id,
            command=command,
            vin=vin,
            driver_name=driver_name,
            success=success,
            error_message=error_message,
            duration_ms=duration_ms,
            session_data=extra_info
        )

    def log_user_interaction(self, user_id: int, chat_id: int, interaction_type: str,
                            details: Optional[str] = None, success: bool = True) -> bool:
        """Log user interactions like button clicks, menu selections, etc."""
        return self.log_dashboard_event(
            event_type="USER_INTERACTION",
            user_id=user_id,
            chat_id=chat_id,
            command=interaction_type,
            success=success,
            session_data=details
        )

    def _get_fleet_status_records_safe(self):
        """
        Get fleet status records safely, bypassing gspread header duplication issues
        """
        try:
            # Try the normal method first
            return self.fleet_status_worksheet.get_all_records()
        except Exception as e:
            if "header row in the worksheet is not unique" in str(e):
                logger.warning("Working around header duplication issue in fleet status worksheet")
                # Fallback to manual record creation
                try:
                    all_data = self.fleet_status_worksheet.get_all_values()
                    if len(all_data) < 2:
                        return []

                    headers = [h.strip() for h in all_data[0] if h.strip()]  # Remove empty headers
                    data_rows = all_data[1:]

                    records = []
                    for row in data_rows:
                        # Pad row if it's shorter than headers
                        padded_row = row + [''] * (len(headers) - len(row))
                        # Only use non-empty headers
                        record = dict(zip(headers, padded_row[:len(headers)]))
                        records.append(record)

                    logger.info(f"Successfully retrieved {len(records)} fleet status records using fallback method")
                    return records
                except Exception as fallback_e:
                    logger.error(f"Fallback method also failed: {fallback_e}")
                    return []
            else:
                logger.error(f"Error getting fleet status records: {e}")
                return []

    def _get_assets_records_safe(self):
        """
        Get assets records safely using column mapping or fallback to header-based approach
        """
        try:
            if self.use_column_mapping and self.assets_mapper:
                # Use column mapping for robust access
                all_data = self.assets_worksheet.get_all_values()
                if len(all_data) < 2:
                    return []
                
                data_rows = all_data[1:]  # Skip header row
                records = []
                
                for row in data_rows:
                    # Convert row to dictionary using column mapping
                    record = self.assets_mapper.create_row_dict(row)
                    # Only include rows with valid VIN
                    if record.get('vin'):
                        records.append(record)
                
                logger.debug(f"Retrieved {len(records)} assets records using column mapping")
                return records
            else:
                # Fallback to header-based approach
                return self._get_assets_records_header_based()
        except Exception as e:
            logger.error(f"Error getting assets records: {e}")
            # Try fallback method
            try:
                return self._get_assets_records_header_based()
            except Exception as fallback_e:
                logger.error(f"Fallback method also failed: {fallback_e}")
                return []

    def _get_assets_records_header_based(self):
        """Fallback method using header-based record retrieval"""
        try:
            # Try the normal method first
            return self.assets_worksheet.get_all_records()
        except Exception as e:
            if "header row in the worksheet is not unique" in str(e):
                logger.warning("Working around header duplication issue in assets worksheet")
                # Fallback to manual record creation
                try:
                    all_data = self.assets_worksheet.get_all_values()
                    if len(all_data) < 2:
                        return []

                    headers = [h.strip() for h in all_data[0] if h.strip()]  # Remove empty headers
                    data_rows = all_data[1:]

                    records = []
                    for row in data_rows:
                        # Pad row if it's shorter than headers
                        padded_row = row + [''] * (len(headers) - len(row))
                        # Only use non-empty headers
                        record = dict(zip(headers, padded_row[:len(headers)]))
                        records.append(record)

                    logger.info(f"Successfully retrieved {len(records)} assets records using header fallback method")
                    return records
                except Exception as fallback_e:
                    logger.error(f"Header fallback method also failed: {fallback_e}")
                    return []
            else:
                raise e

    def update_fleet_status_sheet(self, trucks: List[Dict[str, Any]]) -> bool:
        """Update fleet_status worksheet with current truck location data from TMS"""
        # Use the fixed method that updates fleet_status worksheet
        try:
            return self.update_fleet_status_sheet_fixed(trucks)
        except Exception as e:
            logger.error(f"Error in fleet_status update: {e}")
            return False

    def update_asset_tracking_sheet(self, trucks: List[Dict[str, Any]]) -> bool:
        """DEPRECATED: Use update_fleet_status_sheet instead - this was updating fleet_status worksheet"""
        logger.warning("update_asset_tracking_sheet is deprecated, use update_fleet_status_sheet instead")
        return self.update_fleet_status_sheet(trucks)

    def cleanup_fleet_status_duplicates(self, dry_run: bool = True) -> Dict[str, Any]:
        """
        Clean up duplicate entries in fleet_status worksheet (EMERGENCY CLEANUP)

        Args:
            dry_run: If True, only analyze without making changes

        Returns:
            Dictionary with cleanup statistics
        """
        if not self.fleet_status_worksheet:
            return {"error": "Fleet status worksheet not available"}

        try:
            logger.info("Starting fleet status cleanup analysis...")

            # Get all data
            all_data = self.fleet_status_worksheet.get_all_values()
            if len(all_data) < 2:
                return {"error": "No data to clean"}

            headers = all_data[0]
            data_rows = all_data[1:]

            # Group by VIN to find duplicates
            vin_groups = {}
            for i, row in enumerate(data_rows):
                if len(row) > 0:
                    vin = str(row[0]).strip().upper()
                    if vin:
                        if vin not in vin_groups:
                            vin_groups[vin] = []
                        vin_groups[vin].append({"row_index": i + 2, "data": row})  # +2 for header and 1-based indexing

            # Find duplicates
            duplicates = {vin: entries for vin, entries in vin_groups.items() if len(entries) > 1}
            total_rows = len(data_rows)
            duplicate_count = sum(len(entries) - 1 for entries in duplicates.values())  # Keep latest, remove others

            cleanup_stats = {
                "total_rows": total_rows,
                "unique_vins": len(vin_groups),
                "duplicate_entries": duplicate_count,
                "vins_with_duplicates": len(duplicates),
                "estimated_cells": total_rows * len(headers),
                "cells_to_remove": duplicate_count * len(headers)
            }

            logger.info(f"Cleanup analysis: {cleanup_stats}")

            if not dry_run and duplicate_count > 0:
                logger.warning("PERFORMING ACTUAL CLEANUP - This will delete duplicate rows!")

                # Sort rows to delete in reverse order (highest row numbers first)
                rows_to_delete = []
                for vin, entries in duplicates.items():
                    # Keep the latest entry (last in list), delete others
                    entries_to_delete = sorted(entries[:-1], key=lambda x: x["row_index"], reverse=True)
                    rows_to_delete.extend([entry["row_index"] for entry in entries_to_delete])

                rows_to_delete.sort(reverse=True)

                # Delete rows (from bottom to top to maintain row numbers)
                deleted_count = 0
                for row_num in rows_to_delete:
                    try:
                        self.fleet_status_worksheet.delete_rows(row_num)
                        deleted_count += 1
                        logger.info(f"Deleted duplicate row {row_num}")

                        # Add delay to avoid rate limiting
                        import time
                        time.sleep(0.1)

                    except Exception as e:
                        logger.error(f"Error deleting row {row_num}: {e}")

                cleanup_stats["rows_deleted"] = deleted_count
                logger.info(f"Cleanup complete: deleted {deleted_count} duplicate rows")

            return cleanup_stats

        except Exception as e:
            logger.error(f"Error during fleet status cleanup: {e}")
            return {"error": str(e)}

    def update_fleet_status_sheet_fixed(self, trucks: List[Dict[str, Any]]) -> bool:
        """
        Update fleet_status worksheet with TMS truck data - properly handles existing rows
        without creating duplicates. Updates fleet_status not assets!
        """
        if not self.enable_dashboard_logging or not self.fleet_status_worksheet:
            return False

        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Get existing data using safe method to handle header issues
            try:
                all_data = self.fleet_status_worksheet.get_all_values()
                if len(all_data) < 2:
                    # Use correct fleet_status headers
                    headers = [
                        "vin", "driver_name", "last_updated", "latitude", "longitude", "address",
                        "speed_mph", "status", "movement_status", "risk_level", "group_chat_id", "last_contact"
                    ]
                    self.fleet_status_worksheet.update('A1', [headers])
                    existing_records = []
                else:
                    existing_records = all_data[1:]

                # Create VIN to row mapping
                existing_vins = {}
                for i, row in enumerate(existing_records):
                    if len(row) > 0 and row[0]:
                        vin = str(row[0]).strip().upper()
                        existing_vins[vin] = i + 2  # +2 for header and 1-based indexing

            except Exception as e:
                logger.error(f"Error reading existing data: {e}")
                return False

            updates_made = 0
            batch_updates = []

            for truck in trucks:
                vin = str(truck.get('vin', '')).upper()
                if not vin:
                    continue

                # Get driver name from assets worksheet
                try:
                    driver_name = self.get_driver_name_by_vin(vin)
                except Exception:
                    driver_name = None

                # Format row data to match fleet_status headers
                row_data = [
                    vin,                                        # vin
                    driver_name or truck.get('name', ''),      # driver_name
                    current_time,                               # last_updated
                    truck.get('lat', ''),                       # latitude
                    truck.get('lng', ''),                       # longitude
                    truck.get('address', ''),                   # address
                    truck.get('speed', 0),                      # speed_mph
                    truck.get('status', ''),                    # status
                    'MOVING' if truck.get('speed', 0) > 2 else 'STOPPED',  # movement_status
                    'NORMAL',                                   # risk_level
                    '',                                         # group_chat_id (filled when group registers)
                    current_time                                # last_contact
                ]

                if vin in existing_vins:
                    # UPDATE existing row - use batch updates for efficiency
                    row_num = existing_vins[vin]
                    batch_updates.append({
                        'range': f'A{row_num}:L{row_num}',  # A to L = 12 columns for fleet_status
                        'values': [row_data]
                    })
                    updates_made += 1
                else:
                    # DO NOT add new rows due to 10M cell limit
                    logger.debug(f"Skipping new VIN {vin} - only updating existing records due to cell limit")

                # Limit updates to prevent overload
                if updates_made >= 50:  # Process max 50 trucks per run
                    break

            # Execute batch updates
            if batch_updates:
                try:
                    self.fleet_status_worksheet.batch_update(batch_updates)
                    logger.info(f"Batch updated {len(batch_updates)} existing records")
                except Exception as e:
                    logger.error(f"Batch update failed: {e}")
                    # Fall back to individual updates
                    for update in batch_updates:
                        try:
                            range_name = update['range']
                            values = update['values']
                            self.fleet_status_worksheet.update(range_name, values)
                        except Exception as individual_e:
                            logger.error(f"Individual update failed for {range_name}: {individual_e}")

            logger.info(f"Updated {updates_made} fleet_status records")
            return True

        except Exception as e:
            logger.error(f"Error in fleet_status sheet update: {e}")
            return False

    def update_assets_with_current_data(self, limit: int = None) -> Dict[str, Any]:
        """
        Manual command to update assets worksheet with current TMS truck data

        Args:
            limit: Maximum number of assets to update in one run

        Returns:
            Dictionary with update statistics
        """
        if not self.assets_worksheet:
            return {"error": "Assets worksheet not available"}

        try:
            from tms_integration import TMSIntegration
            from robust_sheets_writer import RobustSheetsWriter

            # Initialize TMS integration
            tms = TMSIntegration(self.config)

            # Load current truck data from TMS
            logger.info("Loading current truck data from TMS...")
            trucks = tms.load_truck_list()

            if not trucks:
                return {"error": "No truck data available from TMS"}

            logger.info(f"Retrieved {len(trucks)} trucks from TMS")

            # Apply limit only if specified (remove artificial cap)
            if limit is not None and limit > 0:
                trucks = trucks[:limit]
                logger.info(f"Limited to {limit} trucks for this run")

            # Get existing assets records directly from worksheet to avoid column mapping issues
            try:
                all_values = self.assets_worksheet.get_all_values()
                if len(all_values) < 2:
                    return {"error": "Assets sheet has no data rows"}
                
                headers = all_values[0]
                
                # Find VIN column index
                vin_col_idx = None
                for i, header in enumerate(headers):
                    if 'vin' in header.lower():
                        vin_col_idx = i
                        break
                
                if vin_col_idx is None:
                    return {"error": "Could not find VIN column in assets sheet"}
                
                # Build existing records with proper VIN mapping
                existing_records = []
                for i, row in enumerate(all_values[1:], 2):  # Skip header, start from row 2
                    if len(row) > vin_col_idx:
                        vin = str(row[vin_col_idx]).strip()
                        if vin and len(vin) >= 10:  # Valid VIN
                            record = {'VIN': vin}
                            # Add other fields as needed
                            for j, header in enumerate(headers):
                                if j < len(row):
                                    record[header] = row[j]
                            existing_records.append(record)
                
                logger.info(f"Loaded {len(existing_records)} existing records from assets sheet")
                
            except Exception as e:
                logger.error(f"Error loading existing records: {e}")
                return {"error": f"Failed to load existing records: {e}"}

            # Use robust writer with safety flag to prevent automatic VIN addition
            writer = RobustSheetsWriter(self.assets_worksheet, chunk_size=200, allow_new_trucks=False)
            return writer.write_tms_data_to_assets(trucks, existing_records, headers)

        except Exception as e:
            logger.error(f"Error updating assets with current data: {e}", exc_info=True)
            return {"error": f"Update failed: {e}"}

    def debug_worksheet_columns(self) -> Dict[str, Any]:
        """Debug function to check what columns are available in your assets worksheet"""
        try:
            # Get first few rows to see structure
            header_row = self.assets_worksheet.row_values(1)
            sample_rows = self.assets_worksheet.get_all_values()[:3]  # Header + 2 data rows

            # Get all records to see field names - use safe method
            records = self._get_assets_records_safe()
            sample_record = records[0] if records else {}

            debug_info = {
                "header_row": header_row,
                "sample_rows": sample_rows,
                "available_fields": list(sample_record.keys()) if sample_record else [],
                "sample_record": sample_record,
                "total_records": len(records)
            }

            logger.info("=== WORKSHEET DEBUG INFO ===")
            logger.info(f"Header row: {header_row}")
            logger.info(f"Available fields: {debug_info['available_fields']}")
            logger.info(f"Sample record: {sample_record}")
            logger.info("===========================")

            return debug_info

        except Exception as e:
            logger.error(f"Error debugging worksheet structure: {e}")
            return {"error": str(e)}

    def add_new_truck_to_assets(self, vin: str, driver_name: str = None) -> Dict[str, Any]:
        """
        Manually add a specific truck by VIN to the assets worksheet

        Args:
            vin: The VIN of the truck to add
            driver_name: Optional driver name (recommended for proper tracking)

        Returns:
            Dictionary with operation result
        """
        if not self.assets_worksheet:
            return {"error": "Assets worksheet not available"}

        try:
            from tms_integration import TMSIntegration

            # Initialize TMS integration
            tms = TMSIntegration(self.config)

            # Load current truck data from TMS
            trucks = tms.load_truck_list()

            if not trucks:
                return {"error": "No truck data available from TMS"}

            # Find the specific truck by VIN
            target_truck = None
            vin_upper = vin.strip().upper()

            for truck in trucks:
                if str(truck.get('vin', '')).strip().upper() == vin_upper:
                    target_truck = truck
                    break

            if not target_truck:
                return {"error": f"Truck with VIN {vin_upper} not found in TMS data"}

            # Check if truck already exists in assets
            existing_records = self._get_assets_records_safe()
            for record in existing_records:
                existing_vin = str(record.get('vin', '')).strip().upper()
                if existing_vin == vin_upper:
                    return {"error": f"Truck with VIN {vin_upper} already exists in assets worksheet"}

            # Format truck info for consistent data
            truck_info = tms.format_truck_info(target_truck)
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Prepare new row data matching the assets worksheet structure
            new_row = [
                target_truck.get('vin', ''),  # VIN
                target_truck.get('unit', ''),  # Unit
                driver_name or '',  # Driver Name (use provided name or empty)
                '',  # Phone (empty, to be filled manually)
                truck_info.get('latitude', ''),  # Lat
                truck_info.get('longitude', ''),  # Lng
                truck_info.get('location', ''),  # Current Location
                truck_info.get('speed_display', '0 mph'),  # Speed
                truck_info.get('status', 'Unknown'),  # Status
                current_time,  # Last Updated
                truck_info.get('heading', ''),  # Heading
                truck_info.get('source', 'TMS'),  # Source
                '',  # Load id (empty, to be filled manually)
                '',  # PU address (empty, to be filled manually)
                '',  # PU appt (empty, to be filled manually)
                '',  # DEL address (empty, to be filled manually)
                '',  # DEL appt (empty, to be filled manually)
                '',  # ETA (empty, calculated later)
                'NORMAL',  # Risk Status
                current_time,  # Sync Time
            ]

            # Add the new row to the worksheet
            self.assets_worksheet.append_row(new_row)

            logger.info(f"Successfully added new truck VIN {vin_upper} to assets worksheet")

            return {
                "success": True,
                "vin": vin_upper,
                "unit": target_truck.get('unit', ''),
                "location": truck_info.get('location', ''),
                "status": truck_info.get('status', 'Unknown'),
                "timestamp": current_time
            }

        except Exception as e:
            logger.error(f"Error adding new truck {vin}: {e}")
            return {"error": str(e)}

    def list_new_trucks_found(self, limit: int = 20) -> Dict[str, Any]:
        """
        Get a list of trucks that are in TMS but not in assets worksheet

        Args:
            limit: Maximum number of new trucks to return

        Returns:
            Dictionary with new trucks information
        """
        if not self.assets_worksheet:
            return {"error": "Assets worksheet not available"}

        try:
            from tms_integration import TMSIntegration

            # Initialize TMS integration
            tms = TMSIntegration(self.config)

            # Load current truck data from TMS
            trucks = tms.load_truck_list()

            if not trucks:
                return {"error": "No truck data available from TMS"}

            # Get existing assets VINs
            existing_records = self._get_assets_records_safe()
            existing_vins = set()

            for record in existing_records:
                vin = str(record.get('vin', '')).strip().upper()
                if vin:
                    existing_vins.add(vin)

            # Find new trucks
            new_trucks = []
            for truck in trucks:
                vin = str(truck.get('vin', '')).strip().upper()
                if vin and vin not in existing_vins:
                    truck_info = tms.format_truck_info(truck)
                    new_trucks.append({
                        "vin": vin,
                        "unit": truck.get('unit', ''),
                        "location": truck_info.get('location', ''),
                        "status": truck_info.get('status', 'Unknown'),
                        "name": truck_info.get('name', 'Unknown')
                    })

                if len(new_trucks) >= limit:
                    break

            logger.info(f"Found {len(new_trucks)} new trucks not in assets worksheet")

            return {
                "success": True,
                "new_trucks": new_trucks,
                "total_found": len(new_trucks),
                "existing_assets": len(existing_vins),
                "total_tms_trucks": len(trucks)
            }

        except Exception as e:
            logger.error(f"Error listing new trucks: {e}")
            return {"error": str(e)}


def test_google_integration(config: Config) -> bool:
    """Test Google Sheets integration with enhanced driver name testing and QC Panel sync"""
    try:
        print("🧪 Testing Google Sheets integration with QC Panel sync...")

        # Create integration instance
        google_integration = GoogleSheetsIntegration(config)

        # Test basic connection
        driver_names = google_integration.get_all_driver_names()

        if driver_names:
            print(f"✅ Successfully loaded {len(driver_names)} driver names from Google Sheets")

            # Show sample driver names
            print(f"📋 Sample driver names: {driver_names[:5]}")

            # Test the specific VIN that was showing wrong name
            test_vin = "4V4NC9EH7PN336858"
            driver_name, phone = google_integration.get_driver_contact_info_by_vin(test_vin)

            if driver_name:
                print(f"✅ Driver name for VIN {test_vin}: '{driver_name}' (Phone: {phone})")

                # Test reverse lookup
                found_vin = google_integration.find_vin_by_driver_name(driver_name)
                if found_vin:
                    print(f"✅ Reverse lookup successful: '{driver_name}' -> VIN: {found_vin}")
                else:
                    print(f"⚠️ Reverse lookup failed for: '{driver_name}'")
            else:
                print(f"❌ No driver name found for VIN: {test_vin}")

            # Test QC Panel integration if configured
            if config.QC_PANEL_SPREADSHEET_ID:
                print(f"🔄 Testing QC Panel sync...")
                active_loads = google_integration.get_active_load_map()
                print(f"✅ Found {len(active_loads)} active loads from QC Panel")

                # Test sync to assets
                updates = google_integration.sync_active_loads_to_assets()
                print(f"✅ Synced {updates} load updates to assets sheet")

                # Test VIN lookup
                if active_loads:
                    sample_vin = list(active_loads.keys())[0]
                    load_status = google_integration.get_active_load_status_for_vin(sample_vin)
                    if load_status:
                        print(f"✅ Load status lookup: VIN {sample_vin} -> {load_status.get('load_id', 'N/A')}")
            else:
                print(f"⚠️ QC Panel not configured, skipping sync tests")

            # Test worksheet structure
            debug_info = google_integration.debug_worksheet_columns()
            if 'Driver Name' in debug_info.get('available_fields', []):
                print(f"✅ 'Driver Name' column found in worksheet")
            else:
                print(f"❌ 'Driver Name' column not found. Available fields: {debug_info.get('available_fields', [])}")

            print("✅ Google Sheets integration test completed successfully")
            return True
        else:
            print("❌ No driver names loaded from Google Sheets")
            return False

    except Exception as e:
        print(f"❌ Google Sheets integration test failed: {str(e)}")
        return False

    # =====================================================
    # COMPREHENSIVE SHEETS MODEL INTERFACE METHODS
    # =====================================================

    async def upsert_assets_from_tms(self, tms_assets) -> int:
        """Interface to sheets model for assets upsert"""
        if self.sheets_model:
            return await self.sheets_model.upsert_assets_from_tms(tms_assets)
        else:
            logger.error("Sheets model not initialized")
            return 0

    async def register_or_update_group(self, group_id: int, title: str, vin: str,
                                     owner_user_id: Optional[int] = None) -> None:
        """Interface to sheets model for group registration"""
        if self.sheets_model:
            await self.sheets_model.register_or_update_group(group_id, title, vin, owner_user_id)
        else:
            logger.error("Sheets model not initialized")

    async def record_group_rename(self, group_id: int, new_title: str) -> None:
        """Interface to sheets model for group rename tracking"""
        if self.sheets_model:
            await self.sheets_model.record_group_rename(group_id, new_title)
        else:
            logger.error("Sheets model not initialized")

    def batch_update_eld_tracker(self, points) -> int:
        """Interface to sheets model for ELD_tracker F:K batch updates"""
        if self.sheets_model:
            return self.sheets_model.batch_update_eld_tracker(points)
        else:
            logger.error("Sheets model not initialized")
            return 0

    def upsert_fleet_status(self, rows) -> int:
        """Interface to sheets model for fleet_status upserts"""
        if self.sheets_model:
            return self.sheets_model.upsert_fleet_status(rows)
        else:
            logger.error("Sheets model not initialized")
            return 0

    def append_location_logs(self, events) -> int:
        """Interface to sheets model for location_logs appends"""
        if self.sheets_model:
            return self.sheets_model.append_location_logs(events)
        else:
            logger.error("Sheets model not initialized")
            return 0

    def append_ack_audit(self, entry: dict) -> None:
        """Interface to sheets model for ACK audit logging"""
        if self.sheets_model:
            self.sheets_model.append_ack_audit(entry)
        else:
            logger.error("Sheets model not initialized")

    def append_dashboard_hourly(self, summary: dict) -> None:
        """Interface to sheets model for dashboard KPI logging"""
        if self.sheets_model:
            self.sheets_model.append_dashboard_hourly(summary)
        else:
            logger.error("Sheets model not initialized")

    def prune_location_logs_older_than(self, days: int) -> int:
        """Interface to sheets model for retention management"""
        if self.sheets_model:
            return self.sheets_model.prune_location_logs_older_than(days)
        else:
            logger.error("Sheets model not initialized")
            return 0

    def log_severe_error(self, component: str, severity: str, summary: str,
                        detail: str = "", context: str = "") -> None:
        """Interface to sheets model for severe error logging"""
        if self.sheets_model:
            self.sheets_model.log_severe_error(component, severity, summary, detail, context)
        else:
            logger.error("Sheets model not initialized")

    def get_sheets_metrics(self) -> Dict[str, Any]:
        """Get comprehensive sheets model metrics"""
        if self.sheets_model:
            return self.sheets_model.get_metrics()
        else:
            logger.error("Sheets model not initialized")
            return {}

    # =====================================================
    # RATE LIMITING AND MONITORING METHODS
    # =====================================================
    
    def get_rate_limiting_stats(self) -> Dict[str, Any]:
        """Get rate limiting and caching statistics"""
        if not self.rate_limiter:
            return {"error": "Rate limiting not enabled"}
        
        stats = self.rate_limiter.get_cache_stats()
        
        # Add additional stats
        stats.update({
            'driver_names_cache_size': len(self.cached_driver_names),
            'driver_names_cache_age': (datetime.now() - self.last_fetch_time).total_seconds() 
                                    if self.last_fetch_time else None,
            'active_loads_cache_size': len(self._active_cache) if hasattr(self, '_active_cache') else 0,
            'active_loads_cache_age': (datetime.utcnow() - self._active_cache_ts).total_seconds() 
                                    if hasattr(self, '_active_cache_ts') and self._active_cache_ts else None,
            'rate_limiting_enabled': True
        })
        
        return stats
    
    def force_cache_refresh(self) -> Dict[str, Any]:
        """Force refresh of all caches"""
        results = {}
        
        try:
            # Refresh driver names cache
            logger.info("Forcing driver names cache refresh...")
            self.last_fetch_time = None
            self.cached_driver_names = []
            driver_names = self.get_all_driver_names()
            results['driver_names'] = {
                'success': True,
                'count': len(driver_names)
            }
        except Exception as e:
            results['driver_names'] = {
                'success': False,
                'error': str(e)
            }
        
        try:
            # Refresh active loads cache
            logger.info("Forcing active loads cache refresh...")
            self._active_cache_ts = None
            self._active_cache = {}
            active_loads = self.get_active_load_map()
            results['active_loads'] = {
                'success': True,
                'count': len(active_loads)
            }
        except Exception as e:
            results['active_loads'] = {
                'success': False,
                'error': str(e)
            }
        
        # Clean up rate limiter cache if available
        if self.rate_limiter:
            try:
                self.rate_limiter._cleanup_expired_cache()
                results['rate_limiter_cache'] = {
                    'success': True,
                    'message': 'Expired entries cleaned up'
                }
            except Exception as e:
                results['rate_limiter_cache'] = {
                    'success': False,
                    'error': str(e)
                }
        
        return results