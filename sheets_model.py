"""
Comprehensive Sheets Model Implementation
Handles 8 worksheets with proper schemas, cadences, and data governance.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Iterable, Optional, Any, Tuple
from zoneinfo import ZoneInfo
from dataclasses import dataclass
import time
import hashlib

from data_contracts import FleetPoint
from config import Config


logger = logging.getLogger(__name__)


@dataclass
class SheetSchema:
    """Schema definition for a worksheet"""
    name: str
    headers: List[str]
    key_columns: List[str]  # Primary key columns
    required_columns: List[str]
    is_append_only: bool = False


# Worksheet schemas according to specification
SHEET_SCHEMAS = {
    'assets': SheetSchema(
        name='assets',
        headers=[
            'UNIT', 'VIN', 'Driver Name', 'Phone', 'Branch/Team', 'Status',
            'Notes', 'First Seen UTC', 'Last Seen UTC', 'Last Sync Source'
        ],
        key_columns=['VIN'],
        required_columns=['VIN', 'Driver Name']
    ),
    'groups': SheetSchema(
        name='groups',
        headers=[
            'group_id', 'group_title', 'vin', 'status', 'last_updated',
            'owner_user_id', 'last_message_id', 'tz_hint'
        ],
        key_columns=['group_id'],
        required_columns=['group_id', 'vin', 'status']
    ),
    'assets': SheetSchema(
        name='assets',
        headers=[
            'VIN', 'A', 'B', 'C', 'D', 'Last Known Location',
            'Latitude', 'Longitude', 'Status', 'Update Time', 'Source'
        ],
        key_columns=['VIN'],
        required_columns=['VIN']
    ),
    'fleet_status': SheetSchema(
        name='fleet_status',
        headers=[
            'VIN', 'Driver', 'On Load', 'Load ID', 'PU City/State', 'DEL City/State',
            'Appt Time', 'ETA', 'Late?', 'Speed mph', 'Stopped Min', 'Risk Flag',
            'Last Refresh', 'Source'
        ],
        key_columns=['VIN'],
        required_columns=['VIN']
    ),
    'location_logs': SheetSchema(
        name='location_logs',
        headers=[
            'ts_utc', 'ts_ny', 'event_type', 'group_id', 'group_title', 'VIN',
            'Driver', 'lat', 'lon', 'speed_mph', 'status', 'location_str',
            'eta_ny', 'appt_ny', 'late_flag', 'message_id', 'thread_id', 'source'
        ],
        key_columns=[],  # No primary key - append only
        required_columns=['ts_utc', 'event_type', 'VIN'],
        is_append_only=True
    ),
    'dashboard_logs': SheetSchema(
        name='dashboard_logs',
        headers=[
            'date', 'fleet_size', 'updates_sent', 'risk_alerts', 'late_pu',
            'late_del', 'avg_stop_min', 'ors_fallbacks', 'tms_errors', 'telegram_429s'
        ],
        key_columns=['date'],
        required_columns=['date'],
        is_append_only=True
    ),
    'ack_audit': SheetSchema(
        name='ack_audit',
        headers=[
            'ts_ny', 'driver_id', 'stop_type', 'location_hash', 'vin',
            'group_id', 'user_id', 'user_name', 'expires_ny'
        ],
        key_columns=[],  # No primary key - append only
        required_columns=['ts_ny', 'driver_id', 'stop_type', 'vin'],
        is_append_only=True
    ),
    'errors': SheetSchema(
        name='errors',
        headers=['ts_ny', 'component', 'sev', 'summary', 'detail', 'context'],
        key_columns=[],  # No primary key - append only
        required_columns=['ts_ny', 'component', 'sev', 'summary'],
        is_append_only=True
    )
}


class SheetsModelManager:
    """Comprehensive Sheets model manager with proper data governance"""

    def __init__(self, google_integration, config: Config):
        self.google = google_integration
        self.config = config
        self.ny_tz = ZoneInfo('America/New_York')
        self.utc_tz = ZoneInfo('UTC')

        # Outbox for deduplication (in production, use Redis)
        self.location_logs_outbox: Dict[str, datetime] = {}
        self.outbox_ttl = timedelta(hours=24)

        # Performance metrics
        self.metrics = {
            'assets_upserted': 0,
            'groups_updated': 0,
            'eld_tracker_updates': 0,
            'fleet_status_upserted': 0,
            'location_logs_appended': 0,
            'dashboard_entries': 0,
            'ack_entries': 0,
            'errors_logged': 0,
            'retention_pruned': 0
        }

    def _get_ny_time(self, utc_dt: datetime = None) -> str:
        """Get NY timezone formatted string"""
        if utc_dt is None:
            utc_dt = datetime.now(self.utc_tz)

        ny_time = utc_dt.astimezone(self.ny_tz)
        tz_name = ny_time.strftime('%Z')  # EDT or EST
        return ny_time.strftime(f'%Y-%m-%d %H:%M:%S {tz_name}')

    def _normalize_headers(self, headers: List[str]) -> Dict[str, int]:
        """Create normalized header to column index mapping"""
        return {
            header.strip().lower().replace(' ', '_'): i
            for i, header in enumerate(headers)
            if header.strip()
        }

    def _find_header_column(
            self, header_map: Dict[str, int], target: str) -> Optional[int]:
        """Find column index for target header with flexible matching"""
        target_norm = target.lower().replace(' ', '_')

        # Direct match
        if target_norm in header_map:
            return header_map[target_norm]

        # Fuzzy matching for common variations
        alternatives = {
            'vin': ['vehicle_id', 'truck_id'],
            'driver_name': ['driver', 'driver_name'],
            'last_known_location': ['location', 'address', 'last_location'],
            'update_time': ['updated', 'timestamp', 'last_updated'],
            'group_id': ['chat_id', 'telegram_id'],
        }

        if target_norm in alternatives:
            for alt in alternatives[target_norm]:
                if alt in header_map:
                    return header_map[alt]

        return None

    def _get_worksheet_safe(self, sheet_name: str):
        """Get worksheet with proper error handling"""
        try:
            return self.google.spreadsheet.worksheet(sheet_name)
        except Exception as e:
            logger.warning(f"Worksheet '{sheet_name}' not found: {e}")
            return None

    def _ensure_worksheet_headers(
            self,
            worksheet,
            schema: SheetSchema) -> bool:
        """Ensure worksheet has required headers"""
        try:
            if not worksheet:
                return False

            # Get current headers
            try:
                current_headers = worksheet.row_values(1)
            except Exception as e:
                logger.error(f"Failed to read headers from {schema.name}: {e}")
                return False

            # Check if we need to add headers
            if not current_headers or len(
                    current_headers) < len(schema.headers):
                logger.info(f"Updating headers for {schema.name}")
                worksheet.update('1:1', [schema.headers])
                return True

            return True

        except Exception as e:
            logger.error(f"Failed to ensure headers for {schema.name}: {e}")
            return False

    # =====================================================
    # ASSETS WORKSHEET (VIN ↔ driver mapping, nightly sync)
    # =====================================================

    async def upsert_assets_from_tms(
            self,
            tms_assets: Iterable[dict],
            allow_new_trucks: bool = False) -> int:
        """Nightly sync: upsert by VIN, protect human-maintained fields"""
        try:
            worksheet = self._get_worksheet_safe('assets')
            if not worksheet:
                logger.error("Assets worksheet not available")
                return 0

            schema = SHEET_SCHEMAS['assets']
            if not self._ensure_worksheet_headers(worksheet, schema):
                return 0

            # Get all current data
            all_data = worksheet.get_all_values()
            if len(all_data) < 1:
                logger.error("Assets worksheet has no header row")
                return 0

            headers = all_data[0]
            header_map = self._normalize_headers(headers)
            existing_rows = all_data[1:] if len(all_data) > 1 else []

            # Build VIN to row mapping
            vin_col = self._find_header_column(header_map, 'VIN')
            if vin_col is None:
                logger.error("VIN column not found in assets")
                return 0

            vin_to_row = {}
            for i, row in enumerate(existing_rows):
                if len(row) > vin_col and row[vin_col]:
                    vin = str(row[vin_col]).strip().upper()
                    if vin:
                        vin_to_row[vin] = i + 2  # +2 for header and 1-based

            # Process TMS assets
            current_time_utc = datetime.now(self.utc_tz)
            current_time_ny = self._get_ny_time(current_time_utc)

            batch_updates = []
            new_rows = []
            upserted_count = 0

            for asset in tms_assets:
                vin = str(asset.get('vin', '')).strip().upper()
                if not vin or len(vin) != 17:  # VIN validation
                    continue

                unit = str(asset.get('unit', '')).strip()
                driver_name = str(asset.get('driver_name', '')).strip()
                status = 'Active'  # From TMS, assume active
                source = asset.get('source', 'TMS Nightly Sync')

                if vin in vin_to_row:
                    # Update existing row - protect human fields
                    row_num = vin_to_row[vin]
                    existing_row = existing_rows[row_num - 2]

                    # Only update if human fields are blank
                    updates = {}

                    # Always update these fields from TMS
                    unit_col = self._find_header_column(header_map, 'UNIT')
                    if unit_col is not None:
                        updates[unit_col] = unit

                    driver_col = self._find_header_column(
                        header_map, 'Driver Name')
                    if (driver_col is not None and driver_name and (
                            len(existing_row) <= driver_col or not existing_row[driver_col].strip())):
                        updates[driver_col] = driver_name

                    status_col = self._find_header_column(header_map, 'Status')
                    if status_col is not None:
                        updates[status_col] = status

                    last_seen_col = self._find_header_column(
                        header_map, 'Last Seen UTC')
                    if last_seen_col is not None:
                        updates[last_seen_col] = current_time_utc.isoformat()

                    source_col = self._find_header_column(
                        header_map, 'Last Sync Source')
                    if source_col is not None:
                        updates[source_col] = source

                    # Create batch updates
                    for col_idx, value in updates.items():
                        col_letter = chr(65 + col_idx)  # Convert to A, B, C...
                        batch_updates.append({
                            'range': f'{col_letter}{row_num}',
                            'values': [[value]]
                        })

                    upserted_count += 1
                else:
                    # New VIN found - check if we should add it
                    if allow_new_trucks:
                        # New VIN - append row (only if explicitly allowed)
                        new_row = [''] * len(headers)

                        # Fill in the data we have
                        if self._find_header_column(
                                header_map, 'UNIT') is not None:
                            new_row[self._find_header_column(
                                header_map, 'UNIT')] = unit
                        if self._find_header_column(
                                header_map, 'VIN') is not None:
                            new_row[self._find_header_column(
                                header_map, 'VIN')] = vin
                        if self._find_header_column(
                                header_map, 'Driver Name') is not None and driver_name:
                            new_row[self._find_header_column(
                                header_map, 'Driver Name')] = driver_name
                        if self._find_header_column(
                                header_map, 'Status') is not None:
                            new_row[self._find_header_column(
                                header_map, 'Status')] = status
                        if self._find_header_column(
                                header_map, 'First Seen UTC') is not None:
                            new_row[self._find_header_column(
                                header_map, 'First Seen UTC')] = current_time_utc.isoformat()
                        if self._find_header_column(
                                header_map, 'Last Seen UTC') is not None:
                            new_row[self._find_header_column(
                                header_map, 'Last Seen UTC')] = current_time_utc.isoformat()
                        if self._find_header_column(
                                header_map, 'Last Sync Source') is not None:
                            new_row[self._find_header_column(
                                header_map, 'Last Sync Source')] = source

                        new_rows.append(new_row)
                        upserted_count += 1
                        logger.info(
                            f"Will add new truck: {vin} (auto-addition enabled)")
                    else:
                        # Skip new VIN (safety mode)
                        logger.debug(
                            f"Skipped new truck: {vin} (auto-addition disabled for safety)")

            # Execute batch updates
            if batch_updates:
                # Process in chunks to avoid API limits
                chunk_size = 50
                for i in range(0, len(batch_updates), chunk_size):
                    chunk = batch_updates[i:i + chunk_size]
                    try:
                        worksheet.batch_update(chunk)
                        await asyncio.sleep(0.1)  # Rate limiting
                    except Exception as e:
                        logger.error(
                            f"Batch update failed for assets chunk {i//chunk_size}: {e}")

            # Append new rows
            if new_rows:
                try:
                    # Append in chunks
                    chunk_size = 25
                    for i in range(0, len(new_rows), chunk_size):
                        chunk = new_rows[i:i + chunk_size]
                        for row in chunk:
                            worksheet.append_row(row)
                            # Small delay between appends
                            await asyncio.sleep(0.05)
                except Exception as e:
                    logger.error(f"Failed to append new assets: {e}")

            self.metrics['assets_upserted'] += upserted_count
            logger.info(
                f"Assets sync complete: {upserted_count} VINs processed")
            return upserted_count

        except Exception as e:
            logger.error(f"Error in assets upsert: {e}")
            return 0

    # =====================================================
    # GROUPS WORKSHEET (registration + lifecycle)
    # =====================================================

    async def register_or_update_group(
            self,
            group_id: int,
            title: str,
            vin: str,
            owner_user_id: Optional[int] = None) -> None:
        """Register group or update existing registration"""
        try:
            worksheet = self._get_worksheet_safe('groups')
            if not worksheet:
                logger.error("Groups worksheet not available")
                return

            schema = SHEET_SCHEMAS['groups']
            if not self._ensure_worksheet_headers(worksheet, schema):
                return

            # Use safe method to get records
            try:
                all_data = worksheet.get_all_values()
                headers = all_data[0] if all_data else schema.headers
                existing_rows = all_data[1:] if len(all_data) > 1 else []
            except Exception as e:
                logger.error(f"Error reading groups data: {e}")
                return

            header_map = self._normalize_headers(headers)

            # Find existing group
            group_id_col = self._find_header_column(header_map, 'group_id')
            existing_row_num = None

            for i, row in enumerate(existing_rows):
                if (len(row) > group_id_col and
                        str(row[group_id_col]) == str(group_id)):
                    existing_row_num = i + 2  # +2 for header and 1-based
                    break

            current_time_ny = self._get_ny_time()
            vin_upper = vin.strip().upper()

            row_data = [
                str(group_id),  # group_id
                title,          # group_title
                vin_upper,      # vin
                'ACTIVE',       # status
                current_time_ny,  # last_updated
                str(owner_user_id) if owner_user_id else '',  # owner_user_id
                '',             # last_message_id (updated during posts)
                ''              # tz_hint
            ]

            if existing_row_num:
                # Update existing
                worksheet.update(f'A{existing_row_num}', [row_data])
                logger.info(
                    f"Updated group registration: {group_id} -> {vin_upper}")
            else:
                # Add new
                worksheet.append_row(row_data)
                logger.info(f"Registered new group: {group_id} -> {vin_upper}")

            self.metrics['groups_updated'] += 1

        except Exception as e:
            logger.error(f"Error in group registration {group_id}: {e}")

    async def record_group_rename(self, group_id: int, new_title: str) -> None:
        """Update group title when group is renamed"""
        try:
            worksheet = self._get_worksheet_safe('groups')
            if not worksheet:
                return

            # Find and update the group
            all_data = worksheet.get_all_values()
            if len(all_data) < 2:
                return

            headers = all_data[0]
            header_map = self._normalize_headers(headers)

            group_id_col = self._find_header_column(header_map, 'group_id')
            title_col = self._find_header_column(header_map, 'group_title')
            updated_col = self._find_header_column(header_map, 'last_updated')

            if None in (group_id_col, title_col):
                logger.error("Required columns not found for group rename")
                return

            for i, row in enumerate(all_data[1:], start=2):
                if (len(row) > group_id_col and
                        str(row[group_id_col]) == str(group_id)):

                    # Update title and timestamp
                    updates = [
                        {'range': f'{chr(65 + title_col)}{i}', 'values': [[new_title]]}
                    ]

                    if updated_col is not None:
                        updates.append({
                            'range': f'{chr(65 + updated_col)}{i}',
                            'values': [[self._get_ny_time()]]
                        })

                    worksheet.batch_update(updates)
                    logger.info(
                        f"Updated group {group_id} title to: {new_title}")
                    break

        except Exception as e:
            logger.error(f"Error recording group rename {group_id}: {e}")

    # =====================================================
    # ELD_TRACKER WORKSHEET (5-min F:K batch snapshot)
    # =====================================================

    def batch_update_eld_tracker(self, points: Iterable[FleetPoint]) -> int:
        """5-minute batch update of F:K columns matched by VIN"""
        try:
            worksheet = self._get_worksheet_safe('assets')
            if not worksheet:
                logger.warning("assets worksheet not available")
                return 0

            # Get all data to match by VIN
            all_data = worksheet.get_all_values()
            if len(all_data) < 2:
                logger.warning("assets sheet has no data rows")
                return 0

            headers = all_data[0]
            data_rows = all_data[1:]
            header_map = self._normalize_headers(headers)

            # Find VIN column (should be column A)
            vin_col = self._find_header_column(header_map, 'VIN')
            if vin_col is None:
                logger.error("VIN column not found in assets sheet")
                return 0

            # Build VIN to row mapping
            vin_to_row = {}
            for i, row in enumerate(data_rows):
                if len(row) > vin_col and row[vin_col]:
                    vin = str(row[vin_col]).strip().upper()
                    if vin:
                        vin_to_row[vin] = i + 2  # +2 for header and 1-based

            # Prepare batch updates for F:K columns (indices 5-10)
            batch_updates = []
            updated_count = 0

            for fleet_point in points:
                if fleet_point.vin not in vin_to_row:
                    continue  # Skip unknown VINs

                row_num = vin_to_row[fleet_point.vin]

                # Get NY time string
                ny_time_str = ""
                if fleet_point.updated_at_utc:
                    ny_time_str = self._get_ny_time(fleet_point.updated_at_utc)

                # F:K columns data (indices 5-10)
                f_k_data = [
                    fleet_point.location_str or "",     # F: Last Known Location
                    fleet_point.lat or "",              # G: Latitude
                    fleet_point.lon or "",              # H: Longitude
                    fleet_point.status or "",           # I: Status
                    ny_time_str,                        # J: Update Time
                    fleet_point.source                  # K: Source
                ]

                batch_updates.append({
                    'range': f'F{row_num}:K{row_num}',
                    'values': [f_k_data]
                })
                updated_count += 1

            # Execute batch update
            if batch_updates:
                # Split into chunks to avoid API limits
                chunk_size = 50
                for i in range(0, len(batch_updates), chunk_size):
                    chunk = batch_updates[i:i + chunk_size]
                    try:
                        worksheet.batch_update(chunk)
                        time.sleep(0.1)  # Rate limiting
                    except Exception as e:
                        logger.error(
                            f"assets sheet batch update failed for chunk {i//chunk_size}: {e}")

                logger.info(f"assets sheet updated: {updated_count} VINs")

            self.metrics['eld_tracker_updates'] += updated_count
            return updated_count

        except Exception as e:
            logger.error(f"Error updating assets sheet: {e}")
            return 0

    # =====================================================
    # FLEET_STATUS WORKSHEET (hourly VIN snapshot with load/ETA flags)
    # =====================================================

    def upsert_fleet_status(self, rows: Iterable[dict]) -> int:
        """Hourly fleet snapshot upsert by VIN"""
        try:
            worksheet = self._get_worksheet_safe('fleet_status')
            if not worksheet:
                logger.error("Fleet_status worksheet not available")
                return 0

            schema = SHEET_SCHEMAS['fleet_status']
            if not self._ensure_worksheet_headers(worksheet, schema):
                return 0

            # Get existing data
            all_data = worksheet.get_all_values()
            headers = all_data[0] if all_data else schema.headers
            existing_rows = all_data[1:] if len(all_data) > 1 else []
            header_map = self._normalize_headers(headers)

            # Build VIN to row mapping
            vin_col = self._find_header_column(header_map, 'VIN')
            if vin_col is None:
                logger.error("VIN column not found in fleet_status")
                return 0

            vin_to_row = {}
            for i, row in enumerate(existing_rows):
                if len(row) > vin_col and row[vin_col]:
                    vin = str(row[vin_col]).strip().upper()
                    if vin:
                        vin_to_row[vin] = i + 2

            # Process fleet status rows
            batch_updates = []
            new_rows = []
            upserted_count = 0
            current_time_ny = self._get_ny_time()

            for row_data in rows:
                vin = str(row_data.get('vin', '')).strip().upper()
                if not vin:
                    continue

                # Build complete row according to schema
                fleet_row = [
                    vin,                                    # VIN
                    row_data.get('driver', ''),            # Driver
                    'Y' if row_data.get('on_load') else 'N',  # On Load
                    row_data.get('load_id', ''),           # Load ID
                    row_data.get('pu_city_state', ''),     # PU City/State
                    row_data.get('del_city_state', ''),    # DEL City/State
                    row_data.get('appt_time_ny', ''),      # Appt Time
                    row_data.get('eta_ny', ''),            # ETA
                    row_data.get('late_flag', 'N'),        # Late?
                    str(row_data.get('speed_mph', 0)),     # Speed mph
                    str(row_data.get('stopped_min', 0)),   # Stopped Min
                    'Y' if row_data.get('risk_flag') else 'N',  # Risk Flag
                    current_time_ny,                       # Last Refresh
                    row_data.get('source', 'Hourly Update')  # Source
                ]

                if vin in vin_to_row:
                    # Update existing row
                    row_num = vin_to_row[vin]
                    batch_updates.append({
                        'range': f'A{row_num}:{chr(65 + len(fleet_row) - 1)}{row_num}',
                        'values': [fleet_row]
                    })
                else:
                    # New row
                    new_rows.append(fleet_row)

                upserted_count += 1

            # Execute updates
            if batch_updates:
                chunk_size = 25
                for i in range(0, len(batch_updates), chunk_size):
                    chunk = batch_updates[i:i + chunk_size]
                    try:
                        worksheet.batch_update(chunk)
                        time.sleep(0.1)
                    except Exception as e:
                        logger.error(f"Fleet_status batch update failed: {e}")

            if new_rows:
                try:
                    for row in new_rows:
                        worksheet.append_row(row)
                        time.sleep(0.05)
                except Exception as e:
                    logger.error(
                        f"Failed to append new fleet_status rows: {e}")

            self.metrics['fleet_status_upserted'] += upserted_count
            logger.info(f"Fleet_status updated: {upserted_count} VINs")
            return upserted_count

        except Exception as e:
            logger.error(f"Error updating fleet_status: {e}")
            return 0

    # =====================================================
    # LOCATION_LOGS WORKSHEET (append-only audit trail)
    # =====================================================

    def append_location_logs(self, events: Iterable[dict]) -> int:
        """Append location log events with deduplication"""
        try:
            worksheet = self._get_worksheet_safe('location_logs')
            if not worksheet:
                logger.error("Location_logs worksheet not available")
                return 0

            schema = SHEET_SCHEMAS['location_logs']
            if not self._ensure_worksheet_headers(worksheet, schema):
                return 0

            # Clean old outbox entries
            cutoff = datetime.now(self.utc_tz) - self.outbox_ttl
            old_keys = [
                k for k,
                v in self.location_logs_outbox.items() if v < cutoff]
            for key in old_keys:
                del self.location_logs_outbox[key]

            # Process events with deduplication
            new_rows = []
            appended_count = 0

            for event in events:
                # Create deduplication key
                ts_bucket = int(
                    event.get(
                        'ts_utc_timestamp',
                        time.time()) //
                    300)  # 5-min buckets
                outbox_key = f"{ts_bucket}|{event.get('VIN', '')}|{event.get('event_type', '')}"

                if outbox_key in self.location_logs_outbox:
                    logger.debug(
                        f"Skipping duplicate location log: {outbox_key}")
                    continue

                # Build log row
                log_row = [
                    event.get('ts_utc', ''),           # ts_utc
                    event.get('ts_ny', ''),            # ts_ny
                    event.get('event_type', ''),       # event_type
                    str(event.get('group_id', '')),    # group_id
                    event.get('group_title', ''),      # group_title
                    event.get('VIN', ''),              # VIN
                    event.get('Driver', ''),           # Driver
                    str(event.get('lat', '')),         # lat
                    str(event.get('lon', '')),         # lon
                    str(event.get('speed_mph', '')),   # speed_mph
                    event.get('status', ''),           # status
                    event.get('location_str', ''),     # location_str
                    event.get('eta_ny', ''),           # eta_ny
                    event.get('appt_ny', ''),          # appt_ny
                    event.get('late_flag', ''),        # late_flag
                    str(event.get('message_id', '')),  # message_id
                    str(event.get('thread_id', '')),   # thread_id
                    event.get('source', '')            # source
                ]

                new_rows.append(log_row)
                self.location_logs_outbox[outbox_key] = datetime.now(
                    self.utc_tz)
                appended_count += 1

            # Append new rows
            if new_rows:
                try:
                    for row in new_rows:
                        worksheet.append_row(row)
                        time.sleep(0.05)  # Rate limiting
                except Exception as e:
                    logger.error(f"Failed to append location logs: {e}")
                    return 0

            self.metrics['location_logs_appended'] += appended_count
            logger.info(f"Location logs appended: {appended_count} events")
            return appended_count

        except Exception as e:
            logger.error(f"Error appending location logs: {e}")
            return 0

    # =====================================================
    # DASHBOARD_LOGS WORKSHEET (hourly KPI aggregation)
    # =====================================================

    def append_dashboard_hourly(self, summary: dict) -> None:
        """Append hourly dashboard summary"""
        try:
            worksheet = self._get_worksheet_safe('dashboard_logs')
            if not worksheet:
                logger.error("Dashboard_logs worksheet not available")
                return

            schema = SHEET_SCHEMAS['dashboard_logs']
            if not self._ensure_worksheet_headers(worksheet, schema):
                return

            # Build summary row
            summary_row = [
                summary.get('date', ''),                    # date
                str(summary.get('fleet_size', 0)),          # fleet_size
                str(summary.get('updates_sent', 0)),        # updates_sent
                str(summary.get('risk_alerts', 0)),         # risk_alerts
                str(summary.get('late_pu', 0)),             # late_pu
                str(summary.get('late_del', 0)),            # late_del
                str(summary.get('avg_stop_min', 0)),        # avg_stop_min
                str(summary.get('ors_fallbacks', 0)),       # ors_fallbacks
                str(summary.get('tms_errors', 0)),          # tms_errors
                str(summary.get('telegram_429s', 0))        # telegram_429s
            ]

            worksheet.append_row(summary_row)
            self.metrics['dashboard_entries'] += 1
            logger.info(
                f"Dashboard summary appended for {summary.get('date')}")

        except Exception as e:
            logger.error(f"Error appending dashboard summary: {e}")

    # =====================================================
    # ACK_AUDIT WORKSHEET (acknowledgment tracking)
    # =====================================================

    def append_ack_audit(self, entry: dict) -> None:
        """Append ACK button audit entry"""
        try:
            worksheet = self._get_worksheet_safe('ack_audit')
            if not worksheet:
                logger.error("Ack_audit worksheet not available")
                return

            schema = SHEET_SCHEMAS['ack_audit']
            if not self._ensure_worksheet_headers(worksheet, schema):
                return

            # Build audit row
            audit_row = [
                entry.get('ts_ny', ''),                     # ts_ny
                entry.get('driver_id', ''),                 # driver_id
                # stop_type (PU/DEL)
                entry.get('stop_type', ''),
                entry.get('location_hash', ''),             # location_hash
                entry.get('vin', ''),                       # vin
                str(entry.get('group_id', '')),             # group_id
                str(entry.get('user_id', '')),              # user_id
                entry.get('user_name', ''),                 # user_name
                entry.get('expires_ny', '')                 # expires_ny
            ]

            worksheet.append_row(audit_row)
            self.metrics['ack_entries'] += 1
            logger.info(
                f"ACK audit logged: {entry.get('driver_id')} - {entry.get('stop_type')}")

        except Exception as e:
            logger.error(f"Error appending ACK audit: {e}")

    # =====================================================
    # ERRORS WORKSHEET (SEV-2+ incident logging)
    # =====================================================

    def log_severe_error(self, component: str, severity: str, summary: str,
                         detail: str = "", context: str = "") -> None:
        """Log SEV-2+ errors to errors worksheet"""
        try:
            worksheet = self._get_worksheet_safe('errors')
            if not worksheet:
                logger.error("Errors worksheet not available")
                return

            schema = SHEET_SCHEMAS['errors']
            if not self._ensure_worksheet_headers(worksheet, schema):
                return

            # Build error row
            error_row = [
                self._get_ny_time(),    # ts_ny
                component,              # component
                severity,               # sev
                summary,                # summary
                detail,                 # detail
                context                 # context
            ]

            worksheet.append_row(error_row)
            self.metrics['errors_logged'] += 1
            logger.info(
                f"Severe error logged: {component} - {severity} - {summary}")

        except Exception as e:
            logger.error(f"Error logging severe error: {e}")

    # =====================================================
    # RETENTION AND MAINTENANCE
    # =====================================================

    def prune_location_logs_older_than(self, days: int) -> int:
        """Prune location logs older than specified days"""
        try:
            worksheet = self._get_worksheet_safe('location_logs')
            if not worksheet:
                logger.error(
                    "Location_logs worksheet not available for pruning")
                return 0

            # Get all data
            all_data = worksheet.get_all_values()
            if len(all_data) < 2:
                return 0

            headers = all_data[0]
            data_rows = all_data[1:]
            header_map = self._normalize_headers(headers)

            ts_utc_col = self._find_header_column(header_map, 'ts_utc')
            if ts_utc_col is None:
                logger.error("ts_utc column not found for pruning")
                return 0

            # Find rows to delete (older than cutoff)
            cutoff_date = datetime.now(self.utc_tz) - timedelta(days=days)
            rows_to_delete = []

            for i, row in enumerate(data_rows):
                if len(row) > ts_utc_col and row[ts_utc_col]:
                    try:
                        row_date = datetime.fromisoformat(
                            row[ts_utc_col].replace('Z', '+00:00'))
                        if row_date < cutoff_date:
                            # +2 for header and 1-based
                            rows_to_delete.append(i + 2)
                    except ValueError:
                        # Skip rows with invalid timestamps
                        continue

            # Delete in chunks (from bottom to top to maintain row numbers)
            if rows_to_delete:
                rows_to_delete.sort(reverse=True)
                chunk_size = 50  # Delete in chunks to avoid API limits
                deleted_count = 0

                for i in range(0, len(rows_to_delete), chunk_size):
                    chunk = rows_to_delete[i:i + chunk_size]
                    for row_num in chunk:
                        try:
                            worksheet.delete_rows(row_num)
                            deleted_count += 1
                            time.sleep(0.1)  # Rate limiting
                        except Exception as e:
                            logger.error(
                                f"Failed to delete row {row_num}: {e}")

                self.metrics['retention_pruned'] += deleted_count
                logger.info(
                    f"Pruned {deleted_count} location log entries older than {days} days")
                return deleted_count

            return 0

        except Exception as e:
            logger.error(f"Error pruning location logs: {e}")
            return 0

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        return self.metrics.copy()
