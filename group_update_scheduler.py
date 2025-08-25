"""
Group Update Scheduler - Hourly visible updates + 5-min silent refresh with jitter and semaphore.
Implements exactly-once-ish delivery and central rate limiting.
"""
import asyncio
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, List
from zoneinfo import ZoneInfo

from telegram import Bot
from telegram.error import TelegramError, BadRequest, Forbidden

from config import Config
from google_integration import GoogleSheetsIntegration
from tms_service import TMSService
from data_contracts import FleetPoint


logger = logging.getLogger(__name__)


class GroupUpdateScheduler:
    """
    Dual-mode scheduler:
    1. Hourly visible updates to groups with registered VINs
    2. 5-minute silent refresh to ELD_tracker sheet (F:K columns)
    
    Features:
    - Central semaphore(12) for Telegram sends
    - Jitter to prevent bursts
    - Exactly-once-ish with outbox deduplication
    - Exponential backoff with circuit breaker
    - Idempotent job design
    """
    
    def __init__(self, config: Config, bot: Bot, google_integration: GoogleSheetsIntegration):
        self.config = config
        self.bot = bot
        self.google = google_integration
        
        # Track startup time to prevent premature updates on restart
        self.startup_time = time.time()
        
        # Central rate limiter for Telegram sends
        self.telegram_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_TELEGRAM_SENDS)
        
        # Outbox for exactly-once-ish delivery: {outbox_key: timestamp}
        self.outbox: Dict[str, datetime] = {}
        self.outbox_ttl = timedelta(hours=2)  # Clean old entries
        
        # Circuit breaker state
        self.circuit_breaker = {
            'failures': 0,
            'last_failure': None,
            'is_open': False
        }
        
        # Job state tracking
        self.running_jobs: Set[str] = set()
        self.last_runs: Dict[str, datetime] = {}
        
        # Performance metrics
        self.metrics = {
            'hourly_updates_sent': 0,
            'silent_refreshes': 0,
            'telegram_429s': 0,
            'circuit_breaker_trips': 0
        }
    
    async def start_scheduling(self, job_queue):
        """Start both hourly and 5-minute job schedules with jitter"""
        if not job_queue:
            logger.error("Job queue not available - cannot start scheduler")
            return
        
        # Schedule hourly visible updates with minimum startup delay to allow TMS refresh
        # Use minimum 5 minutes (300s) startup delay to ensure TMS data is fresh before sending updates
        min_startup_delay = 300  # 5 minutes minimum delay on restart
        max_startup_delay = min_startup_delay + self.config.SCHEDULER_JITTER_MAX_SECONDS
        hourly_jitter = random.randint(min_startup_delay, max_startup_delay)
        logger.info(f"Scheduling hourly group updates with {hourly_jitter}s initial delay (min {min_startup_delay}s to allow TMS refresh)")
        
        job_queue.run_repeating(
            self._hourly_group_updates_job,
            interval=self.config.GROUP_LOCATION_INTERVAL,
            first=hourly_jitter,
            name="hourly_group_updates"
        )
        
        # Schedule 5-minute silent refresh with different jitter
        refresh_jitter = random.randint(30, 90)  # 30-90 second jitter
        logger.info(f"Scheduling 5-minute silent refresh with {refresh_jitter}s initial jitter")
        
        job_queue.run_repeating(
            self._silent_refresh_job,
            interval=300,  # 5 minutes
            first=refresh_jitter,
            name="silent_refresh"
        )
        
        # Schedule housekeeping tasks
        job_queue.run_repeating(
            self._housekeeping_job,
            interval=1800,  # 30 minutes
            first=300,  # 5 minutes
            name="housekeeping"
        )
        
        logger.info("Group update scheduler started successfully")
    
    async def _hourly_group_updates_job(self, context):
        """Hourly visible updates to groups with registered VINs"""
        job_id = f"hourly_updates_{int(time.time())}"
        
        # Additional safety check: ensure enough time has passed since startup for TMS refresh
        time_since_startup = time.time() - self.startup_time
        min_startup_delay = 240  # 4 minutes minimum (slightly less than scheduler delay for race condition)
        if time_since_startup < min_startup_delay:
            logger.warning(f"Skipping updates - only {time_since_startup:.0f}s since startup, need {min_startup_delay}s for TMS refresh")
            return
        
        if job_id in self.running_jobs:
            logger.warning("Hourly update job already running, skipping")
            return
        
        if self.circuit_breaker['is_open']:
            logger.warning("Circuit breaker open, skipping hourly updates")
            return
        
        self.running_jobs.add(job_id)
        start_time = time.time()
        
        try:
            logger.info("Starting hourly group updates")
            
            # Get active groups with VINs
            active_groups = self._get_active_groups()
            if not active_groups:
                logger.info("No active groups found for updates")
                return
            
            logger.info(f"Found {len(active_groups)} active groups")
            
            # Fetch latest fleet data
            async with TMSService(self.config) as tms:
                fleet_points = await tms.fetch_fleet_locations()
            
            if not fleet_points:
                logger.warning("No fleet data available for updates")
                return
            
            # Create VIN lookup map
            fleet_map = {fp.vin: fp for fp in fleet_points}
            
            # Process each group with jitter and rate limiting
            updates_sent = 0
            for group_data in active_groups:
                try:
                    await self._send_group_update(group_data, fleet_map)
                    updates_sent += 1
                    
                    # Add jitter between group updates
                    jitter = random.uniform(0.5, 2.0)
                    await asyncio.sleep(jitter)
                    
                except Exception as e:
                    logger.error(f"Error updating group {group_data.get('group_id')}: {e}")
                    self._record_failure()
            
            self.metrics['hourly_updates_sent'] += updates_sent
            duration = time.time() - start_time
            
            logger.info(f"Hourly updates complete: {updates_sent} sent in {duration:.1f}s")
            self.last_runs['hourly_updates'] = datetime.utcnow()
            
        except Exception as e:
            logger.error(f"Hourly update job failed: {e}", exc_info=True)
            self._record_failure()
        finally:
            self.running_jobs.discard(job_id)
    
    async def _silent_refresh_job(self, context):
        """5-minute silent refresh - updates ELD_tracker sheet and warms geocode cache"""
        job_id = f"silent_refresh_{int(time.time())}"
        
        if job_id in self.running_jobs:
            logger.debug("Silent refresh job already running, skipping")
            return
        
        self.running_jobs.add(job_id)
        start_time = time.time()
        
        try:
            logger.debug("Starting silent refresh")
            
            # Fetch latest fleet data
            async with TMSService(self.config) as tms:
                fleet_points = await tms.fetch_fleet_locations()
            
            if not fleet_points:
                logger.debug("No fleet data for silent refresh")
                return
            
            # Update ELD_tracker sheet (F:K columns)
            updated_count = await self._update_eld_tracker(fleet_points)
            
            # Warm reverse geocode cache for new locations
            warmed_count = await self._warm_geocode_cache(fleet_points)
            
            self.metrics['silent_refreshes'] += 1
            duration = time.time() - start_time
            
            logger.debug(f"Silent refresh complete: {updated_count} records, {warmed_count} cached in {duration:.1f}s")
            self.last_runs['silent_refresh'] = datetime.utcnow()
            
        except Exception as e:
            logger.error(f"Silent refresh job failed: {e}", exc_info=True)
        finally:
            self.running_jobs.discard(job_id)
    
    async def _warm_geocode_cache(self, fleet_points: List[FleetPoint]) -> int:
        """Warm reverse geocode cache during silent refresh"""
        try:
            from reverse_geocode_service import ReverseGeocodeService
            
            async with ReverseGeocodeService(self.config) as geocode_service:
                warmed_count = await geocode_service.warm_cache_for_fleet(fleet_points)
                return warmed_count
                
        except Exception as e:
            logger.error(f"Error warming geocode cache: {e}")
            return 0
    
    async def _send_group_update(self, group_data: Dict, fleet_map: Dict[str, FleetPoint]):
        """Send location update to a specific group with deduplication"""
        group_id = group_data.get('group_id')
        vin = group_data.get('vin', '').upper()
        
        if not group_id or not vin:
            return
        
        # Check outbox for deduplication
        time_bucket = int(time.time() // 3600)  # Hour buckets
        outbox_key = f"{group_id}|{vin}|{time_bucket}"
        
        if outbox_key in self.outbox:
            logger.debug(f"Update already sent to group {group_id} this hour")
            return
        
        fleet_point = fleet_map.get(vin)
        if not fleet_point:
            logger.debug(f"No fleet data for VIN {vin} in group {group_id}")
            return
        
        try:
            # Build HTML-formatted message
            message = self._build_location_message(fleet_point)
            
            # Send with rate limiting
            async with self.telegram_semaphore:
                await self.bot.send_message(
                    chat_id=group_id,
                    text=message,
                    parse_mode='HTML',
                    disable_web_page_preview=False
                )
            
            # Record in outbox
            self.outbox[outbox_key] = datetime.utcnow()
            logger.debug(f"Location update sent to group {group_id} for VIN {vin}")
            
        except Forbidden:
            logger.warning(f"Bot was removed from group {group_id}")
            # Mark group as inactive
            await self._deactivate_group(group_id, "Bot removed from group")
            
        except BadRequest as e:
            if "429" in str(e) or "rate limit" in str(e).lower():
                self.metrics['telegram_429s'] += 1
                logger.warning(f"Rate limited sending to group {group_id}")
                # Don't mark as sent - will retry next cycle
                raise
            else:
                logger.error(f"Bad request sending to group {group_id}: {e}")
                
        except TelegramError as e:
            logger.error(f"Telegram error sending to group {group_id}: {e}")
            self._record_failure()
            raise
    
    def _build_location_message(self, fleet_point: FleetPoint) -> str:
        """Build HTML-formatted message using centralized renderer"""
        # Use feature flag to allow rollback to old renderer
        if not getattr(self.config, 'ENABLE_NEW_LOCATION_RENDERER', True):
            return self._build_legacy_location_message(fleet_point)
        
        from location_renderer import render_location_update
        
        # Look up driver name from assets sheet if not provided by TMS
        driver_name = fleet_point.driver_name
        if not driver_name and hasattr(self, 'google'):
            try:
                driver_name, _ = self.google.get_driver_contact_info_by_vin(fleet_point.vin)
            except Exception as e:
                logger.debug(f"Could not lookup driver name for {fleet_point.vin}: {e}")
        
        return render_location_update(
            driver=driver_name or "Unknown Driver",
            status=fleet_point.status or "Unknown",
            lat=fleet_point.lat or 0.0,
            lon=fleet_point.lon or 0.0,
            speed_mph=fleet_point.speed_mph(),
            updated_at_utc=fleet_point.updated_at_utc,
            location_str=fleet_point.location_str,
            map_source=fleet_point.source
        )
    
    def _build_legacy_location_message(self, fleet_point: FleetPoint) -> str:
        """Legacy message builder for rollback capability"""
        import html
        
        # Get NY time with EDT/EST designation
        ny_time = fleet_point.to_ny_time()
        time_str = "Unknown"
        if ny_time:
            tz_name = ny_time.strftime('%Z')  # EDT or EST
            time_str = ny_time.strftime(f'%Y-%m-%d %H:%M:%S {tz_name}')
        
        # Extract and format speed
        speed_mph = fleet_point.speed_mph()
        
        # Look up driver name from assets sheet if not provided by TMS  
        driver_name = fleet_point.driver_name
        if not driver_name and hasattr(self, 'google'):
            try:
                driver_name, _ = self.google.get_driver_contact_info_by_vin(fleet_point.vin)
            except Exception as e:
                logger.debug(f"Could not lookup driver name for {fleet_point.vin}: {e}")
        
        # Escape HTML in user/sheet data
        driver_name = html.escape(driver_name or "Unknown Driver")
        location = html.escape(fleet_point.location_str or "Location Unavailable")
        status = html.escape(fleet_point.status or "Unknown")
        
        # Build Google Maps link
        map_link = ""
        if fleet_point.lat and fleet_point.lon:
            map_link = f"https://maps.google.com/?q={fleet_point.lat},{fleet_point.lon}"
        
        # Construct HTML message per old spec
        message = f"""🚛 <b>Location Update</b>

👤 <b>Driver:</b> {driver_name}
🛑 <b>Status:</b> {status}
📍 <b>Location:</b> {location}
🏃 <b>Speed:</b> {speed_mph} mph
📡 <b>Updated:</b> {time_str}"""
        
        if map_link:
            message += f"\n\n🗺️ <a href='{map_link}'>View on Map</a>"
        
        return message
    
    async def _update_eld_tracker(self, fleet_points: List[FleetPoint]) -> int:
        """Batch update ELD_tracker sheet F:K columns"""
        try:
            # Get ELD_tracker worksheet
            if not hasattr(self.google, 'spreadsheet'):
                logger.error("Google Sheets not initialized")
                return 0
            
            try:
                eld_worksheet = self.google.spreadsheet.worksheet('ELD_tracker')
            except Exception as e:
                logger.warning(f"ELD_tracker sheet not found: {e}")
                return 0
            
            # Get existing data to match by VIN
            try:
                all_data = eld_worksheet.get_all_values()
                if len(all_data) < 2:
                    logger.warning("ELD_tracker sheet has no data rows")
                    return 0
                
                headers = [h.strip().lower() for h in all_data[0]]
                data_rows = all_data[1:]
                
                # Find VIN column index (usually column A)
                vin_col_idx = None
                for i, header in enumerate(headers):
                    if 'vin' in header:
                        vin_col_idx = i
                        break
                
                if vin_col_idx is None:
                    logger.error("VIN column not found in ELD_tracker sheet")
                    return 0
                
                # Build VIN to row mapping
                vin_to_row = {}
                for i, row in enumerate(data_rows):
                    if len(row) > vin_col_idx and row[vin_col_idx]:
                        vin = str(row[vin_col_idx]).strip().upper()
                        if vin:
                            vin_to_row[vin] = i + 2  # +2 for header and 1-based indexing
                
                # Log VIN indexing statistics
                logger.info(f"📊 ELD_tracker scan: {len(data_rows)} total rows, {len(vin_to_row)} valid VINs indexed")
                logger.info(f"🔍 VIN column found at index {vin_col_idx} (schema expects column E=4)")
                
            except Exception as e:
                logger.error(f"Error reading ELD_tracker data: {e}")
                return 0
            
            # Prepare batch updates for F:K columns
            batch_updates = []
            updated_count = 0
            skipped_count = 0
            skipped_samples = []
            
            for fleet_point in fleet_points:
                if fleet_point.vin not in vin_to_row:
                    skipped_count += 1
                    if len(skipped_samples) < 5:
                        skipped_samples.append(fleet_point.vin)
                    continue  # Skip unknown VINs
                
                row_num = vin_to_row[fleet_point.vin]
                
                # Get NY time string
                ny_time = fleet_point.to_ny_time()
                time_str = ""
                if ny_time:
                    tz_name = ny_time.strftime('%Z')
                    time_str = ny_time.strftime(f'%Y-%m-%d %H:%M:%S {tz_name}')
                
                # Prepare row data for F:K (columns 6-11) - prevent auto-merging
                row_data = [
                    fleet_point.location_str or " ",                    # F: Last Known Location (space prevents merging)
                    str(fleet_point.lat) if fleet_point.lat else " ",  # G: Latitude (always string)
                    str(fleet_point.lon) if fleet_point.lon else " ",  # H: Longitude (always string)
                    fleet_point.status or "Unknown",                   # I: Status (explicit default)
                    time_str or " ",                                    # J: Update Time (space prevents merging)
                    fleet_point.source or "TMS"                        # K: Source (explicit default)
                ]
                
                batch_updates.append({
                    'range': f'F{row_num}:K{row_num}',
                    'values': [row_data]
                })
                updated_count += 1
            
            # Execute batch update
            if batch_updates:
                # Split into chunks to avoid API limits
                chunk_size = 50
                for i in range(0, len(batch_updates), chunk_size):
                    chunk = batch_updates[i:i + chunk_size]
                    try:
                        eld_worksheet.batch_update(chunk)
                        # Small delay between chunks
                        await asyncio.sleep(0.1)
                    except Exception as e:
                        logger.error(f"Batch update failed for chunk {i//chunk_size}: {e}")
                
                # Enhanced logging with diagnostic information
                total_tms_vins = len(fleet_points)
                matched_vins = updated_count
                logger.info(f"📈 Update summary: {matched_vins}/{total_tms_vins} TMS VINs matched sheet")
                logger.info(f"⚠️ Skipped {skipped_count} unknown VINs: {skipped_samples[:5]}")
                logger.info(f"✅ Executed {len(batch_updates)} updates in {len(range(0, len(batch_updates), 50))} chunks")
                logger.info(f"Updated {updated_count} records in ELD_tracker")
            
            return updated_count
            
        except Exception as e:
            logger.error(f"Error updating ELD_tracker: {e}")
            return 0
    
    def _get_active_groups(self) -> List[Dict]:
        """Get active groups with registered VINs from Google Sheets"""
        try:
            records = self.google._get_groups_records_safe()
            active_groups = []
            
            for record in records:
                if (record.get('status', '').upper() == 'ACTIVE' and 
                    record.get('vin') and 
                    record.get('group_id')):
                    active_groups.append({
                        'group_id': int(record['group_id']),
                        'vin': record['vin'].strip().upper(),
                        'group_title': record.get('group_title', '')
                    })
            
            return active_groups
            
        except Exception as e:
            logger.error(f"Error getting active groups: {e}")
            return []
    
    async def _deactivate_group(self, group_id: int, reason: str):
        """Mark group as inactive with reason"""
        try:
            records = self.google._get_groups_records_safe()
            
            for i, record in enumerate(records):
                if int(record.get('group_id', 0)) == group_id:
                    row_num = i + 2  # +2 for header and 1-based indexing
                    
                    # Update status and add note
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    self.google.groups_worksheet.update(f'E{row_num}', [['INACTIVE']])  # Status column
                    self.google.groups_worksheet.update(f'J{row_num}', [[f'{reason} - {current_time}']])  # Notes column
                    
                    logger.info(f"Deactivated group {group_id}: {reason}")
                    break
                    
        except Exception as e:
            logger.error(f"Error deactivating group {group_id}: {e}")
    
    def _record_failure(self):
        """Record failure for circuit breaker logic"""
        self.circuit_breaker['failures'] += 1
        self.circuit_breaker['last_failure'] = datetime.utcnow()
        
        # Open circuit breaker after 5 failures
        if self.circuit_breaker['failures'] >= 5:
            self.circuit_breaker['is_open'] = True
            self.metrics['circuit_breaker_trips'] += 1
            logger.error("Circuit breaker opened due to repeated failures")
    
    async def _housekeeping_job(self, context):
        """Clean up old outbox entries and reset circuit breaker"""
        try:
            # Clean old outbox entries
            cutoff = datetime.utcnow() - self.outbox_ttl
            old_keys = [k for k, v in self.outbox.items() if v < cutoff]
            
            for key in old_keys:
                del self.outbox[key]
            
            if old_keys:
                logger.debug(f"Cleaned {len(old_keys)} old outbox entries")
            
            # Reset circuit breaker if it's been closed for long enough
            if self.circuit_breaker['is_open']:
                last_failure = self.circuit_breaker['last_failure']
                if last_failure and (datetime.utcnow() - last_failure) > timedelta(minutes=10):
                    self.circuit_breaker['is_open'] = False
                    self.circuit_breaker['failures'] = 0
                    logger.info("Circuit breaker reset")
            
        except Exception as e:
            logger.error(f"Housekeeping job failed: {e}")
    
    def get_scheduler_stats(self) -> Dict:
        """Get scheduler performance statistics"""
        return {
            'metrics': self.metrics.copy(),
            'circuit_breaker': self.circuit_breaker.copy(),
            'running_jobs': len(self.running_jobs),
            'outbox_size': len(self.outbox),
            'last_runs': self.last_runs.copy(),
            'telegram_semaphore_available': self.telegram_semaphore._value
        }