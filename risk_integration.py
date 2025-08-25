#!/usr/bin/env python3
"""
Risk Integration - Enhanced with QC Panel sync and ETA monitoring
Integration of Cargo Theft Risk Detection with Asset Tracking Bot
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)

# Import the risk detection module
try:
    from cargo_risk_detection import CargoTheftRiskDetector, RiskLevel, RiskAlert
    RISK_DETECTION_AVAILABLE = True
except ImportError as e:
    RISK_DETECTION_AVAILABLE = False
    logger.warning(f"Risk detection import failed: {e}")
    
    # Create dummy classes for fallback
    class RiskLevel:
        LOW = "LOW"
        MODERATE = "MODERATE"
        HIGH = "HIGH"
        CRITICAL = "CRITICAL"
    
    class RiskAlert:
        pass
    
    class CargoTheftRiskDetector:
        def __init__(self, config, tms_integration=None):
            self.risk_zones = []
        
        def get_zone_statistics(self):
            return {'total_zones': 0, 'zones_by_risk': {}, 'active_drivers': 0}

# Import ETA service
try:
    from eta_service import ETAService
    ETA_SERVICE_AVAILABLE = True
except ImportError as e:
    ETA_SERVICE_AVAILABLE = False
    logger.warning(f"ETA service import failed: {e}")
    
    class ETAService:
        def __init__(self, ors_api_key: str):
            pass
        
        def eta_from_now(self, src_lat, src_lon, address):
            return None
        
        def is_late(self, eta_utc, appt_str, grace_min):
            return False, 0

class RiskDetectionMixin:
    """
    Enhanced mixin class to add risk detection capabilities with QC Panel integration
    """
    
    def init_risk_detection(self):
        """Initialize risk detection with ETA service - call this in your bot's __init__"""
        if not RISK_DETECTION_AVAILABLE:
            logger.warning("Risk detection modules not available - cargo theft monitoring disabled")
            self.enable_risk_monitoring = False
            return
        
        try:
            # Initialize risk detector (only config and tms_integration)
            self.risk_detector = CargoTheftRiskDetector(
                self.config, 
                self.tms_integration
            )
            
            # Set google_integration separately after initialization
            if hasattr(self, 'google_integration'):
                self.risk_detector.google_integration = self.google_integration
            
            # Initialize ETA service if available
            if ETA_SERVICE_AVAILABLE and hasattr(self.config, 'ORS_API_KEY'):
                self.eta_service = ETAService(self.config.ORS_API_KEY)
                logger.info("ETA service initialized for late notifications")
            else:
                self.eta_service = ETAService("")  # Dummy service
                logger.warning("ETA service not available - late notifications disabled")
            
            # Risk monitoring settings from config
            self.qc_chat_id = getattr(self.config, 'QC_TEAM_CHAT_ID', None)
            self.mgmt_chat_id = getattr(self.config, 'MGMT_CHAT_ID', None)
            self.risk_check_interval = getattr(self.config, 'RISK_CHECK_INTERVAL', 300)
            self.assets_update_interval = getattr(self.config, 'ASSETS_UPDATE_INTERVAL', 3600)
            self.enable_risk_monitoring = getattr(self.config, 'ENABLE_RISK_MONITORING', True)
            self.enable_asset_updates = getattr(self.config, 'ENABLE_ASSET_SHEET_UPDATES', True)
            
            # Track last assets update to ensure hourly intervals
            self.last_assets_update = None
            
            # ETA alerting settings
            self.send_qc_late_alerts = getattr(self.config, 'SEND_QC_LATE_ALERTS', True)
            self.eta_grace_minutes = getattr(self.config, 'ETA_GRACE_MINUTES', 10)
            self.risk_require_late = getattr(self.config, 'RISK_REQUIRE_LATE', False)
            
            # Alert muting system
            self.mute_store = {}  # In production, use Redis or persistent storage
            
            logger.info(f"Risk detection initialized - QC Chat: {self.qc_chat_id}, MGMT Chat: {self.mgmt_chat_id}")
            logger.info(f"Risk zones loaded: {len(self.risk_detector.risk_zones)}")
            logger.info(f"ETA alerting enabled: {self.send_qc_late_alerts}")
            
        except Exception as e:
            logger.error(f"Failed to initialize risk detection: {e}")
            self.enable_risk_monitoring = False
    
    def schedule_risk_monitoring(self, context):
        """Schedule periodic risk monitoring job with QC Panel sync"""
        if not RISK_DETECTION_AVAILABLE or not getattr(self, 'enable_risk_monitoring', False):
            logger.info("Risk monitoring disabled or unavailable")
            return
        
        if not context.job_queue:
            logger.error("Job queue not available for risk monitoring")
            return
        
        try:
            # Schedule risk monitoring job (5 minutes, no assets updates)
            context.job_queue.run_repeating(
                callback=self._enhanced_risk_monitoring_callback,
                interval=self.risk_check_interval,
                first=60,  # Start after 1 minute
                name="enhanced_cargo_risk_monitoring",
                data={'type': 'enhanced_risk_monitoring'}
            )
            logger.info(f"Scheduled enhanced risk monitoring every {self.risk_check_interval}s")
            
            # Schedule separate assets update job (hourly)
            context.job_queue.run_repeating(
                callback=self._assets_update_callback,
                interval=self.assets_update_interval,
                first=300,  # Start after 5 minutes (avoid startup rush)
                name="assets_location_updates",
                data={'type': 'assets_updates'}
            )
            logger.info(f"Scheduled assets location updates every {self.assets_update_interval}s")
            
        except Exception as e:
            logger.error(f"Failed to schedule monitoring jobs: {e}")
    
    async def _enhanced_risk_monitoring_callback(self, context):
        """Enhanced periodic callback with QC Panel sync and ETA monitoring"""
        if not RISK_DETECTION_AVAILABLE or not hasattr(self, 'risk_detector'):
            return
            
        try:
            # Step 1: Sync QC Panel data to assets (every cycle)
            if hasattr(self.google_integration, 'sync_active_loads_to_assets'):
                try:
                    updates = self.google_integration.sync_active_loads_to_assets()
                    if updates > 0:
                        logger.info(f"Synced {updates} QC Panel updates to assets")
                    else:
                        logger.debug(f"No QC Panel updates needed (0 updates)")
                except Exception as e:
                    logger.error(f"QC Panel sync failed: {e}")
            
            # Step 2: Load trucks and check for qualified loads
            trucks = self.tms_integration.load_truck_list()
            alerts_sent = 0
            drivers_checked = 0
            eta_alerts_sent = 0
            
            # Update fleet_status sheet if enabled (lightweight, keep in risk monitoring)
            if self.enable_asset_updates and trucks:
                try:
                    if hasattr(self.google_integration, 'update_fleet_status_sheet'):
                        self.google_integration.update_fleet_status_sheet(trucks)
                        logger.debug("Fleet status sheet updated")
                except Exception as e:
                    logger.error(f"Fleet status sheet update failed: {e}")
            
            # Step 3: Check each truck for risk and ETA alerts
            for truck in trucks:
                if not all([truck.get('vin'), truck.get('lat'), truck.get('lng')]):
                    continue
                
                vin = truck.get('vin', '')
                lat = truck.get('lat')
                lng = truck.get('lng')
                driver_name = truck.get('name', 'Unknown')
                
                # Get active load status from QC Panel data
                active = None
                if hasattr(self.google_integration, 'get_active_load_status_for_vin'):
                    active = self.google_integration.get_active_load_status_for_vin(vin)
                
                # Gate: Only monitor drivers with qualifying delivery status
                if not active or not active.get("in_transit"):
                    continue  # Only monitor drivers whose DEL status is transit/will be late/risky
                
                drivers_checked += 1
                
                # Prepare enhanced driver data
                driver_data = {
                    'driver_name': driver_name,
                    'vin': vin,
                    'lat': lat,
                    'lng': lng,
                    'speed': truck.get('speed', 0),
                    'address': truck.get('address', 'Unknown'),
                    # QC Panel data
                    'is_late': bool(active.get("is_late")),
                    'load_id': active.get("load_id"),
                    'del_address': active.get("del_address"),
                    'del_appt': active.get("del_appt"),
                    'pu_address': active.get("pu_address"),
                    'pu_appt': active.get("pu_appt")
                }
                
                # Step 4: ETA checks for late notifications (DEL)
                if (self.send_qc_late_alerts and driver_data["del_address"] and 
                    driver_data["del_appt"] and ETA_SERVICE_AVAILABLE):
                    
                    eta_info = self.eta_service.eta_from_now(lat, lng, driver_data["del_address"])
                    if eta_info:
                        is_late, diff_min = self.eta_service.is_late(
                            eta_info["eta_utc"], 
                            driver_data["del_appt"], 
                            self.eta_grace_minutes
                        )
                        
                        if is_late and not self.is_muted(f"ACK_LATE_DEL:{vin}:{driver_data['load_id']}"):
                            success = await self._send_qc_late_alert_del(
                                context=context,
                                destinations=self.google_integration.resolve_destinations(vin),
                                vin=vin,
                                driver=driver_name,
                                load_id=driver_data["load_id"],
                                miles=eta_info["miles"],
                                eta_utc=eta_info["eta_utc"],
                                appt_str=driver_data["del_appt"],
                                late_min=diff_min,
                                del_address=driver_data["del_address"]
                            )
                            if success:
                                eta_alerts_sent += 1
                
                # Step 5: ETA checks for late notifications (PU)
                if (self.send_qc_late_alerts and driver_data["pu_address"] and 
                    driver_data["pu_appt"] and ETA_SERVICE_AVAILABLE):
                    
                    eta_info = self.eta_service.eta_from_now(lat, lng, driver_data["pu_address"])
                    if eta_info:
                        is_late, diff_min = self.eta_service.is_late(
                            eta_info["eta_utc"], 
                            driver_data["pu_appt"], 
                            self.eta_grace_minutes
                        )
                        
                        if is_late and not self.is_muted(f"ACK_LATE_PU:{vin}:{driver_data['load_id']}"):
                            success = await self._send_qc_late_alert_pu(
                                context=context,
                                destinations=self.google_integration.resolve_destinations(vin),
                                vin=vin,
                                driver=driver_name,
                                load_id=driver_data["load_id"],
                                miles=eta_info["miles"],
                                eta_utc=eta_info["eta_utc"],
                                appt_str=driver_data["pu_appt"],
                                late_min=diff_min,
                                pu_address=driver_data["pu_address"]
                            )
                            if success:
                                eta_alerts_sent += 1
                
                # Step 6: Cargo theft risk detection
                # Skip acknowledged alerts
                if hasattr(self, 'is_alert_acknowledged') and self.is_alert_acknowledged(vin):
                    continue
                
                # Apply "late required" filter if configured
                if self.risk_require_late and not driver_data.get("is_late"):
                    continue
                
                # Check for theft risk alert
                alert = self.risk_detector.update_driver_state(driver_data)
                
                if alert:
                    success = await self._send_risk_alert_with_ack(context, alert)
                    if success:
                        alerts_sent += 1
            
            # Log summary
            if alerts_sent > 0 or eta_alerts_sent > 0:
                logger.warning(f"Enhanced monitoring: {alerts_sent} risk alerts, {eta_alerts_sent} ETA alerts, {drivers_checked} drivers checked")
            else:
                logger.debug(f"Enhanced monitoring: No alerts, {drivers_checked} drivers checked")
            
            # Periodic cleanup
            if datetime.now().minute == 0:
                self.risk_detector.cleanup_old_states()
                if hasattr(self, 'cleanup_acknowledged_alerts'):
                    self.cleanup_acknowledged_alerts()
                self._cleanup_muted_alerts()
            
        except Exception as e:
            logger.error(f"Enhanced risk monitoring callback error: {e}")
    
    async def _assets_update_callback(self, context):
        """Separate hourly callback for assets sheet location/timestamp updates"""
        from datetime import datetime, timedelta
        import time
        
        job_id = f"assets_update_{int(time.time())}"
        
        if job_id in getattr(self, 'running_jobs', set()):
            logger.warning("Assets update job already running, skipping")
            return
        
        if not hasattr(self, 'running_jobs'):
            self.running_jobs = set()
        self.running_jobs.add(job_id)
        start_time = time.time()
        
        try:
            logger.info("🔄 Starting hourly assets sheet location/timestamp updates...")
            
            # Check if enough time has passed since last update (prevent restart spam)
            now = datetime.utcnow()
            if self.last_assets_update:
                time_since_last = (now - self.last_assets_update).total_seconds()
                min_interval = self.assets_update_interval - 300  # Allow 5min tolerance
                
                if time_since_last < min_interval:
                    logger.info(f"⏭️ Skipping assets update - only {time_since_last:.0f}s since last update (minimum: {min_interval}s)")
                    return
            
            if not self.enable_asset_updates:
                logger.info("❌ Assets updates disabled in configuration")
                return
                
            # Update assets sheet with current TMS location/timestamp data
            if hasattr(self.google_integration, 'update_assets_with_current_data'):
                logger.info("📊 Fetching current TMS data for assets update...")
                result = self.google_integration.update_assets_with_current_data()  # Process all trucks, no artificial limit
                logger.debug(f"Assets update result: {result}")
                
                if "error" not in result:
                    assets_updated = result.get('assets_updated', 0)
                    field_updates = result.get('field_updates_made', 0)
                    trucks_processed = result.get('trucks_processed', 0)
                    new_trucks = result.get('new_trucks_found', 0)
                    
                    if assets_updated > 0:
                        logger.info(f"✅ Updated {assets_updated}/{trucks_processed} assets with {field_updates} field updates from TMS")
                        if new_trucks > 0:
                            logger.info(f"📋 Found {new_trucks} new trucks in TMS not in assets sheet")
                    else:
                        logger.info("ℹ️ No asset location updates needed - all data current")
                    
                    # Update timestamp of successful update
                    self.last_assets_update = now
                    
                else:
                    logger.error(f"❌ Assets location update failed: {result['error']}")
                    
            else:
                logger.error("❌ Method update_assets_with_current_data not found on google_integration")
            
            duration = time.time() - start_time
            logger.info(f"⏱️ Assets update completed in {duration:.1f}s")
            
        except Exception as e:
            logger.error(f"Assets update callback error: {e}", exc_info=True)
        finally:
            if hasattr(self, 'running_jobs'):
                self.running_jobs.discard(job_id)
    
    def _mute_key(self, key: str, hours: int = 6):
        """Mute alert key for specified hours"""
        expiry_time = datetime.now() + timedelta(hours=hours)
        self.mute_store[key] = expiry_time
        logger.debug(f"Muted alert key '{key}' until {expiry_time}")
    
    def is_muted(self, key: str) -> bool:
        """Check if alert key is currently muted"""
        if key not in self.mute_store:
            return False
        
        if datetime.now() > self.mute_store[key]:
            # Expired, remove from store
            del self.mute_store[key]
            return False
        
        return True
    
    def _cleanup_muted_alerts(self):
        """Clean up expired muted alerts"""
        current_time = datetime.now()
        expired_keys = [key for key, expiry in self.mute_store.items() if current_time > expiry]
        
        for key in expired_keys:
            del self.mute_store[key]
        
        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired muted alerts")
    
    def _fmt_local(self, dt_utc):
        """Format UTC datetime to local time string"""
        # Convert to EDT for display
        import pytz
        edt_tz = pytz.timezone('America/New_York')
        local_dt = dt_utc.replace(tzinfo=pytz.utc).astimezone(edt_tz)
        return local_dt.strftime("%-I:%M %p %Z %m/%d")
    
    def _map_link(self, vin: str, address: str) -> str:
        """Generate map link for route"""
        # Simple Google Maps link - could be enhanced to show route
        return f"https://maps.google.com/?q={address.replace(' ', '+')}"
    
    async def _send_qc_late_alert_del(self, context, destinations: list, vin: str, driver: str, 
                                     load_id: str, miles: int, eta_utc: datetime, appt_str: str, 
                                     late_min: int, del_address: str) -> bool:
        """Send QC late alert for delivery"""
        try:
            text = (
                "🚨 *QC Late Alert – Delivery*\n\n"
                f"👤 *Driver:* {driver}\n"
                f"🚛 *VIN:* `{vin}`  •  *Load:* `{load_id}`\n"
                f"📦 *DEL:* {del_address}\n"
                f"🕒 *Appt:* {appt_str}\n"
                f"🛣️ *Distance:* {miles} mi  •  *ETA:* {self._fmt_local(eta_utc)}\n\n"
                f"⚠️ *Status:* Late by {late_min} min"
            )
            
            kb = [
                [InlineKeyboardButton("✅ Acknowledge", callback_data=f"ACK_LATE_DEL:{vin}:{load_id}")],
                [InlineKeyboardButton("🗺 View Route", url=self._map_link(vin, del_address))]
            ]
            
            success_count = 0
            for chat_id in destinations:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id, 
                        text=text, 
                        reply_markup=InlineKeyboardMarkup(kb), 
                        parse_mode="Markdown"
                    )
                    success_count += 1
                except Exception as e:
                    logger.error(f"Failed to send DEL late alert to {chat_id}: {e}")
            
            logger.info(f"Sent DEL late alert for {vin} to {success_count}/{len(destinations)} chats")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error sending DEL late alert: {e}")
            return False
    
    async def _send_qc_late_alert_pu(self, context, destinations: list, vin: str, driver: str, 
                                    load_id: str, miles: int, eta_utc: datetime, appt_str: str, 
                                    late_min: int, pu_address: str) -> bool:
        """Send QC late alert for pickup"""
        try:
            text = (
                "🚨 *QC Late Alert – Pickup*\n\n"
                f"👤 *Driver:* {driver}\n"
                f"🚛 *VIN:* `{vin}`  •  *Load:* `{load_id}`\n"
                f"📍 *PU:* {pu_address}\n"
                f"🕒 *Appt:* {appt_str}\n"
                f"🛣️ *Distance:* {miles} mi  •  *ETA:* {self._fmt_local(eta_utc)}\n\n"
                f"⚠️ *Status:* Late by {late_min} min"
            )
            
            kb = [
                [InlineKeyboardButton("✅ Acknowledge", callback_data=f"ACK_LATE_PU:{vin}:{load_id}")],
                [InlineKeyboardButton("🗺 View Route", url=self._map_link(vin, pu_address))]
            ]
            
            success_count = 0
            for chat_id in destinations:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id, 
                        text=text, 
                        reply_markup=InlineKeyboardMarkup(kb), 
                        parse_mode="Markdown"
                    )
                    success_count += 1
                except Exception as e:
                    logger.error(f"Failed to send PU late alert to {chat_id}: {e}")
            
            logger.info(f"Sent PU late alert for {vin} to {success_count}/{len(destinations)} chats")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error sending PU late alert: {e}")
            return False
    
    async def _send_risk_alert_with_ack(self, context, alert) -> bool:
        """Send risk alert with acknowledgment button"""
        try:
            message = self.risk_detector.format_risk_alert_message(alert)
            
            keyboard = [
                [InlineKeyboardButton("✅ Acknowledged", callback_data=f"ack_alert_{alert.alert_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Send to QC team
            success_count = 0
            if getattr(self, 'qc_chat_id', None):
                try:
                    await context.bot.send_message(
                        chat_id=self.qc_chat_id,
                        text=message,
                        parse_mode='Markdown',
                        reply_markup=reply_markup,
                        disable_web_page_preview=True
                    )
                    success_count += 1
                except Exception as e:
                    logger.error(f"Failed to send risk alert to QC chat: {e}")
            
            # Send to management if configured
            if getattr(self, 'mgmt_chat_id', None):
                try:
                    # Parse management chat ID(s)
                    mgmt_ids = []
                    if isinstance(self.mgmt_chat_id, str) and ',' in self.mgmt_chat_id:
                        mgmt_ids = [int(x.strip()) for x in self.mgmt_chat_id.split(',') if x.strip()]
                    elif self.mgmt_chat_id:
                        mgmt_ids = [int(self.mgmt_chat_id)]
                    
                    for mgmt_id in mgmt_ids:
                        await context.bot.send_message(
                            chat_id=mgmt_id,
                            text=message,
                            parse_mode='Markdown',
                            reply_markup=reply_markup,
                            disable_web_page_preview=True
                        )
                        success_count += 1
                        
                except Exception as e:
                    logger.error(f"Failed to send risk alert to management: {e}")
            
            if success_count > 0:
                logger.info(f"Risk alert sent for {alert.vin} to {success_count} chats")
                return True
            else:
                logger.warning("Risk alert generated but no chat IDs configured")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send risk alert: {e}")
            return False
    
    async def handle_risk_alert_callback(self, update, context):
        """Handle risk alert button callbacks including ETA acknowledgments"""
        query = update.callback_query
        await query.answer()
        
        if not RISK_DETECTION_AVAILABLE:
            await query.edit_message_text("⚠️ Risk detection not available")
            return
        
        callback_data = query.data
        
        try:
            if callback_data.startswith("ack_alert_"):
                # Extract VIN from alert ID
                alert_id = callback_data.replace("ack_alert_", "")
                vin = alert_id.split("_")[0] if "_" in alert_id else alert_id
                
                # Acknowledge the alert
                if hasattr(self, 'acknowledge_alert'):
                    self.acknowledge_alert(vin)
                
                # Update message
                original_text = query.message.text
                acknowledged_text = (
                    f"{original_text}\n\n"
                    f"✅ **ACKNOWLEDGED** by {query.from_user.first_name or 'User'} "
                    f"at {datetime.now().strftime('%I:%M %p')} EDT\n"
                    f"🔕 Alerts suppressed for 24 hours"
                )
                
                keyboard = [
                    [InlineKeyboardButton("📞 Contact Driver", callback_data=f"contact_driver_{vin}")]
                ]
                
                await query.edit_message_text(
                    acknowledged_text,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    disable_web_page_preview=True
                )
                
                logger.info(f"Alert acknowledged for VIN {vin}")
            
            elif callback_data.startswith("ACK_LATE_DEL:"):
                # Acknowledge delivery late alert
                key = callback_data
                self._mute_key(key, hours=6)
                
                await query.edit_message_text(
                    f"{query.message.text}\n\n✅ **ACKNOWLEDGED** - Delivery late alerts muted for 6h",
                    parse_mode='Markdown'
                )
                logger.info(f"DEL late alert acknowledged: {key}")
            
            elif callback_data.startswith("ACK_LATE_PU:"):
                # Acknowledge pickup late alert
                key = callback_data
                self._mute_key(key, hours=6)
                
                await query.edit_message_text(
                    f"{query.message.text}\n\n✅ **ACKNOWLEDGED** - Pickup late alerts muted for 6h",
                    parse_mode='Markdown'
                )
                logger.info(f"PU late alert acknowledged: {key}")
                
            elif callback_data.startswith("contact_driver_"):
                vin = callback_data.replace("contact_driver_", "")
                await self._handle_contact_driver(query, vin)
                
        except Exception as e:
            logger.error(f"Risk alert callback error: {e}")
            await query.edit_message_text(f"❌ Error: {str(e)}")
    
    async def _handle_contact_driver(self, query, vin: str):
        """Handle contact driver request"""
        try:
            # Get driver contact info
            driver_name = "Unknown"
            driver_phone = "Not available"
            
            if hasattr(self, 'google_integration') and hasattr(self.google_integration, 'get_driver_contact_info_by_vin'):
                try:
                    name, phone = self.google_integration.get_driver_contact_info_by_vin(vin)
                    if name:
                        driver_name = name
                    if phone:
                        driver_phone = phone
                except Exception as e:
                    logger.error(f"Error getting contact info for {vin}: {e}")
            
            contact_text = (
                f"📞 **Driver Contact Information**\n\n"
                f"🚛 **VIN:** {vin}\n"
                f"👤 **Driver:** {driver_name}\n"
                f"📱 **Phone:** {driver_phone}\n\n"
                f"💡 **Recommended Actions:**\n"
                f"• Call driver to verify status\n"
                f"• Confirm load security\n"
                f"• Ask about planned departure time\n"
                f"• Document contact in log"
            )
            
            await query.edit_message_text(contact_text, parse_mode='Markdown')
            
        except Exception as e:
            await query.edit_message_text(f"❌ Error getting contact info: {str(e)}")
    
    async def _handle_risk_status(self, update, context):
        """Handle risk status display with enhanced info"""
        if not RISK_DETECTION_AVAILABLE or not hasattr(self, 'risk_detector'):
            await update.callback_query.edit_message_text(
                "⚠️ **Risk Detection Not Available**",
                parse_mode='Markdown'
            )
            return
        
        try:
            stats = self.risk_detector.get_zone_statistics()
            
            # Get acknowledgment info
            active_acks = len(getattr(self, 'acknowledged_alerts', {}))
            
            # Get muted alerts info
            active_mutes = len(self.mute_store)
            
            # QC Panel status
            qc_panel_status = "✅ Configured" if getattr(self.config, 'QC_PANEL_SPREADSHEET_ID', None) else "❌ Not configured"
            
            risk_msg = (
                f"🛡️ **Enhanced Cargo Theft Risk Status**\n\n"
                f"**Zone Coverage:**\n"
                f"• Total zones: {stats['total_zones']}\n"
                f"• Critical zones: {stats['zones_by_risk'].get('CRITICAL', 0)}\n"
                f"• High zones: {stats['zones_by_risk'].get('HIGH', 0)}\n"
                f"• Moderate zones: {stats['zones_by_risk'].get('MODERATE', 0)}\n\n"
                f"**Current Status:**\n"
                f"• Active drivers: {stats['active_drivers']}\n"
                f"• Stopped drivers: {stats['stopped_drivers']}\n"
                f"• Drivers in risk zones: {stats['drivers_in_risk_zones']}\n"
                f"• Recent alerts: {stats['recent_alerts']}\n\n"
                f"**Alert Management:**\n"
                f"• Risk acknowledgments: {active_acks} active\n"
                f"• ETA alert mutes: {active_mutes} active\n\n"
                f"**Integration Status:**\n"
                f"• QC Panel sync: {qc_panel_status}\n"
                f"• ETA alerting: {'✅ Enabled' if self.send_qc_late_alerts else '❌ Disabled'}\n"
                f"• Grace period: {self.eta_grace_minutes} minutes\n\n"
                f"**Settings:**\n"
                f"• Monitoring: {'✅ Enabled' if getattr(self, 'enable_risk_monitoring', False) else '❌ Disabled'}\n"
                f"• QC Chat: {'✅ Configured' if getattr(self, 'qc_chat_id', None) else '❌ Not set'}\n"
                f"• MGMT Chat: {'✅ Configured' if getattr(self, 'mgmt_chat_id', None) else '❌ Not set'}\n"
                f"• Risk check interval: {getattr(self, 'risk_check_interval', 300)//60} minutes\n"
                f"• Assets update interval: {getattr(self, 'assets_update_interval', 3600)//60} minutes"
            )
            
            keyboard = [
                [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_risk_status")],
                [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
            ]
            
            await update.callback_query.edit_message_text(
                risk_msg,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Risk status error: {e}")
            await update.callback_query.edit_message_text(
                f"❌ **Risk Status Error**\n\n{str(e)}",
                parse_mode='Markdown'
            )