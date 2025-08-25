
# --- HOTFIX: ensure a global error handler is always defined BEFORE use ---
import logging
from telegram.ext import ContextTypes
try:
    from telegram.error import BadRequest  # in case not imported below
except Exception:
    class BadRequest(Exception):  # fallback type
        pass

async def _global_error_handler(update, context: ContextTypes.DEFAULT_TYPE):
    """Global error handler to prevent 'No error handlers are registered' messages."""
    try:
        logging.getLogger(__name__).error("Unhandled exception in handler", exc_info=context.error)
        # Best-effort: if it's a callback query, try to answer so Telegram doesn't keep spinning
        cq = getattr(update, "callback_query", None) if update else None
        if cq:
            try:
                await cq.answer(cache_time=0)
            except BadRequest:
                pass
    except Exception:
        # Never raise from the error handler
        pass
# --- END HOTFIX ---

import logging
import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from functools import wraps
import pytz

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters
)

from config import Config
from google_integration import GoogleSheetsIntegration
from tms_integration import TMSIntegration

# Import risk detection components
try:
    from risk_integration import RiskDetectionMixin
    RISK_DETECTION_AVAILABLE = True
except ImportError:
    RISK_DETECTION_AVAILABLE = False
    # Create a dummy mixin if risk detection is not available
    class RiskDetectionMixin:
        def init_risk_detection(self):
            pass
        def schedule_risk_monitoring(self, context):
            pass
        async def handle_risk_alert_callback(self, update, context):
            pass

# Import ETA service components
try:
    from eta_service import ETAService
    ETA_SERVICE_AVAILABLE = True
except ImportError:
    ETA_SERVICE_AVAILABLE = False

logger = logging.getLogger(__name__)

# Conversation states
ASK_DRIVER_NAME, ASK_VIN, ASK_STOP_LOCATION, ASK_APPOINTMENT = range(4)

# Callback data constants
CB_GET_UPDATE = "get_update"
CB_SET_VIN = "set_vin"
CB_SEND_STOP = "send_stop"
CB_SEND_APPOINTMENT = "send_appointment"
CB_CALCULATE_ETA = "calculate_eta"
CB_HELP = "help"
CB_ADMIN_CONTACT = "admin_contact"
CB_RELOAD = "reload"  # Owner only
CB_STATUS = "status"  # Owner only
CB_RISK_STATUS = "risk_status"  # Owner only - NEW
CB_REFRESH_RISK_STATUS = "refresh_risk_status"  # NEW
CB_STOP_AUTO_REFRESH = "stop_auto_refresh"
CB_START_AUTO_REFRESH = "start_auto_refresh"
CB_BACK_TO_MAIN = "back_to_main"
CB_BACK = "back"

@dataclass
class SessionData:
    """Session data for tracking user/group state with stop duration tracking"""
    driver_name: Optional[str] = None
    vin: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    stop_address: Optional[str] = None
    appointment: Optional[str] = None
    last_updated: Optional[datetime] = None
    auto_refresh_enabled: bool = False
    auto_refresh_job_name: Optional[str] = None
    delivery_address: Optional[str] = None
    current_state: Optional[int] = None
    previous_menu: Optional[str] = None
    is_group_registered: bool = False
    last_activity: Optional[datetime] = None
    
    # Stop duration tracking
    last_speed: Optional[float] = None
    is_stopped: bool = False
    stop_start_time: Optional[datetime] = None
    total_stop_duration: timedelta = field(default_factory=lambda: timedelta())
    last_stop_duration_shown: bool = False
    last_location: Optional[tuple] = None  # (lat, lng) for location-based stop detection
    consecutive_stop_count: int = 0

class EnhancedLocationBot(RiskDetectionMixin):
    """Enhanced bot with simplified group workflow, persistent ETA options, and cargo theft risk detection"""
    
    def __init__(self, config: Config):
        self.config = config
        self.google_integration = GoogleSheetsIntegration(config)
        self.tms_integration = TMSIntegration(config)
        
        # Owner permissions
        self.owner_id = config.OWNER_TELEGRAM_ID
        
        # Session storage - in production, use Redis or database
        self.sessions: Dict[int, SessionData] = {}
        self.session_timeout_hours = getattr(config, 'SESSION_TIMEOUT_HOURS', 24)
        
        # Initialize all the attributes from __post_init__
        self.__post_init__()
    
    def log_command(self, command_name: str):
        """Decorator to log command executions to dashboard_logs worksheet"""
        def decorator(func):
            @wraps(func)
            async def wrapper(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
                start_time = time.time()
                user_id = update.effective_user.id if update.effective_user else 0
                chat_id = update.effective_chat.id if update.effective_chat else 0
                success = True
                error_message = None
                extra_info = None
                
                try:
                    # Get session info for context
                    session = self.get_session(chat_id)
                    if session.current_vin:
                        extra_info = f"VIN: {session.current_vin}"
                    elif session.registered_vin:
                        extra_info = f"Registered VIN: {session.registered_vin}"
                    
                    # Execute the command
                    result = await func(self, update, context)
                    return result
                    
                except Exception as e:
                    success = False
                    error_message = str(e)
                    logging.getLogger(__name__).error(f"Error in command {command_name}: {e}")
                    raise
                    
                finally:
                    # Log the command execution
                    duration_ms = int((time.time() - start_time) * 1000)
                    try:
                        self.google_integration.log_command_execution(
                            user_id=user_id,
                            chat_id=chat_id,
                            command=command_name,
                            success=success,
                            error_message=error_message,
                            duration_ms=duration_ms,
                            extra_info=extra_info
                        )
                    except Exception as log_error:
                        # Don't let logging errors break the command
                        logging.getLogger(__name__).warning(f"Failed to log command {command_name}: {log_error}")
                        
            return wrapper
        return decorator
    
    def __post_init__(self):
        """Initialize attributes after main __init__"""
        # Acknowledgment system for risk alerts
        self.acknowledged_alerts: Dict[str, datetime] = {}  # VIN -> acknowledgment time
        self.acknowledgment_duration = timedelta(hours=24)  # Fixed 24-hour acknowledgment duration
        
        # ETA alert muting system
        self.mute_store: Dict[str, datetime] = {}  # Alert key -> expiry time
        
        # Different intervals for different purposes
        self.group_location_interval = self.config.GROUP_LOCATION_INTERVAL  # 1 hour for location updates
        self.live_tracking_interval = self.config.LIVE_TRACKING_INTERVAL   # 5 minutes for ETA tracking
        
        # Job queue reference (set by main application)
        self.job_queue = None
    
    async def restore_group_schedules(self, context: ContextTypes.DEFAULT_TYPE):
        """Restore scheduled location updates for all registered groups on bot restart"""
        if not context.job_queue:
            logger.warning("Job queue not available, cannot restore group schedules")
            return
        
        logger.info("🔄 Restoring group schedules after bot restart...")
        
        try:
            # Get all registered groups from Google Sheets
            groups_records = self.google_integration._get_groups_records_safe()
            restored_count = 0
            
            for record in groups_records:
                try:
                    group_id = int(record.get('group_id', 0))
                    vin = record.get('vin', '').strip().upper()
                    status = record.get('status', '').strip().upper()
                    group_title = record.get('group_title', f'Group {group_id}')
                    
                    # Only restore active groups with VINs
                    if status == 'ACTIVE' and vin and group_id:
                        # Create/restore session
                        session = self.get_session(group_id)
                        session.vin = vin
                        session.is_group_registered = True
                        
                        # Schedule group location updates
                        self._schedule_group_location_updates(group_id, context)
                        restored_count += 1
                        
                        logger.debug(f"Restored schedule for group {group_id} (VIN: {vin[-4:]})")
                        
                except Exception as e:
                    logger.error(f"Failed to restore schedule for group record {record}: {e}")
                    continue
            
            logger.info(f"✅ Restored {restored_count} group location schedules on bot restart")
            
        except Exception as e:
            logger.error(f"Failed to restore group schedules: {e}")
            logger.warning("Groups may need to manually restart their location updates")
    
        # Bot instance reference (will be set when application is built)
        self.bot_instance = None
        
        # Risk monitoring settings
        self.enable_risk_monitoring = self.config.ENABLE_RISK_MONITORING
        
        # Initialize ETA service
        if ETA_SERVICE_AVAILABLE:
            try:
                from eta_service import ETAService
                self.eta_service = ETAService(self.config.ORS_API_KEY)
                logger.info("ETA service initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize ETA service: {e}")
                self.eta_service = None
        else:
            logger.warning("ETA service not available - continuing without ETA alerting")
            self.eta_service = None
        
        # Initialize risk detection (from RiskDetectionMixin)
        if RISK_DETECTION_AVAILABLE:
            try:
                self.init_risk_detection()
                logger.info("Risk detection initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize risk detection: {e}")
                self.enable_risk_monitoring = False
        else:
            logger.warning("Risk detection not available - continuing without cargo theft monitoring")
            self.enable_risk_monitoring = False

    def get_session(self, chat_id: int) -> SessionData:
        """Get or create session data for chat"""
        if chat_id not in self.sessions:
            self.sessions[chat_id] = SessionData()
        
        # Update last activity timestamp
        self.sessions[chat_id].last_activity = datetime.now()
        return self.sessions[chat_id]

    def clear_session(self, chat_id: int):
        """Clear session data"""
        if chat_id in self.sessions:
            session = self.sessions[chat_id]
            if session.auto_refresh_job_name:
                self._cancel_job(chat_id, session.auto_refresh_job_name)
            del self.sessions[chat_id]

    def _cancel_job(self, chat_id: int, job_name: str):
        """Cancel any job for a chat"""
        try:
            if self.job_queue:
                current_jobs = self.job_queue.get_jobs_by_name(job_name)
                for job in current_jobs:
                    job.schedule_removal()
                logger.info(f"Cancelled job '{job_name}' for chat {chat_id}")
        except Exception as e:
            logger.error(f"Error cancelling job: {e}")

    def is_alert_acknowledged(self, vin: str) -> bool:
        """Check if alert for VIN has been acknowledged and is still valid"""
        if vin not in self.acknowledged_alerts:
            return False
        
        ack_time = self.acknowledged_alerts[vin]
        if datetime.now() - ack_time > self.acknowledgment_duration:
            # Acknowledgment expired, remove it
            del self.acknowledged_alerts[vin]
            return False
        
        return True

    def cleanup_expired_sessions(self):
        """Remove sessions that have been inactive beyond the timeout"""
        if not self.session_timeout_hours:
            return
        
        timeout_delta = timedelta(hours=self.session_timeout_hours)
        cutoff_time = datetime.now() - timeout_delta
        expired_chats = []
        
        for chat_id, session in self.sessions.items():
            # Check if session has never had activity or is expired
            if (session.last_activity is None or 
                session.last_activity < cutoff_time):
                expired_chats.append(chat_id)
        
        # Clean up expired sessions
        for chat_id in expired_chats:
            logger.info(f"Cleaning up expired session for chat {chat_id}")
            self.clear_session(chat_id)
        
        if expired_chats:
            logger.info(f"Cleaned up {len(expired_chats)} expired sessions")
    
    def acknowledge_alert(self, vin: str):
        """Acknowledge alert for a VIN"""
        self.acknowledged_alerts[vin] = datetime.now()
        logger.info(f"Alert acknowledged for VIN {vin}")

    def cleanup_acknowledged_alerts(self):
        """Clean up expired acknowledgments and muted alerts"""
        current_time = datetime.now()
        expired_vins = []
        
        for vin, ack_time in self.acknowledged_alerts.items():
            if current_time - ack_time > self.acknowledgment_duration:
                expired_vins.append(vin)
        
        for vin in expired_vins:
            del self.acknowledged_alerts[vin]
        
        if expired_vins:
            logger.info(f"Cleaned up {len(expired_vins)} expired acknowledgments")
        
        # Also cleanup muted alerts
        self._cleanup_muted_alerts()

    async def handle_risk_alert_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle risk alert acknowledgment callbacks"""
        query = update.callback_query
        await query.answer()
        
        if not RISK_DETECTION_AVAILABLE:
            await query.edit_message_text("⚠️ Risk detection not available", parse_mode='Markdown')
            return
        
        callback_data = query.data
        
        try:
            if callback_data.startswith("ack_alert_"):
                # Extract VIN from alert ID (format: VIN_timestamp)
                alert_id = callback_data.replace("ack_alert_", "")
                vin = alert_id.split("_")[0] if "_" in alert_id else alert_id
                
                # Acknowledge the alert
                self.acknowledge_alert(vin)
                
                # Update the message to show acknowledgment
                original_text = query.message.text
                acknowledged_text = (
                    f"{original_text}\n\n"
                    f"✅ **ACKNOWLEDGED** by {query.from_user.first_name or 'User'} "
                    f"at {datetime.now().strftime('%I:%M %p')} EDT\n"
                    f"🔕 Alerts suppressed for 24 hours for this driver"
                )
                
                # Remove the acknowledgment button and add info buttons
                keyboard = [
                    [InlineKeyboardButton("🔄 Refresh Status", callback_data=CB_REFRESH_RISK_STATUS)],
                    [InlineKeyboardButton("📞 Contact Driver", callback_data=f"contact_driver_{vin}")]
                ]
                
                await query.edit_message_text(
                    acknowledged_text,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    disable_web_page_preview=True
                )
                
                logger.info(f"Risk alert acknowledged for VIN {vin} by user {query.from_user.id}")
                
            elif callback_data.startswith("contact_driver_"):
                vin = callback_data.replace("contact_driver_", "")
                await self._handle_contact_driver(query, vin)
                
            elif callback_data == CB_REFRESH_RISK_STATUS:
                await self._handle_risk_status(update, context)
                
            elif callback_data.startswith("ACK_LATE_DEL:"):
                # Handle delivery late alert acknowledgment
                await self._handle_eta_late_ack(query, callback_data, "delivery")
                
            elif callback_data.startswith("ACK_LATE_PU:"):
                # Handle pickup late alert acknowledgment
                await self._handle_eta_late_ack(query, callback_data, "pickup")
                
            else:
                await query.edit_message_text("⚠️ Unknown risk alert action", parse_mode='Markdown')
                
        except Exception as e:
            logger.error(f"Error handling risk alert callback: {e}")
            await query.edit_message_text(f"❌ Error: {str(e)}", parse_mode='Markdown')

    async def _handle_contact_driver(self, query, vin: str):
        """Handle contact driver request"""
        try:
            # Try to get driver contact info from Google Sheets
            driver_name = "Unknown"
            driver_phone = "Not available"
            
            if hasattr(self.google_integration, 'get_driver_contact_info'):
                try:
                    name, phone = self.google_integration.get_driver_contact_info(vin)
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
            
            await query.edit_message_text(
                contact_text,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            await query.edit_message_text(f"❌ Error getting contact info: {str(e)}", parse_mode='Markdown')

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

    async def _handle_eta_late_ack(self, query, callback_data: str, alert_type: str):
        """Handle ETA late alert acknowledgment"""
        try:
            # Mute the alert key for 6 hours
            self._mute_key(callback_data, hours=6)
            
            alert_type_display = "Delivery" if alert_type == "delivery" else "Pickup"
            
            await query.edit_message_text(
                f"{query.message.text}\n\n✅ **ACKNOWLEDGED** - {alert_type_display} late alerts muted for 6h",
                parse_mode='Markdown'
            )
            logger.info(f"{alert_type_display} late alert acknowledged: {callback_data}")
            
        except Exception as e:
            logger.error(f"Error handling ETA late acknowledgment: {e}")
            await query.edit_message_text(f"❌ Error: {str(e)}", parse_mode='Markdown')


    def _schedule_group_location_updates(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
        """Schedule automatic hourly location updates for groups with registered VINs"""
        session = self.get_session(chat_id)
        
        # Check if job queue is available
        if not context.job_queue:
            logger.error(f"Job queue not available for chat {chat_id}, cannot schedule updates")
            return
            
        # Cancel existing job if any
        if session.auto_refresh_job_name:
            self._cancel_job(chat_id, session.auto_refresh_job_name)
        
        # Create new job for location updates
        job_name = f"group_location_{chat_id}_{datetime.now().timestamp()}"
        
        try:
            context.job_queue.run_repeating(
                callback=self._group_location_callback,
                interval=self.group_location_interval,
                first=10,  # First run after 10 seconds
                name=job_name,
                chat_id=chat_id,
                data={'chat_id': chat_id, 'type': 'location'}
            )
            
            session.auto_refresh_job_name = job_name
            session.auto_refresh_enabled = True
            
            logger.info(f"Scheduled group location updates for chat {chat_id} every {self.group_location_interval}s")
        except Exception as e:
            logger.error(f"Failed to schedule group location updates for chat {chat_id}: {e}")

    def _schedule_live_eta_tracking(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
        """Schedule silent data refresh for ETA accuracy (no messages sent)"""
        session = self.get_session(chat_id)
        
        # Check if job queue is available
        if not context.job_queue:
            logger.error(f"Job queue not available for chat {chat_id}, cannot schedule ETA tracking")
            return
            
        # Cancel existing location job and replace with silent data refresh
        if session.auto_refresh_job_name:
            self._cancel_job(chat_id, session.auto_refresh_job_name)
        
        # Create new job for SILENT data refresh (not message sending)
        job_name = f"silent_refresh_{chat_id}_{datetime.now().timestamp()}"
        
        try:
            context.job_queue.run_repeating(
                callback=self._live_eta_callback,
                interval=self.live_tracking_interval,
                first=self.live_tracking_interval,  # First run after 5 minutes
                name=job_name,
                chat_id=chat_id,
                data={'chat_id': chat_id, 'type': 'silent_refresh'}
            )
            
            session.auto_refresh_job_name = job_name
            session.auto_refresh_enabled = True
            
            logger.info(f"Scheduled SILENT data refresh for chat {chat_id} every {self.live_tracking_interval}s (no messages)")
        except Exception as e:
            logger.error(f"Failed to schedule silent data refresh for chat {chat_id}: {e}")

    async def _group_location_callback(self, context: ContextTypes.DEFAULT_TYPE):
        """Callback for automatic group location updates (hourly)"""
        chat_id = context.job.data['chat_id']
        session = self.get_session(chat_id)
        
        try:
            # Get VIN for this group
            vin = self._get_group_vin(chat_id)
            if not vin:
                logger.debug(f"No VIN for group {chat_id}, stopping auto-updates")
                self._cancel_job(chat_id, session.auto_refresh_job_name)
                return
            
            # Fetch fresh location data
            trucks = self.tms_integration.load_truck_list()
            truck = self.tms_integration.find_truck_by_vin(trucks, vin)
            
            if not truck:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"⚠️ Truck with VIN {vin} not found in TMS data."
                )
                return
            
            # Update session with fresh coordinates
            session.lat = truck.get('lat')
            session.lng = truck.get('lng')
            session.driver_name = truck.get('name', session.driver_name)
            session.vin = vin
            session.last_updated = datetime.now()
            
            # Send location update with persistent ETA options
            await self._send_group_location_update(context, chat_id, session, truck)
            
        except Exception as e:
            logger.error(f"Group location update failed for chat {chat_id}: {e}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"⚠️ Location update error: {str(e)}"
            )

    async def _live_eta_callback(self, context: ContextTypes.DEFAULT_TYPE):
        """Callback for live ETA data refresh (silent updates for accuracy)"""
        chat_id = context.job.data['chat_id']
        session = self.get_session(chat_id)
        
        try:
            # Only refresh if we have complete data for ETA calculation
            if not all([session.vin, session.stop_address]):
                logger.debug(f"Skipping silent ETA refresh for chat {chat_id} - incomplete data")
                return
            
            # Fetch fresh location data SILENTLY
            trucks = self.tms_integration.load_truck_list()
            truck = self.tms_integration.find_truck_by_vin(trucks, session.vin)
            
            if not truck:
                logger.warning(f"Truck {session.vin} not found during silent refresh for chat {chat_id}")
                return
            
            # Update session with fresh coordinates (NO MESSAGE SENT)
            old_lat, old_lng = session.lat, session.lng
            session.lat = truck.get('lat')
            session.lng = truck.get('lng')
            session.driver_name = truck.get('name', session.driver_name)
            session.last_updated = datetime.now()
            
            # Log the silent update for debugging
            if old_lat != session.lat or old_lng != session.lng:
                logger.debug(f"Silent location update for chat {chat_id}: {old_lat},{old_lng} → {session.lat},{session.lng}")
            
            # Only send message if there's a significant change or error
            # This keeps the data fresh without spamming the chat
            
        except Exception as e:
            logger.error(f"Silent ETA refresh failed for chat {chat_id}: {e}")
            # Don't send error messages for silent updates unless critical

    def _update_stop_duration_tracking(self, session: SessionData, truck: dict, current_time_edt: datetime) -> Optional[str]:
        """Update stop duration tracking and return stop message if applicable"""
        current_speed = self._normalize_speed(truck.get('speed', 0))
        current_location = (truck.get('lat'), truck.get('lng'))
        
        # Consider stopped if speed is less than 2 mph and location hasn't changed significantly
        is_currently_stopped = current_speed < 2.0
        
        # Check if location has changed significantly (more than ~50 meters)
        if session.last_location and current_location and current_location[0] and current_location[1]:
            lat_diff = abs(current_location[0] - session.last_location[0])
            lng_diff = abs(current_location[1] - session.last_location[1])
            location_changed = (lat_diff > 0.0005) or (lng_diff > 0.0005)  # ~50m threshold
            
            # If location changed significantly, consider it moving regardless of speed
            if location_changed and current_speed > 0.5:
                is_currently_stopped = False
        
        stop_message = None
        
        # State transitions
        if is_currently_stopped:
            if not session.is_stopped:
                # Just stopped
                session.is_stopped = True
                session.stop_start_time = current_time_edt
                session.consecutive_stop_count = 1
                session.last_stop_duration_shown = False
                logger.info(f"Driver {session.vin} stopped at {current_time_edt}")
            else:
                # Still stopped - increment counter
                session.consecutive_stop_count += 1
        else:
            if session.is_stopped:
                # Just started moving
                if session.stop_start_time and not session.last_stop_duration_shown:
                    stop_duration = current_time_edt - session.stop_start_time
                    session.total_stop_duration += stop_duration
                    
                    # Format stop duration message
                    hours = int(stop_duration.total_seconds() // 3600)
                    minutes = int((stop_duration.total_seconds() % 3600) // 60)
                    
                    if hours > 0:
                        duration_str = f"{hours}h {minutes}m"
                    else:
                        duration_str = f"{minutes}m"
                    
                    stop_message = f"🟢 **Driver resumed after {duration_str} stop**"
                    session.last_stop_duration_shown = True
                    
                    logger.info(f"Driver {session.vin} resumed after {duration_str} stop")
                
                session.is_stopped = False
                session.stop_start_time = None
                session.consecutive_stop_count = 0
        
        # Update tracking variables
        session.last_speed = current_speed
        session.last_location = current_location
        
        return stop_message

    def _normalize_speed(self, speed_value: Any) -> float:
        """Normalize speed value to float for stop detection"""
        if speed_value is None:
            return 0.0
        
        try:
            if isinstance(speed_value, (int, float)):
                return float(speed_value)
            
            if isinstance(speed_value, str):
                # Remove common speed unit suffixes
                speed_clean = speed_value.lower().replace('mph', '').replace('kmh', '').replace('kph', '').strip()
                if speed_clean:
                    return float(speed_clean)
            
            return 0.0
            
        except (ValueError, TypeError):
            return 0.0

    def _get_stop_status_indicator(self, session: SessionData, current_speed: float) -> str:
        """Get stop status indicator for display"""
        if session.is_stopped and session.stop_start_time:
            edt_tz = pytz.timezone('America/New_York')
            current_time_edt = datetime.now(edt_tz)
            stop_duration = current_time_edt - session.stop_start_time
            
            hours = int(stop_duration.total_seconds() // 3600)
            minutes = int((stop_duration.total_seconds() % 3600) // 60)
            
            if hours > 0:
                return f"🔴 **Stopped** ({hours}h {minutes}m)"
            else:
                return f"🔴 **Stopped** ({minutes}m)"
        elif current_speed > 0:
            return f"🟢 **Moving**"
        else:
            return f"🔴 **Idle**"

    async def _send_group_location_update(self, context: ContextTypes.DEFAULT_TYPE, chat_id: int, session: SessionData, truck: dict):
        """Send hourly location update for groups with stop duration tracking"""
        try:
            # Use EDT timezone for all time calculations
            edt_tz = pytz.timezone('America/New_York')
            now_edt = datetime.now(edt_tz)
            map_url = f"https://maps.google.com/?q={session.lat},{session.lng}"
            
            # Update stop duration tracking and get stop message if applicable
            stop_message = self._update_stop_duration_tracking(session, truck, now_edt)
            
            # Base message format with proper speed handling and stop status
            speed_display = self._format_speed_for_display(truck.get('speed', 0))
            current_speed = self._normalize_speed(truck.get('speed', 0))
            stop_status = self._get_stop_status_indicator(session, current_speed)
            
            # Get correct driver name from Google Sheets assets data
            driver_name = self.google_integration.get_driver_name_by_vin(session.vin) or session.driver_name or 'Unknown'
            
            # Choose appropriate status emoji based on movement
            header_emoji = "🟢" if current_speed > 0 else "🔴"
            status_emoji = "🟢" if current_speed > 0 else "🔴"
            
            message = (
                f"🚛 **LIVE UPDATE** {header_emoji}\n\n"
                f"👤 **Driver:** {driver_name}\n"
                f"**Speed:** {speed_display}\n"
                f"{status_emoji} **Status:** {truck.get('status', 'Unknown').title()}\n"
            )
            
            # Add stop status indicator
            message += f"**Movement:** {stop_status}\n"
            
            # Add stop resumption message if driver just started moving
            if stop_message:
                message += f"{stop_message}\n"
            
            message += f"📍 **Current Location:** {truck.get('address', 'Unknown')}\n"
            
            # Check if we have stop location for route calculation
            if session.stop_address:
                try:
                    # Calculate route for delivery info
                    dest_coords = self.tms_integration.geocode(session.stop_address)
                    if dest_coords:
                        origin = [session.lng, session.lat]
                        route = self.tms_integration.get_route(origin, dest_coords)
                        
                        if route:
                            # Add delivery information
                            message += f"📦 **Delivery Address:** {session.stop_address}\n\n"
                            
                            # Add route information with EDT timezone
                            eta_time_edt = now_edt + route['duration']
                            message += (
                                f"🛣️ **Route Information:**\n"
                                f"• Distance: {route['distance_miles']} miles\n"
                                f"• Duration: {route['duration']}\n"
                                f"• ETA: {eta_time_edt.strftime('%I:%M %p')} EDT\n"
                                f"• Appointment: {session.appointment or '—'}\n\n"
                            )
                            
                            # Determine status based on appointment with EDT timezone
                            status_emoji = "✅"
                            status_text = "On Time"
                            
                            if session.appointment:
                                try:
                                    # Parse appointment time in EDT
                                    appt_str = session.appointment.replace("EDT", "").replace("EST", "").strip()
                                    from datetime import datetime as dt
                                    appt_time_naive = dt.strptime(appt_str, "%I:%M %p")
                                    appt_time_edt = edt_tz.localize(appt_time_naive.replace(
                                        year=now_edt.year, 
                                        month=now_edt.month, 
                                        day=now_edt.day
                                    ))
                                    
                                    if eta_time_edt > appt_time_edt:
                                        status_emoji = "⚠️"
                                        status_text = "Running Late"
                                        delay = eta_time_edt - appt_time_edt
                                        delay_minutes = int(delay.total_seconds() / 60)
                                        status_text += f" ({delay_minutes} min)"
                                except Exception as e:
                                    logger.error(f"Error parsing appointment time in group update: {e}")
                                    pass
                            
                            message += f"{status_emoji} **Status:** {status_text}\n"
                        else:
                            # Route calculation failed, but we have stop address
                            message += f"📦 **Delivery Address:** {session.stop_address}\n\n"
                except Exception as e:
                    # Geocoding or route calculation failed
                    logger.error(f"Route calculation failed in group update: {e}")
                    message += f"📦 **Delivery Address:** {session.stop_address}\n\n"
            
            # Add timestamp and next update info with EDT timezone
            message += (
                f"📡 **Last Updated:** {now_edt.strftime('%I:%M:%S %p')} EDT\n"
            )
            
            # Add data freshness warning if TMS data is stale
            data_age_warning = self._get_data_age_warning(truck)
            if data_age_warning:
                message += f"⚠️ {data_age_warning}\n"
                
            message += (
                f"\n🗺️ [View Current Location]({map_url})\n"
                f"🔄 **Next update in** 1 hour"
            )
            
            # Persistent ETA calculation buttons
            keyboard = [
                [
                    InlineKeyboardButton("📍 Set Stop Location", callback_data=CB_SEND_STOP),
                    InlineKeyboardButton("⏰ Set Appointment", callback_data=CB_SEND_APPOINTMENT)
                ]
            ]
            
            # Add Calculate ETA button if we have stop location
            if session.stop_address:
                keyboard.insert(0, [InlineKeyboardButton("↪️ Calculate ETA", callback_data=CB_CALCULATE_ETA)])
            else:
                keyboard.insert(0, [InlineKeyboardButton("💡 Set Stop Location for ETA", callback_data=CB_SEND_STOP)])
            
            # Add control buttons
            keyboard.extend([
                [InlineKeyboardButton("🛑 Stop Location Updates", callback_data=CB_STOP_AUTO_REFRESH)],
                [InlineKeyboardButton("🛠 Change VIN", callback_data=CB_SET_VIN)]
            ])
            
            await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard),
                disable_web_page_preview=True
            )
            
        except Exception as e:
            logger.error(f"Error sending group location update: {e}")
            # Send minimal error message
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"⚠️ Location update error: {str(e)[:100]}...",
                parse_mode='Markdown'
            )

    def _format_speed_for_display(self, speed_value: Any) -> str:
        """Format speed value for display in messages"""
        try:
            if speed_value is None:
                return "0 mph"
            
            # Handle different speed formats from TMS
            if isinstance(speed_value, (int, float)):
                speed = float(speed_value)
            elif isinstance(speed_value, str):
                # Remove common suffixes and convert
                speed_clean = speed_value.lower().replace('mph', '').replace('kmh', '').replace('kph', '').strip()
                speed = float(speed_clean) if speed_clean else 0.0
            else:
                speed = 0.0
            
            # Format for display
            if speed == 0.0:
                return "0 mph"
            elif speed < 1.0:
                return f"{speed:.1f} mph"
            else:
                return f"{int(speed)} mph"
                
        except (ValueError, TypeError):
            return "0 mph"

    def _get_data_age_warning(self, truck: dict) -> str:
        """Get warning message if TMS data is stale"""
        update_time_str = truck.get("update_time", "")
        if not update_time_str:
            return "GPS data timestamp unavailable"
            
        try:
            import pytz
            # Parse the TMS timestamp
            update_dt = datetime.strptime(
                update_time_str.replace("EST", "").replace("EDT", ""), 
                "%m-%d-%Y %H:%M:%S "
            ).replace(tzinfo=pytz.timezone("America/New_York"))
            
            # Calculate age in hours
            now_utc = datetime.now(pytz.utc)
            update_utc = update_dt.astimezone(pytz.utc)
            age_hours = (now_utc - update_utc).total_seconds() / 3600
            
            # More aggressive warnings for very old data
            if age_hours > 168:  # 1 week
                days = int(age_hours / 24)
                return f"🚨 GPS data is {days} days old - VERY OUTDATED"
            elif age_hours > 48:  # 2 days
                days = age_hours / 24
                return f"🚨 GPS data is {days:.1f} days old - OUTDATED"
            elif age_hours > 12:
                return f"⚠️ GPS data is {age_hours:.1f} hours old - may be outdated"
            elif age_hours > 4:
                return f"⚠️ GPS data is {age_hours:.1f} hours old"
            elif age_hours > 1:
                return f"GPS data is {age_hours:.1f} hours old"
            else:
                return ""  # Data is fresh (less than 1 hour)
                
        except Exception as e:
            logger.debug(f"Could not parse TMS update time '{update_time_str}': {e}")
            return "GPS data timestamp invalid"

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start command handler with simplified group workflow"""
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        user_id = update.effective_user.id if update.effective_user else 0
        
        # Log command execution
        self.google_integration.log_command_execution(
            user_id=user_id,
            chat_id=chat_id,
            command="start",
            success=True,
            extra_info=f"Chat type: {chat_type}"
        )
        
        # Clear any previous state
        session = self.get_session(chat_id)
        session.current_state = None
        session.previous_menu = "main"
        
        # Check if group has VIN registered
        if chat_type != 'private':
            vin = self._get_group_vin(chat_id)
            if vin:
                session.vin = vin
                session.is_group_registered = True
                
                # Start automatic location updates for registered groups (with error handling)
                try:
                    if context.job_queue:
                        self._schedule_group_location_updates(chat_id, context)
                        welcome_msg = (
                            "🚚 **Asset Tracking Bot - Group Ready**\n\n"
                            f"✅ VIN registered: {vin}\n"
                            f"📍 Hourly location updates: **ACTIVE**\n"
                            f"🔄 Next update in ~1 hour\n"
                        )
                        
                        # Add risk detection status if available
                        if RISK_DETECTION_AVAILABLE and hasattr(self, 'enable_risk_monitoring') and self.enable_risk_monitoring:
                            welcome_msg += f"🛡️ Cargo theft monitoring: **ACTIVE**\n"
                            welcome_msg += f"🔕 Alert acknowledgments: {len(self.acknowledged_alerts)} active\n"
                        
                        welcome_msg += "\n💡 **For ETA tracking:** Use buttons in location updates\nor set stop location below to start live tracking."
                    else:
                        logger.warning(f"Job queue not available for group {chat_id}, auto-updates disabled")
                        welcome_msg = (
                            "🚚 **Asset Tracking Bot - Group Ready**\n\n"
                            f"✅ VIN registered: {vin}\n"
                            f"⚠️ Automatic updates unavailable (job queue error)\n"
                            f"💡 Use manual buttons for updates"
                        )
                except Exception as e:
                    logger.error(f"Error scheduling group updates for {chat_id}: {e}")
                    welcome_msg = (
                        "🚚 **Asset Tracking Bot - Group Ready**\n\n"
                        f"✅ VIN registered: {vin}\n"
                        f"⚠️ Auto-updates failed to start: {str(e)[:50]}...\n"
                        f"💡 Use manual buttons for updates"
                    )
            else:
                welcome_msg = (
                    "🚚 **Asset Tracking Bot - Group Setup**\n\n"
                    "This group needs VIN registration for automatic tracking.\n\n"
                    "**Next Step:** Set VIN to enable hourly location updates"
                )
        else:
            welcome_msg = (
                "🚚 **Asset Tracking Bot - Private Mode**\n\n"
                "In private chat, you can track any driver by name.\n\n"
                "**Available Actions:**"
            )
        
        keyboard = self._build_main_menu(chat_type, chat_id, session, user_id)
        
        try:
            await update.message.reply_text(
                welcome_msg,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            # If reply fails, try sending directly to chat
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=welcome_msg,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except Exception as fallback_e:
                logger.error(f"Failed to send start message to {chat_id}: {e}, fallback: {fallback_e}")

    def _build_main_menu(self, chat_type: str, chat_id: int, session: SessionData, user_id: Optional[int] = None) -> List[List[InlineKeyboardButton]]:
        """Build main menu based on chat type and registration status"""
        keyboard = []
        
        if chat_type == 'private':
            # Private chat menu
            keyboard = [
                [InlineKeyboardButton("🛰 Track Driver", callback_data=CB_GET_UPDATE)],
                [
                    InlineKeyboardButton("📍 Set Stop Location", callback_data=CB_SEND_STOP),
                    InlineKeyboardButton("⏰ Set Appointment", callback_data=CB_SEND_APPOINTMENT)
                ],
                [InlineKeyboardButton("↪️ Calculate ETA", callback_data=CB_CALCULATE_ETA)],
            ]
        else:
            # Group chat menu
            if session.is_group_registered:
                # Registered group - focus on ETA functions
                keyboard = [
                    [InlineKeyboardButton("📍 Set Stop Location", callback_data=CB_SEND_STOP)],
                    [InlineKeyboardButton("⏰ Set Appointment", callback_data=CB_SEND_APPOINTMENT)],
                    [InlineKeyboardButton("↪️ Calculate ETA", callback_data=CB_CALCULATE_ETA)],
                    [InlineKeyboardButton("🛰 Get Current Location", callback_data=CB_GET_UPDATE)],
                    [InlineKeyboardButton("🛠 Change VIN", callback_data=CB_SET_VIN)]
                ]
            else:
                # Unregistered group - focus on setup
                keyboard = [
                    [InlineKeyboardButton("🛠 Set VIN (Required)", callback_data=CB_SET_VIN)],
                    [InlineKeyboardButton("🛰 Manual Update", callback_data=CB_GET_UPDATE)],
                ]
        
        # Common buttons
        keyboard.extend([
            [
                InlineKeyboardButton("🆘 Help", callback_data=CB_HELP),
                InlineKeyboardButton("📬 Contact Admin", callback_data=CB_ADMIN_CONTACT)
            ]
        ])
        
        # Owner-only buttons (FIXED: Compare user ID not chat ID)
        if self.owner_id and user_id == self.owner_id:
            owner_buttons = [
                InlineKeyboardButton("🔁 Reload", callback_data=CB_RELOAD),
                InlineKeyboardButton("📊 Status", callback_data=CB_STATUS)
            ]
            
            # Add risk status button if risk detection is available
            if RISK_DETECTION_AVAILABLE:
                owner_buttons.append(InlineKeyboardButton("🛡️ Risk Status", callback_data=CB_RISK_STATUS))
            
            keyboard.append(owner_buttons)
        
        return keyboard

    def _get_group_vin(self, group_id: int) -> Optional[str]:
        """Get VIN for a group from Google Sheets groups worksheet"""
        try:
            vin = self.google_integration.get_group_vin(group_id)
            logger.info(f"Retrieved VIN for group {group_id}: {vin}")
            return vin
        except Exception as e:
            logger.error(f"Error getting group VIN for {group_id}: {e}")
            return None

    def _save_group_vin(self, group_id: int, group_title: str, vin: str, driver_name: Optional[str] = None) -> bool:
        """Save VIN for a group to Google Sheets groups worksheet"""
        try:
            success = self.google_integration.save_group_vin(group_id, group_title, vin, driver_name)
            if success:
                logger.info(f"Successfully saved VIN for group {group_id}: {vin}")
            else:
                logger.error(f"Failed to save VIN for group {group_id}")
            return success
        except Exception as e:
            logger.error(f"Error saving group VIN for {group_id}: {e}")
            return False

    async def button_router(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Route button callbacks to appropriate handlers"""
        query = update.callback_query
        
        # Check if callback is too old (Telegram callbacks expire after ~1 hour)
        if query and hasattr(query, 'message') and query.message and hasattr(query.message, 'date'):
            callback_age = (datetime.now(query.message.date.tzinfo) - query.message.date).total_seconds()
            if callback_age > 3300:  # 55 minutes (give 5 min buffer before 1hr expiry)
                logger.warning(f"CallbackQuery too old ({callback_age:.0f}s), skipping")
                try:
                    await query.answer("❌ This button has expired. Please request a new location update.", show_alert=True)
                except Exception:
                    pass
                return
        
        # Answer quickly to avoid "query is too old" errors
        try:
            await query.answer(cache_time=0)
        except BadRequest as e:
            logger.warning(f"CallbackQuery answer skipped: {e}")
        
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        user_id = update.effective_user.id if update.effective_user else 0
        callback_data = query.data
        
        # Log button interaction
        self.google_integration.log_user_interaction(
            user_id=user_id,
            chat_id=chat_id,
            interaction_type="button_click",
            details=f"Button: {callback_data}"
        )
        
        try:
            if callback_data == CB_GET_UPDATE:
                await self._handle_get_update(update, context)
            elif callback_data == CB_SET_VIN:
                await self._handle_set_vin(update, context)
            elif callback_data == CB_SEND_STOP:
                await self._handle_send_stop_location(update, context)
            elif callback_data == CB_SEND_APPOINTMENT:
                await self._handle_send_appointment(update, context)
            elif callback_data == CB_CALCULATE_ETA:
                await self._handle_calculate_eta(update, context)
            elif callback_data == CB_HELP:
                await self._handle_help(update, context)
            elif callback_data == CB_ADMIN_CONTACT:
                await self._handle_admin_contact(update, context)
            elif callback_data == CB_STOP_AUTO_REFRESH:
                await self._handle_stop_auto_refresh(update, context)
            elif callback_data == CB_START_AUTO_REFRESH:
                await self._handle_start_auto_refresh(update, context)
            elif callback_data == CB_BACK_TO_MAIN:
                await self._handle_back_to_main(update, context)
            elif callback_data == CB_RELOAD and self.owner_id == user_id:
                await self._handle_reload(update, context)
            elif callback_data == CB_STATUS and self.owner_id == user_id:
                await self._handle_status(update, context)
            # NEW RISK-RELATED CALLBACKS
            elif callback_data == CB_RISK_STATUS and self.owner_id == user_id:
                if RISK_DETECTION_AVAILABLE:
                    await self._handle_risk_status(update, context)
                else:
                    await query.edit_message_text("⚠️ Risk detection not available", parse_mode='Markdown')
            elif callback_data == CB_REFRESH_RISK_STATUS and self.owner_id == user_id:
                if RISK_DETECTION_AVAILABLE:
                    await self._handle_risk_status(update, context)
                else:
                    await query.edit_message_text("⚠️ Risk detection not available", parse_mode='Markdown')
            elif callback_data.startswith("DRIVER_SELECT|"):
                logger.info(f"Driver selection button clicked: {callback_data}")
                await self._handle_driver_selection(update, context)
            elif callback_data.startswith(("contact_driver_", "ack_alert_", "escalate_alert_")):
                if RISK_DETECTION_AVAILABLE:
                    await self.handle_risk_alert_callback(update, context)
                else:
                    await query.edit_message_text("⚠️ Risk detection not available", parse_mode='Markdown')
            # VIN SUGGESTION SYSTEM CALLBACKS
            elif callback_data.startswith("VINSEL|") or callback_data == "MANUAL_SEARCH":
                try:
                    from vin_suggestion_handlers import on_vin_selected
                    await on_vin_selected(update, context)
                except ImportError:
                    logger.error("VIN suggestion handlers not available")
                    await query.edit_message_text("❌ VIN suggestion system not available", parse_mode='Markdown')
            # DIAGNOSTIC BUTTONS
            elif callback_data == "run_groups_diagnostic":
                # Only allow owner to run diagnostics
                if update.effective_user.id == self.owner_id:
                    await query.answer("Running groups diagnostic...")
                    # Create a fake update for the diagnostic command
                    fake_update = update
                    await self.groups_diagnostic_command(fake_update, context)
                else:
                    await query.answer("❌ Only the owner can run diagnostics", show_alert=True)
            elif callback_data == "view_sheet_structure":
                if update.effective_user.id == self.owner_id:
                    await query.edit_message_text(
                        "📋 **Expected Groups Sheet Structure:**\n\n"
                        "**Columns:**\n"
                        "• A: group_id (Telegram group ID)\n"
                        "• B: group_title (Group name)\n" 
                        "• C: vin (Vehicle VIN)\n"
                        "• D: driver_name (Driver name)\n"
                        "• E: status (ACTIVE/INACTIVE)\n"
                        "• F: last_updated (Timestamp)\n\n"
                        "**Example row:**\n"
                        "| -1001234567 | Driver Group | ABC123 | John Doe | ACTIVE | 2024-08-24 12:30:00 |",
                        parse_mode='Markdown'
                    )
                else:
                    await query.answer("❌ Only the owner can view structure", show_alert=True)
            else:
                await query.edit_message_text("⚠️ Unknown action", parse_mode='Markdown')
                
        except Exception as e:
            logger.error(f"Error in button router: {e}")
            try:
                await query.edit_message_text(f"❌ Error: {str(e)}", parse_mode='Markdown')
            except Exception as fallback_error:
                await context.bot.send_message(chat_id, f"❌ Error: {str(e)}")

    async def _handle_get_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle get current location request"""
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        session = self.get_session(chat_id)
        
        if chat_type != 'private' and session.vin:
            # Group with registered VIN
            await self._send_manual_location_update(update, context, session.vin)
        elif chat_type == 'private':
            # Private chat - ask for driver name
            session.current_state = ASK_DRIVER_NAME
            session.previous_menu = "get_update"
            
            await update.callback_query.edit_message_text(
                "👤 **Enter Driver Name:**\n\nPlease send the driver name to track:",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )
        else:
            # Group without VIN
            await update.callback_query.edit_message_text(
                "⚠️ **VIN Required**\n\nThis group needs VIN registration first.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🛠 Set VIN", callback_data=CB_SET_VIN)],
                    [InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]
                ])
            )

    async def _send_manual_location_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE, vin: str):
        """Send manual location update for registered groups"""
        chat_id = update.effective_chat.id
        session = self.get_session(chat_id)
        
        try:
            # Fetch current location
            trucks = self.tms_integration.load_truck_list()
            truck = self.tms_integration.find_truck_by_vin(trucks, vin)
            
            if not truck:
                # Check why the truck wasn't found (may be filtered due to old data)
                try:
                    vin_status = self.tms_integration.check_vin_status(vin)
                    
                    if vin_status.get("found") and vin_status.get("filtered"):
                        reason = vin_status.get("reason", "unknown")
                        if reason == "too_old":
                            age_hours = vin_status.get("age_hours", 0)
                            days = int(age_hours / 24)
                            message = (
                                f"🚨 **GPS Data Too Old**\n\n"
                                f"VIN: `{vin}`\n"
                                f"Status: Data is **{days} days old**\n\n"
                                f"❌ Location tracking unavailable\n"
                                f"📞 Contact driver directly for current location\n\n"
                                f"_GPS data over 8 hours old is filtered out for accuracy._"
                            )
                        else:
                            message = (
                                f"⚠️ **GPS Data Unavailable**\n\n"
                                f"VIN: `{vin}`\n"
                                f"Issue: {vin_status.get('message')}\n\n"
                                f"📞 Contact driver directly for location updates."
                            )
                    else:
                        message = f"⚠️ **Truck Not Found**\n\nVIN {vin} not found in TMS data."
                        
                except Exception as e:
                    logger.error(f"Error checking VIN status: {e}")
                    message = f"⚠️ **Truck Not Found**\n\nVIN {vin} not found in TMS data."
                
                await update.callback_query.edit_message_text(
                    message,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
                )
                return
            
            # Update session
            session.lat = truck.get('lat')
            session.lng = truck.get('lng')
            session.driver_name = truck.get('name')
            session.vin = vin
            session.last_updated = datetime.now()
            
            # Send location update
            await self._send_group_location_update(context, chat_id, session, truck)
            
        except Exception as e:
            logger.error(f"Manual location update failed: {e}")
            await update.callback_query.edit_message_text(
                f"❌ **Update Failed**\n\n{str(e)}",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )

    async def _handle_set_vin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle VIN setting request - now uses intelligent VIN suggestion system"""
        chat_type = update.effective_chat.type
        
        if chat_type == 'private':
            await update.callback_query.edit_message_text(
                "⚠️ **Groups Only**\n\nVIN registration is only available for group chats.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )
            return
        
        # Try auto-registration first, then fallback to manual suggestion
        try:
            from vin_suggestion_handlers import auto_register_vin_on_group_join, suggest_vin_on_group_join
            # First attempt auto-registration (won't send message if no high-confidence match)
            await auto_register_vin_on_group_join(update, context)
            
            # Check if auto-registration succeeded by checking if VIN is now set
            from vin_suggestion_handlers import get_existing_group_vin
            existing_vin = await get_existing_group_vin(update.effective_chat.id, context)
            
            if not existing_vin:
                # Auto-registration failed, show manual options
                await suggest_vin_on_group_join(update, context)
        except ImportError:
            # Fallback to old manual method if VIN suggestion system isn't available
            logger.warning("VIN suggestion system not available, using manual entry")
            chat_id = update.effective_chat.id
            session = self.get_session(chat_id)
            session.current_state = ASK_VIN
            session.previous_menu = "set_vin"
            
            await update.callback_query.edit_message_text(
                "🛠 **VIN Registration**\n\n"
                "Please send the VIN number for this group's truck.\n\n"
                "This will enable automatic hourly location updates.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )
        except Exception as e:
            logger.error(f"Error in VIN suggestion system: {e}")
            await update.callback_query.edit_message_text(
                "❌ **Error**\n\nUnable to access VIN suggestion system. Please try again later.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )

    async def _handle_send_stop_location(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle stop location setting"""
        chat_id = update.effective_chat.id
        session = self.get_session(chat_id)
        
        session.current_state = ASK_STOP_LOCATION
        session.previous_menu = "stop_location"
        
        await update.callback_query.edit_message_text(
            "📍 **Set Stop Location**\n\n"
            "Send the delivery address or stop location for ETA calculation.\n\n"
            "**Examples:**\n"
            "• 123 Main St, New York, NY\n"
            "• Walmart Distribution Center, Phoenix\n"
            "• 85323 (ZIP code)",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
        )

    async def _handle_send_appointment(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle appointment time setting"""
        chat_id = update.effective_chat.id
        session = self.get_session(chat_id)
        
        session.current_state = ASK_APPOINTMENT
        session.previous_menu = "appointment"
        
        await update.callback_query.edit_message_text(
            "⏰ **Set Appointment Time**\n\n"
            "Send the appointment time for delivery comparison.\n\n"
            "**Format Examples:**\n"
            "• 2:30 PM\n"
            "• 08:15 AM\n"
            "• 14:45\n\n"
            "Time will be interpreted as EDT timezone.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
        )

    async def _handle_calculate_eta(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle ETA calculation request"""
        chat_id = update.effective_chat.id
        session = self.get_session(chat_id)
        
        # Check if we have all required data
        if not session.vin:
            await update.callback_query.edit_message_text(
                "⚠️ **VIN Required**\n\nPlease set VIN first for ETA calculation.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🛠 Set VIN", callback_data=CB_SET_VIN)],
                    [InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]
                ])
            )
            return
        
        if not session.stop_address:
            await update.callback_query.edit_message_text(
                "⚠️ **Stop Location Required**\n\nPlease set stop location first for ETA calculation.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📍 Set Stop Location", callback_data=CB_SEND_STOP)],
                    [InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]
                ])
            )
            return
        
        try:
            # Get current location
            trucks = self.tms_integration.load_truck_list()
            truck = self.tms_integration.find_truck_by_vin(trucks, session.vin)
            
            if not truck:
                # Check why the truck wasn't found (may be filtered due to old data)
                try:
                    vin_status = self.tms_integration.check_vin_status(session.vin)
                    
                    if vin_status.get("found") and vin_status.get("filtered"):
                        reason = vin_status.get("reason", "unknown")
                        if reason == "too_old":
                            age_hours = vin_status.get("age_hours", 0)
                            days = int(age_hours / 24)
                            message = (
                                f"🚨 **GPS Data Too Old for ETA**\n\n"
                                f"VIN: `{session.vin}`\n"
                                f"GPS Age: **{days} days old**\n\n"
                                f"❌ Cannot calculate ETA with outdated location\n"
                                f"📞 Contact driver for current status\n\n"
                                f"_GPS data must be less than 8 hours old for ETA calculation._"
                            )
                        else:
                            message = (
                                f"⚠️ **GPS Data Unavailable for ETA**\n\n"
                                f"VIN: `{session.vin}`\n"
                                f"Issue: {vin_status.get('message')}\n\n"
                                f"📞 Contact driver directly for ETA updates."
                            )
                    else:
                        message = f"⚠️ **Truck Not Found**\n\nVIN {session.vin} not found in TMS data."
                        
                except Exception as e:
                    logger.error(f"Error checking VIN status for ETA: {e}")
                    message = f"⚠️ **Truck Not Found**\n\nVIN {session.vin} not found in TMS data."
                
                await update.callback_query.edit_message_text(
                    message,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
                )
                return
            
            # Update session with current data
            session.lat = truck.get('lat')
            session.lng = truck.get('lng')
            session.driver_name = truck.get('name')
            session.last_updated = datetime.now()
            
            # Calculate route
            dest_coords = self.tms_integration.geocode(session.stop_address)
            if not dest_coords:
                await update.callback_query.edit_message_text(
                    f"⚠️ **Geocoding Failed**\n\nCould not find coordinates for: {session.stop_address}",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📍 Change Stop", callback_data=CB_SEND_STOP)],
                        [InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]
                    ])
                )
                return
            
            origin = [session.lng, session.lat]
            route = self.tms_integration.get_route(origin, dest_coords)
            
            if not route:
                await update.callback_query.edit_message_text(
                    "⚠️ **Route Calculation Failed**\n\nCould not calculate route. Please try again.",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Retry", callback_data=CB_CALCULATE_ETA)],
                        [InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]
                    ])
                )
                return
            
            # Switch to silent data refresh mode for this chat
            if context.job_queue:
                self._schedule_live_eta_tracking(chat_id, context)
                logger.info(f"Switched chat {chat_id} to live ETA tracking mode")
            
            # Send detailed ETA summary
            await self._send_eta_summary(update, context, session, route)
            
        except Exception as e:
            logger.error(f"ETA calculation failed: {e}")
            await update.callback_query.edit_message_text(
                f"❌ **ETA Calculation Failed**\n\n{str(e)}",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )

    async def _send_eta_summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: SessionData, route: dict):
        """Send detailed ETA summary with correct timezone handling"""
        # Use EDT timezone for calculations
        edt_tz = pytz.timezone('America/New_York')
        now_edt = datetime.now(edt_tz)
        eta_time_edt = now_edt + route['duration']
        
        # Determine status
        status_emoji = "✅"
        status_text = "On Time"
        
        if session.appointment:
            # Parse appointment and compare
            try:
                appt_str = session.appointment.replace("EDT", "").replace("EST", "").strip()
                from datetime import datetime as dt
                
                # Parse appointment time and set it to EDT timezone
                appt_time_naive = dt.strptime(appt_str, "%I:%M %p")
                appt_time_edt = edt_tz.localize(appt_time_naive.replace(
                    year=now_edt.year, 
                    month=now_edt.month, 
                    day=now_edt.day
                ))
                
                if eta_time_edt > appt_time_edt:
                    status_emoji = "⚠️"
                    status_text = "Running Late"
                    delay = eta_time_edt - appt_time_edt
                    delay_minutes = int(delay.total_seconds() / 60)
                    status_text += f" ({delay_minutes} min)"
                    
            except Exception as e:
                logger.error(f"Error parsing appointment time: {e}")
                pass
        
        map_url = f"https://maps.google.com/?q={session.lat},{session.lng}"
        
        message = (
            f"🚛 **ETA Calculation Summary**\n\n"
            f"👤 **Driver:** {session.driver_name or 'Unknown'}\n"
            f"🚛 **Unit:** {session.vin}\n"
            f"📍 **Current Location:** {session.lat:.4f}, {session.lng:.4f}\n"
            f"📦 **Delivery Address:** {session.stop_address}\n\n"
            f"🛣️ **Route Information:**\n"
            f"• Distance: {route['distance_miles']} miles\n"
            f"• Duration: {route['duration']}\n"
            f"• ETA: {eta_time_edt.strftime('%I:%M %p')} EDT\n"
            f"• Appointment: {session.appointment or '—'}\n\n"
            f"{status_emoji} **Status:** {status_text}\n"
            f"📡 **Calculated:** {now_edt.strftime('%Y-%m-%d %I:%M %p')} EDT\n\n"
            f"🗺️ [View Route on Map]({map_url})"
        )
        
        keyboard = [
            [InlineKeyboardButton("🔄 Recalculate ETA", callback_data=CB_CALCULATE_ETA)],
            [
                InlineKeyboardButton("📍 Change Stop", callback_data=CB_SEND_STOP),
                InlineKeyboardButton("⏰ Change Appointment", callback_data=CB_SEND_APPOINTMENT)
            ],
            [InlineKeyboardButton("🛑 Stop Data Refresh", callback_data=CB_STOP_AUTO_REFRESH)],
            [InlineKeyboardButton("🏠 Back to Main Menu", callback_data=CB_BACK_TO_MAIN)]
        ]
        
        await update.callback_query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard),
            disable_web_page_preview=True
        )

    async def _handle_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle help request"""
        chat_type = update.effective_chat.type
        
        if chat_type == 'private':
            help_text = (
                "🆘 **Private Chat Help**\n\n"
                "**Available Commands:**\n"
                "• 🛰 **Track Driver** - Get location by driver name\n"
                "• 📍 **Set Stop Location** - Set delivery address\n"
                "• ⏰ **Set Appointment** - Set delivery time\n"
                "• ↪️ **Calculate ETA** - Get route and arrival time\n\n"
                "**How to Use:**\n"
                "1. Click 'Track Driver' and enter driver name\n"
                "2. Set stop location for ETA calculation\n"
                "3. Optionally set appointment time\n"
                "4. Calculate ETA for detailed route info"
            )
        else:
            help_text = (
                "🆘 **Group Chat Help**\n\n"
                "**Setup (One-time):**\n"
                "• 🛠 **Set VIN** - Register truck VIN for auto-updates\n\n"
                "**Features:**\n"
                "• 📍 **Hourly Location Updates** - Automatic after VIN setup\n"
                "• ↪️ **ETA Calculation** - Real-time route tracking\n"
                "• 🛑 **Stop Duration Tracking** - Monitor driver breaks\n"
                "• 🕐 **EDT Timezone** - Accurate appointment comparison\n"
            )
            
            # Add risk detection info if available
            if RISK_DETECTION_AVAILABLE:
                help_text += (
                    "• 🛡️ **Cargo Theft Protection** - Automatic risk zone monitoring\n"
                    "• ✅ **Alert Acknowledgment** - Suppress alerts for drivers at home\n"
                )
            
            help_text += (
                "\n**ETA Tracking:**\n"
                "1. Set stop location using buttons in updates\n"
                "2. Set appointment time (optional)\n"
                "3. Bot provides continuous ETA updates\n"
                "4. Silent data refresh every 5 minutes for accuracy\n\n"
                "**Alert System:**\n"
                "• Risk alerts sent to QC team\n"
                "• Click ✅ Acknowledged to stop alerts for 24h\n"
                "• Use for drivers at home or authorized stops\n\n"
                "**Bot Response Policy:**\n"
                "• Responds to commands and button clicks\n"
                "• Responds when mentioned (@botname)\n"
                "• Responds to replies to bot messages\n"
                "• Won't respond to general group messages"
            )
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]]
        
        await update.callback_query.edit_message_text(
            help_text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def _handle_admin_contact(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin contact request"""
        admin_username = getattr(self.config, 'ADMIN_USERNAME', 'your_admin')
        
        contact_text = (
            "📬 **Contact Administrator**\n\n"
            f"For support or issues, contact: @{admin_username}\n\n"
            "**Common Issues:**\n"
            "• Truck not found - Check VIN spelling\n"
            "• Location outdated - May take up to 1 hour for fresh data\n"
            "• ETA inaccurate - Verify stop location address\n"
            "• Missing features - Ensure bot has proper permissions"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]]
        
        await update.callback_query.edit_message_text(
            contact_text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def _handle_stop_auto_refresh(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle stopping auto refresh"""
        chat_id = update.effective_chat.id
        session = self.get_session(chat_id)
        
        if session.auto_refresh_job_name:
            self._cancel_job(chat_id, session.auto_refresh_job_name)
            session.auto_refresh_enabled = False
            session.auto_refresh_job_name = None
            
            await update.callback_query.edit_message_text(
                "🛑 **Auto-Updates Stopped**\n\n"
                "Automatic location updates have been disabled.\n\n"
                "You can restart them anytime or use manual updates.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Restart Auto-Updates", callback_data=CB_START_AUTO_REFRESH)],
                    [InlineKeyboardButton("🏠 Back to Main Menu", callback_data=CB_BACK_TO_MAIN)]
                ])
            )
        else:
            await update.callback_query.edit_message_text(
                "ℹ️ **No Active Updates**\n\n"
                "No automatic updates are currently running.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Back to Main Menu", callback_data=CB_BACK_TO_MAIN)]])
            )

    async def _handle_start_auto_refresh(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle starting auto refresh"""
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        session = self.get_session(chat_id)
        
        if chat_type != 'private' and session.vin:
            if context.job_queue:
                self._schedule_group_location_updates(chat_id, context)
                
                await update.callback_query.edit_message_text(
                    "🔄 **Auto-Updates Restarted**\n\n"
                    "Hourly location updates have been restarted.\n\n"
                    "Next update in ~1 hour.",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Back to Main Menu", callback_data=CB_BACK_TO_MAIN)]])
                )
            else:
                await update.callback_query.edit_message_text(
                    "⚠️ **Job Queue Unavailable**\n\n"
                    "Cannot start auto-updates (job queue error).",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Back to Main Menu", callback_data=CB_BACK_TO_MAIN)]])
                )
        else:
            await update.callback_query.edit_message_text(
                "⚠️ **VIN Required**\n\n"
                "Please set VIN first to enable auto-updates.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🛠 Set VIN", callback_data=CB_SET_VIN)],
                    [InlineKeyboardButton("🏠 Back to Main Menu", callback_data=CB_BACK_TO_MAIN)]
                ])
            )

    async def _handle_back_to_main(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle back to main menu"""
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        user_id = update.effective_user.id if update.effective_user else None
        session = self.get_session(chat_id)
        
        # Clear conversation state
        session.current_state = None
        session.previous_menu = "main"
        
        welcome_msg = "🚚 **Asset Tracking Bot - Main Menu**"
        keyboard = self._build_main_menu(chat_type, chat_id, session, user_id)
        
        await update.callback_query.edit_message_text(
            welcome_msg,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def _handle_reload(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle reload command (owner only)"""
        try:
            # Reload integrations
            self.google_integration = GoogleSheetsIntegration(self.config)
            self.tms_integration = TMSIntegration(self.config)
            
            reload_msg = (
                "🔁 **Reload Complete**\n\n"
                "✅ Google Sheets integration reloaded\n"
                "✅ TMS integration reloaded\n"
                "✅ Configuration refreshed"
            )
            
            # Add risk detection reload status if available
            if RISK_DETECTION_AVAILABLE and hasattr(self, 'risk_detector'):
                reload_msg += "\n✅ Risk detection zones refreshed"
            
            await update.callback_query.edit_message_text(
                reload_msg,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Back to Main Menu", callback_data=CB_BACK_TO_MAIN)]])
            )
        except Exception as e:
            await update.callback_query.edit_message_text(
                f"❌ **Reload Failed**\n\n{str(e)}",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Back to Main Menu", callback_data=CB_BACK_TO_MAIN)]])
            )

    async def _handle_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle status command (owner only) with TMS and Google Sheets debug info"""
        active_sessions = len([s for s in self.sessions.values() if s.auto_refresh_enabled])
        total_sessions = len(self.sessions)
        
        # Get TMS data for debugging
        try:
            trucks = self.tms_integration.load_truck_list()
            total_trucks = len(trucks) if trucks else 0
        except Exception as e:
            total_trucks = f"Error: {str(e)}"
        
        # Get Google Sheets data for debugging
        try:
            driver_names = self.google_integration.get_all_driver_names()
            total_drivers = len(driver_names)
            sample_drivers = driver_names[:5] if driver_names else []
            sample_text = "\n".join([f"• {name}" for name in sample_drivers]) if sample_drivers else "No drivers found"
        except Exception as e:
            total_drivers = f"Error: {str(e)}"
            sample_text = f"Google Sheets Error: {str(e)}"
        
        # Get Enhanced Group Scheduler status
        scheduler_status = "❌ Not Available"
        scheduled_groups = 0
        try:
            # Check if we can access the scheduler from main.py global variable
            import main
            if hasattr(main, 'scheduler_instance') and main.scheduler_instance:
                try:
                    active_groups = main.scheduler_instance.backend.get_active_groups()
                    scheduled_groups = len(active_groups)
                    scheduler_status = f"✅ Active ({scheduled_groups} groups)"
                    
                    # Get scheduler stats if available
                    if hasattr(main.scheduler_instance, 'stats'):
                        stats = main.scheduler_instance.stats
                        successful = stats.get('successful_sends', 0)
                        failed = stats.get('failed_sends', 0)
                        scheduler_status += f", {successful}✅/{failed}❌"
                except Exception as e:
                    scheduler_status = f"⚠️ Backend Error: {str(e)[:50]}"
            else:
                scheduler_status = "⚠️ Instance Not Available"
        except Exception as e:
            scheduler_status = f"❌ Error: {str(e)[:50]}"

        status_msg = (
            f"📊 **Bot Status**\n\n"
            f"👥 **Active Sessions:** {total_sessions}\n"
            f"🔄 **Session Auto-Updates:** {active_sessions} (user-enabled)\n"
            f"⚙️ **Enhanced Scheduler:** {scheduler_status}\n"
            f"⏱️ **Group Interval:** {self.group_location_interval//60} min\n"
            f"🔄 **Live Interval:** {self.live_tracking_interval//60} min\n"
            f"💾 **Job Queue:** {'✅ Available' if context.job_queue else '❌ Unavailable'}\n"
            f"🔕 **Acknowledged Alerts:** {len(self.acknowledged_alerts)}\n\n"
            f"🚛 **TMS Data:**\n"
            f"• Total Trucks: {total_trucks}\n\n"
            f"📋 **Google Sheets Data:**\n"
            f"• Total Drivers: {total_drivers}\n"
            f"• Sample Drivers:\n{sample_text}"
        )
        
        # Add risk detection status if available
        if RISK_DETECTION_AVAILABLE and hasattr(self, 'risk_detector'):
            risk_zones = len(self.risk_detector.risk_zones) if hasattr(self.risk_detector, 'risk_zones') else 0
            risk_enabled = getattr(self, 'enable_risk_monitoring', False)
            status_msg += (
                f"\n\n🛡️ **Risk Detection:**\n"
                f"• Risk zones: {risk_zones}\n"
                f"• Monitoring: {'✅ Active' if risk_enabled else '❌ Disabled'}\n"
                f"• QC alerts: {'✅ Configured' if getattr(self, 'qc_chat_id', None) else '❌ Not configured'}"
            )
        else:
            status_msg += f"\n\n🛡️ **Risk Detection:** ❌ Not available"
        
        # Add available owner commands
        status_msg += (
            f"\n\n💼 **Owner Commands:**\n"
            f"• `/updateassets` - Update assets worksheet with TMS data\n"
            f"• `/listnewtrucks` - List trucks in TMS not in assets\n"
            f"• `/addtruck <VIN>` - Add specific truck to assets\n"
            f"• `/updateall` - Send updates to all registered groups"
        )
        
        await update.callback_query.edit_message_text(
            status_msg,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Back to Main Menu", callback_data=CB_BACK_TO_MAIN)]])
        )

    async def _handle_risk_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle risk status display (owner only)"""
        if not RISK_DETECTION_AVAILABLE or not hasattr(self, 'risk_detector'):
            await update.callback_query.edit_message_text(
                "⚠️ **Risk Detection Not Available**\n\nRisk detection modules are not installed or configured.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )
            return
        
        try:
            # Get risk statistics
            stats = self.risk_detector.get_zone_statistics()
            
            # Get acknowledgment info
            active_acks = len(self.acknowledged_alerts)
            ack_list = []
            for vin, ack_time in self.acknowledged_alerts.items():
                time_left = self.acknowledgment_duration - (datetime.now() - ack_time)
                hours_left = int(time_left.total_seconds() // 3600)
                ack_list.append(f"• {vin}: {hours_left}h remaining")
            
            ack_text = "\n".join(ack_list[:5]) if ack_list else "None"
            if len(ack_list) > 5:
                ack_text += f"\n• ... and {len(ack_list) - 5} more"
            
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
                f"• ETA alerting: {'✅ Enabled' if getattr(self.config, 'SEND_QC_LATE_ALERTS', True) else '❌ Disabled'}\n"
                f"• Grace period: {getattr(self.config, 'ETA_GRACE_MINUTES', 10)} minutes\n\n"
                f"**Settings:**\n"
                f"• Monitoring: {'✅ Enabled' if getattr(self, 'enable_risk_monitoring', False) else '❌ Disabled'}\n"
                f"• QC Chat: {'✅ Configured' if getattr(self, 'qc_chat_id', None) else '❌ Not set'}\n"
                f"• MGMT Chat: {'✅ Configured' if getattr(self, 'mgmt_chat_id', None) else '❌ Not set'}\n"
                f"• Risk check interval: {getattr(self, 'risk_check_interval', 300)//60} minutes\n"
                f"• Assets update interval: {getattr(self, 'assets_update_interval', 3600)//60} minutes"
            )
            
            keyboard = [
                [InlineKeyboardButton("🔄 Refresh", callback_data=CB_REFRESH_RISK_STATUS)],
                [InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]
            ]
            
            await update.callback_query.edit_message_text(
                risk_msg,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Error getting risk status: {e}")
            await update.callback_query.edit_message_text(
                f"❌ **Risk Status Error**\n\n{str(e)}",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )

    async def update_all_groups(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command to manually trigger location updates for all registered groups (owner only)"""
        user_id = update.effective_user.id if update.effective_user else 0
        chat_id = update.effective_chat.id
        
        # Check if user is owner
        if user_id != self.owner_id:
            await update.message.reply_text("❌ This command is only available to the bot owner.")
            return
        
        # Log command execution
        self.google_integration.log_command_execution(
            user_id=user_id,
            chat_id=chat_id,
            command="updateall",
            success=True,
            extra_info="Owner command execution"
        )
        
        try:
            # Get all registered groups from Google Sheets
            logger.info("Attempting to get groups records from Google Sheets...")
            
            # Check if groups worksheet is available
            if not self.google_integration.groups_worksheet:
                await update.message.reply_text("❌ **Groups worksheet not initialized**\n\nThere may be a connection issue with Google Sheets.")
                logger.error("Groups worksheet is None - not initialized properly")
                return
            
            groups_records = self.google_integration._get_groups_records_safe()
            logger.info(f"Retrieved {len(groups_records)} total records from groups sheet")
            
            # Debug: Show first few records
            if groups_records:
                logger.debug(f"Sample record: {groups_records[0] if groups_records else 'No records'}")
            
            # Filter for active groups with VINs
            groups = []
            skipped_count = 0
            for i, record in enumerate(groups_records):
                group_id = record.get('group_id')
                vin = record.get('vin', '').strip()
                status = record.get('status', '').strip().upper()
                
                logger.debug(f"Record {i}: group_id={group_id}, vin={vin}, status={status}")
                
                if group_id and vin and status == 'ACTIVE':
                    try:
                        groups.append({
                            'group_id': int(group_id),
                            'vin': vin,
                            'group_title': record.get('group_title', ''),
                            'status': status
                        })
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Skipped record {i} due to conversion error: {e}")
                        skipped_count += 1
                        continue
                else:
                    skipped_count += 1
                    if not group_id:
                        logger.debug(f"Record {i} skipped: missing group_id")
                    elif not vin:
                        logger.debug(f"Record {i} skipped: missing VIN")
                    elif status != 'ACTIVE':
                        logger.debug(f"Record {i} skipped: status is '{status}', not 'ACTIVE'")
            
            logger.info(f"Processed groups: {len(groups)} active, {skipped_count} skipped")
            
            if not groups:
                # Provide more detailed error message with actionable steps
                error_msg = f"📭 **No active groups found**\n\n"
                error_msg += f"📊 **Sheet Analysis:**\n"
                error_msg += f"• Total records: {len(groups_records)}\n"
                error_msg += f"• Skipped records: {skipped_count}\n\n"
                error_msg += f"**Troubleshooting steps:**\n"
                error_msg += f"1. Use `/groupsdiag` for detailed sheet analysis\n"
                error_msg += f"2. Check that groups have status 'ACTIVE'\n"
                error_msg += f"3. Verify group_id and vin columns are filled\n"
                error_msg += f"4. Add groups to the 'groups' sheet manually\n\n"
                error_msg += f"💡 **Auto-register groups with VIN suggestion system**"
                
                keyboard = [
                    [InlineKeyboardButton("🔍 Run Diagnostic", callback_data="run_groups_diagnostic")],
                    [InlineKeyboardButton("📋 View Sheet Structure", callback_data="view_sheet_structure")]
                ]
                
                await update.message.reply_text(
                    error_msg, 
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return
            
            # Send initial status message
            status_msg = await update.message.reply_text(
                f"🔄 **Triggering location updates for all groups...**\n\n"
                f"📊 Found {len(groups)} registered groups\n"
                f"⏳ Processing updates..."
            )
            
            success_count = 0
            error_count = 0
            errors = []
            
            # Process each group
            for i, group in enumerate(groups):
                group_id = group.get('group_id')
                vin = group.get('vin')
                
                if not group_id or not vin:
                    error_count += 1
                    errors.append(f"Group {group_id or 'Unknown'}: Missing VIN or ID")
                    continue
                
                try:
                    # Get truck location from TMS
                    trucks = self.tms_integration.load_truck_list()
                    truck = self.tms_integration.find_truck_by_vin(trucks, vin)
                    
                    if not truck:
                        error_count += 1
                        errors.append(f"Group {group_id}: Truck not found for VIN {vin}")
                        continue
                    
                    # Format truck info
                    truck_info = self.tms_integration.format_truck_info(truck)
                    
                    # Build location update message
                    lat = truck_info.get('latitude', 0)
                    lng = truck_info.get('longitude', 0)
                    
                    # Get correct driver name from Google Sheets assets data
                    driver_name = self.google_integration.get_driver_name_by_vin(vin) or 'Unknown Driver'
                    
                    status = truck_info.get('status', 'Unknown')
                    location = truck_info.get('location', 'Unknown Location')
                    speed = truck_info.get('speed_display', '0 mph')
                    normalized_speed = truck_info.get('speed', 0)
                    
                    # Choose appropriate status emoji based on movement
                    status_emoji = "🟢" if normalized_speed > 0 else "🔴"
                    
                    message = (
                        f"🚛 **Manual Location Update**\n\n"
                        f"👤 **Driver:** {driver_name}\n"
                        f"🚛 **Unit:** {vin}\n"
                        f"{status_emoji} **Status:** {status}\n"
                        f"📍 **Location:** {location}\n"
                        f"🏃 **Speed:** {speed}\n"
                        f"📡 **Updated:** {datetime.now(pytz.timezone('America/New_York')).strftime('%H:%M:%S ET')}\n\n"
                        f"🗺️ [View on Map](https://maps.google.com/?q={lat},{lng})"
                    )
                    
                    # Send message to group
                    await context.bot.send_message(
                        chat_id=group_id,
                        text=message,
                        parse_mode="Markdown",
                        disable_web_page_preview=True
                    )
                    
                    success_count += 1
                    
                    # Update progress every 5 groups
                    if (i + 1) % 5 == 0:
                        progress_msg = (
                            f"🔄 **Progress Update**\n\n"
                            f"✅ Processed: {i + 1}/{len(groups)} groups\n"
                            f"📤 Successful: {success_count}\n"
                            f"❌ Errors: {error_count}"
                        )
                        await status_msg.edit_text(progress_msg, parse_mode="Markdown")
                    
                    # Small delay to avoid rate limiting
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    error_count += 1
                    errors.append(f"Group {group_id}: {str(e)}")
                    logger.error(f"Error updating group {group_id}: {e}")
            
            # Send final status
            final_msg = (
                f"✅ **Update All Groups Complete**\n\n"
                f"📊 **Summary:**\n"
                f"• Total groups: {len(groups)}\n"
                f"• Successful updates: {success_count}\n"
                f"• Failed updates: {error_count}\n"
            )
            
            if errors and len(errors) <= 10:
                final_msg += f"\n❌ **Errors:**\n"
                for error in errors[:10]:
                    final_msg += f"• {error}\n"
                if len(errors) > 10:
                    final_msg += f"• ... and {len(errors) - 10} more errors"
            elif errors:
                final_msg += f"\n❌ **{len(errors)} errors occurred** (check logs for details)"
            
            await status_msg.edit_text(final_msg, parse_mode="Markdown")
            
        except Exception as e:
            logger.error(f"Error in update_all_groups command: {e}")
            await update.message.reply_text(
                f"❌ **Command Failed**\n\n"
                f"Error: {str(e)}\n\n"
                f"Check logs for more details."
            )

    async def update_assets_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command to manually update assets worksheet with current TMS data (owner only)"""
        user_id = update.effective_user.id if update.effective_user else 0
        chat_id = update.effective_chat.id
        
        # Check if user is owner
        if user_id != self.owner_id:
            await update.message.reply_text("❌ This command is only available to the bot owner.")
            return
        
        # Log command execution
        self.google_integration.log_command_execution(
            user_id=user_id,
            chat_id=chat_id,
            command="updateassets",
            success=True,
            extra_info="Owner command execution"
        )
        
        try:
            # Send initial status message
            status_msg = await update.message.reply_text(
                f"🔄 **Updating Assets Worksheet...**\n\n"
                f"📊 Loading current truck data from TMS...\n"
                f"⏳ This may take a minute..."
            )
            
            # Call the Google integration method to update assets (no limit - process all trucks)
            result = self.google_integration.update_assets_with_current_data()
            
            if "error" in result:
                await status_msg.edit_text(
                    f"❌ **Assets Update Failed**\n\n"
                    f"Error: {result['error']}\n\n"
                    f"Check logs for more details.",
                    parse_mode="Markdown"
                )
                return
            
            # Build success message
            success_msg = (
                f"✅ **Assets Update Complete**\n\n"
                f"📊 **Summary:**\n"
                f"• Trucks processed: {result.get('trucks_processed', 0)}\n"
                f"• Assets updated: {result.get('assets_updated', 0)}\n"
                f"• Field updates made: {result.get('field_updates_made', 0)}\n"
                f"• New trucks found: {result.get('new_trucks_found', 0)}\n"
                f"• Errors: {result.get('errors', 0)}\n"
                f"• Completed at: {result.get('timestamp', 'Unknown')}\n"
            )
            
            # Add error details if any
            error_details = result.get('error_details', [])
            if error_details:
                success_msg += f"\n⚠️ **Errors encountered:**\n"
                for error in error_details[:5]:  # Show max 5 errors
                    success_msg += f"• {error}\n"
                if len(error_details) > 5:
                    success_msg += f"• ... and {len(error_details) - 5} more errors"
            
            # Add helpful info
            if result.get('new_trucks_found', 0) > 0:
                success_msg += (
                    f"\n💡 **Note:** Found {result.get('new_trucks_found', 0)} trucks in TMS "
                    f"that are not in the assets worksheet. Check logs for VINs."
                )
            
            await status_msg.edit_text(success_msg, parse_mode="Markdown")
            
        except Exception as e:
            logger.error(f"Error in update_assets_command: {e}")
            try:
                await status_msg.edit_text(
                    f"❌ **Command Failed**\n\n"
                    f"Error: {str(e)}\n\n"
                    f"Check logs for more details.",
                    parse_mode="Markdown"
                )
            except Exception as fallback_error:
                await update.message.reply_text(
                    f"❌ **Command Failed**\n\n"
                    f"Error: {str(e)}\n\n"
                    f"Check logs for more details."
                )

    async def list_new_trucks_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command to list trucks found in TMS but not in assets worksheet (owner only)"""
        user_id = update.effective_user.id if update.effective_user else 0
        chat_id = update.effective_chat.id
        
        # Check if user is owner
        if user_id != self.owner_id:
            await update.message.reply_text("❌ This command is only available to the bot owner.")
            return
        
        # Log command execution
        self.google_integration.log_command_execution(
            user_id=user_id,
            chat_id=chat_id,
            command="listnewtrucks",
            success=True,
            extra_info="Owner command execution"
        )
        
        try:
            # Send initial status message
            status_msg = await update.message.reply_text(
                f"🔍 **Scanning for New Trucks...**\n\n"
                f"📊 Comparing TMS data with assets worksheet...\n"
                f"⏳ This may take a moment..."
            )
            
            # Get list of new trucks
            result = self.google_integration.list_new_trucks_found(limit=20)
            
            if "error" in result:
                await status_msg.edit_text(
                    f"❌ **Scan Failed**\n\n"
                    f"Error: {result['error']}\n\n"
                    f"Check logs for more details.",
                    parse_mode="Markdown"
                )
                return
            
            new_trucks = result.get('new_trucks', [])
            
            if not new_trucks:
                await status_msg.edit_text(
                    f"✅ **Scan Complete**\n\n"
                    f"🎉 No new trucks found - all TMS trucks are already in assets worksheet!\n\n"
                    f"📊 **Summary:**\n"
                    f"• Total TMS trucks: {result.get('total_tms_trucks', 0)}\n"
                    f"• Existing assets: {result.get('existing_assets', 0)}",
                    parse_mode="Markdown"
                )
                return
            
            # Build message with new trucks list
            msg_lines = [
                f"🆕 **New Trucks Found**\n",
                f"Found {len(new_trucks)} trucks in TMS that are not in assets worksheet:\n"
            ]
            
            for i, truck in enumerate(new_trucks, 1):
                msg_lines.append(
                    f"**{i}.** `{truck['vin']}`\n"
                    f"   📍 Unit: {truck.get('unit', 'Unknown')}\n"
                    f"   📍 Location: {truck.get('location', 'Unknown')}\n"
                    f"   🔄 Status: {truck.get('status', 'Unknown')}\n"
                )
            
            msg_lines.extend([
                f"\n📊 **Summary:**\n"
                f"• Total TMS trucks: {result.get('total_tms_trucks', 0)}\n"
                f"• Existing assets: {result.get('existing_assets', 0)}\n"
                f"• New trucks found: {len(new_trucks)}\n",
                f"\n💡 **To add a truck:** Use `/addtruck <VIN>`"
            ])
            
            success_msg = "\n".join(msg_lines)
            
            # Split message if too long
            if len(success_msg) > 4000:
                # Send first part with summary
                summary_msg = (
                    f"🆕 **New Trucks Found**\n\n"
                    f"Found {len(new_trucks)} trucks in TMS that are not in assets worksheet.\n\n"
                    f"📊 **Summary:**\n"
                    f"• Total TMS trucks: {result.get('total_tms_trucks', 0)}\n"
                    f"• Existing assets: {result.get('existing_assets', 0)}\n"
                    f"• New trucks found: {len(new_trucks)}\n\n"
                    f"💡 **To add a truck:** Use `/addtruck <VIN>`\n\n"
                    f"📋 **First 10 trucks:**"
                )
                await status_msg.edit_text(summary_msg, parse_mode="Markdown")
                
                # Send VINs list
                vins_list = "\n".join([f"`{truck['vin']}`" for truck in new_trucks[:10]])
                await update.message.reply_text(vins_list, parse_mode="Markdown")
                
                if len(new_trucks) > 10:
                    remaining_vins = "\n".join([f"`{truck['vin']}`" for truck in new_trucks[10:]])
                    await update.message.reply_text(f"**Remaining trucks:**\n{remaining_vins}", parse_mode="Markdown")
            else:
                await status_msg.edit_text(success_msg, parse_mode="Markdown")
            
        except Exception as e:
            logger.error(f"Error in list_new_trucks_command: {e}")
            try:
                await status_msg.edit_text(
                    f"❌ **Command Failed**\n\n"
                    f"Error: {str(e)}\n\n"
                    f"Check logs for more details.",
                    parse_mode="Markdown"
                )
            except Exception as fallback_error:
                await update.message.reply_text(
                    f"❌ **Command Failed**\n\n"
                    f"Error: {str(e)}\n\n"
                    f"Check logs for more details."
                )

    async def add_truck_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command to manually add a specific truck by VIN to assets worksheet (owner only)"""
        user_id = update.effective_user.id if update.effective_user else 0
        chat_id = update.effective_chat.id
        
        # Check if user is owner
        if user_id != self.owner_id:
            await update.message.reply_text("❌ This command is only available to the bot owner.")
            return
        
        # Check if VIN was provided
        if not context.args or len(context.args) != 1:
            await update.message.reply_text(
                "❌ **Usage Error**\n\n"
                "Please provide a VIN: `/addtruck <VIN>`\n\n"
                "Example: `/addtruck 1HGBH41JXMN109186`",
                parse_mode="Markdown"
            )
            return
        
        vin = context.args[0].strip()
        
        # Log command execution
        self.google_integration.log_command_execution(
            user_id=user_id,
            chat_id=chat_id,
            command="addtruck",
            success=True,
            extra_info=f"Adding VIN: {vin}"
        )
        
        try:
            # Send initial status message
            status_msg = await update.message.reply_text(
                f"🔄 **Adding Truck to Assets...**\n\n"
                f"🆔 VIN: `{vin}`\n"
                f"📊 Searching TMS data...\n"
                f"⏳ Please wait..."
            )
            
            # Add the truck
            result = self.google_integration.add_new_truck_to_assets(vin)
            
            if "error" in result:
                await status_msg.edit_text(
                    f"❌ **Add Truck Failed**\n\n"
                    f"🆔 VIN: `{vin}`\n"
                    f"Error: {result['error']}\n\n"
                    f"💡 **Tips:**\n"
                    f"• Check if VIN exists in TMS\n"
                    f"• Check if truck already in assets\n"
                    f"• Use `/listnewtrucks` to see available trucks",
                    parse_mode="Markdown"
                )
                return
            
            # Build success message
            success_msg = (
                f"✅ **Truck Added Successfully**\n\n"
                f"🆔 VIN: `{result.get('vin', 'Unknown')}`\n"
                f"📍 Unit: {result.get('unit', 'Unknown')}\n"
                f"📍 Location: {result.get('location', 'Unknown')}\n"
                f"🔄 Status: {result.get('status', 'Unknown')}\n"
                f"⏰ Added at: {result.get('timestamp', 'Unknown')}\n\n"
                f"💡 **Note:** Driver name and phone need to be filled manually in the worksheet."
            )
            
            await status_msg.edit_text(success_msg, parse_mode="Markdown")
            
        except Exception as e:
            logger.error(f"Error in add_truck_command: {e}")
            try:
                await status_msg.edit_text(
                    f"❌ **Command Failed**\n\n"
                    f"🆔 VIN: `{vin}`\n"
                    f"Error: {str(e)}\n\n"
                    f"Check logs for more details.",
                    parse_mode="Markdown"
                )
            except Exception as fallback_error:
                await update.message.reply_text(
                    f"❌ **Command Failed**\n\n"
                    f"🆔 VIN: `{vin}`\n"
                    f"Error: {str(e)}\n\n"
                    f"Check logs for more details."
                )

    async def groups_diagnostic_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Diagnostic command to check groups sheet status"""
        if update.effective_user.id != self.owner_id:
            await update.message.reply_text("❌ This command is only available to the owner.")
            return

        try:
            msg = "🔍 **Groups Sheet Diagnostic**\n\n"
            
            # Check worksheet initialization
            if not self.google_integration.groups_worksheet:
                msg += "❌ **Groups worksheet not initialized**\n"
                msg += "Check Google Sheets connection and worksheet name.\n"
            else:
                msg += "✅ **Groups worksheet connected**\n\n"
                
                # Get worksheet info
                try:
                    worksheet = self.google_integration.groups_worksheet
                    msg += f"📋 **Worksheet Info:**\n"
                    msg += f"• Title: {worksheet.title}\n"
                    msg += f"• ID: {worksheet.id}\n"
                    msg += f"• Row count: {worksheet.row_count}\n"
                    msg += f"• Col count: {worksheet.col_count}\n\n"
                    
                    # Get headers
                    try:
                        headers = worksheet.row_values(1)
                        msg += f"📝 **Headers ({len(headers)}):**\n"
                        for i, header in enumerate(headers[:10]):  # Show first 10
                            msg += f"• Col {i+1}: '{header}'\n"
                        if len(headers) > 10:
                            msg += f"• ... and {len(headers) - 10} more\n"
                        msg += "\n"
                    except Exception as e:
                        msg += f"❌ **Error reading headers:** {e}\n\n"
                    
                    # Get record count
                    try:
                        records = self.google_integration._get_groups_records_safe()
                        msg += f"📊 **Records:** {len(records)} total\n"
                        
                        if records:
                            # Analyze first record
                            sample = records[0]
                            msg += f"🔍 **Sample record keys:**\n"
                            for key in list(sample.keys())[:8]:  # Show first 8 keys
                                msg += f"• '{key}': '{sample.get(key, '')}'\n"
                            
                            # Count by status
                            status_counts = {}
                            for record in records:
                                status = record.get('status', '').strip().upper()
                                status_counts[status] = status_counts.get(status, 0) + 1
                            
                            msg += f"\n📈 **Status breakdown:**\n"
                            for status, count in status_counts.items():
                                msg += f"• '{status}': {count}\n"
                        
                    except Exception as e:
                        msg += f"❌ **Error reading records:** {e}\n"
                        
                except Exception as e:
                    msg += f"❌ **Error accessing worksheet:** {e}\n"
            
            await update.message.reply_text(msg)
            
        except Exception as e:
            await update.message.reply_text(f"❌ **Diagnostic failed:** {str(e)}")
    
    async def validate_data_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Validate data integrity between sheets and TMS"""
        if update.effective_user.id != self.owner_id:
            await update.message.reply_text("❌ This command is only available to the owner.")
            return

        try:
            status_msg = await update.message.reply_text("🔍 **Validating data integrity...**\n\n⏳ Checking assets sheet...")
            
            # Check for common VIN mapping conflicts
            validation_results = []
            
            try:
                # Get all assets records
                assets_records = self.google_integration._get_assets_records_safe()
                
                # Check for duplicate VINs
                vin_counts = {}
                driver_vin_map = {}
                
                for record in assets_records:
                    vin = record.get('VIN', '').strip().upper()
                    driver = record.get('Driver Name', '').strip()
                    
                    if vin:
                        vin_counts[vin] = vin_counts.get(vin, 0) + 1
                        if vin in driver_vin_map and driver_vin_map[vin] != driver:
                            validation_results.append(f"❌ **VIN {vin}** mapped to multiple drivers: '{driver_vin_map[vin]}' and '{driver}'")
                        else:
                            driver_vin_map[vin] = driver
                
                # Find duplicate VINs
                duplicate_vins = [vin for vin, count in vin_counts.items() if count > 1]
                for vin in duplicate_vins:
                    validation_results.append(f"⚠️ **Duplicate VIN:** {vin} appears {vin_counts[vin]} times")
                
                # Check specific case mentioned by user
                test_vin = "1FUJHHDR4LLLN2336"
                if test_vin in driver_vin_map:
                    actual_driver = driver_vin_map[test_vin]
                    validation_results.append(f"ℹ️ **VIN {test_vin}** is mapped to: '{actual_driver}'")
                
                await status_msg.edit_text(
                    f"🔍 **Data Validation Results**\n\n"
                    f"📊 **Assets Sheet:**\n"
                    f"• Total records: {len(assets_records)}\n"
                    f"• Unique VINs: {len(vin_counts)}\n"
                    f"• Duplicate VINs: {len(duplicate_vins)}\n\n"
                    f"🔧 **Issues Found:**\n" + 
                    ("\n".join(validation_results) if validation_results else "✅ No major issues detected")
                )
                
            except Exception as e:
                await status_msg.edit_text(f"❌ **Validation failed:** {str(e)}")
                
        except Exception as e:
            await update.message.reply_text(f"❌ **Validation error:** {str(e)}")
    
    async def worksheets_health_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /workshealth command to check worksheet update status"""
        user_id = update.effective_user.id if update.effective_user else 0
        chat_id = update.effective_chat.id
        
        # Check if user is owner
        if user_id != self.owner_id:
            await update.message.reply_text("❌ This command is only available to the bot owner.")
            return
        
        try:
            # Import worksheet monitor
            from worksheet_monitor import create_worksheet_monitor
            
            # Create monitor instance
            monitor = create_worksheet_monitor(self.google_integration, self.config)
            
            # Send initial status message
            status_message = await update.message.reply_text(
                "🔍 **Checking Worksheet Health...**\n\nAnalyzing all active worksheets...",
                parse_mode='Markdown'
            )
            
            # Check all worksheets
            statuses = await monitor.check_all_worksheets()
            
            # Generate report
            report = monitor.generate_health_report(statuses)
            
            # Update the message with the full report
            await status_message.edit_text(
                report,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
            
            # Log the action
            self.google_integration.log_command_execution(
                user_id=user_id,
                chat_id=chat_id,
                command="workshealth",
                success=True,
                extra_info=f"Checked {len(statuses)} worksheets"
            )
            
        except Exception as e:
            logger.error(f"Error in worksheets_health_command: {e}")
            await update.message.reply_text(f"❌ Error checking worksheet health: {str(e)}")
            
            # Log the error
            self.google_integration.log_command_execution(
                user_id=user_id,
                chat_id=chat_id,
                command="workshealth",
                success=False,
                error_message=str(e)
            )

    async def auto_register_groups_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Command to automatically register groups by parsing driver names from titles (owner only)"""
        user_id = update.effective_user.id if update.effective_user else 0
        chat_id = update.effective_chat.id
        
        # Check if user is owner
        if user_id != self.owner_id:
            await update.message.reply_text("❌ This command is only available to the bot owner.")
            return
        
        # Log command execution
        self.google_integration.log_command_execution(
            user_id=user_id,
            chat_id=chat_id,
            command="autoregister",
            success=True,
            extra_info="Owner command execution"
        )
        
        try:
            # Send initial status message
            status_msg = await update.message.reply_text(
                f"🤖 **Auto-Registering Groups...**\n\n"
                f"📊 Parsing driver names from group titles...\n"
                f"🔍 Matching to VINs in assets sheet...\n"
                f"⏳ This may take a few minutes..."
            )
            
            # Import the driver name matcher
            from driver_name_matcher import DriverNameMatcher
            
            # Initialize the matcher
            matcher = DriverNameMatcher(self.google_integration)
            
            # Get all groups from the bot
            groups_data = []
            
            # Get groups from bot's group cache or iterate through known groups
            if hasattr(self, 'group_cache') and self.group_cache:
                for group_id, group_info in self.group_cache.items():
                    groups_data.append({
                        'group_id': group_id,
                        'title': group_info.get('title', ''),
                        'owner_user_id': user_id
                    })
            else:
                # If no group cache, we'll need to get groups from Telegram API
                # For now, we'll show a message about this limitation
                await status_msg.edit_text(
                    f"❌ **Auto-Registration Failed**\n\n"
                    f"Group cache not available. This feature requires the bot to have "
                    f"access to group information.\n\n"
                    f"**Alternative:** Use the manual registration with parsed driver names.",
                    parse_mode="Markdown"
                )
                return
            
            if not groups_data:
                await status_msg.edit_text(
                    f"❌ **No Groups Found**\n\n"
                    f"No groups available for auto-registration.\n"
                    f"Make sure the bot is added to groups and has access to group information.",
                    parse_mode="Markdown"
                )
                return
            
            # Perform batch auto-registration
            results = await matcher.batch_auto_register_groups(groups_data)
            
            # Build results message
            success_msg = (
                f"✅ **Auto-Registration Complete**\n\n"
                f"📊 **Summary:**\n"
                f"• Total groups processed: {results['total_groups']}\n"
                f"• Successfully registered: {results['successful']}\n"
                f"• Failed registrations: {results['failed']}\n"
                f"• Success rate: {(results['successful'] / results['total_groups'] * 100):.1f}%\n\n"
            )
            
            # Show successful registrations
            if results['successes']:
                success_msg += f"✅ **Successfully Registered:**\n"
                for success in results['successes'][:10]:  # Show first 10
                    success_msg += f"• {success['driver_name']} → {success['vin']} ({success['confidence']})\n"
                if len(results['successes']) > 10:
                    success_msg += f"• ... and {len(results['successes']) - 10} more\n"
                success_msg += "\n"
            
            # Show errors
            if results['errors']:
                success_msg += f"❌ **Errors:**\n"
                for error in results['errors'][:5]:  # Show first 5 errors
                    success_msg += f"• {error}\n"
                if len(results['errors']) > 5:
                    success_msg += f"• ... and {len(results['errors']) - 5} more errors\n"
                success_msg += "\n"
            
            # Add helpful tips
            success_msg += (
                f"💡 **Tips:**\n"
                f"• Use `/autoregister` to run this again after adding new groups\n"
                f"• Check group titles follow the format: 'ID - Code - Driver Name - (Code) - Truck_XXX'\n"
                f"• Manual registration still available with `/addtruck`\n"
            )
            
            await status_msg.edit_text(success_msg, parse_mode="Markdown")
            
        except Exception as e:
            logger.error(f"Error in auto_register_groups_command: {e}")
            try:
                await status_msg.edit_text(
                    f"❌ **Auto-Registration Failed**\n\n"
                    f"Error: {str(e)}\n\n"
                    f"Check logs for more details.",
                    parse_mode="Markdown"
                )
            except Exception as fallback_error:
                await update.message.reply_text(
                    f"❌ **Auto-Registration Failed**\n\n"
                    f"Error: {str(e)}\n\n"
                    f"Check logs for more details."
                )

    async def handle_text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages based on conversation state - respond to all messages immediately"""
        
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        user_id = update.effective_user.id if update.effective_user else 0
        session = self.get_session(chat_id)
        user_input = update.message.text.strip()
        
        # Log user message interaction
        self.google_integration.log_user_interaction(
            user_id=user_id,
            chat_id=chat_id,
            interaction_type="text_message",
            details=f"State: {session.current_state}, Input: {user_input[:50]}{'...' if len(user_input) > 50 else ''}"
        )
        
        # Remove bot mention from input if present
        if self.bot_instance and self.bot_instance.username:
            bot_mention = f"@{self.bot_instance.username}"
            if bot_mention in user_input:
                user_input = user_input.replace(bot_mention, "").strip()
        
        if session.current_state == ASK_DRIVER_NAME:
            await self._process_driver_name(update, context, user_input)
        elif session.current_state == ASK_VIN:
            await self._process_vin(update, context, user_input)
        elif session.current_state == ASK_STOP_LOCATION:
            await self._process_stop_location(update, context, user_input)
        elif session.current_state == ASK_APPOINTMENT:
            await self._process_appointment(update, context, user_input)
        else:
            # No active conversation state - only respond in groups if mentioned or replied to
            if chat_type == 'private':
                response_text = "ℹ️ Use the menu buttons to interact with the bot."
                await update.message.reply_text(
                    response_text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data=CB_BACK_TO_MAIN)]])
                )
            elif chat_type in ['group', 'supergroup']:
                # In groups, only respond if bot was mentioned or message is a reply to bot
                bot_mentioned = (self.bot_instance and self.bot_instance.username and 
                               f"@{self.bot_instance.username}" in update.message.text)
                is_reply_to_bot = (update.message.reply_to_message and 
                                 update.message.reply_to_message.from_user.is_bot)
                
                if bot_mentioned or is_reply_to_bot:
                    response_text = "ℹ️ Use the menu buttons or mention me to interact."
                    await update.message.reply_text(
                        response_text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data=CB_BACK_TO_MAIN)]])
                    )
                # Otherwise, ignore the message to prevent group spam

    async def _process_driver_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE, driver_name: str):
        """Process driver name input with Google Sheets lookup first"""
        chat_id = update.effective_chat.id
        session = self.get_session(chat_id)
        
        try:
            # Step 1: Look up driver name in Google Sheets to get VIN
            logger.info(f"Looking up driver '{driver_name}' in Google Sheets...")
            vin = self._find_vin_by_driver_name(driver_name)
            
            # Check if we should show fuzzy matching options
            # Show fuzzy matching if:
            # 1. No exact match found, OR
            # 2. Input is very short (likely partial name), OR  
            # 3. Input appears to be partial (no spaces and short)
            should_show_fuzzy = (
                not vin or  # No match found
                len(driver_name.strip()) <= 4 or  # Very short input
                (len(driver_name.strip()) <= 8 and " " not in driver_name.strip())  # Short single word
            )
            
            if should_show_fuzzy:
                logger.info(f"Showing fuzzy matching for input '{driver_name}' (vin_found: {bool(vin)}, length: {len(driver_name)})")
                # Try to provide helpful suggestions from Google Sheets
                suggestions = self._find_similar_driver_names_from_sheets(driver_name)
                
                if suggestions:
                    logger.info(f"Creating fuzzy matching buttons for {len(suggestions)} driver suggestions")
                    # Create inline buttons for suggestions
                    keyboard = []
                    for name in suggestions[:5]:  # Limit to 5 suggestions
                        # Truncate long names for button display, but be smarter about it
                        if len(name) > 25:
                            # Try to keep important parts (names before slashes)
                            parts = name.split(' / ')
                            if len(parts) > 1:
                                display_name = parts[0][:15] + "..." if len(parts[0]) > 15 else parts[0]
                                display_name += f" / {parts[-1][:8]}..." if len(parts[-1]) > 8 else f" / {parts[-1]}"
                            else:
                                display_name = name[:25] + "..."
                        else:
                            display_name = name
                        
                        callback_data = f"DRIVER_SELECT|{name}"
                        logger.debug(f"Creating button: '{display_name}' with callback: '{callback_data}'")
                        keyboard.append([InlineKeyboardButton(f"👤 {display_name}", callback_data=callback_data)])
                    
                    # Add back button
                    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)])
                    
                    logger.info(f"Sending fuzzy matching message with {len(keyboard)} buttons")
                    
                    if vin:
                        # Found a match but showing options anyway
                        message = (
                            f"🔍 **Driver Name Suggestions**\n\n"
                            f"Input: **{driver_name}**\n\n"
                            f"**Multiple drivers found:**\n"
                            f"💡 Tap the correct driver name below:"
                        )
                    else:
                        # No match found
                        message = (
                            f"⚠️ **Driver Not Found**\n\n"
                            f"No exact match for: **{driver_name}**\n\n"
                            f"**Similar names found:**\n"
                            f"💡 Tap a driver name below to select:"
                        )
                else:
                    message = (
                        f"⚠️ **Driver Not Found**\n\n"
                        f"No driver found with name: **{driver_name}**\n\n"
                        f"💡 **Tips:**\n"
                        f"• Check spelling\n"
                        f"• Try first name only\n"
                        f"• Try last name only\n"
                        f"• Make sure driver is registered in system"
                    )
                    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]]
                
                try:
                    await update.message.reply_text(
                        message,
                        parse_mode='Markdown',
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    logger.info("Fuzzy matching message sent successfully with inline buttons")
                except Exception as button_error:
                    logger.error(f"Failed to send fuzzy matching buttons: {button_error}")
                    # Fallback without buttons
                    await update.message.reply_text(
                        message + f"\n\n⚠️ **Button error**: {str(button_error)}",
                        parse_mode='Markdown'
                    )
                return
            
            # If we have a VIN and didn't show fuzzy matching, proceed with location lookup
            if not vin:
                logger.warning(f"No VIN found for driver {driver_name} and no suggestions available")
                await update.message.reply_text(
                    f"⚠️ **Driver Not Found**\n\nNo driver found with name: **{driver_name}**",
                    parse_mode='Markdown'
                )
                return
            
            # Step 2: Verify the VIN-to-driver mapping is correct
            logger.info(f"Found VIN {vin} for driver {driver_name}, verifying mapping...")
            
            # Check if this VIN actually belongs to a different driver in the sheets
            try:
                # Do reverse lookup: VIN -> correct driver name  
                correct_driver = self.google_integration.get_driver_name_by_vin(vin)
                if correct_driver and correct_driver.lower() != driver_name.lower():
                    logger.warning(f"VIN mapping conflict! Input: '{driver_name}' -> VIN: {vin}, but VIN {vin} actually belongs to '{correct_driver}'")
                    # Use the correct driver name instead
                    driver_name = correct_driver
                    logger.info(f"Corrected driver name to: {driver_name}")
            except Exception as e:
                logger.debug(f"Could not verify VIN mapping: {e}")
            
            # Step 3: Get current location from TMS using VIN
            logger.info(f"Getting location from TMS for VIN {vin} (driver: {driver_name})...")
            trucks = self.tms_integration.load_truck_list()
            truck = self.tms_integration.find_truck_by_vin(trucks, vin)
            
            if not truck:
                await update.message.reply_text(
                    f"⚠️ **Truck Not Found in TMS**\n\n"
                    f"Driver: **{driver_name}**\n"
                    f"VIN: **{vin}**\n\n"
                    f"The VIN was found in our database but the truck is not currently reporting location data.",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
                )
                return
            
            # Step 3: Update session with combined data
            session.driver_name = driver_name  # Use the searched name, not TMS coded name
            session.vin = vin
            session.lat = truck.get('lat')
            session.lng = truck.get('lng')
            session.last_updated = datetime.now()
            session.current_state = None
            
            logger.info(f"Successfully found driver {driver_name} at {session.lat}, {session.lng}")
            
            # Send location update - handle both message and callback query contexts
            await self._send_private_location_update(update, context, session, truck)
            
        except Exception as e:
            logger.error(f"Error processing driver name: {e}")
            
            # Handle both message and callback contexts
            error_message = f"❌ **Error:** {str(e)}"
            error_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            
            try:
                if update.message:
                    await update.message.reply_text(error_message, parse_mode='Markdown', reply_markup=error_markup)
                elif update.callback_query:
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=error_message,
                        parse_mode='Markdown',
                        reply_markup=error_markup
                    )
            except Exception as send_error:
                logger.error(f"Failed to send error message: {send_error}")

    def _find_vin_by_driver_name(self, driver_name: str) -> Optional[str]:
        """Find VIN by driver name using Google Sheets lookup"""
        try:
            return self.google_integration.find_vin_by_driver_name(driver_name)
        except Exception as e:
            logger.error(f"Error finding VIN for driver {driver_name}: {e}")
            return None

    def _find_similar_driver_names_from_sheets(self, search_name: str) -> List[str]:
        """Find similar driver names from Google Sheets for suggestions"""
        try:
            return self.google_integration.find_similar_driver_names(search_name)
        except Exception as e:
            logger.error(f"Error finding similar names for {search_name}: {e}")
            return []

    async def _handle_driver_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle driver selection from suggestion buttons"""
        query = update.callback_query
        await query.answer()
        
        try:
            # Extract driver name from callback data
            logger.debug(f"Processing driver selection callback: {query.data}")
            _, driver_name = query.data.split("|", 1)
            
            logger.info(f"Driver selected from fuzzy matching: {driver_name}")
            
            # Update the fuzzy matching message to show selection and progress
            try:
                await query.edit_message_text(
                    f"✅ **Selected:** {driver_name}\n\n🔄 Getting location...",
                    parse_mode='Markdown'
                )
                # Store the message for potential cleanup later
                context.user_data = context.user_data or {}
                context.user_data['progress_message'] = query.message
            except Exception as edit_error:
                logger.debug(f"Could not edit fuzzy matching message: {edit_error}")
            
            # Process the selected driver name
            await self._process_driver_name(update, context, driver_name)
            
        except Exception as e:
            logger.error(f"Error handling driver selection: {e}")
            await query.edit_message_text(
                f"❌ **Error:** {str(e)}",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )

    async def _send_private_location_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE, session: SessionData, truck: dict):
        """Send location update for private chats"""
        try:
            map_url = f"https://maps.google.com/?q={session.lat},{session.lng}"
            speed_display = self._format_speed_for_display(truck.get('speed', 0))
            current_speed = self._normalize_speed(truck.get('speed', 0))
            
            # Get correct driver name from Google Sheets assets data
            sheets_driver = self.google_integration.get_driver_name_by_vin(session.vin) or session.driver_name or 'Unknown'
            
            # Debug: Check for driver name conflicts
            tms_driver = truck.get('driver_name', 'Not in TMS')
            
            if tms_driver and tms_driver != 'Not in TMS' and tms_driver.lower() != sheets_driver.lower():
                logger.warning(f"Driver name mismatch for VIN {session.vin}: TMS='{tms_driver}' vs Sheets='{sheets_driver}'")
            
            # Use Sheets driver name as primary, but show TMS if different
            display_driver = sheets_driver
            if tms_driver and tms_driver != 'Not in TMS' and tms_driver.lower() != sheets_driver.lower():
                display_driver = f"{sheets_driver} (TMS: {tms_driver})"
            
            # Choose appropriate status emoji based on movement
            status_emoji = "🟢" if current_speed > 0 else "🔴"
            
            # Use NY timezone for updated time
            edt_tz = pytz.timezone('America/New_York')
            updated_time_edt = session.last_updated.replace(tzinfo=pytz.utc).astimezone(edt_tz) if session.last_updated else datetime.now(edt_tz)
            
            message = (
                f"📍 **Driver Location**\n\n"
                f"👤 **Driver:** {display_driver}\n"
                f"🚛 **Unit:** {session.vin}\n"
                f"**Speed:** {speed_display}\n"
                f"{status_emoji} **Status:** {truck.get('status', 'Unknown').title()}\n"
                f"📍 **Location:** {truck.get('address', 'Unknown')}\n"
                f"📡 **Updated:** {updated_time_edt.strftime('%I:%M %p')} ET\n"
            )
            
            # Add data freshness warning if TMS data is stale
            data_age_warning = self._get_data_age_warning(truck)
            if data_age_warning:
                message += f"⚠️ {data_age_warning}\n"
                
            message += f"\n🗺️ [View on Map]({map_url})"
            
            keyboard = [
                [
                    InlineKeyboardButton("📍 Set Stop Location", callback_data=CB_SEND_STOP),
                    InlineKeyboardButton("⏰ Set Appointment", callback_data=CB_SEND_APPOINTMENT)
                ],
                [InlineKeyboardButton("↪️ Calculate ETA", callback_data=CB_CALCULATE_ETA)],
                [InlineKeyboardButton("🏠 Main Menu", callback_data=CB_BACK_TO_MAIN)]
            ]
            
            logger.debug(f"Sending private location update with {len(keyboard)} button rows")
            
            # Determine how to send the message based on update type
            send_method = None
            if update.message:
                # Direct message - use reply_text
                send_method = update.message.reply_text
            elif update.callback_query:
                # Callback query - send new message to the chat
                send_method = lambda text, **kwargs: context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=text,
                    **kwargs
                )
            else:
                logger.error("Unable to determine send method - no message or callback_query")
                return
            
            # Try sending with buttons first
            try:
                await send_method(
                    message,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    disable_web_page_preview=True
                )
                logger.info("Private location update sent successfully with inline buttons")
                
                # Clean up any progress message from fuzzy matching selection
                try:
                    progress_message = context.user_data.get('progress_message') if context.user_data else None
                    if progress_message:
                        await context.bot.delete_message(
                            chat_id=progress_message.chat.id,
                            message_id=progress_message.message_id
                        )
                        context.user_data.pop('progress_message', None)
                        logger.debug("Cleaned up progress message after location update")
                except Exception as cleanup_error:
                    logger.debug(f"Could not clean up progress message: {cleanup_error}")
                
            except Exception as button_error:
                logger.error(f"Failed to send buttons with location message: {button_error}")
                # Send without buttons as fallback
                try:
                    await send_method(
                        message + f"\n\n⚠️ **Buttons failed**: {str(button_error)}",
                        parse_mode='Markdown',
                        disable_web_page_preview=True
                    )
                except Exception as fallback_error:
                    logger.error(f"Fallback send also failed: {fallback_error}")
            
        except Exception as e:
            logger.error(f"Error in _send_private_location_update: {e}")
            # Fallback - send without buttons if there's an error
            try:
                # Get correct driver name from Google Sheets assets data
                fallback_driver_name = self.google_integration.get_driver_name_by_vin(session.vin) or session.driver_name or 'Unknown'
                
                # Choose appropriate status emoji based on movement  
                fallback_status_emoji = "🟢" if current_speed > 0 else "🔴"
                
                # Use NY timezone for updated time
                fallback_updated_time = updated_time_edt.strftime('%I:%M %p') if 'updated_time_edt' in locals() else session.last_updated.strftime('%I:%M %p')
                
                fallback_message = (
                    f"📍 **Driver Location**\n\n"
                    f"👤 **Driver:** {fallback_driver_name}\n"
                    f"🚛 **Unit:** {session.vin}\n"
                    f"**Speed:** {speed_display}\n"
                    f"{fallback_status_emoji} **Status:** {truck.get('status', 'Unknown').title()}\n"
                    f"📍 **Location:** {truck.get('address', 'Unknown')}\n"
                    f"📡 **Updated:** {fallback_updated_time} ET\n\n"
                    f"🗺️ [View on Map]({map_url})\n\n"
                    f"⚠️ Inline buttons unavailable due to error: {str(e)}"
                )
                await update.message.reply_text(
                    fallback_message,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
            except Exception as fallback_error:
                logger.error(f"Fallback message also failed: {fallback_error}")

    async def _process_vin(self, update: Update, context: ContextTypes.DEFAULT_TYPE, vin: str):
        """Process VIN input for group registration"""
        chat_id = update.effective_chat.id
        chat_type = update.effective_chat.type
        session = self.get_session(chat_id)
        
        try:
            # Validate VIN exists in TMS
            trucks = self.tms_integration.load_truck_list()
            truck = self.tms_integration.find_truck_by_vin(trucks, vin)
            
            if not truck:
                await update.message.reply_text(
                    f"⚠️ **VIN Not Found**\n\nVIN {vin} not found in TMS data.\n\nPlease check the VIN and try again.",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
                )
                return
            
            # Save VIN to Google Sheets
            group_title = update.effective_chat.title or f"Group {chat_id}"
            driver_name = truck.get('name')
            
            success = self._save_group_vin(chat_id, group_title, vin, driver_name)
            
            if not success:
                await update.message.reply_text(
                    "❌ **Registration Failed**\n\nCould not save VIN to database. Please try again.",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
                )
                return
            
            # Update session
            session.vin = vin
            session.driver_name = driver_name
            session.is_group_registered = True
            session.current_state = None
            
            # Start automatic location updates
            if context.job_queue:
                self._schedule_group_location_updates(chat_id, context)
                success_msg = (
                    f"✅ **VIN Set & Auto-Updates Started**\n\n"
                    f"🚛 **VIN:** {vin}\n"
                    f"👤 **Driver:** {driver_name}\n"
                    f"📍 **Hourly location updates:** ACTIVE\n"
                    f"🔄 **Next update:** ~1 hour\n"
                )
                
                # Add risk monitoring status if available
                if RISK_DETECTION_AVAILABLE and hasattr(self, 'enable_risk_monitoring') and self.enable_risk_monitoring:
                    success_msg += f"🛡️ **Cargo theft monitoring:** ACTIVE\n"
                
                success_msg += f"\n💡 Use buttons in location updates for ETA tracking!"
            else:
                success_msg = (
                    f"✅ **VIN Set Successfully**\n\n"
                    f"🚛 **VIN:** {vin}\n"
                    f"👤 **Driver:** {driver_name}\n"
                    f"⚠️ **Auto-updates unavailable** (job queue error)\n\n"
                    f"Use manual buttons for updates."
                )
            
            await update.message.reply_text(
                success_msg,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data=CB_BACK_TO_MAIN)]])
            )
            
        except Exception as e:
            logger.error(f"Error processing VIN: {e}")
            await update.message.reply_text(
                f"❌ **Error:** {str(e)}",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )

    async def _process_stop_location(self, update: Update, context: ContextTypes.DEFAULT_TYPE, location: str):
        """Process stop location input"""
        chat_id = update.effective_chat.id
        session = self.get_session(chat_id)
        
        try:
            # Test geocoding
            coords = self.tms_integration.geocode(location)
            if not coords:
                await update.message.reply_text(
                    f"⚠️ **Location Not Found**\n\nCould not find coordinates for: {location}\n\nPlease try a more specific address.",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
                )
                return
            
            # Save location
            session.stop_address = location
            session.current_state = None
            
            await update.message.reply_text(
                f"✅ **Stop Location Set**\n\n📍 **Address:** {location}\n\n💡 Now you can calculate ETA!",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("↪️ Calculate ETA", callback_data=CB_CALCULATE_ETA)],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data=CB_BACK_TO_MAIN)]
                ])
            )
            
        except Exception as e:
            logger.error(f"Error processing stop location: {e}")
            await update.message.reply_text(
                f"❌ **Error:** {str(e)}",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )

    async def _process_appointment(self, update: Update, context: ContextTypes.DEFAULT_TYPE, appointment: str):
        """Process appointment time input"""
        chat_id = update.effective_chat.id
        session = self.get_session(chat_id)
        
        try:
            # Validate time format
            from datetime import datetime as dt
            
            # Try different time formats
            time_formats = ["%I:%M %p", "%H:%M", "%I:%M%p"]
            parsed_time = None
            
            for fmt in time_formats:
                try:
                    parsed_time = dt.strptime(appointment.upper(), fmt)
                    break
                except ValueError:
                    continue
            
            if not parsed_time:
                await update.message.reply_text(
                    f"⚠️ **Invalid Time Format**\n\n"
                    f"Could not parse: {appointment}\n\n"
                    f"**Try these formats:**\n"
                    f"• 2:30 PM\n"
                    f"• 08:15 AM\n"
                    f"• 14:45",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
                )
                return
            
            # Save appointment (always store in consistent format)
            formatted_time = parsed_time.strftime("%I:%M %p")
            session.appointment = formatted_time
            session.current_state = None
            
            await update.message.reply_text(
                f"✅ **Appointment Time Set**\n\n⏰ **Time:** {formatted_time} EDT\n\n💡 Calculate ETA to compare with appointment!",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("↪️ Calculate ETA", callback_data=CB_CALCULATE_ETA)],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data=CB_BACK_TO_MAIN)]
                ])
            )
            
        except Exception as e:
            logger.error(f"Error processing appointment: {e}")
            await update.message.reply_text(
                f"❌ **Error:** {str(e)}",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=CB_BACK_TO_MAIN)]])
            )

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel command handler"""
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id if update.effective_user else 0
        session = self.get_session(chat_id)
        
        # Log command execution
        self.google_integration.log_command_execution(
            user_id=user_id,
            chat_id=chat_id,
            command="cancel",
            extra_info=f"Previous state: {session.current_state}"
        )
        
        # Clear conversation state
        session.current_state = None
        
        await update.message.reply_text(
            "🚫 **Operation Cancelled**\n\nReturning to main menu.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data=CB_BACK_TO_MAIN)]])
        )


def build_application(config: Config) -> Application:
    """Build and configure the Telegram application with enhanced group features and risk detection."""
    try:
        # Create the enhanced bot instance
        enhanced_bot = EnhancedLocationBot(config)
        
        # Build the application with job queue enabled (correct method for v20.8)
        application = (
            ApplicationBuilder()
            .token(config.TELEGRAM_BOT_TOKEN)
            .build()
        )
        
        # Verify job queue is available (it should be enabled by default in v20.8)
        if not application.job_queue:
            logger.warning("Job queue not available, trying to create one manually")
            # Try to initialize job queue manually if needed
            try:
                from telegram.ext import JobQueue
                if not hasattr(application, '_job_queue') or application._job_queue is None:
                    # Job queue should be created automatically, but let's ensure it exists
                    logger.info("Job queue will be created during application initialization")
            except Exception as jq_error:
                logger.error(f"Could not initialize job queue: {jq_error}")
        
        # Store bot instance for access in handlers
        application.bot_data['enhanced_bot'] = enhanced_bot
        
        # Set job queue reference in bot
        enhanced_bot.job_queue = application.job_queue
        
        # Set bot instance reference for mention detection
        enhanced_bot.bot_instance = application.bot
        
        # Add handlers
        application.add_handler(CommandHandler("start", enhanced_bot.start))
        application.add_handler(CommandHandler("cancel", enhanced_bot.cancel))
        application.add_handler(CommandHandler("updateall", enhanced_bot.update_all_groups))
        application.add_handler(CommandHandler("updateassets", enhanced_bot.update_assets_command))
        application.add_handler(CommandHandler("listnewtrucks", enhanced_bot.list_new_trucks_command))
        application.add_handler(CommandHandler("addtruck", enhanced_bot.add_truck_command))
        application.add_handler(CommandHandler("workshealth", enhanced_bot.worksheets_health_command))
        application.add_handler(CommandHandler("autoregister", enhanced_bot.auto_register_groups_command))
        application.add_handler(CommandHandler("groupsdiag", enhanced_bot.groups_diagnostic_command))
        application.add_handler(CommandHandler("validatedata", enhanced_bot.validate_data_command))
        
        # VIN Suggestion System Integration
        try:
            from vin_suggestion_handlers import register_vin_handlers, auto_register_vin_on_group_join
            register_vin_handlers(application)
            
            # Set up bot data for VIN system
            application.bot_data['google_integration'] = enhanced_bot.google_integration
            application.bot_data['assets_ws'] = enhanced_bot.google_integration.assets_worksheet
            
            # Add auto VIN registration when bot joins groups
            from telegram.ext import ChatMemberHandler
            
            async def on_bot_added_to_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
                """Trigger auto VIN registration when bot is added to group"""
                member_update = update.my_chat_member
                if member_update and member_update.new_chat_member:
                    if member_update.new_chat_member.status in ['member', 'administrator']:
                        # Bot was added to group, attempt auto-registration
                        await auto_register_vin_on_group_join(update, context)
            
            application.add_handler(ChatMemberHandler(on_bot_added_to_group, ChatMemberHandler.MY_CHAT_MEMBER))
            
            logger.info("VIN suggestion system with auto-registration integrated successfully")
        except ImportError as e:
            logger.warning(f"VIN suggestion system not available: {e}")
        except Exception as e:
            logger.error(f"Failed to integrate VIN suggestion system: {e}")
        
        application.add_handler(CallbackQueryHandler(enhanced_bot.button_router, block=False))
        
        
        application.add_error_handler(_global_error_handler)
# Add risk alert callback handlers (NEW) - only if risk detection is available
        if RISK_DETECTION_AVAILABLE:
            application.add_handler(CallbackQueryHandler(
                enhanced_bot.handle_risk_alert_callback,
                pattern="^(contact_driver_|ack_alert_|escalate_alert_|refresh_risk_status|ACK_LATE_DEL:|ACK_LATE_PU:)"
            ))
            logger.info("Risk alert callback handlers registered with ETA late acknowledgments")
        
        # Add text message handler for conversation states
        # This handler now includes the logic to ignore messages when appropriate
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, 
            enhanced_bot.handle_text_message
        ))
        
        # Schedule risk monitoring if available and enabled
        if RISK_DETECTION_AVAILABLE and hasattr(enhanced_bot, 'enable_risk_monitoring'):
            if enhanced_bot.enable_risk_monitoring:
                # Schedule risk monitoring to start after application is running
                async def start_risk_monitoring():
                    enhanced_bot.schedule_risk_monitoring(application)
                
                # Add this to run after the application starts
                application.add_handler(CallbackQueryHandler(
                    lambda update, context: start_risk_monitoring(),
                    pattern="^__start_risk_monitoring__$"  # This won't match any real callback
                ))
        
        # Schedule session cleanup if timeout is configured
        if enhanced_bot.session_timeout_hours and enhanced_bot.session_timeout_hours > 0:
            logger.info(f"Scheduling session cleanup every {enhanced_bot.session_timeout_hours} hours")
            
            # Schedule cleanup to run every hour (more frequent than timeout for better cleanup)
            cleanup_interval_hours = min(1, enhanced_bot.session_timeout_hours / 2)
            cleanup_interval_seconds = int(cleanup_interval_hours * 3600)
            
            if application.job_queue:
                async def session_cleanup_job(context):
                    enhanced_bot.cleanup_expired_sessions()
                
                application.job_queue.run_repeating(
                    callback=session_cleanup_job,
                    interval=cleanup_interval_seconds,
                    first=cleanup_interval_seconds,  # Start after the interval
                    name="session_cleanup"
                )
                logger.info(f"Session cleanup scheduled to run every {cleanup_interval_hours} hours")
            else:
                logger.warning("Job queue not available, session cleanup will not be scheduled")
        
        # Schedule group schedule restoration to run after application starts
        if application.job_queue:
            async def restore_schedules_job(context):
                """Job to restore group location schedules after bot restart"""
                await enhanced_bot.restore_group_schedules(context)
            
            # Schedule this to run once, shortly after startup (30 seconds delay)
            application.job_queue.run_once(
                callback=restore_schedules_job,
                when=30,  # 30 seconds after startup
                name="restore_group_schedules"
            )
            logger.info("Group schedule restoration scheduled to run 30 seconds after startup")
        else:
            logger.warning("Job queue not available, group schedules will not be restored automatically")
        
        logger.info("Enhanced Telegram application built successfully")
        logger.info(f"Job queue available: {application.job_queue is not None}")
        logger.info(f"Risk detection available: {RISK_DETECTION_AVAILABLE}")
        
        if RISK_DETECTION_AVAILABLE and hasattr(enhanced_bot, 'risk_detector'):
            risk_zones = len(enhanced_bot.risk_detector.risk_zones) if hasattr(enhanced_bot.risk_detector, 'risk_zones') else 0
            logger.info(f"Risk zones loaded: {risk_zones}")
            logger.info(f"Risk monitoring enabled: {getattr(enhanced_bot, 'enable_risk_monitoring', False)}")
            logger.info(f"Acknowledgment system: {len(enhanced_bot.acknowledged_alerts)} active acknowledgments")
        
        return application
        
    except Exception as e:
        logger.error(f"Failed to build Telegram application: {e}", exc_info=True)
        raise