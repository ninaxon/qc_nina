"""
Health endpoint for Railway deployment.
Provides HTTP health checks and system status monitoring.
"""
import asyncio
import json
import time
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any, Optional
from threading import Thread
import logging

from config import Config


logger = logging.getLogger(__name__)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check endpoints"""
    
    def __init__(self, *args, health_monitor=None, **kwargs):
        self.health_monitor = health_monitor
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests for health checks"""
        if self.path == '/health':
            self._handle_health_check()
        elif self.path == '/ready':
            self._handle_readiness_check()
        elif self.path == '/metrics':
            self._handle_metrics()
        else:
            self._send_response(404, {'error': 'Not found'})
    
    def _handle_health_check(self):
        """Basic liveness check"""
        health_data = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'service': 'telegram-logistics-bot'
        }
        self._send_response(200, health_data)
    
    def _handle_readiness_check(self):
        """Readiness check with dependency verification"""
        if not self.health_monitor:
            self._send_response(503, {'status': 'not ready', 'reason': 'Health monitor not available'})
            return
        
        ready_status = self.health_monitor.check_readiness()
        status_code = 200 if ready_status['ready'] else 503
        self._send_response(status_code, ready_status)
    
    def _handle_metrics(self):
        """Expose basic metrics"""
        if not self.health_monitor:
            self._send_response(503, {'error': 'Metrics not available'})
            return
        
        metrics = self.health_monitor.get_metrics()
        self._send_response(200, metrics)
    
    def _send_response(self, status_code: int, data: Dict[str, Any]):
        """Send JSON response"""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        
        response = json.dumps(data, indent=2)
        self.wfile.write(response.encode('utf-8'))
    
    def log_message(self, format, *args):
        """Override to use our logger"""
        logger.debug(f"HTTP: {format % args}")


class HealthMonitor:
    """Health monitoring and metrics collection"""
    
    def __init__(self, config: Config):
        self.config = config
        self.start_time = time.time()
        self.last_checks = {}
        self.metrics = {
            'requests_total': 0,
            'errors_total': 0,
            'telegram_messages_sent': 0,
            'sheets_operations': 0,
            'circuit_breaker_trips': 0
        }
    
    def check_readiness(self) -> Dict[str, Any]:
        """Check if the service is ready to handle requests"""
        checks = {
            'telegram_bot': self._check_telegram_connection(),
            'google_sheets': self._check_google_sheets(),
            'tms_service': self._check_tms_service(),
        }
        
        all_ready = all(check['healthy'] for check in checks.values())
        
        return {
            'ready': all_ready,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'checks': checks
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get service metrics"""
        uptime_seconds = time.time() - self.start_time
        
        return {
            'uptime_seconds': uptime_seconds,
            'uptime_human': self._format_uptime(uptime_seconds),
            'metrics': self.metrics.copy(),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'version': '1.0.0',
            'config': {
                'telegram_rate_limit': self.config.MAX_CONCURRENT_TELEGRAM_SENDS,
                'group_update_interval': self.config.GROUP_LOCATION_INTERVAL,
                'silent_refresh_interval': self.config.LIVE_TRACKING_INTERVAL,
                'risk_monitoring_enabled': self.config.ENABLE_RISK_MONITORING
            }
        }
    
    def _check_telegram_connection(self) -> Dict[str, Any]:
        """Check Telegram bot connection"""
        try:
            # This would ideally make a quick API call to Telegram
            # For now, just check if token is configured
            if self.config.TELEGRAM_BOT_TOKEN:
                return {'healthy': True, 'message': 'Telegram token configured'}
            else:
                return {'healthy': False, 'message': 'Telegram token not configured'}
        except Exception as e:
            return {'healthy': False, 'message': f'Telegram check failed: {e}'}
    
    def _check_google_sheets(self) -> Dict[str, Any]:
        """Check Google Sheets connection"""
        try:
            # Check if service account file exists
            import os
            if os.path.exists(self.config.SHEETS_SERVICE_ACCOUNT_FILE):
                return {'healthy': True, 'message': 'Google Sheets credentials available'}
            else:
                return {'healthy': False, 'message': 'Google Sheets credentials not found'}
        except Exception as e:
            return {'healthy': False, 'message': f'Google Sheets check failed: {e}'}
    
    def _check_tms_service(self) -> Dict[str, Any]:
        """Check TMS service configuration"""
        try:
            if (self.config.TMS_API_URL and 
                self.config.TMS_API_KEY and 
                self.config.TMS_API_HASH):
                return {'healthy': True, 'message': 'TMS service configured'}
            else:
                return {'healthy': False, 'message': 'TMS service not fully configured'}
        except Exception as e:
            return {'healthy': False, 'message': f'TMS check failed: {e}'}
    
    def _format_uptime(self, seconds: float) -> str:
        """Format uptime in human-readable format"""
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"
    
    def increment_metric(self, metric_name: str, value: int = 1):
        """Increment a metric counter"""
        if metric_name in self.metrics:
            self.metrics[metric_name] += value
        else:
            self.metrics[metric_name] = value
    
    def record_error(self, error_type: str = 'general'):
        """Record an error occurrence"""
        self.metrics['errors_total'] += 1
        error_key = f'errors_{error_type}'
        self.increment_metric(error_key)


class HealthServer:
    """HTTP server for health checks"""
    
    def __init__(self, config: Config, health_monitor: HealthMonitor, port: int = 8080):
        self.config = config
        self.health_monitor = health_monitor
        self.port = port
        self.server: Optional[HTTPServer] = None
        self.server_thread: Optional[Thread] = None
    
    def start(self):
        """Start the health check server"""
        try:
            # Create handler class with health monitor
            def handler_factory(*args, **kwargs):
                return HealthCheckHandler(*args, health_monitor=self.health_monitor, **kwargs)
            
            self.server = HTTPServer(('0.0.0.0', self.port), handler_factory)
            
            # Start server in a separate thread
            self.server_thread = Thread(target=self.server.serve_forever, daemon=True)
            self.server_thread.start()
            
            logger.info(f"Health server started on port {self.port}")
            logger.info(f"Health endpoints: http://0.0.0.0:{self.port}/health, /ready, /metrics")
            
        except Exception as e:
            logger.error(f"Failed to start health server: {e}")
            raise
    
    def stop(self):
        """Stop the health check server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            logger.info("Health server stopped")
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


def create_health_system(config: Config) -> tuple[HealthMonitor, HealthServer]:
    """Create and configure the health monitoring system"""
    health_monitor = HealthMonitor(config)
    
    # Railway uses PORT environment variable
    import os
    port = int(os.getenv('PORT', 8080))
    
    health_server = HealthServer(config, health_monitor, port)
    
    return health_monitor, health_server