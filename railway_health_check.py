# health_check.py
"""
Simple health check endpoint for Railway deployment
"""

import asyncio
import json
import logging
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from typing import Optional

logger = logging.getLogger(__name__)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """Simple health check HTTP handler"""

    def __init__(self, bot_status_func=None, *args, **kwargs):
        self.bot_status_func = bot_status_func
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """Handle GET requests"""
        if self.path == '/health':
            self.send_health_response()
        elif self.path == '/status':
            self.send_status_response()
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')

    def send_health_response(self):
        """Send basic health check response"""
        try:
            # Basic health check
            health_data = {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "service": "enhanced-asset-tracking-bot",
                "version": "2.0.0-dual-mode"
            }

            # Try to get bot status if available
            if hasattr(
                    self.server,
                    'bot_status_func') and self.server.bot_status_func:
                try:
                    bot_status = self.server.bot_status_func()
                    health_data.update(bot_status)
                except Exception as e:
                    health_data["bot_status_error"] = str(e)

            response_data = json.dumps(health_data, indent=2)

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response_data)))
            self.end_headers()
            self.wfile.write(response_data.encode())

        except Exception as e:
            logger.error(f"Health check error: {e}")
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'Health check failed')

    def send_status_response(self):
        """Send detailed status response"""
        try:
            status_data = {
                "service": "enhanced-asset-tracking-bot",
                "version": "2.0.0-dual-mode",
                "timestamp": datetime.now().isoformat(),
                "features": {
                    "dual_mode_scheduling": True,
                    "group_location_updates": True,
                    "silent_data_refresh": True,
                    "enhanced_bot_integration": True
                }
            }

            # Add bot-specific status if available
            if hasattr(
                    self.server,
                    'bot_status_func') and self.server.bot_status_func:
                try:
                    bot_status = self.server.bot_status_func()
                    status_data["bot_status"] = bot_status
                except Exception as e:
                    status_data["bot_status_error"] = str(e)

            response_data = json.dumps(status_data, indent=2)

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(response_data)))
            self.end_headers()
            self.wfile.write(response_data.encode())

        except Exception as e:
            logger.error(f"Status check error: {e}")
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'Status check failed')

    def log_message(self, format, *args):
        """Override to use our logger"""
        logger.debug(f"Health check: {format % args}")


class HealthCheckServer:
    """Health check server for Railway deployment"""

    def __init__(
            self,
            port: int = 8000,
            bot_status_func: Optional[callable] = None):
        self.port = port
        self.bot_status_func = bot_status_func
        self.server: Optional[HTTPServer] = None
        self.server_thread: Optional[Thread] = None

    def start(self):
        """Start the health check server"""
        try:
            # Create server
            handler = lambda *args, **kwargs: HealthCheckHandler(
                *args, **kwargs)
            self.server = HTTPServer(('0.0.0.0', self.port), handler)
            self.server.bot_status_func = self.bot_status_func

            # Start in separate thread
            self.server_thread = Thread(
                target=self.server.serve_forever, daemon=True)
            self.server_thread.start()

            logger.info(f"Health check server started on port {self.port}")
            print(
                f"🏥 Health check server running on http://0.0.0.0:{self.port}/health")

        except Exception as e:
            logger.error(f"Failed to start health check server: {e}")
            raise

    def stop(self):
        """Stop the health check server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            logger.info("Health check server stopped")

    def get_bot_status(self):
        """Get bot status for health checks"""
        try:
            # This will be called by the health check endpoint
            return {
                "bot_running": True,
                "last_check": datetime.now().isoformat(),
                "dual_mode_active": True
            }
        except Exception as e:
            return {
                "bot_running": False,
                "error": str(e),
                "last_check": datetime.now().isoformat()
            }


# Global health check server instance
health_server: Optional[HealthCheckServer] = None


def start_health_check_server(port: int = 8000,
                              bot_status_func: Optional[callable] = None):
    """Start the global health check server"""
    global health_server

    if health_server is None:
        health_server = HealthCheckServer(port, bot_status_func)
        health_server.start()

    return health_server


def stop_health_check_server():
    """Stop the global health check server"""
    global health_server

    if health_server:
        health_server.stop()
        health_server = None
