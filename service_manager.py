#!/usr/bin/env python3
"""
Service Management for Asset Tracking Bot
Handles start/stop/status/restart operations with proper PID management
"""

import os
import sys
import time
import signal
import psutil
import logging
from pathlib import Path
from typing import Optional
import subprocess
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class ServiceManager:
    """Manages the bot service lifecycle"""

    def __init__(self, service_name: str = "asset_tracking_bot"):
        self.service_name = service_name
        self.pid_file = Path(f"{service_name}.pid")
        self.log_file = Path(f"{service_name}.log")
        self.status_file = Path(f"{service_name}.status")
        self.main_script = Path("main.py")

    def get_pid(self) -> Optional[int]:
        """Get PID from PID file"""
        try:
            if self.pid_file.exists():
                with open(self.pid_file, 'r') as f:
                    pid = int(f.read().strip())
                    return pid
            return None
        except (ValueError, FileNotFoundError):
            return None

    def is_running(self) -> bool:
        """Check if service is running"""
        pid = self.get_pid()
        if pid is None:
            return False

        try:
            # Check if process exists and is our bot
            process = psutil.Process(pid)
            cmdline = ' '.join(process.cmdline())

            # Check if it's our Python script
            if 'python' in cmdline.lower() and 'main.py' in cmdline:
                return True
            else:
                # PID file points to different process, clean it up
                self._cleanup_pid_file()
                return False

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            # Process doesn't exist, clean up PID file
            self._cleanup_pid_file()
            return False

    def _cleanup_pid_file(self):
        """Clean up stale PID file"""
        try:
            if self.pid_file.exists():
                self.pid_file.unlink()
        except Exception as e:
            logger.warning(f"Failed to clean up PID file: {e}")

    def _write_pid(self, pid: int):
        """Write PID to file"""
        try:
            with open(self.pid_file, 'w') as f:
                f.write(str(pid))
        except Exception as e:
            logger.error(f"Failed to write PID file: {e}")

    def _update_status(self, status: str, details: str = ""):
        """Update status file"""
        try:
            status_data = {
                "status": status,
                "timestamp": datetime.now().isoformat(),
                "pid": self.get_pid(),
                "details": details
            }

            with open(self.status_file, 'w') as f:
                json.dump(status_data, f, indent=2)

        except Exception as e:
            logger.warning(f"Failed to update status file: {e}")

    def start(self, daemon: bool = True) -> bool:
        """Start the service"""
        if self.is_running():
            print(f"❌ Service '{self.service_name}' is already running")
            return False

        if not self.main_script.exists():
            print(f"❌ Main script not found: {self.main_script}")
            return False

        try:
            print(f"🚀 Starting service '{self.service_name}'...")

            if daemon:
                # Start as daemon process
                process = subprocess.Popen([
                    sys.executable, str(self.main_script)
                ],
                    stdout=open(self.log_file, 'a'),
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True
                )
            else:
                # Start in foreground
                process = subprocess.Popen([
                    sys.executable, str(self.main_script)
                ])

            # Write PID file
            self._write_pid(process.pid)

            # Wait a moment to check if it started successfully
            time.sleep(2)

            if self.is_running():
                self._update_status("running", "Service started successfully")
                print(
                    f"✅ Service '{self.service_name}' started successfully (PID: {process.pid})")

                if daemon:
                    print(f"📋 Log file: {self.log_file}")
                    print(f"📊 Status file: {self.status_file}")

                return True
            else:
                print(f"❌ Service '{self.service_name}' failed to start")
                self._update_status("failed", "Service failed to start")
                return False

        except Exception as e:
            print(f"Failed to start service: {e}")
            self._update_status("error", str(e))
            return False

    def stop(self, force: bool = False) -> bool:
        """Stop the service"""
        if not self.is_running():
            print(f"Service '{self.service_name}' is not running")
            return False

        pid = self.get_pid()
        if pid is None:
            return False

        try:
            print(f"Stopping service '{self.service_name}' (PID: {pid})...")

            process = psutil.Process(pid)

            if force:
                # Force kill
                process.kill()
                print("Force killed service")
            else:
                # Graceful shutdown
                process.terminate()

                # Wait for graceful shutdown
                try:
                    process.wait(timeout=10)
                    print("Service stopped gracefully")
                except psutil.TimeoutExpired:
                    print("Graceful shutdown timed out, force killing...")
                    process.kill()
                    print("Service force killed")

            # Clean up files
            self._cleanup_pid_file()
            self._update_status("stopped", "Service stopped")

            print(f"Service '{self.service_name}' stopped successfully")
            return True

        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            print(f"Failed to stop service: {e}")
            self._cleanup_pid_file()
            return False

    def restart(self) -> bool:
        """Restart the service"""
        print(f"Restarting service '{self.service_name}'...")

        # Stop if running
        if self.is_running():
            if not self.stop():
                return False

        # Wait a moment
        time.sleep(1)

        # Start again
        return self.start()

    def status(self) -> dict:
        """Get service status"""
        is_running = self.is_running()
        pid = self.get_pid()

        status_data = {
            "service_name": self.service_name,
            "is_running": is_running,
            "pid": pid,
            "pid_file": str(self.pid_file),
            "log_file": str(self.log_file),
            "status_file": str(self.status_file)
        }

        # Add process info if running
        if is_running and pid:
            try:
                process = psutil.Process(pid)
                status_data.update({
                    "start_time": datetime.fromtimestamp(process.create_time()).isoformat(),
                    "cpu_percent": process.cpu_percent(),
                    "memory_mb": round(process.memory_info().rss / 1024 / 1024, 1),
                    "status": process.status()
                })
            except Exception as e:
                status_data["process_error"] = str(e)

        # Read status file if exists
        if self.status_file.exists():
            try:
                with open(self.status_file, 'r') as f:
                    file_status = json.load(f)
                    status_data["last_status"] = file_status
            except Exception as e:
                status_data["status_file_error"] = str(e)

        return status_data

    def logs(self, lines: int = 50, follow: bool = False):
        """Show service logs"""
        if not self.log_file.exists():
            print(f"❌ Log file not found: {self.log_file}")
            return

        try:
            if follow:
                # Follow logs (like tail -f)
                print(
                    f"Following logs from {self.log_file} (Press Ctrl+C to stop)...")

                # Show last few lines first
                with open(self.log_file, 'r') as f:
                    lines_list = f.readlines()
                    for line in lines_list[-lines:]:
                        print(line.rstrip())

                # Follow new lines
                with open(self.log_file, 'r') as f:
                    f.seek(0, 2)  # Go to end of file

                    try:
                        while True:
                            line = f.readline()
                            if line:
                                print(line.rstrip())
                            else:
                                time.sleep(0.1)
                    except KeyboardInterrupt:
                        print("\nStopped following logs")
            else:
                # Show last N lines
                with open(self.log_file, 'r') as f:
                    lines_list = f.readlines()
                    for line in lines_list[-lines:]:
                        print(line.rstrip())

        except Exception as e:
            print(f"❌ Error reading logs: {e}")

    def cleanup(self):
        """Clean up service files"""
        files_to_clean = [self.pid_file, self.status_file]

        cleaned = 0
        for file_path in files_to_clean:
            try:
                if file_path.exists():
                    file_path.unlink()
                    cleaned += 1
            except Exception as e:
                print(f"Failed to clean {file_path}: {e}")

        print(f"🧹 Cleaned up {cleaned} service files")


def main():
    """CLI interface for service management"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Asset Tracking Bot Service Manager")
    parser.add_argument(
        "action",
        choices=[
            "start",
            "stop",
            "restart",
            "status",
            "logs",
            "cleanup"],
        help="Service action to perform")
    parser.add_argument("--daemon", action="store_true", default=True,
                        help="Run as daemon (default: True)")
    parser.add_argument("--foreground", action="store_true",
                        help="Run in foreground")
    parser.add_argument("--force", action="store_true",
                        help="Force stop service")
    parser.add_argument("--lines", type=int, default=50,
                        help="Number of log lines to show")
    parser.add_argument("--follow", action="store_true",
                        help="Follow logs")

    args = parser.parse_args()

    # Setup basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s')

    service = ServiceManager()

    if args.action == "start":
        daemon = args.daemon and not args.foreground
        success = service.start(daemon=daemon)
        sys.exit(0 if success else 1)

    elif args.action == "stop":
        success = service.stop(force=args.force)
        sys.exit(0 if success else 1)

    elif args.action == "restart":
        success = service.restart()
        sys.exit(0 if success else 1)

    elif args.action == "status":
        status = service.status()

        print(f"   Service Status: {status['service_name']}")
        print(f"   Running: {'Yes' if status['is_running'] else '❌ No'}")

        if status['is_running']:
            print(f"   PID: {status['pid']}")
            if 'start_time' in status:
                print(f"   Started: {status['start_time']}")
                print(f"   CPU: {status.get('cpu_percent', 'N/A')}%")
                print(f"   Memory: {status.get('memory_mb', 'N/A')} MB")

        print(f"   PID File: {status['pid_file']}")
        print(f"   Log File: {status['log_file']}")

        if 'last_status' in status:
            last = status['last_status']
            print(
                f"   Last Status: {last.get('status', 'N/A')} ({last.get('timestamp', 'N/A')})")

    elif args.action == "logs":
        service.logs(lines=args.lines, follow=args.follow)

    elif args.action == "cleanup":
        service.cleanup()


if __name__ == "__main__":
    main()
