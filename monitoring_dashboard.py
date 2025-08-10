#!/usr/bin/env python3
"""
Live Update Monitoring Dashboard
Real-time monitoring for the enhanced asset tracking bot

Usage:
    python monitoring_dashboard.py --live      # Live dashboard
    python monitoring_dashboard.py --report    # Generate report
    python monitoring_dashboard.py --health    # Health check only
"""

import time
import json
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import psutil
import logging

# Rich for beautiful terminal output
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress
    from rich.live import Live
    from rich.layout import Layout
    from rich.text import Text
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Install 'rich' for enhanced display: pip install rich")

class LiveUpdateMonitor:
    """Monitor for live update system"""
    
    def __init__(self):
        self.console = Console() if RICH_AVAILABLE else None
        self.pid_file = Path("asset_tracking_bot.pid")
        self.status_file = Path("asset_tracking_bot.status")
        self.log_files = {
            'main': Path("logs/bot.log"),
            'error': Path("logs/error.log"),
            'live': Path("logs/live_updates.log")
        }
        
    def get_bot_process(self) -> Optional[psutil.Process]:
        """Get bot process if running"""
        try:
            if self.pid_file.exists():
                with open(self.pid_file) as f:
                    pid = int(f.read().strip())
                return psutil.Process(pid)
        except (FileNotFoundError, psutil.NoSuchProcess, ValueError):
            pass
        return None
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get system performance statistics"""
        process = self.get_bot_process()
        
        stats = {
            'timestamp': datetime.now().isoformat(),
            'bot_running': process is not None,
            'system': {
                'cpu_percent': psutil.cpu_percent(interval=1),
                'memory_percent': psutil.virtual_memory().percent,
                'disk_percent': psutil.disk_usage('/').percent,
                'load_avg': psutil.getloadavg() if hasattr(psutil, 'getloadavg') else None
            }
        }
        
        if process:
            try:
                stats['bot_process'] = {
                    'pid': process.pid,
                    'cpu_percent': process.cpu_percent(),
                    'memory_mb': round(process.memory_info().rss / 1024 / 1024, 1),
                    'memory_percent': process.memory_percent(),
                    'threads': process.num_threads(),
                    'status': process.status(),
                    'create_time': datetime.fromtimestamp(process.create_time()).isoformat(),
                    'connections': len(process.connections()) if hasattr(process, 'connections') else 0
                }
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                stats['bot_process'] = {'error': 'Access denied or process not found'}
        
        return stats
    
    def analyze_logs(self, hours: int = 1) -> Dict[str, Any]:
        """Analyze recent log entries"""
        since = datetime.now() - timedelta(hours=hours)
        analysis = {
            'live_updates': {'count': 0, 'errors': 0, 'success': 0},
            'tms_calls': {'count': 0, 'errors': 0, 'success': 0},
            'job_queue': {'scheduled': 0, 'completed': 0, 'failed': 0},
            'sessions': {'created': 0, 'destroyed': 0, 'active_estimate': 0},
            'errors': []
        }
        
        # Analyze live updates log
        if self.log_files['live'].exists():
            analysis.update(self._analyze_live_log(since))
        
        # Analyze main log
        if self.log_files['main'].exists():
            analysis.update(self._analyze_main_log(since))
        
        # Analyze error log
        if self.log_files['error'].exists():
            analysis['errors'] = self._get_recent_errors(since)
        
        return analysis
    
    def _analyze_live_log(self, since: datetime) -> Dict[str, Any]:
        """Analyze live updates log file"""
        stats = {
            'live_updates': {'count': 0, 'errors': 0, 'success': 0},
            'auto_refresh': {'started': 0, 'stopped': 0, 'failed': 0}
        }
        
        try:
            with open(self.log_files['live'], 'r') as f:
                for line in f:
                    if self._is_recent_log_line(line, since):
                        if 'auto-refresh' in line.lower():
                            if 'started' in line.lower():
                                stats['auto_refresh']['started'] += 1
                            elif 'stopped' in line.lower() or 'cancelled' in line.lower():
                                stats['auto_refresh']['stopped'] += 1
                            elif 'failed' in line.lower() or 'error' in line.lower():
                                stats['auto_refresh']['failed'] += 1
                        
                        if 'live update' in line.lower():
                            stats['live_updates']['count'] += 1
                            if 'error' in line.lower() or 'failed' in line.lower():
                                stats['live_updates']['errors'] += 1
                            else:
                                stats['live_updates']['success'] += 1
        
        except FileNotFoundError:
            pass
        
        return stats
    
    def _analyze_main_log(self, since: datetime) -> Dict[str, Any]:
        """Analyze main log file"""
        stats = {
            'tms_calls': {'count': 0, 'errors': 0, 'success': 0},
            'job_queue': {'scheduled': 0, 'completed': 0, 'failed': 0}
        }
        
        try:
            with open(self.log_files['main'], 'r') as f:
                for line in f:
                    if self._is_recent_log_line(line, since):
                        # TMS API calls
                        if 'tms' in line.lower():
                            stats['tms_calls']['count'] += 1
                            if 'error' in line.lower() or 'failed' in line.lower():
                                stats['tms_calls']['errors'] += 1
                            elif 'success' in line.lower() or 'loaded' in line.lower():
                                stats['tms_calls']['success'] += 1
                        
                        # Job queue activity
                        if 'job' in line.lower():
                            if 'scheduled' in line.lower() or 'added' in line.lower():
                                stats['job_queue']['scheduled'] += 1
                            elif 'completed' in line.lower():
                                stats['job_queue']['completed'] += 1
                            elif 'failed' in line.lower():
                                stats['job_queue']['failed'] += 1
        
        except FileNotFoundError:
            pass
        
        return stats
    
    def _get_recent_errors(self, since: datetime, limit: int = 10) -> List[Dict[str, str]]:
        """Get recent errors from error log"""
        errors = []
        
        try:
            with open(self.log_files['error'], 'r') as f:
                lines = f.readlines()
                
            for line in reversed(lines):
                if len(errors) >= limit:
                    break
                    
                if self._is_recent_log_line(line, since):
                    # Parse error line
                    parts = line.strip().split(' - ', 3)
                    if len(parts) >= 4:
                        errors.append({
                            'timestamp': parts[0],
                            'level': parts[2],
                            'message': parts[3][:100] + '...' if len(parts[3]) > 100 else parts[3]
                        })
        
        except FileNotFoundError:
            pass
        
        return list(reversed(errors))
    
    def _is_recent_log_line(self, line: str, since: datetime) -> bool:
        """Check if log line is from recent timeframe"""
        try:
            # Extract timestamp from log line (format: YYYY-MM-DD HH:MM:SS,mmm)
            timestamp_str = line.split(' - ')[0]
            log_time = datetime.strptime(timestamp_str.split(',')[0], '%Y-%m-%d %H:%M:%S')
            return log_time >= since
        except (ValueError, IndexError):
            return False
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive status report"""
        report = {
            'generated_at': datetime.now().isoformat(),
            'system': self.get_system_stats(),
            'log_analysis_1h': self.analyze_logs(hours=1),
            'log_analysis_24h': self.analyze_logs(hours=24),
            'health_score': 0
        }
        
        # Calculate health score (0-100)
        health_factors = []
        
        # Bot running (30 points)
        if report['system']['bot_running']:
            health_factors.append(30)
        
        # Low error rate (25 points)
        errors_1h = report['log_analysis_1h'].get('live_updates', {}).get('errors', 0)
        success_1h = report['log_analysis_1h'].get('live_updates', {}).get('success', 0)
        if success_1h > 0:
            error_rate = errors_1h / (errors_1h + success_1h)
            if error_rate < 0.05:  # Less than 5% error rate
                health_factors.append(25)
            elif error_rate < 0.15:  # Less than 15% error rate
                health_factors.append(15)
        
        # System resources (25 points)
        cpu = report['system']['system']['cpu_percent']
        memory = report['system']['system']['memory_percent']
        if cpu < 50 and memory < 70:
            health_factors.append(25)
        elif cpu < 80 and memory < 85:
            health_factors.append(15)
        
        # Recent activity (20 points)
        live_updates = report['log_analysis_1h'].get('live_updates', {}).get('count', 0)
        if live_updates > 0:
            health_factors.append(20)
        elif report['log_analysis_24h'].get('live_updates', {}).get('count', 0) > 0:
            health_factors.append(10)
        
        report['health_score'] = sum(health_factors)
        
        return report
    
    def display_live_dashboard(self):
        """Display live updating dashboard"""
        if not RICH_AVAILABLE:
            print("Rich not available. Install with: pip install rich")
            return
        
        def make_layout():
            """Create dashboard layout"""
            layout = Layout()
            
            layout.split(
                Layout(name="header", size=3),
                Layout(name="main", ratio=1),
                Layout(name="footer", size=3),
            )
            
            layout["main"].split_row(
                Layout(name="left"),
                Layout(name="right"),
            )
            
            layout["left"].split(
                Layout(name="system", ratio=1),
                Layout(name="process", ratio=1),
            )
            
            layout["right"].split(
                Layout(name="activity", ratio=1),
                Layout(name="errors", ratio=1),
            )
            
            return layout
        
        def generate_dashboard():
            """Generate dashboard content"""
            report = self.generate_report()
            layout = make_layout()
            
            # Header
            header_text = Text("🔴 Live Asset Tracking Bot Dashboard", style="bold magenta")
            header_text.append(f" | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", style="dim")
            layout["header"].update(Panel(header_text, box=box.ROUNDED))
            
            # System stats
            sys_stats = report['system']['system']
            system_table = Table(title="System Resources", box=box.SIMPLE)
            system_table.add_column("Metric", style="cyan")
            system_table.add_column("Value", style="green")
            system_table.add_row("CPU", f"{sys_stats['cpu_percent']:.1f}%")
            system_table.add_row("Memory", f"{sys_stats['memory_percent']:.1f}%")
            system_table.add_row("Disk", f"{sys_stats['disk_percent']:.1f}%")
            layout["system"].update(Panel(system_table, title="System", border_style="blue"))
            
            # Process stats
            if report['system']['bot_running']:
                proc_stats = report['system']['bot_process']
                process_table = Table(title="Bot Process", box=box.SIMPLE)
                process_table.add_column("Metric", style="cyan")
                process_table.add_column("Value", style="green")
                process_table.add_row("PID", str(proc_stats['pid']))
                process_table.add_row("Memory", f"{proc_stats['memory_mb']} MB")
                process_table.add_row("CPU", f"{proc_stats['cpu_percent']:.1f}%")
                process_table.add_row("Threads", str(proc_stats['threads']))
                process_table.add_row("Status", proc_stats['status'])
                layout["process"].update(Panel(process_table, title="Process", border_style="green"))
            else:
                layout["process"].update(Panel("❌ Bot Not Running", title="Process", border_style="red"))
            
            # Activity stats
            activity_1h = report['log_analysis_1h']
            activity_table = Table(title="Last Hour Activity", box=box.SIMPLE)
            activity_table.add_column("Activity", style="cyan")
            activity_table.add_column("Count", style="yellow")
            activity_table.add_column("Errors", style="red")
            
            activity_table.add_row(
                "Live Updates",
                str(activity_1h.get('live_updates', {}).get('count', 0)),
                str(activity_1h.get('live_updates', {}).get('errors', 0))
            )
            activity_table.add_row(
                "TMS Calls",
                str(activity_1h.get('tms_calls', {}).get('count', 0)),
                str(activity_1h.get('tms_calls', {}).get('errors', 0))
            )
            activity_table.add_row(
                "Jobs Scheduled",
                str(activity_1h.get('job_queue', {}).get('scheduled', 0)),
                str(activity_1h.get('job_queue', {}).get('failed', 0))
            )
            
            layout["activity"].update(Panel(activity_table, title="Activity", border_style="yellow"))
            
            # Recent errors
            errors = report['log_analysis_1h'].get('errors', [])
            if errors:
                error_text = "\n".join([f"• {err['message']}" for err in errors[:5]])
            else:
                error_text = "✅ No recent errors"
            
            layout["errors"].update(Panel(error_text, title="Recent Errors", border_style="red"))
            
            # Footer with health score
            health_score = report['health_score']
            if health_score >= 80:
                health_style = "bold green"
                health_emoji = "🟢"
            elif health_score >= 60:
                health_style = "bold yellow"
                health_emoji = "🟡"
            else:
                health_style = "bold red"
                health_emoji = "🔴"
            
            footer_text = Text(f"{health_emoji} Health Score: {health_score}/100", style=health_style)
            footer_text.append(" | Press Ctrl+C to exit", style="dim")
            layout["footer"].update(Panel(footer_text, box=box.ROUNDED))
            
            return layout
        
        # Run live dashboard
        try:
            with Live(generate_dashboard(), refresh_per_second=0.5, screen=True):
                while True:
                    time.sleep(2)
        except KeyboardInterrupt:
            self.console.print("\n👋 Dashboard stopped", style="yellow")
    
    def display_simple_status(self):
        """Display simple status without rich"""
        report = self.generate_report()
        
        print("\n" + "="*50)
        print("🔴 LIVE ASSET TRACKING BOT STATUS")
        print("="*50)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Health Score: {report['health_score']}/100")
        
        # System status
        print(f"\n🖥️  SYSTEM:")
        sys_stats = report['system']['system']
        print(f"   CPU: {sys_stats['cpu_percent']:.1f}%")
        print(f"   Memory: {sys_stats['memory_percent']:.1f}%")
        print(f"   Disk: {sys_stats['disk_percent']:.1f}%")
        
        # Bot process
        print(f"\n🤖 BOT PROCESS:")
        if report['system']['bot_running']:
            proc = report['system']['bot_process']
            print(f"   Status: ✅ Running (PID: {proc['pid']})")
            print(f"   Memory: {proc['memory_mb']} MB")
            print(f"   CPU: {proc['cpu_percent']:.1f}%")
            print(f"   Threads: {proc['threads']}")
        else:
            print("   Status: ❌ Not Running")
        
        # Activity (last hour)
        print(f"\n📊 ACTIVITY (Last Hour):")
        activity = report['log_analysis_1h']
        live_updates = activity.get('live_updates', {})
        tms_calls = activity.get('tms_calls', {})
        jobs = activity.get('job_queue', {})
        
        print(f"   Live Updates: {live_updates.get('count', 0)} (Errors: {live_updates.get('errors', 0)})")
        print(f"   TMS API Calls: {tms_calls.get('count', 0)} (Errors: {tms_calls.get('errors', 0)})")
        print(f"   Jobs Scheduled: {jobs.get('scheduled', 0)} (Failed: {jobs.get('failed', 0)})")
        
        # Recent errors
        errors = activity.get('errors', [])
        print(f"\n🚨 RECENT ERRORS ({len(errors)}):")
        if errors:
            for err in errors[:3]:
                print(f"   • {err['message']}")
        else:
            print("   ✅ No recent errors")
        
        print("\n" + "="*50)

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Live Update Bot Monitoring Dashboard")
    parser.add_argument("--live", action="store_true", help="Show live updating dashboard")
    parser.add_argument("--report", action="store_true", help="Generate JSON report")
    parser.add_argument("--health", action="store_true", help="Health check only")
    parser.add_argument("--simple", action="store_true", help="Simple text output")
    parser.add_argument("--output", help="Output file for report")
    
    args = parser.parse_args()
    
    monitor = LiveUpdateMonitor()
    
    if args.health:
        # Health check for monitoring systems
        report = monitor.generate_report()
        health_score = report['health_score']
        
        if health_score >= 80:
            print(f"OK - Health Score: {health_score}/100")
            sys.exit(0)
        elif health_score >= 60:
            print(f"WARNING - Health Score: {health_score}/100")
            sys.exit(1)
        else:
            print(f"CRITICAL - Health Score: {health_score}/100")
            sys.exit(2)
    
    elif args.report:
        # Generate JSON report
        report = monitor.generate_report()
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"Report saved to {args.output}")
        else:
            print(json.dumps(report, indent=2))
    
    elif args.live and not args.simple:
        # Live dashboard with rich
        monitor.display_live_dashboard()
    
    else:
        # Simple status display
        monitor.display_simple_status()

if __name__ == "__main__":
    main()