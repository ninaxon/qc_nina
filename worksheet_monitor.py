#!/usr/bin/env python3
"""
Worksheet Monitor - Ensures all active pages are updated timely
Monitors and reports on worksheet update status and health
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import pytz

logger = logging.getLogger(__name__)

@dataclass
class WorksheetStatus:
    """Status information for a worksheet"""
    name: str
    last_update: Optional[datetime]
    expected_interval_seconds: int
    is_healthy: bool
    row_count: int
    error_message: Optional[str] = None
    update_method: str = "Unknown"

class WorksheetMonitor:
    """
    Monitors all active worksheets to ensure timely updates
    """
    
    def __init__(self, google_integration, config):
        self.google_integration = google_integration
        self.config = config
        self.last_check = None
        
        # Define expected update intervals for each worksheet
        self.worksheet_specs = {
            'assets': {
                'expected_interval': 300,  # 5 minutes (QC sync)
                'worksheet': 'assets_worksheet',
                'update_method': 'QC Panel sync + Manual updates',
                'critical': True
            },
            'fleet_status': {
                'expected_interval': 300,  # 5 minutes (risk monitoring)
                'worksheet': 'fleet_status_worksheet', 
                'update_method': 'TMS truck location/status data via risk monitoring',
                'critical': True
            },
            'groups': {
                'expected_interval': 3600,  # 1 hour (group updates)
                'worksheet': 'groups_worksheet',
                'update_method': 'Group registration/updates',
                'critical': False
            },
            'dashboard_logs': {
                'expected_interval': 60,  # 1 minute (user interactions)
                'worksheet': 'dashboard_logs_worksheet',
                'update_method': 'User interaction logging',
                'critical': False
            }
        }
    
    async def check_all_worksheets(self) -> Dict[str, WorksheetStatus]:
        """Check status of all worksheets"""
        statuses = {}
        
        for worksheet_name, spec in self.worksheet_specs.items():
            try:
                status = await self._check_worksheet(worksheet_name, spec)
                statuses[worksheet_name] = status
                
                if not status.is_healthy and spec.get('critical', False):
                    logger.error(f"CRITICAL: {worksheet_name} worksheet is unhealthy - {status.error_message}")
                elif not status.is_healthy:
                    logger.warning(f"WARNING: {worksheet_name} worksheet is unhealthy - {status.error_message}")
                    
            except Exception as e:
                logger.error(f"Error checking {worksheet_name}: {e}")
                statuses[worksheet_name] = WorksheetStatus(
                    name=worksheet_name,
                    last_update=None,
                    expected_interval_seconds=spec['expected_interval'],
                    is_healthy=False,
                    row_count=0,
                    error_message=str(e),
                    update_method=spec['update_method']
                )
        
        self.last_check = datetime.now(pytz.timezone('America/New_York'))
        return statuses
    
    async def _check_worksheet(self, worksheet_name: str, spec: Dict[str, Any]) -> WorksheetStatus:
        """Check individual worksheet status"""
        worksheet_attr = spec['worksheet']
        worksheet = getattr(self.google_integration, worksheet_attr, None)
        
        if not worksheet:
            return WorksheetStatus(
                name=worksheet_name,
                last_update=None,
                expected_interval_seconds=spec['expected_interval'],
                is_healthy=False,
                row_count=0,
                error_message=f"Worksheet {worksheet_attr} not available",
                update_method=spec['update_method']
            )
        
        try:
            # Get basic worksheet info
            all_values = worksheet.get_all_values()
            row_count = len(all_values) - 1  # Exclude header
            
            # Try to determine last update time
            last_update = await self._get_last_update_time(worksheet_name, worksheet, all_values)
            
            # Check if healthy based on expected interval
            is_healthy = self._is_worksheet_healthy(last_update, spec['expected_interval'])
            
            error_message = None
            if not is_healthy:
                if last_update:
                    age_minutes = (datetime.now(pytz.timezone('America/New_York')) - last_update).total_seconds() / 60
                    error_message = f"Last update {age_minutes:.1f} minutes ago (expected every {spec['expected_interval']/60:.1f} min)"
                else:
                    error_message = "No recent updates detected"
            
            return WorksheetStatus(
                name=worksheet_name,
                last_update=last_update,
                expected_interval_seconds=spec['expected_interval'],
                is_healthy=is_healthy,
                row_count=row_count,
                error_message=error_message,
                update_method=spec['update_method']
            )
            
        except Exception as e:
            return WorksheetStatus(
                name=worksheet_name,
                last_update=None,
                expected_interval_seconds=spec['expected_interval'],
                is_healthy=False,
                row_count=0,
                error_message=f"Error reading worksheet: {str(e)}",
                update_method=spec['update_method']
            )
    
    async def _get_last_update_time(self, worksheet_name: str, worksheet, all_values: List[List[str]]) -> Optional[datetime]:
        """Try to determine last update time for a worksheet"""
        if len(all_values) < 2:
            return None
        
        ny_tz = pytz.timezone('America/New_York')
        
        try:
            if worksheet_name == 'assets':
                # Look for most recent sync_time or last_updated column
                headers = [h.lower() for h in all_values[0]] if all_values else []
                time_cols = []
                
                for i, header in enumerate(headers):
                    if any(time_field in header for time_field in ['sync_time', 'last_updated', 'updated_at']):
                        time_cols.append(i)
                
                latest_time = None
                for row in all_values[1:]:
                    for col_idx in time_cols:
                        if col_idx < len(row) and row[col_idx]:
                            try:
                                # Try parsing the timestamp
                                time_str = row[col_idx].strip()
                                if time_str:
                                    parsed_time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                                    parsed_time = ny_tz.localize(parsed_time)
                                    if not latest_time or parsed_time > latest_time:
                                        latest_time = parsed_time
                            except ValueError:
                                continue
                
                return latest_time
                
            elif worksheet_name == 'fleet_status':
                # Look for sync_time column (last column usually)
                if len(all_values) > 1 and len(all_values[-1]) > 0:
                    last_row = all_values[-1]
                    # sync_time is typically the last column
                    if len(last_row) >= 20:  # fleet_status has 20 columns
                        sync_time = last_row[-1]  # Last column is sync_time
                        if sync_time:
                            try:
                                parsed_time = datetime.strptime(sync_time, '%Y-%m-%d %H:%M:%S')
                                return ny_tz.localize(parsed_time)
                            except ValueError:
                                pass
                
            elif worksheet_name == 'dashboard_logs':
                # Look for timestamp in first column
                if len(all_values) > 1 and len(all_values[-1]) > 0:
                    last_row = all_values[-1]
                    if last_row[0]:  # timestamp is first column
                        try:
                            parsed_time = datetime.strptime(last_row[0], '%Y-%m-%d %H:%M:%S')
                            return ny_tz.localize(parsed_time)
                        except ValueError:
                            pass
                            
            elif worksheet_name == 'groups':
                # Look for updated_at column
                headers = [h.lower() for h in all_values[0]] if all_values else []
                if 'updated_at' in headers:
                    col_idx = headers.index('updated_at')
                    latest_time = None
                    
                    for row in all_values[1:]:
                        if col_idx < len(row) and row[col_idx]:
                            try:
                                time_str = row[col_idx].strip()
                                if time_str:
                                    parsed_time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                                    parsed_time = ny_tz.localize(parsed_time)
                                    if not latest_time or parsed_time > latest_time:
                                        latest_time = parsed_time
                            except ValueError:
                                continue
                    
                    return latest_time
                    
        except Exception as e:
            logger.debug(f"Error parsing update time for {worksheet_name}: {e}")
        
        return None
    
    def _is_worksheet_healthy(self, last_update: Optional[datetime], expected_interval_seconds: int) -> bool:
        """Determine if worksheet is healthy based on last update time"""
        if not last_update:
            return False
        
        current_time = datetime.now(pytz.timezone('America/New_York'))
        time_since_update = current_time - last_update
        
        # Allow 2x the expected interval as grace period
        max_allowed_age = timedelta(seconds=expected_interval_seconds * 2)
        
        return time_since_update <= max_allowed_age
    
    def generate_health_report(self, statuses: Dict[str, WorksheetStatus]) -> str:
        """Generate human-readable health report"""
        report_lines = []
        report_lines.append("📊 **Worksheet Health Report**")
        report_lines.append(f"Generated: {datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S %Z')}")
        report_lines.append("")
        
        healthy_count = sum(1 for status in statuses.values() if status.is_healthy)
        total_count = len(statuses)
        
        report_lines.append(f"**Overall Status: {healthy_count}/{total_count} worksheets healthy**")
        report_lines.append("")
        
        for name, status in statuses.items():
            spec = self.worksheet_specs.get(name, {})
            is_critical = spec.get('critical', False)
            
            icon = "✅" if status.is_healthy else ("🔴" if is_critical else "⚠️")
            priority = "CRITICAL" if is_critical else "Normal"
            
            report_lines.append(f"{icon} **{name.title()} Worksheet** ({priority})")
            report_lines.append(f"   • Rows: {status.row_count}")
            report_lines.append(f"   • Expected interval: {status.expected_interval_seconds//60} min")
            report_lines.append(f"   • Update method: {status.update_method}")
            
            if status.last_update:
                age_minutes = (datetime.now(pytz.timezone('America/New_York')) - status.last_update).total_seconds() / 60
                report_lines.append(f"   • Last update: {age_minutes:.1f} min ago")
            else:
                report_lines.append(f"   • Last update: Unknown")
                
            if not status.is_healthy and status.error_message:
                report_lines.append(f"   • Issue: {status.error_message}")
                
            report_lines.append("")
        
        # Add recommendations
        unhealthy = [name for name, status in statuses.items() if not status.is_healthy]
        if unhealthy:
            report_lines.append("**🔧 Recommendations:**")
            for name in unhealthy:
                spec = self.worksheet_specs.get(name, {})
                if name == 'assets':
                    report_lines.append(f"   • Check QC Panel sync job (every {spec['expected_interval']//60} min)")
                elif name == 'fleet_status':
                    report_lines.append(f"   • Check TMS integration and risk monitoring jobs")
                elif name == 'dashboard_logs':
                    report_lines.append(f"   • Check user interaction logging")
                elif name == 'groups':
                    report_lines.append(f"   • Check group registration updates")
            report_lines.append("")
        
        return "\n".join(report_lines)

def create_worksheet_monitor(google_integration, config) -> WorksheetMonitor:
    """Factory function to create worksheet monitor"""
    return WorksheetMonitor(google_integration, config)

# Example usage for testing
if __name__ == "__main__":
    import asyncio
    
    async def test_monitor():
        # This would need actual google_integration and config objects
        print("Worksheet Monitor Test - would need real integration to run")
    
    asyncio.run(test_monitor())
