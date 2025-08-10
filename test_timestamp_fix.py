#!/usr/bin/env python3
"""
Quick test for timestamp staleness detection
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def test_timestamp_logic():
    """Test the timestamp staleness detection logic"""
    now_utc = datetime.now(ZoneInfo('UTC'))
    
    # Simulate stale data from TMS (August 3rd)
    stale_time = datetime(2025, 8, 3, 8, 43, 21, tzinfo=ZoneInfo('UTC'))
    
    # Calculate age
    age_hours = (now_utc - stale_time).total_seconds() / 3600
    max_age_hours = 12  # From config
    
    print(f"Current UTC time: {now_utc}")
    print(f"TMS timestamp: {stale_time}")
    print(f"Data age: {age_hours:.1f} hours")
    print(f"Max allowed age: {max_age_hours} hours")
    print(f"Is stale: {age_hours > max_age_hours}")
    
    if age_hours > max_age_hours:
        print(f"✅ FIXING: Using current time instead of stale timestamp")
        corrected_time = now_utc
        
        # Show NY time conversion
        ny_time = corrected_time.astimezone(ZoneInfo('America/New_York'))
        tz_name = ny_time.strftime('%Z')  # EDT or EST
        ny_str = ny_time.strftime(f'%Y-%m-%d %H:%M:%S {tz_name}')
        
        print(f"Corrected UTC: {corrected_time}")
        print(f"Corrected NY: {ny_str}")
    else:
        print("❌ Data is fresh, no correction needed")

if __name__ == "__main__":
    test_timestamp_logic()