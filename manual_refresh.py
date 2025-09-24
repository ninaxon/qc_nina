#!/usr/bin/env python3
"""
Manual ELD_tracker refresh script to update timestamps
This will fetch current fleet data and update the ELD_tracker sheet
"""
import asyncio
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def manual_eld_refresh():
    """Manually refresh ELD_tracker with current timestamps"""
    try:
        # Import here to handle potential import issues
        from config import Config
        from google_integration import GoogleSheetsIntegration
        from group_update_scheduler import GroupUpdateScheduler

        print("🔄 Starting manual ELD_tracker refresh...")

        # Load configuration
        config = Config()

        # Create Google integration
        google_integration = GoogleSheetsIntegration(config)

        # Mock bot for scheduler (we only need the update function)
        class MockBot:
            pass

        # Create scheduler instance
        scheduler = GroupUpdateScheduler(
            config=config,
            bot=MockBot(),
            google_integration=google_integration
        )

        print("📡 Fetching current fleet data from TMS...")

        # Import TMS service
        from tms_service import TMSService

        # Fetch fresh fleet data
        async with TMSService(config) as tms:
            fleet_points = await tms.fetch_fleet_locations()

        if not fleet_points:
            print("❌ No fleet data available")
            return False

        print(f"✅ Fetched {len(fleet_points)} fleet points")

        # Show timestamp sample
        if fleet_points:
            sample = fleet_points[0]
            ny_time = sample.to_ny_time()
            if ny_time:
                tz_name = ny_time.strftime('%Z')
                ny_str = ny_time.strftime(f'%Y-%m-%d %H:%M:%S {tz_name}')
                print(f"📅 Sample timestamp: {sample.vin} -> {ny_str}")

        print("📊 Updating ELD_tracker sheet...")

        # Update ELD_tracker sheet
        updated_count = await scheduler._update_eld_tracker(fleet_points)

        if updated_count > 0:
            print(
                f"✅ Successfully updated {updated_count} records in ELD_tracker")
            print(
                f"🕐 Timestamps should now show current time: {datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M:%S %Z')}")
            return True
        else:
            print("❌ No records were updated")
            return False

    except Exception as e:
        print(f"❌ Error during manual refresh: {e}")
        logger.error(f"Manual refresh error: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = asyncio.run(manual_eld_refresh())
    if success:
        print("\n🎉 Manual refresh completed successfully!")
        print("📋 Please check the ELD_tracker sheet for updated timestamps")
    else:
        print("\n💥 Manual refresh failed")

    sys.exit(0 if success else 1)
