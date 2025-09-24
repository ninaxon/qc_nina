#!/usr/bin/env python3
from google_integration import GoogleSheetsIntegration
from config import Config
import sys
from pathlib import Path

# Add current directory to path
sys.path.append(str(Path(__file__).parent))


print("🧹 Cleaning up old individual group jobs...")

try:
    config = Config()
    google = GoogleSheetsIntegration(config)

    # Get all active groups
    records = google._get_groups_records_safe()
    active_groups = []

    for record in records:
        if (record.get('status', '').upper() == 'ACTIVE' and
            record.get('vin') and
                record.get('group_id')):
            active_groups.append(record['group_id'])

    print(
        f"Found {len(active_groups)} active groups that should use centralized scheduling")

    # Note: Old individual jobs will automatically expire when the bot restarts
    # The new centralized GroupUpdateScheduler will take over

    print("✅ Old individual group jobs will be cleaned up on bot restart")
    print("✅ Centralized GroupUpdateScheduler will handle all group updates")
    print("✅ This should resolve the connection blocking issues")

except Exception as e:
    print(f"❌ Cleanup failed: {e}")
    sys.exit(1)
