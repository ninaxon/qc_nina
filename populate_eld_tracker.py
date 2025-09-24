#!/usr/bin/env python3
"""
Populate ELD_tracker sheet with initial VIN data from assets sheet
This will create the base data that the silent refresh can then update
"""
from config import Config
from google_integration import GoogleSheetsIntegration


def populate_eld_tracker():
    """Populate ELD_tracker with VIN data from assets sheet"""

    config = Config()
    google = GoogleSheetsIntegration(config)

    try:
        print("📊 Reading assets sheet for VIN data...")

        # Get assets data
        assets_data = google.assets_worksheet.get_all_values()
        if len(assets_data) < 2:
            print("❌ Assets sheet has no data")
            return False

        assets_headers = [h.strip().lower() for h in assets_data[0]]
        assets_rows = assets_data[1:]

        print(f"✅ Found {len(assets_rows)} records in assets sheet")

        # Find required columns
        vin_col = None
        driver_col = None
        gateway_col = None
        serial_col = None
        phone_col = None

        for i, header in enumerate(assets_headers):
            if 'vin' in header:
                vin_col = i
            elif 'driver' in header or 'name' in header:
                driver_col = i
            elif 'gateway' in header or 'name gateway' in header:
                gateway_col = i
            elif 'serial' in header:
                serial_col = i
            elif 'phone' in header:
                phone_col = i

        if vin_col is None:
            print("❌ Could not find VIN column in assets sheet")
            return False

        print(f"📋 Column mapping:")
        print(
            f"  VIN: Column {vin_col} ({assets_headers[vin_col] if vin_col is not None else 'Not found'})")
        print(
            f"  Driver: Column {driver_col} ({assets_headers[driver_col] if driver_col is not None else 'Not found'})")
        print(
            f"  Gateway: Column {gateway_col} ({assets_headers[gateway_col] if gateway_col is not None else 'Not found'})")

        # Get ELD_tracker worksheet
        eld_worksheet = google.spreadsheet.worksheet('ELD_tracker')

        print("📝 Preparing ELD_tracker data...")

        # Prepare data for ELD_tracker
        eld_data = []
        valid_count = 0

        for row in assets_rows:
            if len(row) > vin_col and row[vin_col].strip():
                vin = str(row[vin_col]).strip().upper()
                if vin:  # Only include rows with valid VINs
                    timestamp = ""  # Will be filled by silent refresh
                    gateway = row[gateway_col] if gateway_col is not None and len(
                        row) > gateway_col else ""
                    serial = row[serial_col] if serial_col is not None and len(
                        row) > serial_col else ""
                    driver = row[driver_col] if driver_col is not None and len(
                        row) > driver_col else ""
                    phone = row[phone_col] if phone_col is not None and len(
                        row) > phone_col else ""

                    eld_row = [
                        timestamp,      # A: Timestamp (empty for now)
                        gateway,        # B: Name Gateway
                        serial,         # C: Serial Current
                        driver,         # D: Driver Name
                        vin,            # E: VIN
                        "",
                        # F: Last Known Location (updated by silent refresh)
                        "",
                        # G: Latitude (updated by silent refresh)
                        "",
                        # H: Longitude (updated by silent refresh)
                        "",            # I: Status (updated by silent refresh)
                        "",
                        # J: Update Time (updated by silent refresh)
                        "",            # K: Source (updated by silent refresh)
                        phone,         # L: Phone
                        "",            # M: Home address
                        "",            # N: Group link
                        ""             # O: Load id
                    ]

                    eld_data.append(eld_row)
                    valid_count += 1

        print(f"✅ Prepared {valid_count} records for ELD_tracker")

        if eld_data:
            # Clear existing data (except headers)
            print("🧹 Clearing existing ELD_tracker data...")
            # Clear using batch_clear instead
            eld_worksheet.batch_clear(["A2:O1000"])

            # Insert new data starting at row 2
            print("📝 Writing data to ELD_tracker sheet...")
            range_name = f"A2:O{len(eld_data) + 1}"
            eld_worksheet.update(range_name, eld_data)

            print(
                f"✅ Successfully populated ELD_tracker with {len(eld_data)} records")
            print("🔄 The silent refresh job can now update columns F-K with live data")

            return True
        else:
            print("❌ No valid VIN data found")
            return False

    except Exception as e:
        print(f"❌ Error populating ELD_tracker: {e}")
        return False


if __name__ == "__main__":
    success = populate_eld_tracker()
    if success:
        print("\n🎉 ELD_tracker populated successfully!")
        print("📊 The sheet now has VIN data that can be updated by silent refresh")
    else:
        print("\n💥 Failed to populate ELD_tracker")
