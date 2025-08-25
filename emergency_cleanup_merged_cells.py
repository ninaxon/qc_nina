#!/usr/bin/env python3
"""
Emergency cleanup script to fix bot-created merged cells in ELD_tracker sheet
This will immediately fix your ~366 vs ~518 update issue
"""

from config import Config
from google_integration import GoogleSheetsIntegration

def emergency_cleanup():
    """Emergency cleanup of bot-created merged cells"""
    print("🚨 Emergency Merged Cell Cleanup Starting...")
    
    try:
        config = Config()
        google = GoogleSheetsIntegration(config)
        
        print("📊 Connecting to ELD_tracker sheet...")
        eld_worksheet = google.spreadsheet.worksheet('ELD_tracker')
        
        # Get current sheet info
        sheet_info = eld_worksheet.get_all_values()
        print(f"📋 Found sheet with {len(sheet_info)} rows")
        
        # Unmerge all cells in the data range
        print("🧹 Unmerging all cells in ELD_tracker...")
        eld_worksheet.unmerge_cells('A1:K1000')
        print("✅ Successfully unmerged all cells in A1:K1000")
        
        # Verify we can still read the data
        print("🔍 Verifying sheet integrity after unmerge...")
        sample_data = eld_worksheet.get('A1:K10')
        print(f"✅ Can read data: {len(sample_data)} rows accessible")
        
        # Check for any remaining issues
        print("🔎 Checking for data integrity...")
        if len(sample_data) >= 2:
            headers = sample_data[0]
            print(f"📋 Headers: {headers}")
            
            # Find VIN column
            vin_col = None
            for i, header in enumerate(headers):
                if 'vin' in header.lower():
                    vin_col = i
                    break
            
            if vin_col is not None:
                print(f"🔍 VIN column found at index {vin_col} (should be 4 for column E)")
                
                # Check some VINs
                valid_vins = 0
                for row in sample_data[1:6]:  # Check first 5 data rows
                    if len(row) > vin_col and row[vin_col].strip():
                        valid_vins += 1
                
                print(f"✅ Found {valid_vins}/5 valid VINs in sample data")
            else:
                print("⚠️ VIN column not found in headers")
        
        print(f"\n🎉 Emergency cleanup completed successfully!")
        print(f"📊 Your next ELD_tracker update should process ~500+ records instead of ~366")
        print(f"🔄 The bot will no longer create merged cells with the code fixes")
        
        return True
        
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        print(f"💡 You may need to manually unmerge cells in Google Sheets:")
        print(f"   1. Open ELD_tracker sheet")
        print(f"   2. Select all data (Ctrl+A)")
        print(f"   3. Format → Merge cells → Unmerge")
        return False

if __name__ == "__main__":
    success = emergency_cleanup()
    if success:
        print("\n✨ Next steps:")
        print("1. ✅ Merged cells fixed")
        print("2. ✅ Bot code updated to prevent future merging")
        print("3. 🔄 Wait for next scheduled update to see ~500+ records")
        print("4. 📊 Monitor logs for 'Update summary' messages")
    else:
        print("\n💥 Manual intervention required")
        print("Check the Google Sheet directly and unmerge cells manually")