#!/usr/bin/env python3
"""
Create the missing ELD_tracker sheet with proper structure
"""
from config import Config
from google_integration import GoogleSheetsIntegration

def create_eld_tracker_sheet():
    """Create ELD_tracker sheet with proper headers matching the screenshot"""
    
    config = Config()
    google = GoogleSheetsIntegration(config)
    
    try:
        print("📋 Creating ELD_tracker worksheet...")
        
        # Create the worksheet
        worksheet = google.spreadsheet.add_worksheet(
            title="ELD_tracker",
            rows=1000,  # Start with 1000 rows
            cols=15     # Plenty of columns
        )
        
        print("✅ ELD_tracker worksheet created")
        
        # Set up headers based on the screenshot
        headers = [
            "Timestamp",           # A
            "Name Gateway",        # B  
            "Serial Current",      # C
            "Driver Name",         # D
            "VIN",                 # E
            "Last Known Location", # F - Updated by silent refresh
            "Latitude",            # G - Updated by silent refresh  
            "Longitude",           # H - Updated by silent refresh
            "Status",              # I - Updated by silent refresh
            "Update Time",         # J - Updated by silent refresh
            "Source",              # K - Updated by silent refresh
            "Phone",               # L
            "Home address",        # M
            "Group link",          # N
            "Load id"              # O
        ]
        
        # Set headers
        worksheet.update('A1:O1', [headers])
        
        print("✅ Headers set successfully")
        print("📊 ELD_tracker structure:")
        print("  Columns A-E: Static data (not updated by silent refresh)")  
        print("  Columns F-K: Live data (updated every 5 minutes)")
        print("  Columns L-O: Additional static data")
        
        # Format headers (bold)
        worksheet.format('A1:O1', {
            'textFormat': {'bold': True},
            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
        })
        
        print("✅ Header formatting applied")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating ELD_tracker: {e}")
        return False

if __name__ == "__main__":
    success = create_eld_tracker_sheet()
    if success:
        print("\n🎉 ELD_tracker sheet created successfully!")
        print("📝 The sheet is now ready for silent refresh updates")
    else:
        print("\n💥 Failed to create ELD_tracker sheet")