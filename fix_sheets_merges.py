#!/usr/bin/env python3
"""
Merge Detection and Cleanup Script for Assets Sheet
Detects merged cells, removes them, and performs health check write
"""

import os, base64, json
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from config import Config

def creds_from_env():
    """Load credentials from environment or service account file"""
    # Try base64 encoded credentials first
    b64 = os.getenv("GOOGLE_SA_JSON_B64", "").strip()
    if b64:
        try:
            info = json.loads(base64.b64decode(b64))
            scopes = ["https://www.googleapis.com/auth/spreadsheets"]
            return Credentials.from_service_account_info(info, scopes=scopes)
        except Exception as e:
            print(f"Failed to load base64 credentials: {e}")
    
    # Fallback to service account file in credentials/
    credentials_file = "credentials/data-warehouse-452216-cb7ee86d19ea.json"
    if os.path.exists(credentials_file):
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        return Credentials.from_service_account_file(credentials_file, scopes=scopes)
    
    raise RuntimeError("No valid Google credentials found. Set GOOGLE_SA_JSON_B64 or provide service account file in credentials/")

def main():
    # Load config
    config = Config()
    SPREADSHEET_ID = config.SPREADSHEET_ID
    ASSETS_TITLE = "assets"  # exact tab name

    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID not configured")

    print(f"🔍 Checking spreadsheet: {SPREADSHEET_ID}")
    print(f"📊 Target worksheet: {ASSETS_TITLE}")

    # Authenticate
    creds = creds_from_env()
    gc = gspread.authorize(creds)

    # Open sheet & find the assets worksheet and its sheetId
    ss = gc.open_by_key(SPREADSHEET_ID)
    ws = ss.worksheet(ASSETS_TITLE)
    sheet_id = ws._properties["sheetId"]
    print(f"✅ Opened spreadsheet, assets sheetId: {sheet_id}")

    # Raw Sheets API to inspect merges and filter views
    service = build("sheets", "v4", credentials=creds)
    ss_meta = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        includeGridData=False
    ).execute()

    # List merges on this sheet
    merges = []
    for sht in ss_meta["sheets"]:
        if sht["properties"]["sheetId"] == sheet_id:
            merges = sht.get("merges", [])
            break

    print(f"📋 Found {len(merges)} merged cell ranges")
    if merges:
        print("🔍 Merge details:")
        for i, m in enumerate(merges[:10]):  # Show first 10
            start_row = m.get("startRowIndex", 0) + 1  # Convert to 1-based
            end_row = m.get("endRowIndex", start_row)
            start_col = chr(65 + m.get("startColumnIndex", 0))  # Convert to A, B, C
            end_col = chr(65 + m.get("endColumnIndex", start_col))
            print(f"  {i+1}. {start_col}{start_row}:{end_col}{end_row}")
        
        if len(merges) > 10:
            print(f"  ... and {len(merges) - 10} more")

    # Build batchUpdate to unmerge all on this sheet + clear filter views
    requests = []
    if merges:
        requests.append({
            "unmergeCells": {
                "range": {"sheetId": sheet_id}
            }
        })
        print("🔧 Preparing unmerge request...")

    # Clear basic filter if active (can block writes)
    requests.append({"clearBasicFilter": {"sheetId": sheet_id}})
    print("🔧 Preparing filter clear request...")

    if requests:
        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID, body={"requests": requests}
            ).execute()
            print("✅ Successfully unmerged cells and cleared filters")
        except Exception as e:
            print(f"❌ Error during batch update: {e}")
            return False

    # Test write: ensure we can write to the sheet
    try:
        import datetime
        test_value = f"HEALTHCHECK_{datetime.datetime.now().strftime('%H%M%S')}"
        ws.update_acell("A1", test_value)
        print(f"✅ Health check write successful: A1 = '{test_value}'")
    except Exception as e:
        print(f"❌ Health check write failed: {e}")
        return False

    # Check current sheet size and data extent
    try:
        all_values = ws.get_all_values()
        data_rows = len([row for row in all_values if any(cell.strip() for cell in row)])
        sheet_rows = ws.row_count
        sheet_cols = ws.col_count
        
        print(f"📊 Sheet dimensions: {sheet_rows} rows x {sheet_cols} cols")
        print(f"📊 Data extent: {data_rows} rows with content")
        
        if data_rows > 0:
            headers = all_values[0] if all_values else []
            print(f"📋 Headers ({len(headers)}): {headers[:8]}{'...' if len(headers) > 8 else ''}")
        
    except Exception as e:
        print(f"⚠️ Could not check sheet dimensions: {e}")

    print("🎉 Sheet health check completed successfully!")
    print("\n📝 Next steps:")
    print("   1. The assets sheet is now ready for dynamic writes")
    print("   2. Restart the bot to use the fixed sheets writer")
    print("   3. Monitor logs for successful TMS → assets updates every 8 minutes")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except Exception as e:
        print(f"💥 Script failed: {e}")
        exit(1)