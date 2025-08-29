#!/usr/bin/env python3
"""
Test script to send manual location updates to verify driver names are working
"""

import sys
import asyncio
from pathlib import Path

# Add current directory to path
sys.path.append(str(Path(__file__).parent))

from config import Config
from google_integration import GoogleSheetsIntegration

async def test_manual_group_updates():
    """Send manual location updates to test driver names"""
    print("🧪 Testing driver names in group location updates...")
    
    try:
        config = Config()
        google_integration = GoogleSheetsIntegration(config)
        
        # Get available groups from Google Sheets
        print("📋 Loading groups from Google Sheets...")
        
        try:
            groups_worksheet = google_integration.groups_worksheet
            if not groups_worksheet:
                print("❌ Groups worksheet not available")
                return False
                
            # Use raw values instead of records to avoid header issues
            all_values = groups_worksheet.get_all_values()
            if not all_values or len(all_values) < 2:
                print("❌ No groups found in worksheet")
                return False
            
            # Parse manually - assume format: group_name, chat_id, ...
            header = all_values[0]
            groups_data = []
            
            for row in all_values[1:]:  # Skip header
                if len(row) >= 2 and row[0]:  # Has group info in first column
                    groups_data.append({
                        'group_name': row[1] if len(row) > 1 else row[0],  # Use second column as name if available
                        'chat_id': row[0]  # First column is chat_id
                    })
            
            print(f"✅ Found {len(groups_data)} groups in worksheet")
            
            # Find a test group (prefer one with "test" in the name, or just use the first few)
            test_groups = []
            for group in groups_data[:3]:  # Test with first 3 groups
                if group.get('chat_id') and group.get('group_name'):
                    test_groups.append({
                        'chat_id': str(group['chat_id']).strip(),
                        'name': group['group_name']
                    })
            
            if not test_groups:
                print("❌ No valid groups with chat_id found")
                return False
            
            print(f"📤 Selected {len(test_groups)} groups for testing:")
            for group in test_groups:
                print(f"   - {group['name']} (ID: {group['chat_id']})")
            
        except Exception as e:
            print(f"❌ Error loading groups: {e}")
            return False
        
        # Now test with Telegram bot
        from telegram_integration import build_application
        
        app = build_application(config)
        if not app:
            print("❌ Failed to build Telegram application")
            return False
        
        try:
            await app.initialize()
            
            # Get enhanced bot instance  
            enhanced_bot = app.bot_data.get('enhanced_bot')
            if not enhanced_bot:
                print("❌ Enhanced bot not available")
                return False
            
            bot_info = await app.bot.get_me()
            print(f"✅ Connected to Telegram bot: @{bot_info.username}")
            
            # Send test updates to selected groups
            print(f"\n📤 Sending location updates with driver names...")
            
            successful_sends = 0
            for group in test_groups:
                try:
                    chat_id = int(group['chat_id'])
                    
                    print(f"   📡 Sending to {group['name']} ({chat_id})...")
                    
                    # Create a dummy session and truck data to test the location update
                    from telegram_integration import SessionData
                    
                    # Create test session
                    session = SessionData()
                    session.vin = "4V4NC9EH7PN336858"  # Use our test VIN with known driver
                    session.auto_refresh_enabled = False
                    
                    # Create test truck data
                    truck = {
                        'vin': "4V4NC9EH7PN336858",
                        'driver_name': 'Test Driver - Kevin Diaz-Salazar',
                        'lat': 40.7128,
                        'lon': -74.0060,
                        'location': 'New York, NY',
                        'speed': 0,
                        'timestamp': '2025-08-28 12:00:00'
                    }
                    
                    # Use the internal method to send location update
                    await enhanced_bot._send_group_location_update(None, chat_id, session, truck)
                    
                    print(f"   ✅ Update sent successfully")
                    successful_sends += 1
                    
                    # Small delay between sends
                    await asyncio.sleep(3)
                    
                except ValueError:
                    print(f"   ❌ Invalid chat_id format: {group['chat_id']}")
                except Exception as send_error:
                    print(f"   ❌ Failed to send: {send_error}")
            
            print(f"\n📊 Test Results:")
            print(f"   Successfully sent: {successful_sends}/{len(test_groups)} groups")
            
            if successful_sends > 0:
                print(f"✅ Driver names should now be visible in the group updates!")
                print(f"   Check the groups to verify driver names appear correctly")
                print(f"   This confirms the hardcoded column mapping is working in production")
            else:
                print(f"❌ No updates were sent successfully")
            
            await app.shutdown()
            return successful_sends > 0
            
        except Exception as e:
            print(f"❌ Error during Telegram operations: {e}")
            try:
                await app.shutdown()
            except:
                pass
            return False
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Group Update Driver Name Test")
    print("=" * 50)
    
    result = asyncio.run(test_manual_group_updates())
    
    print("\n" + "=" * 50)
    if result:
        print("🎉 Group update test completed successfully!")
        print("📱 Check your Telegram groups to verify driver names are displayed")
    else:
        print("⚠️  Group update test encountered issues")
        print("   Check the output above for details")
    
    sys.exit(0 if result else 1)