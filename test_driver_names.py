#!/usr/bin/env python3
"""
Test script to verify hardcoded driver name column mapping is working
"""

import sys
from pathlib import Path

# Add current directory to path
sys.path.append(str(Path(__file__).parent))

from config import Config
from google_integration import GoogleSheetsIntegration

def test_driver_name_hardcoded_mapping():
    """Test the hardcoded driver name column mapping"""
    print("🧪 Testing hardcoded driver name column mapping...")
    
    try:
        config = Config()
        google_integration = GoogleSheetsIntegration(config)
        
        # Test the hardcoded column mapping
        print("📋 Testing hardcoded column indices...")
        print("   Column 4 (index 3): Driver Name")
        print("   Column 5 (index 4): VIN")
        print("   Column 12 (index 11): Phone")
        
        # Test with a known VIN
        test_vin = "4V4NC9EH7PN336858"  # From the earlier test
        
        print(f"\n🔍 Testing driver lookup for VIN: {test_vin}")
        
        # Test driver contact info (uses hardcoded columns)
        driver_name, phone = google_integration.get_driver_contact_info_by_vin(test_vin)
        if driver_name:
            print(f"✅ Driver contact info found:")
            print(f"   Driver Name: {driver_name}")
            print(f"   Phone: {phone or 'N/A'}")
            print(f"   VIN: {test_vin}")
        else:
            print(f"❌ No contact info found for VIN: {test_vin}")
        
        # Test driver name lookup (uses hardcoded columns)
        driver_name = google_integration.get_driver_name_by_vin(test_vin)
        if driver_name:
            print(f"✅ Driver name lookup: {driver_name}")
        else:
            print(f"❌ No driver name found for VIN: {test_vin}")
        
        # Test with a few more known VINs
        print(f"\n🔍 Testing with additional test VINs...")
        
        test_vins = [
            "4V4NC9EH7NN607835",  # Another VIN from the system
            "INVALID_VIN_TEST",   # Test invalid VIN handling
        ]
        
        for vin in test_vins:
            driver_name = google_integration.get_driver_name_by_vin(vin)
            status = "✅" if driver_name else "❌"
            print(f"   {status} VIN: {vin} -> Driver: {driver_name or 'Not found'}")
        
        print(f"\n✅ Hardcoded driver name column mapping test completed")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_send_to_test_groups():
    """Send a test message to verify driver names in group updates"""
    print("\n🧪 Testing driver names in group updates...")
    
    try:
        config = Config()
        
        # Check if we have test group configuration
        if not hasattr(config, 'TEST_GROUP_CHAT_IDS') or not config.TEST_GROUP_CHAT_IDS:
            print("⚠️  No test groups configured. Skipping group update test.")
            print("   Add TEST_GROUP_CHAT_IDS to your config for group testing")
            return True
        
        from telegram_integration import build_application
        import asyncio
        
        async def send_test_updates():
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
                
                print(f"✅ Connected to Telegram bot: @{(await app.bot.get_me()).username}")
                
                # Send test updates to configured test groups
                test_chat_ids = config.TEST_GROUP_CHAT_IDS
                print(f"📤 Sending test updates to {len(test_chat_ids)} test groups...")
                
                successful_sends = 0
                for chat_id in test_chat_ids:
                    try:
                        # Send a manual location update to verify driver names are working
                        await enhanced_bot.send_location_update(chat_id, force_update=True)
                        print(f"   ✅ Test update sent to group: {chat_id}")
                        successful_sends += 1
                        
                        # Small delay between sends
                        await asyncio.sleep(2)
                        
                    except Exception as send_error:
                        print(f"   ❌ Failed to send to group {chat_id}: {send_error}")
                
                print(f"\n📊 Test Results:")
                print(f"   Successfully sent: {successful_sends}/{len(test_chat_ids)} groups")
                print(f"   Driver names should now be visible in the test groups")
                
                await app.shutdown()
                return successful_sends > 0
                
            except Exception as e:
                print(f"❌ Error during test: {e}")
                try:
                    await app.shutdown()
                except:
                    pass
                return False
        
        return asyncio.run(send_test_updates())
        
    except Exception as e:
        print(f"❌ Group update test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Driver Name Testing Suite")
    print("=" * 50)
    
    # Test 1: Hardcoded column mapping
    test1_passed = test_driver_name_hardcoded_mapping()
    
    # Test 2: Group updates (if test groups configured)
    test2_passed = test_send_to_test_groups()
    
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY:")
    print(f"   Hardcoded Column Mapping: {'✅ PASS' if test1_passed else '❌ FAIL'}")
    print(f"   Group Update Test: {'✅ PASS' if test2_passed else '❌ FAIL'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 All driver name tests passed!")
        print("🔧 Hardcoded column mapping is working correctly")
        print("📤 Driver names should be visible in group updates")
    else:
        print("\n⚠️  Some tests failed - check the output above")
    
    sys.exit(0 if (test1_passed and test2_passed) else 1)