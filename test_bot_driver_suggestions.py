#!/usr/bin/env python3
"""
Test the bot's driver suggestion logic
"""

import os
import sys
import logging

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_bot_driver_suggestions():
    """Test the bot's driver suggestion logic"""
    
    try:
        from config import Config
        from telegram_integration import EnhancedLocationBot
        
        print("🤖 Testing Bot Driver Suggestions...")
        print("=" * 50)
        
        # Initialize
        config = Config()
        bot = EnhancedLocationBot(config)
        
        # Test the exact method that the bot uses
        test_cases = ["kevin", "kev", "john", "jose"]
        
        for search_term in test_cases:
            print(f"\n🔍 Testing bot method: '{search_term}'")
            
            # Test the exact method the bot uses
            suggestions = bot._find_similar_driver_names_from_sheets(search_term)
            
            if suggestions:
                print(f"   ✅ Bot found {len(suggestions)} suggestions:")
                for i, name in enumerate(suggestions[:5], 1):
                    print(f"      {i}. {name}")
                
                # Simulate what the bot would create
                print(f"   📱 Bot would create these buttons:")
                keyboard = []
                for name in suggestions[:5]:
                    display_name = name[:20] + "..." if len(name) > 20 else name
                    callback_data = f"DRIVER_SELECT|{name}"
                    print(f"      👤 {display_name} → {callback_data}")
                    keyboard.append([f"👤 {display_name}"])
                
                print(f"   🎯 Total buttons: {len(keyboard)}")
            else:
                print(f"   ❌ Bot found no suggestions")
        
        print("\n" + "=" * 50)
        print("✅ Bot driver suggestion test completed")
        print("💡 If the bot is running, it should now show inline buttons!")
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bot_driver_suggestions() 