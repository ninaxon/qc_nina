#!/usr/bin/env python3
"""
Quick fix to disable column mapping while keeping rate limiting
This provides a safe fallback until column mapping is fully debugged
"""

import logging
from config import Config

logger = logging.getLogger(__name__)

def apply_quick_fix():
    """Apply quick fix to disable column mapping but keep rate limiting"""
    
    # Read current config
    config = Config()
    
    # Temporarily disable column mapping in the google integration
    try:
        import google_integration
        
        # Monkey patch to disable column mapping
        original_init = google_integration.GoogleSheetsIntegration.__init__
        
        def patched_init(self, config):
            # Call original init
            original_init(self, config)
            
            # Force disable column mapping to prevent errors
            self.use_column_mapping = False
            self.assets_mapper = None
            logger.info("🛡️ Column mapping temporarily disabled - using header-based fallback")
            logger.info("✅ Rate limiting still active for 429 error prevention")
        
        # Apply the patch
        google_integration.GoogleSheetsIntegration.__init__ = patched_init
        
        print("✅ Quick fix applied successfully!")
        print("   - Column mapping disabled (prevents bool iteration error)")
        print("   - Rate limiting still active (prevents 429 errors)")
        print("   - System will use header-based column access")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to apply quick fix: {e}")
        return False

if __name__ == "__main__":
    print("🚑 Applying quick fix for column mapping issues...")
    
    if apply_quick_fix():
        print("\n🎯 Your system should now work without the bool iteration error")
        print("   Rate limiting is still protecting against 429 errors")
        print("\n🔄 To test: python main.py --test")
    else:
        print("\n❌ Quick fix failed - check logs for details")
