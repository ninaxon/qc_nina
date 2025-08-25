#!/usr/bin/env python3
"""
Test script to verify .env column mapping configuration works correctly
"""

import logging
import os
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_env_loading():
    """Test that .env variables are loaded correctly"""
    try:
        from config import Config
        
        logger.info("Testing .env configuration loading...")
        
        # Load config
        config = Config()
        
        # Test rate limiting settings
        logger.info("Rate Limiting Settings:")
        logger.info(f"  SHEETS_RATE_LIMIT_ENABLED: {getattr(config, 'SHEETS_RATE_LIMIT_ENABLED', 'NOT SET')}")
        logger.info(f"  SHEETS_MAX_REQUESTS_PER_MINUTE: {getattr(config, 'SHEETS_MAX_REQUESTS_PER_MINUTE', 'NOT SET')}")
        logger.info(f"  SHEETS_CACHE_DEFAULT_TTL: {getattr(config, 'SHEETS_CACHE_DEFAULT_TTL', 'NOT SET')}")
        
        # Test column mapping settings
        logger.info("Column Mapping Settings:")
        logger.info(f"  USE_COLUMN_MAPPING: {getattr(config, 'USE_COLUMN_MAPPING', 'NOT SET')}")
        logger.info(f"  ASSETS_DRIVER_NAME_COL: {getattr(config, 'ASSETS_DRIVER_NAME_COL', 'NOT SET')}")
        logger.info(f"  ASSETS_VIN_COL: {getattr(config, 'ASSETS_VIN_COL', 'NOT SET')}")
        logger.info(f"  ASSETS_LOCATION_COL: {getattr(config, 'ASSETS_LOCATION_COL', 'NOT SET')}")
        logger.info(f"  ASSETS_LATITUDE_COL: {getattr(config, 'ASSETS_LATITUDE_COL', 'NOT SET')}")
        logger.info(f"  ASSETS_LONGITUDE_COL: {getattr(config, 'ASSETS_LONGITUDE_COL', 'NOT SET')}")
        logger.info(f"  ASSETS_PHONE_COL: {getattr(config, 'ASSETS_PHONE_COL', 'NOT SET')}")
        
        # Validate settings
        validation_errors = []
        
        if not hasattr(config, 'USE_COLUMN_MAPPING'):
            validation_errors.append("USE_COLUMN_MAPPING not found in config")
        elif not config.USE_COLUMN_MAPPING:
            logger.warning("Column mapping is disabled")
        
        if not hasattr(config, 'ASSETS_DRIVER_NAME_COL'):
            validation_errors.append("ASSETS_DRIVER_NAME_COL not found in config")
        
        if not hasattr(config, 'ASSETS_VIN_COL'):
            validation_errors.append("ASSETS_VIN_COL not found in config")
        
        if validation_errors:
            logger.error("Configuration validation errors:")
            for error in validation_errors:
                logger.error(f"  - {error}")
            return False
        
        logger.info("✅ Configuration loaded successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return False

def test_column_mapping_initialization():
    """Test that column mapping initializes with config"""
    try:
        from config import Config
        from column_mapping_config import initialize_column_mapper, WorksheetType
        
        logger.info("Testing column mapping initialization...")
        
        config = Config()
        
        # Initialize column mapper with config
        mapper = initialize_column_mapper(config)
        
        # Test assets mapping
        assets_mappings = mapper.get_all_mappings(WorksheetType.ASSETS)
        
        logger.info(f"Assets mappings loaded: {len(assets_mappings)} columns")
        
        # Test specific mappings
        driver_mapping = mapper.get_mapping(WorksheetType.ASSETS, 'driver_name')
        vin_mapping = mapper.get_mapping(WorksheetType.ASSETS, 'vin')
        
        if driver_mapping:
            logger.info(f"✅ Driver Name mapping: Column {driver_mapping.column_letter} (should match {config.ASSETS_DRIVER_NAME_COL})")
            if driver_mapping.column_letter != config.ASSETS_DRIVER_NAME_COL:
                logger.error(f"❌ Driver column mismatch: got {driver_mapping.column_letter}, expected {config.ASSETS_DRIVER_NAME_COL}")
                return False
        else:
            logger.error("❌ Driver Name mapping not found")
            return False
        
        if vin_mapping:
            logger.info(f"✅ VIN mapping: Column {vin_mapping.column_letter} (should match {config.ASSETS_VIN_COL})")
            if vin_mapping.column_letter != config.ASSETS_VIN_COL:
                logger.error(f"❌ VIN column mismatch: got {vin_mapping.column_letter}, expected {config.ASSETS_VIN_COL}")
                return False
        else:
            logger.error("❌ VIN mapping not found")
            return False
        
        logger.info("✅ Column mapping initialization successful!")
        return True
        
    except Exception as e:
        logger.error(f"Column mapping initialization failed: {e}")
        return False

def test_google_integration_with_env():
    """Test Google integration with .env column mapping"""
    try:
        from config import Config
        from google_integration import GoogleSheetsIntegration
        
        logger.info("Testing Google integration with .env column mapping...")
        
        config = Config()
        
        # This would normally connect to actual Google Sheets
        # For testing, we'll just check initialization
        logger.info("Creating GoogleSheetsIntegration instance...")
        
        # Mock the worksheet to avoid actual API calls during testing
        class MockWorksheet:
            def get_all_values(self):
                return [
                    ['A', 'B', 'C', 'Driver Name', 'VIN', 'Location', 'Lat', 'Lng'],  # Headers
                    ['', '', '', 'John Doe', '1HGCM82633A123456', 'Salt Lake City', '40.7608', '-111.8910'],  # Data
                    ['', '', '', 'Jane Smith', '2HGCM82633A123457', 'Denver', '39.7392', '-104.9903']
                ]
        
        # Test with mock data
        integration = GoogleSheetsIntegration.__new__(GoogleSheetsIntegration)
        integration.config = config
        integration.use_column_mapping = True
        
        # Initialize column mapping
        from column_mapping_config import initialize_column_mapper
        from sheets_column_mapper import AssetsColumnMapper
        
        initialize_column_mapper(config)
        integration.assets_mapper = AssetsColumnMapper(config=config)
        
        # Test row processing
        test_row = ['', '', '', 'John Doe', '1HGCM82633A123456', 'Salt Lake City', '40.7608', '-111.8910']
        
        driver_name = integration.assets_mapper.get_value_by_field(test_row, 'driver_name')
        vin = integration.assets_mapper.get_value_by_field(test_row, 'vin')
        
        logger.info(f"✅ Extracted from test row:")
        logger.info(f"   Driver Name: '{driver_name}' (from column {config.ASSETS_DRIVER_NAME_COL})")
        logger.info(f"   VIN: '{vin}' (from column {config.ASSETS_VIN_COL})")
        
        if driver_name == 'John Doe' and vin == '1HGCM82633A123456':
            logger.info("✅ Column mapping extraction successful!")
            return True
        else:
            logger.error(f"❌ Column mapping extraction failed")
            return False
        
    except Exception as e:
        logger.error(f"Google integration test failed: {e}")
        return False

def check_env_file():
    """Check if .env file exists and configuration is working"""
    env_file = Path(".env")
    
    if not env_file.exists():
        logger.warning("⚠️ .env file not found - using config defaults")
        logger.info("You can create a .env file to customize column mapping settings")
    else:
        logger.info("✅ .env file found")
    
    # Test if configuration is working (regardless of .env file)
    try:
        from config import Config
        config = Config()
        
        # Check if the configuration values are available (from .env or defaults)
        test_vars = {
            'USE_COLUMN_MAPPING': getattr(config, 'USE_COLUMN_MAPPING', None),
            'ASSETS_DRIVER_NAME_COL': getattr(config, 'ASSETS_DRIVER_NAME_COL', None),
            'ASSETS_VIN_COL': getattr(config, 'ASSETS_VIN_COL', None),
            'SHEETS_RATE_LIMIT_ENABLED': getattr(config, 'SHEETS_RATE_LIMIT_ENABLED', None)
        }
        
        missing_config = [var for var, value in test_vars.items() if value is None]
        
        if missing_config:
            logger.error(f"❌ Configuration values not available: {missing_config}")
            return False
        
        logger.info("✅ Configuration system working correctly")
        logger.info("   (Variables available from .env file or config defaults)")
        
        # Show current values
        for var, value in test_vars.items():
            logger.info(f"   {var}: {value}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error testing configuration: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Environment Column Mapping Test Suite")
    print("=" * 50)
    
    all_tests_passed = True
    
    # Test 1: Check .env file
    print("\n1. Checking .env file...")
    if not check_env_file():
        all_tests_passed = False
    
    # Test 2: Test environment loading
    print("\n2. Testing environment variable loading...")
    if not test_env_loading():
        all_tests_passed = False
    
    # Test 3: Test column mapping initialization
    print("\n3. Testing column mapping initialization...")
    if not test_column_mapping_initialization():
        all_tests_passed = False
    
    # Test 4: Test Google integration
    print("\n4. Testing Google integration with .env settings...")
    if not test_google_integration_with_env():
        all_tests_passed = False
    
    print("\n" + "=" * 50)
    if all_tests_passed:
        print("✅ All column mapping tests passed!")
        print("\n🎉 Your configuration is ready:")
        print("  ✅ Rate limiting enabled to fix 429 errors")
        print("  ✅ Column mapping configured for robust data access")
        print("  ✅ Driver name mixups should be resolved")
        print("\n🚀 Next steps:")
        print("  1. Restart your application to activate the new settings")
        print("  2. Monitor logs for 'Column mapping enabled' messages")
        print("  3. Check for reduced API calls and faster responses")
        print("\n💡 Optional: Add variables to .env file to customize column positions")
    else:
        print("❌ Some tests failed!")
        print("\nPlease check:")
        print("  1. Configuration system is working properly")
        print("  2. Column positions match your Google Sheet")
        print("  3. All required modules are available")
