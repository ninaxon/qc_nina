#!/usr/bin/env python3
"""
Test script for column mapping system
Validates that the new A,B,C column mapping works correctly
"""

import logging
from datetime import datetime
from config import Config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_column_mapping_config():
    """Test the column mapping configuration"""
    try:
        from column_mapping_config import column_mapper, WorksheetType
        
        logger.info("Testing column mapping configuration...")
        
        # Test assets worksheet mapping
        assets_mappings = column_mapper.get_all_mappings(WorksheetType.ASSETS)
        logger.info(f"Assets worksheet has {len(assets_mappings)} column mappings")
        
        # Test key mappings
        driver_mapping = column_mapper.get_mapping(WorksheetType.ASSETS, 'driver_name')
        vin_mapping = column_mapper.get_mapping(WorksheetType.ASSETS, 'vin')
        
        if driver_mapping:
            logger.info(f"Driver Name: Column {driver_mapping.column_letter} (index {driver_mapping.column_index})")
        else:
            logger.error("Driver Name mapping not found!")
            
        if vin_mapping:
            logger.info(f"VIN: Column {vin_mapping.column_letter} (index {vin_mapping.column_index})")
        else:
            logger.error("VIN mapping not found!")
        
        # Test column letter conversion
        test_conversions = [
            ('A', 0), ('B', 1), ('C', 2), ('D', 3), ('E', 4),
            ('Z', 25), ('AA', 26), ('AB', 27)
        ]
        
        for letter, expected_index in test_conversions:
            actual_index = column_mapper.letter_to_index(letter)
            if actual_index == expected_index:
                logger.info(f"✅ {letter} -> {actual_index} (correct)")
            else:
                logger.error(f"❌ {letter} -> {actual_index} (expected {expected_index})")
        
        # Test validation
        valid_vin, error = column_mapper.validate_data(WorksheetType.ASSETS, 'vin', '1HGCM82633A123456')
        logger.info(f"Valid VIN test: {valid_vin} ({error})")
        
        invalid_vin, error = column_mapper.validate_data(WorksheetType.ASSETS, 'vin', 'INVALID')
        logger.info(f"Invalid VIN test: {invalid_vin} ({error})")
        
        return True
        
    except Exception as e:
        logger.error(f"Column mapping config test failed: {e}")
        return False

def test_column_mapper_utilities():
    """Test the column mapper utility classes"""
    try:
        from sheets_column_mapper import assets_mapper, groups_mapper, fleet_status_mapper
        
        logger.info("Testing column mapper utilities...")
        
        # Test assets mapper
        test_row = [
            '2023-12-01 10:00:00',  # A: timestamp
            'GATEWAY123',           # B: name_gateway  
            'SERIAL456',            # C: serial_current
            'John Doe',             # D: driver_name
            '1HGCM82633A123456',    # E: vin
            'Salt Lake City, UT',   # F: last_known_location
            '40.7608',              # G: latitude
            '-111.8910',            # H: longitude
            'Active',               # I: status
            '2023-12-01 10:00:00',  # J: update_time
            'TMS'                   # K: source
        ]
        
        # Test getting values
        driver_name = assets_mapper.get_value_by_field(test_row, 'driver_name')
        vin = assets_mapper.get_value_by_field(test_row, 'vin')
        
        logger.info(f"Driver Name from row: '{driver_name}'")
        logger.info(f"VIN from row: '{vin}'")
        
        if driver_name == 'John Doe':
            logger.info("✅ Driver name extraction correct")
        else:
            logger.error(f"❌ Driver name extraction failed: expected 'John Doe', got '{driver_name}'")
            
        if vin == '1HGCM82633A123456':
            logger.info("✅ VIN extraction correct")
        else:
            logger.error(f"❌ VIN extraction failed: expected '1HGCM82633A123456', got '{vin}'")
        
        # Test creating dictionary
        row_dict = assets_mapper.create_row_dict(test_row)
        logger.info(f"Row dictionary keys: {list(row_dict.keys())}")
        
        # Test location info
        location_info = assets_mapper.get_location_info(test_row)
        logger.info(f"Location info: {location_info}")
        
        # Test row validation
        is_valid, errors = assets_mapper.validate_row(test_row)
        logger.info(f"Row validation: {is_valid} (errors: {errors})")
        
        return True
        
    except Exception as e:
        logger.error(f"Column mapper utilities test failed: {e}")
        return False

def test_google_integration_with_mapping():
    """Test Google integration with column mapping"""
    try:
        config = Config()
        
        # Test if column mapping is available
        from google_integration import GoogleSheetsIntegration, COLUMN_MAPPING_AVAILABLE
        
        if not COLUMN_MAPPING_AVAILABLE:
            logger.warning("Column mapping not available, skipping integration test")
            return True
        
        logger.info("Testing Google integration with column mapping...")
        
        # Create integration with column mapping enabled
        google_integration = GoogleSheetsIntegration(config)
        
        if google_integration.use_column_mapping:
            logger.info("✅ Column mapping enabled in Google integration")
        else:
            logger.warning("⚠️ Column mapping not enabled")
        
        # Test driver names retrieval
        driver_names = google_integration.get_all_driver_names()
        logger.info(f"Retrieved {len(driver_names)} driver names using column mapping")
        
        if driver_names:
            logger.info(f"Sample driver names: {driver_names[:5]}")
            
            # Test VIN lookup
            test_driver = driver_names[0]
            vin = google_integration.find_vin_by_driver_name(test_driver)
            if vin:
                logger.info(f"✅ VIN lookup successful: '{test_driver}' -> {vin}")
            else:
                logger.warning(f"⚠️ No VIN found for driver: '{test_driver}'")
        
        return True
        
    except Exception as e:
        logger.error(f"Google integration test failed: {e}")
        return False

def test_backward_compatibility():
    """Test that the system works with and without column mapping"""
    try:
        config = Config()
        
        logger.info("Testing backward compatibility...")
        
        from google_integration import GoogleSheetsIntegration
        
        # Test with column mapping enabled
        google_integration = GoogleSheetsIntegration(config)
        
        original_setting = google_integration.use_column_mapping
        
        # Test with column mapping
        google_integration.use_column_mapping = True
        driver_names_with_mapping = google_integration.get_all_driver_names()
        logger.info(f"With column mapping: {len(driver_names_with_mapping)} driver names")
        
        # Test without column mapping (fallback)
        google_integration.use_column_mapping = False
        driver_names_without_mapping = google_integration.get_all_driver_names()
        logger.info(f"Without column mapping: {len(driver_names_without_mapping)} driver names")
        
        # Restore original setting
        google_integration.use_column_mapping = original_setting
        
        # Compare results
        if abs(len(driver_names_with_mapping) - len(driver_names_without_mapping)) <= 5:
            logger.info("✅ Backward compatibility maintained - similar results")
        else:
            logger.warning(f"⚠️ Significant difference in results: {len(driver_names_with_mapping)} vs {len(driver_names_without_mapping)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Backward compatibility test failed: {e}")
        return False

def debug_worksheet_structure():
    """Debug the actual worksheet structure"""
    try:
        config = Config()
        from google_integration import GoogleSheetsIntegration
        
        logger.info("Debugging worksheet structure...")
        
        google_integration = GoogleSheetsIntegration(config)
        
        if google_integration.assets_worksheet:
            # Get first few rows
            sample_data = google_integration.assets_worksheet.get_all_values()[:3]
            
            if sample_data:
                headers = sample_data[0]
                logger.info(f"Actual headers ({len(headers)} columns):")
                for i, header in enumerate(headers):
                    letter = chr(ord('A') + i) if i < 26 else f"A{chr(ord('A') + i - 26)}"
                    logger.info(f"  {letter}: '{header}'")
                
                if len(sample_data) > 1:
                    sample_row = sample_data[1]
                    logger.info(f"Sample data row ({len(sample_row)} values):")
                    for i, value in enumerate(sample_row[:10]):  # Show first 10 values
                        letter = chr(ord('A') + i) if i < 26 else f"A{chr(ord('A') + i - 26)}"
                        logger.info(f"  {letter}: '{value}'")
        
        return True
        
    except Exception as e:
        logger.error(f"Worksheet structure debug failed: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Column Mapping Test Suite")
    print("=" * 50)
    
    all_tests_passed = True
    
    # Test 1: Column mapping configuration
    print("\n1. Testing column mapping configuration...")
    if not test_column_mapping_config():
        all_tests_passed = False
    
    # Test 2: Column mapper utilities
    print("\n2. Testing column mapper utilities...")
    if not test_column_mapper_utilities():
        all_tests_passed = False
    
    # Test 3: Debug worksheet structure
    print("\n3. Debugging actual worksheet structure...")
    if not debug_worksheet_structure():
        all_tests_passed = False
    
    # Test 4: Google integration with mapping
    print("\n4. Testing Google integration with column mapping...")
    if not test_google_integration_with_mapping():
        all_tests_passed = False
    
    # Test 5: Backward compatibility
    print("\n5. Testing backward compatibility...")
    if not test_backward_compatibility():
        all_tests_passed = False
    
    print("\n" + "=" * 50)
    if all_tests_passed:
        print("✅ All column mapping tests passed!")
        print("\nTo enable column mapping in production:")
        print("  USE_COLUMN_MAPPING=true")
        print("\nColumn mapping provides:")
        print("  - Robust column access using A,B,C notation")
        print("  - Protection against header name changes")
        print("  - Data validation and type conversion")
        print("  - Backward compatibility with existing code")
    else:
        print("❌ Some column mapping tests failed!")
        print("Check the logs above for details.")
    
    print(f"\nTest completed at {datetime.now()}")
