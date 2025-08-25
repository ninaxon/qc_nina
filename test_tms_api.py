#!/usr/bin/env python3
"""
Test script to verify TMS API integration fixes
"""

import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from tms_integration import TMSIntegration
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_tms_api():
    """Test the updated TMS API integration"""
    try:
        config = Config()
        tms = TMSIntegration(config)
        
        logger.info("=" * 50)
        logger.info("TESTING TMS API INTEGRATION")
        logger.info("=" * 50)
        
        # Test truck list loading
        logger.info("Testing truck list loading...")
        trucks = tms.load_truck_list()
        
        logger.info(f"Loaded {len(trucks)} trucks from TMS API")
        
        if trucks:
            # Show sample truck data
            sample_truck = trucks[0]
            logger.info("Sample truck data:")
            for key, value in sample_truck.items():
                logger.info(f"  {key}: {value}")
            
            # Test VIN status check
            if sample_truck.get('vin'):
                logger.info(f"Testing VIN status check for {sample_truck['vin']}")
                status = tms.check_vin_status(sample_truck['vin'])
                logger.info(f"VIN status: {status}")
        
        # Test data freshness
        fresh_trucks = 0
        old_trucks = 0
        
        for truck in trucks:
            update_time = truck.get('update_time', '')
            if update_time:
                # This will be logged by the TMS integration
                if 'hours old' in str(truck):
                    old_trucks += 1
                else:
                    fresh_trucks += 1
        
        logger.info(f"Data freshness summary:")
        logger.info(f"  Fresh trucks: {fresh_trucks}")
        logger.info(f"  Old trucks filtered: {old_trucks}")
        
        logger.info("=" * 50)
        logger.info("TMS API TEST COMPLETED")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"TMS API test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_tms_api()
    sys.exit(0 if success else 1)