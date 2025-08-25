#!/usr/bin/env python3
"""
Test and demonstration script for Google Sheets rate limiting
Shows how the rate limiting wrapper handles API quota limits
"""

import logging
import time
from datetime import datetime
from config import Config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_rate_limiting():
    """Test the rate limiting functionality"""
    try:
        # Load configuration
        config = Config()
        
        # Import and create rate-limited Google integration
        from rate_limiting_wrapper import create_rate_limited_google_integration
        
        logger.info("Creating rate-limited Google Sheets integration...")
        google_integration = create_rate_limited_google_integration(config)
        
        logger.info("Testing rate limiting with multiple rapid requests...")
        
        # Test rapid successive calls that would normally trigger rate limits
        for i in range(10):
            start_time = time.time()
            try:
                # This would normally cause rate limit issues
                driver_names = google_integration.get_all_driver_names()
                duration = time.time() - start_time
                
                logger.info(f"Request {i+1}: Got {len(driver_names)} driver names in {duration:.2f}s")
                
                # Show cache statistics
                if i == 4:  # Show stats after 5 requests
                    stats = google_integration.get_rate_limiting_stats()
                    logger.info(f"Cache stats: {stats}")
                
            except Exception as e:
                logger.error(f"Request {i+1} failed: {e}")
            
            # Small delay between requests
            time.sleep(0.5)
        
        # Test QC Panel data with rate limiting
        logger.info("\nTesting QC Panel data access...")
        try:
            active_loads = google_integration.get_active_load_map()
            logger.info(f"Got {len(active_loads)} active loads from QC Panel")
        except Exception as e:
            logger.error(f"QC Panel test failed: {e}")
        
        # Show final statistics
        logger.info("\nFinal rate limiting statistics:")
        final_stats = google_integration.get_rate_limiting_stats()
        for key, value in final_stats.items():
            logger.info(f"  {key}: {value}")
        
        # Test health check
        logger.info("\nRunning health check...")
        health = google_integration.health_check()
        logger.info(f"Overall health: {health['overall_status']}")
        for component, status in health['components'].items():
            logger.info(f"  {component}: {status}")
        
        # Test cache refresh
        logger.info("\nTesting cache refresh...")
        refresh_results = google_integration.force_cache_refresh()
        for cache_type, result in refresh_results.items():
            status = "SUCCESS" if result['success'] else "FAILED"
            logger.info(f"  {cache_type}: {status}")
            if 'count' in result:
                logger.info(f"    Count: {result['count']}")
            if 'error' in result:
                logger.info(f"    Error: {result['error']}")
        
        logger.info("\nRate limiting test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Rate limiting test failed: {e}")
        return False

def simulate_high_load():
    """Simulate high load to test circuit breaker"""
    try:
        config = Config()
        from rate_limiting_wrapper import create_rate_limited_google_integration
        
        logger.info("Simulating high load scenario...")
        google_integration = create_rate_limited_google_integration(config)
        
        # Rapid fire requests to test circuit breaker
        for i in range(25):  # More than the circuit breaker threshold
            try:
                start = time.time()
                google_integration.get_all_driver_names()
                duration = time.time() - start
                logger.info(f"High load request {i+1}: {duration:.2f}s")
                
                # Show circuit breaker status every 5 requests
                if (i + 1) % 5 == 0:
                    stats = google_integration.get_rate_limiting_stats()
                    cb_open = stats.get('circuit_breaker_open', False)
                    failures = stats.get('circuit_breaker_failures', 0)
                    logger.info(f"Circuit breaker: Open={cb_open}, Failures={failures}")
                
            except Exception as e:
                logger.warning(f"High load request {i+1} failed: {e}")
            
            time.sleep(0.1)  # Very short delay to simulate rapid requests
        
        logger.info("High load simulation completed")
        
    except Exception as e:
        logger.error(f"High load simulation failed: {e}")

def demonstrate_caching():
    """Demonstrate caching effectiveness"""
    try:
        config = Config()
        from rate_limiting_wrapper import create_rate_limited_google_integration
        
        logger.info("Demonstrating caching effectiveness...")
        google_integration = create_rate_limited_google_integration(config)
        
        # First call - should hit the API
        logger.info("First call (should hit API):")
        start = time.time()
        names1 = google_integration.get_all_driver_names()
        duration1 = time.time() - start
        logger.info(f"  Got {len(names1)} names in {duration1:.2f}s")
        
        # Second call - should use cache
        logger.info("Second call (should use cache):")
        start = time.time()
        names2 = google_integration.get_all_driver_names()
        duration2 = time.time() - start
        logger.info(f"  Got {len(names2)} names in {duration2:.2f}s")
        
        # Show the performance improvement
        speedup = duration1 / duration2 if duration2 > 0 else float('inf')
        logger.info(f"Cache speedup: {speedup:.1f}x faster")
        
        # Show cache statistics
        stats = google_integration.get_rate_limiting_stats()
        logger.info(f"Cache entries: {stats.get('valid_entries', 0)}")
        logger.info(f"Driver names cache age: {stats.get('driver_names_cache_age', 0):.1f}s")
        
    except Exception as e:
        logger.error(f"Caching demonstration failed: {e}")

if __name__ == "__main__":
    print("🔧 Google Sheets Rate Limiting Test Suite")
    print("=" * 50)
    
    # Test basic rate limiting
    print("\n1. Testing basic rate limiting...")
    test_rate_limiting()
    
    print("\n" + "=" * 50)
    
    # Test caching
    print("\n2. Demonstrating caching...")
    demonstrate_caching()
    
    print("\n" + "=" * 50)
    
    # Test high load (uncomment to test circuit breaker)
    print("\n3. Simulating high load (circuit breaker test)...")
    print("Note: This may trigger actual rate limits - use carefully!")
    # simulate_high_load()
    
    print("\n✅ Rate limiting test suite completed!")
    print("\nTo enable rate limiting in production, ensure these environment variables are set:")
    print("  SHEETS_RATE_LIMIT_ENABLED=true")
    print("  SHEETS_MAX_REQUESTS_PER_MINUTE=180")
    print("  SHEETS_CACHE_DEFAULT_TTL=300")
    print("  SHEETS_CACHE_LONG_TTL=1800")
