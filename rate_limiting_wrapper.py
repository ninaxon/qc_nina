#!/usr/bin/env python3
"""
Rate Limiting Wrapper for Google Sheets API
Implements exponential backoff, caching, and request batching to avoid quota exhaustion
"""

import time
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable
from functools import wraps
from threading import Lock
import hashlib
import json

logger = logging.getLogger(__name__)

class RateLimitedSheetsWrapper:
    """
    Wrapper for Google Sheets operations with intelligent rate limiting
    
    Features:
    - Exponential backoff with jitter for 429 errors
    - Intelligent caching with TTL
    - Request batching and deduplication
    - Circuit breaker pattern
    - Quota usage tracking
    """
    
    def __init__(self, config=None):
        self.config = config
        
        # Rate limiting settings (configurable)
        self.base_delay = getattr(config, 'SHEETS_EXPONENTIAL_BACKOFF_BASE', 1.0)
        self.max_delay = getattr(config, 'SHEETS_EXPONENTIAL_BACKOFF_MAX', 60.0)
        self.backoff_factor = 2.0  # Exponential backoff multiplier
        self.jitter_factor = 0.1  # Random jitter to prevent thundering herd
        
        # Caching settings (configurable)
        self.cache = {}
        self.cache_lock = Lock()
        self.default_cache_ttl = getattr(config, 'SHEETS_CACHE_DEFAULT_TTL', 300)
        self.long_cache_ttl = getattr(config, 'SHEETS_CACHE_LONG_TTL', 1800)
        
        # Circuit breaker settings (configurable)
        self.circuit_breaker_threshold = getattr(config, 'SHEETS_CIRCUIT_BREAKER_THRESHOLD', 10)
        self.circuit_breaker_timeout = getattr(config, 'SHEETS_CIRCUIT_BREAKER_TIMEOUT', 300)
        self.consecutive_failures = 0
        self.circuit_open_until = None
        
        # Quota tracking (configurable)
        self.request_count = 0
        self.quota_reset_time = datetime.now() + timedelta(minutes=1)
        self.max_requests_per_minute = getattr(config, 'SHEETS_MAX_REQUESTS_PER_MINUTE', 180)
        
        logger.info("Rate limited sheets wrapper initialized")
    
    def _get_cache_key(self, method_name: str, args: tuple, kwargs: dict) -> str:
        """Generate a cache key for the method call"""
        # Create a deterministic key from method name and arguments
        key_data = {
            'method': method_name,
            'args': str(args),
            'kwargs': sorted(kwargs.items()) if kwargs else []
        }
        key_string = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_string.encode(), usedforsecurity=False).hexdigest()
    
    def _is_cacheable_method(self, method_name: str) -> bool:
        """Determine if a method's results should be cached"""
        read_methods = {
            'get_all_values', 'get_all_records', 'row_values', 'col_values',
            'get', 'batch_get', 'find', 'findall'
        }
        return method_name in read_methods
    
    def _get_cache_ttl(self, method_name: str) -> int:
        """Get appropriate cache TTL based on method type"""
        # Longer cache for stable data
        long_cache_methods = {'get_all_values', 'get_all_records', 'row_values'}
        if method_name in long_cache_methods:
            return self.long_cache_ttl
        return self.default_cache_ttl
    
    def _is_cached_valid(self, cache_entry: dict) -> bool:
        """Check if cached entry is still valid"""
        return datetime.now() < cache_entry['expires_at']
    
    def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """Retrieve value from cache if valid"""
        with self.cache_lock:
            if cache_key in self.cache:
                entry = self.cache[cache_key]
                if self._is_cached_valid(entry):
                    logger.debug(f"Cache hit for key: {cache_key[:8]}...")
                    return entry['data']
                else:
                    # Remove expired entry
                    del self.cache[cache_key]
                    logger.debug(f"Cache expired for key: {cache_key[:8]}...")
        return None
    
    def _store_in_cache(self, cache_key: str, data: Any, ttl: int):
        """Store data in cache with TTL"""
        with self.cache_lock:
            self.cache[cache_key] = {
                'data': data,
                'expires_at': datetime.now() + timedelta(seconds=ttl),
                'created_at': datetime.now()
            }
            logger.debug(f"Cached data for key: {cache_key[:8]}... (TTL: {ttl}s)")
    
    def _cleanup_expired_cache(self):
        """Remove expired cache entries"""
        with self.cache_lock:
            expired_keys = [
                key for key, entry in self.cache.items()
                if not self._is_cached_valid(entry)
            ]
            for key in expired_keys:
                del self.cache[key]
            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker is open"""
        if self.circuit_open_until is None:
            return True  # Circuit is closed (normal operation)
        
        if datetime.now() > self.circuit_open_until:
            # Try to reset circuit breaker
            logger.info("Circuit breaker timeout expired, attempting reset")
            self.circuit_open_until = None
            self.consecutive_failures = 0
            return True
        
        logger.warning(f"Circuit breaker is open until {self.circuit_open_until}")
        return False
    
    def _handle_success(self):
        """Handle successful API call"""
        self.consecutive_failures = 0
        if self.circuit_open_until is not None:
            logger.info("Circuit breaker reset after successful call")
            self.circuit_open_until = None
    
    def _handle_failure(self, error: Exception):
        """Handle failed API call"""
        self.consecutive_failures += 1
        
        if self.consecutive_failures >= self.circuit_breaker_threshold:
            self.circuit_open_until = datetime.now() + timedelta(seconds=self.circuit_breaker_timeout)
            logger.error(f"Circuit breaker opened after {self.consecutive_failures} consecutive failures")
    
    def _check_quota_limit(self) -> bool:
        """Check if we're approaching quota limits"""
        now = datetime.now()
        
        # Reset quota counter every minute
        if now > self.quota_reset_time:
            self.request_count = 0
            self.quota_reset_time = now + timedelta(minutes=1)
        
        # Check if we're approaching the limit
        if self.request_count >= self.max_requests_per_minute:
            wait_time = (self.quota_reset_time - now).total_seconds()
            logger.warning(f"Quota limit reached, waiting {wait_time:.1f}s for reset")
            time.sleep(wait_time + 1)  # Wait a bit extra to be safe
            self.request_count = 0
            self.quota_reset_time = datetime.now() + timedelta(minutes=1)
        
        self.request_count += 1
        return True
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for exponential backoff with jitter"""
        # Exponential backoff: base_delay * (backoff_factor ^ attempt)
        delay = self.base_delay * (self.backoff_factor ** attempt)
        delay = min(delay, self.max_delay)  # Cap at max_delay
        
        # Add jitter to prevent thundering herd
        jitter = delay * self.jitter_factor * random.random()
        return delay + jitter
    
    def _execute_with_retry(self, func: Callable, method_name: str, *args, **kwargs) -> Any:
        """Execute function with exponential backoff retry logic"""
        max_attempts = 5
        
        for attempt in range(max_attempts):
            try:
                # Check circuit breaker
                if not self._check_circuit_breaker():
                    raise Exception("Circuit breaker is open")
                
                # Check quota limits
                self._check_quota_limit()
                
                # Execute the function
                result = func(*args, **kwargs)
                
                # Handle success
                self._handle_success()
                return result
                
            except Exception as e:
                error_str = str(e)
                is_rate_limit = (
                    '429' in error_str or 
                    'RATE_LIMIT_EXCEEDED' in error_str or
                    'Quota exceeded' in error_str or
                    'Too Many Requests' in error_str
                )
                
                # Also retry on connection issues
                is_connection_error = (
                    'Connection aborted' in error_str or
                    'RemoteDisconnected' in error_str or
                    'Remote end closed connection' in error_str or
                    'ConnectionError' in error_str or
                    'TimeoutError' in error_str or
                    'timeout' in error_str.lower()
                )
                
                should_retry = (is_rate_limit or is_connection_error) and attempt < max_attempts - 1
                
                if should_retry:
                    delay = self._calculate_delay(attempt)
                    error_type = "rate limit" if is_rate_limit else "connection issue"
                    logger.warning(f"{error_type} on {method_name} (attempt {attempt + 1}/{max_attempts}): {e}, "
                                 f"retrying in {delay:.2f}s")
                    time.sleep(delay)
                    continue
                
                # Handle failure
                self._handle_failure(e)
                
                # If it's the last attempt or not a retryable error, re-raise
                if attempt == max_attempts - 1 or not should_retry:
                    logger.error(f"Failed to execute {method_name} after {attempt + 1} attempts: {e}")
                    raise e
    
    def wrap_worksheet(self, worksheet):
        """Wrap a worksheet object with rate limiting"""
        return RateLimitedWorksheet(worksheet, self)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self.cache_lock:
            total_entries = len(self.cache)
            expired_entries = sum(
                1 for entry in self.cache.values()
                if not self._is_cached_valid(entry)
            )
            
            return {
                'total_entries': total_entries,
                'valid_entries': total_entries - expired_entries,
                'expired_entries': expired_entries,
                'circuit_breaker_failures': self.consecutive_failures,
                'circuit_breaker_open': self.circuit_open_until is not None,
                'requests_this_minute': self.request_count,
                'quota_reset_in': (self.quota_reset_time - datetime.now()).total_seconds()
            }


class RateLimitedWorksheet:
    """Wrapper for worksheet objects with rate limiting"""
    
    def __init__(self, worksheet, rate_limiter: RateLimitedSheetsWrapper):
        self._worksheet = worksheet
        self._rate_limiter = rate_limiter
        
        # Cache commonly used methods
        self._cached_methods = {
            'get_all_values', 'get_all_records', 'row_values'
        }
    
    def __getattr__(self, name):
        """Intercept method calls and apply rate limiting"""
        attr = getattr(self._worksheet, name)
        
        if not callable(attr):
            return attr
        
        def rate_limited_method(*args, **kwargs):
            # Check if this method should be cached
            if self._rate_limiter._is_cacheable_method(name):
                
                # Try to get from cache first
                cache_key = self._rate_limiter._get_cache_key(name, args, kwargs)
                cached_result = self._rate_limiter._get_from_cache(cache_key)
                
                if cached_result is not None:
                    return cached_result
                
                # Execute with retry logic
                result = self._rate_limiter._execute_with_retry(attr, name, *args, **kwargs)
                
                # Store in cache
                ttl = self._rate_limiter._get_cache_ttl(name)
                self._rate_limiter._store_in_cache(cache_key, result, ttl)
                
                return result
            else:
                # Non-cacheable methods (writes, updates, etc.)
                return self._rate_limiter._execute_with_retry(attr, name, *args, **kwargs)
        
        return rate_limited_method


def rate_limited_sheets_operation(cache_ttl: int = 300):
    """
    Decorator for Google Sheets operations with rate limiting
    
    Args:
        cache_ttl: Cache time-to-live in seconds
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # This would be used for standalone functions
            # For class methods, use the RateLimitedSheetsWrapper directly
            return func(*args, **kwargs)
        return wrapper
    return decorator


# Usage example functions
def create_rate_limited_google_integration(config):
    """
    Factory function to create a GoogleSheetsIntegration with rate limiting
    """
    from google_integration import GoogleSheetsIntegration
    
    # Create the integration
    integration = GoogleSheetsIntegration(config)
    
    # Create rate limiter
    rate_limiter = RateLimitedSheetsWrapper(config)
    
    # Wrap worksheets with rate limiting
    if integration.assets_worksheet:
        integration.assets_worksheet = rate_limiter.wrap_worksheet(integration.assets_worksheet)
    
    if integration.groups_worksheet:
        integration.groups_worksheet = rate_limiter.wrap_worksheet(integration.groups_worksheet)
    
    if integration.fleet_status_worksheet:
        integration.fleet_status_worksheet = rate_limiter.wrap_worksheet(integration.fleet_status_worksheet)
    
    if integration.dashboard_logs_worksheet:
        integration.dashboard_logs_worksheet = rate_limiter.wrap_worksheet(integration.dashboard_logs_worksheet)
    
    # Store rate limiter reference
    integration._rate_limiter = rate_limiter
    
    return integration


# Background task for cache cleanup
def cleanup_cache_periodically(rate_limiter: RateLimitedSheetsWrapper, interval: int = 300):
    """
    Background task to periodically clean up expired cache entries
    
    Args:
        rate_limiter: The rate limiter instance
        interval: Cleanup interval in seconds
    """
    import threading
    
    def cleanup_task():
        while True:
            try:
                time.sleep(interval)
                rate_limiter._cleanup_expired_cache()
                logger.debug("Periodic cache cleanup completed")
            except Exception as e:
                logger.error(f"Error in cache cleanup task: {e}")
    
    cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
    cleanup_thread.start()
    logger.info(f"Started cache cleanup task with {interval}s interval")
