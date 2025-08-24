#!/usr/bin/env python3
"""
Driver Name Matcher
Automatically parses driver names from group titles and matches them to VINs
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

logger = logging.getLogger(__name__)

class DriverNameMatcher:
    """Automatically matches driver names from group titles to VINs in assets sheet"""
    
    def __init__(self, google_integration):
        self.google_integration = google_integration
        self.driver_vin_cache = {}  # Cache driver name -> VIN mapping
        self._build_driver_vin_cache()
    
    def _build_driver_vin_cache(self):
        """Build a cache of driver names to VINs from the assets sheet"""
        try:
            # Get raw data directly from the worksheet to avoid column mapping issues
            worksheet = self.google_integration.assets_worksheet
            all_values = worksheet.get_all_values()
            
            if len(all_values) < 2:
                logger.warning("Assets sheet has no data rows")
                return
            
            headers = all_values[0]
            
            # Find driver name and VIN column indices
            driver_col_idx = None
            vin_col_idx = None
            
            for i, header in enumerate(headers):
                header_lower = header.lower()
                if 'driver' in header_lower and 'name' in header_lower:
                    driver_col_idx = i
                elif 'vin' in header_lower:
                    vin_col_idx = i
            
            if driver_col_idx is None or vin_col_idx is None:
                logger.error(f"Could not find driver name or VIN columns. Headers: {headers}")
                return
            
            logger.info(f"Found driver name column at index {driver_col_idx}, VIN column at index {vin_col_idx}")
            
            # Build cache from raw data
            for i, row in enumerate(all_values[1:], 2):  # Skip header
                if len(row) > max(driver_col_idx, vin_col_idx):
                    driver_name = str(row[driver_col_idx]).strip()
                    vin = str(row[vin_col_idx]).strip()
                    
                    if driver_name and vin and len(driver_name) > 2 and len(vin) >= 10:
                        # Store normalized driver name -> VIN mapping
                        normalized_name = self._normalize_driver_name(driver_name)
                        self.driver_vin_cache[normalized_name] = vin
                        
                        # Also store original name
                        self.driver_vin_cache[driver_name] = vin
                        
                        # Store first name and last name separately for partial matching
                        name_parts = driver_name.split()
                        if len(name_parts) >= 2:
                            first_name = name_parts[0]
                            last_name = name_parts[-1]
                            self.driver_vin_cache[first_name] = vin
                            self.driver_vin_cache[last_name] = vin
            
            logger.info(f"Built driver-VIN cache with {len(self.driver_vin_cache)} entries")
            
        except Exception as e:
            logger.error(f"Error building driver-VIN cache: {e}")
    
    def _normalize_driver_name(self, name: str) -> str:
        """Normalize driver name for better matching"""
        if not name:
            return ""
        
        # Convert to lowercase and remove extra spaces
        normalized = re.sub(r'\s+', ' ', name.lower().strip())
        
        # Remove common prefixes/suffixes
        normalized = re.sub(r'^(mr\.|mrs\.|ms\.|dr\.)\s*', '', normalized)
        
        return normalized
    
    def parse_driver_name_from_group_title(self, group_title: str) -> Optional[str]:
        """
        Parse driver name from group title like:
        "292 - C* - Lok Tamang - (C) - Truck_ 588526. Phone: (678) 409-0007"
        """
        if not group_title:
            return None
        
        # Pattern 1: Look for name between dashes
        # "292 - C* - Lok Tamang - (C) - Truck_ 588526"
        pattern1 = r'-\s*([^-]+?)\s*-'
        match1 = re.search(pattern1, group_title)
        if match1:
            candidate = match1.group(1).strip()
            if self._is_valid_driver_name(candidate):
                return candidate
        
        # Pattern 2: Look for name before "Truck_" or "Phone:"
        # "Lok Tamang - (C) - Truck_ 588526"
        pattern2 = r'([^-]+?)\s*-\s*(?:\([^)]+\)\s*-)?\s*(?:Truck_|Phone:)'
        match2 = re.search(pattern2, group_title)
        if match2:
            candidate = match2.group(1).strip()
            if self._is_valid_driver_name(candidate):
                return candidate
        
        # Pattern 3: Look for name after last dash before phone/truck
        # "292 - C* - Lok Tamang - (C) - Truck_"
        pattern3 = r'-\s*([^-]+?)\s*-\s*(?:\([^)]+\)\s*-)?\s*(?:Truck_|Phone:)'
        match3 = re.search(pattern3, group_title)
        if match3:
            candidate = match3.group(1).strip()
            if self._is_valid_driver_name(candidate):
                return candidate
        
        # Pattern 4: Look for name that looks like "FirstName LastName"
        # Extract words that could be names
        words = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', group_title)
        for word in words:
            if self._is_valid_driver_name(word):
                return word
        
        return None
    
    def _is_valid_driver_name(self, name: str) -> bool:
        """Check if a string looks like a valid driver name"""
        if not name or len(name) < 2:
            return False
        
        # Should contain letters and spaces only (no numbers, special chars)
        if not re.match(r'^[A-Za-z\s]+$', name):
            return False
        
        # Should have at least 2 words (first and last name)
        words = name.split()
        if len(words) < 2:
            return False
        
        # Each word should be at least 2 characters
        if any(len(word) < 2 for word in words):
            return False
        
        # Should not be too long (reasonable name length)
        if len(name) > 50:
            return False
        
        return True
    
    def find_vin_for_driver(self, driver_name: str) -> Optional[str]:
        """Find VIN for a given driver name using fuzzy matching"""
        if not driver_name:
            return None
        
        # Try exact match first
        if driver_name in self.driver_vin_cache:
            return self.driver_vin_cache[driver_name]
        
        # Try normalized match
        normalized_name = self._normalize_driver_name(driver_name)
        if normalized_name in self.driver_vin_cache:
            return self.driver_vin_cache[normalized_name]
        
        # Try case-insensitive match
        driver_name_lower = driver_name.lower()
        for cached_name, vin in self.driver_vin_cache.items():
            if cached_name.lower() == driver_name_lower:
                return vin
        
        # Try partial matching (first name or last name)
        driver_name_lower = driver_name.lower()
        for cached_name, vin in self.driver_vin_cache.items():
            cached_lower = cached_name.lower()
            if (driver_name_lower in cached_lower or 
                cached_lower in driver_name_lower or
                any(part.lower() == driver_name_lower for part in cached_name.split())):
                logger.info(f"Partial match: '{driver_name}' -> '{cached_name}'")
                return vin
        
        # Try fuzzy matching with lower threshold for partial names
        if self.driver_vin_cache:
            # Get all driver names for fuzzy matching
            driver_names = list(self.driver_vin_cache.keys())
            
            # Find best match with adjusted confidence threshold
            best_match = process.extractOne(
                driver_name, 
                driver_names, 
                scorer=fuzz.token_sort_ratio
            )
            
            # Lower threshold for partial names (like "javokhir")
            threshold = 70 if len(driver_name.split()) == 1 else 85
            
            if best_match and best_match[1] >= threshold:
                matched_name = best_match[0]
                logger.info(f"Fuzzy match: '{driver_name}' -> '{matched_name}' (confidence: {best_match[1]}%)")
                return self.driver_vin_cache[matched_name]
        
        return None
    
    async def auto_register_group_with_vin(self, group_id: int, group_title: str, owner_user_id: Optional[int] = None) -> Dict[str, any]:
        """
        Automatically register a group by parsing driver name and finding matching VIN
        
        Returns:
            Dict with result info
        """
        try:
            # Parse driver name from group title
            driver_name = self.parse_driver_name_from_group_title(group_title)
            
            if not driver_name:
                return {
                    "success": False,
                    "error": "Could not parse driver name from group title",
                    "group_title": group_title,
                    "suggestions": self._get_parsing_suggestions(group_title)
                }
            
            # Find matching VIN
            vin = self.find_vin_for_driver(driver_name)
            
            if not vin:
                return {
                    "success": False,
                    "error": f"Could not find VIN for driver: {driver_name}",
                    "driver_name": driver_name,
                    "group_title": group_title,
                    "available_drivers": list(self.driver_vin_cache.keys())[:10]  # Show first 10
                }
            
            # Register the group with the found VIN
            await self.google_integration.register_or_update_group(
                group_id=group_id,
                title=group_title,
                vin=vin,
                owner_user_id=owner_user_id
            )
            
            return {
                "success": True,
                "driver_name": driver_name,
                "vin": vin,
                "group_title": group_title,
                "confidence": "high" if driver_name in self.driver_vin_cache else "fuzzy"
            }
            
        except Exception as e:
            logger.error(f"Error in auto_register_group_with_vin: {e}")
            return {
                "success": False,
                "error": str(e),
                "group_title": group_title
            }
    
    def _get_parsing_suggestions(self, group_title: str) -> List[str]:
        """Get suggestions for improving group title parsing"""
        suggestions = []
        
        # Look for potential name patterns
        words = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', group_title)
        if words:
            suggestions.append(f"Potential names found: {', '.join(words)}")
        
        # Check for common patterns
        if "Truck_" in group_title:
            suggestions.append("Group title contains 'Truck_' - ensure driver name is before this")
        
        if "Phone:" in group_title:
            suggestions.append("Group title contains 'Phone:' - ensure driver name is before this")
        
        # Suggest format
        suggestions.append("Recommended format: 'ID - Code - Driver Name - (Code) - Truck_XXX'")
        
        return suggestions
    
    async def batch_auto_register_groups(self, groups_data: List[Dict]) -> Dict[str, any]:
        """
        Automatically register multiple groups
        
        Args:
            groups_data: List of dicts with 'group_id', 'title', 'owner_user_id' keys
        
        Returns:
            Summary of results
        """
        results = {
            "total_groups": len(groups_data),
            "successful": 0,
            "failed": 0,
            "errors": [],
            "successes": []
        }
        
        for group_data in groups_data:
            group_id = group_data.get('group_id')
            title = group_data.get('title')
            owner_user_id = group_data.get('owner_user_id')
            
            if not group_id or not title:
                results["errors"].append(f"Missing group_id or title: {group_data}")
                results["failed"] += 1
                continue
            
            result = await self.auto_register_group_with_vin(group_id, title, owner_user_id)
            
            if result["success"]:
                results["successes"].append(result)
                results["successful"] += 1
                logger.info(f"Auto-registered group {group_id}: {result['driver_name']} -> {result['vin']}")
            else:
                results["errors"].append(f"Group {group_id}: {result['error']}")
                results["failed"] += 1
        
        return results
    
    def refresh_cache(self):
        """Refresh the driver-VIN cache from the assets sheet"""
        self.driver_vin_cache.clear()
        self._build_driver_vin_cache() 