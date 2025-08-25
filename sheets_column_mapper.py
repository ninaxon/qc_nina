#!/usr/bin/env python3
"""
Sheets Column Mapper - Robust column access using A,B,C notation
Replaces header-name based column access with reliable position-based access
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

from column_mapping_config import WorksheetType, ColumnMapping, initialize_column_mapper

logger = logging.getLogger(__name__)

class SheetsColumnMapper:
    """
    Utility class for robust column mapping in Google Sheets
    Provides safe access to columns using A,B,C notation instead of header names
    """
    
    def __init__(self, worksheet_type: WorksheetType, config=None):
        self.worksheet_type = worksheet_type
        # Initialize column mapper with config if not already done
        if config:
            self.column_mapper = initialize_column_mapper(config)
        else:
            from column_mapping_config import column_mapper as global_mapper
            self.column_mapper = global_mapper
        
        self.mappings = self.column_mapper.get_all_mappings(worksheet_type) if self.column_mapper else {}
    
    def get_value_by_field(self, row: List[Any], field_name: str) -> Any:
        """Get value from row by field name"""
        mapping = self.mappings.get(field_name)
        if not mapping:
            logger.warning(f"Unknown field '{field_name}' for worksheet {self.worksheet_type.value}")
            return None
        
        if mapping.column_index >= len(row):
            logger.debug(f"Row too short for column {mapping.column_letter} (index {mapping.column_index})")
            return None
        
        value = row[mapping.column_index]
        return self._convert_value(value, mapping)
    
    def set_value_by_field(self, row: List[Any], field_name: str, value: Any) -> bool:
        """Set value in row by field name"""
        mapping = self.mappings.get(field_name)
        if not mapping:
            logger.warning(f"Unknown field '{field_name}' for worksheet {self.worksheet_type.value}")
            return False
        
        # Extend row if necessary
        while len(row) <= mapping.column_index:
            row.append('')
        
        # Validate value
        is_valid, error_msg = self.column_mapper.validate_data(self.worksheet_type, field_name, value) if self.column_mapper else (True, "")
        if not is_valid:
            logger.warning(f"Invalid value for {field_name}: {error_msg}")
            # Still set the value but log the warning
        
        row[mapping.column_index] = self._format_value(value, mapping)
        return True
    
    def create_row_dict(self, row: List[Any]) -> Dict[str, Any]:
        """Convert row to dictionary using field names"""
        result = {}
        for field_name, mapping in self.mappings.items():
            result[field_name] = self.get_value_by_field(row, field_name)
        return result
    
    def create_row_from_dict(self, data: Dict[str, Any]) -> List[Any]:
        """Create row from dictionary"""
        # Find maximum column index
        max_index = max(mapping.column_index for mapping in self.mappings.values()) if self.mappings else 0
        row = [''] * (max_index + 1)
        
        for field_name, value in data.items():
            self.set_value_by_field(row, field_name, value)
        
        return row
    
    def get_column_letter(self, field_name: str) -> Optional[str]:
        """Get column letter for field name"""
        mapping = self.mappings.get(field_name)
        return mapping.column_letter if mapping else None
    
    def get_column_index(self, field_name: str) -> Optional[int]:
        """Get column index for field name"""
        mapping = self.mappings.get(field_name)
        return mapping.column_index if mapping else None
    
    def get_field_by_letter(self, column_letter: str) -> Optional[str]:
        """Get field name by column letter"""
        for field_name, mapping in self.mappings.items():
            if mapping.column_letter.upper() == column_letter.upper():
                return field_name
        return None
    
    def get_field_by_index(self, column_index: int) -> Optional[str]:
        """Get field name by column index"""
        for field_name, mapping in self.mappings.items():
            if mapping.column_index == column_index:
                return field_name
        return None
    
    def validate_row(self, row: List[Any]) -> Tuple[bool, List[str]]:
        """Validate entire row against mapping rules"""
        errors = []
        
        for field_name, mapping in self.mappings.items():
            if mapping.required:
                value = self.get_value_by_field(row, field_name)
                is_valid, error_msg = self.column_mapper.validate_data(self.worksheet_type, field_name, value) if self.column_mapper else (True, "")
                if not is_valid:
                    errors.append(f"Column {mapping.column_letter} ({mapping.display_name}): {error_msg}")
        
        return len(errors) == 0, errors
    
    def get_headers_row(self) -> List[str]:
        """Get headers row for the worksheet"""
        return self.column_mapper.get_headers_list(self.worksheet_type) if self.column_mapper else []
    
    def get_a1_range(self, field_names: List[str], start_row: int = 1, end_row: Optional[int] = None) -> str:
        """Get A1 notation range for specified fields"""
        if not field_names:
            return ""
        
        column_letters = []
        for field_name in field_names:
            letter = self.get_column_letter(field_name)
            if letter:
                column_letters.append(letter)
        
        if not column_letters:
            return ""
        
        column_letters.sort()
        start_col = column_letters[0]
        end_col = column_letters[-1]
        
        if end_row:
            return f"{start_col}{start_row}:{end_col}{end_row}"
        else:
            return f"{start_col}{start_row}:{end_col}"
    
    def _convert_value(self, value: Any, mapping: ColumnMapping) -> Any:
        """Convert value from sheet to appropriate Python type"""
        if value is None or value == '':
            return None
        
        try:
            if mapping.data_type == 'int':
                return int(float(str(value)))  # Handle "1.0" -> 1
            elif mapping.data_type == 'float':
                return float(value)
            elif mapping.data_type == 'datetime':
                # Handle various datetime formats
                if isinstance(value, datetime):
                    return value
                # Add more datetime parsing as needed
                return str(value)
            else:  # string
                return str(value).strip()
        except (ValueError, TypeError) as e:
            logger.debug(f"Error converting value '{value}' for {mapping.display_name}: {e}")
            return str(value) if value is not None else None
    
    def _format_value(self, value: Any, mapping: ColumnMapping) -> Any:
        """Format value for writing to sheet"""
        if value is None:
            return ''
        
        if mapping.data_type == 'datetime' and isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        
        return str(value)
    
    def debug_info(self) -> Dict[str, Any]:
        """Get debug information about the column mapping"""
        return self.column_mapper.debug_worksheet_mapping(self.worksheet_type) if self.column_mapper else {}


class AssetsColumnMapper(SheetsColumnMapper):
    """Specialized column mapper for assets worksheet"""
    
    def __init__(self, worksheet_type=None, config=None):
        # Use ASSETS as default worksheet type
        worksheet_type = worksheet_type or WorksheetType.ASSETS
        super().__init__(worksheet_type, config)
    
    def get_driver_name(self, row: List[Any]) -> Optional[str]:
        """Get driver name from row (Column D)"""
        return self.get_value_by_field(row, 'driver_name')
    
    def get_vin(self, row: List[Any]) -> Optional[str]:
        """Get VIN from row (Column E)"""
        vin = self.get_value_by_field(row, 'vin')
        return vin.upper().strip() if vin else None
    
    def get_driver_name_col(self) -> Optional[str]:
        """Get driver name column letter"""
        return self.get_column_letter('driver_name')
    
    def get_vin_col(self) -> Optional[str]:
        """Get VIN column letter"""
        return self.get_column_letter('vin')
    
    def get_location_info(self, row: List[Any]) -> Dict[str, Any]:
        """Get location information from row"""
        return {
            'location': self.get_value_by_field(row, 'last_known_location'),
            'latitude': self.get_value_by_field(row, 'latitude'),
            'longitude': self.get_value_by_field(row, 'longitude'),
            'status': self.get_value_by_field(row, 'status'),
            'update_time': self.get_value_by_field(row, 'update_time')
        }
    
    def get_load_info(self, row: List[Any]) -> Dict[str, Any]:
        """Get load information from row"""
        return {
            'load_id': self.get_value_by_field(row, 'load_id'),
            'pu_address': self.get_value_by_field(row, 'pu_address'),
            'pu_appt': self.get_value_by_field(row, 'pu_appt'),
            'del_address': self.get_value_by_field(row, 'del_address'),
            'del_appt': self.get_value_by_field(row, 'del_appt'),
            'eta': self.get_value_by_field(row, 'eta')
        }
    
    def set_location_info(self, row: List[Any], location_data: Dict[str, Any]) -> bool:
        """Set location information in row"""
        success = True
        
        if 'location' in location_data:
            success &= self.set_value_by_field(row, 'last_known_location', location_data['location'])
        if 'latitude' in location_data:
            success &= self.set_value_by_field(row, 'latitude', location_data['latitude'])
        if 'longitude' in location_data:
            success &= self.set_value_by_field(row, 'longitude', location_data['longitude'])
        if 'status' in location_data:
            success &= self.set_value_by_field(row, 'status', location_data['status'])
        if 'update_time' in location_data:
            success &= self.set_value_by_field(row, 'update_time', location_data['update_time'])
        
        return success
    
    def set_load_info(self, row: List[Any], load_data: Dict[str, Any]) -> bool:
        """Set load information in row"""
        success = True
        
        for field in ['load_id', 'pu_address', 'pu_appt', 'del_address', 'del_appt', 'eta']:
            if field in load_data:
                success &= self.set_value_by_field(row, field, load_data[field])
        
        return success


class GroupsColumnMapper(SheetsColumnMapper):
    """Specialized column mapper for groups worksheet"""
    
    def __init__(self, worksheet_type=None, config=None):
        worksheet_type = worksheet_type or WorksheetType.GROUPS
        super().__init__(worksheet_type, config)
    
    def get_group_info(self, row: List[Any]) -> Dict[str, Any]:
        """Get group information from row"""
        return {
            'group_id': self.get_value_by_field(row, 'group_id'),
            'group_title': self.get_value_by_field(row, 'group_title'),
            'vin': self.get_value_by_field(row, 'vin'),
            'driver_name': self.get_value_by_field(row, 'driver_name'),
            'status': self.get_value_by_field(row, 'status'),
            'last_updated': self.get_value_by_field(row, 'last_updated')
        }


class FleetStatusColumnMapper(SheetsColumnMapper):
    """Specialized column mapper for fleet_status worksheet"""
    
    def __init__(self, worksheet_type=None, config=None):
        worksheet_type = worksheet_type or WorksheetType.FLEET_STATUS
        super().__init__(worksheet_type, config)
    
    def get_tracking_info(self, row: List[Any]) -> Dict[str, Any]:
        """Get tracking information from row"""
        return {
            'vin': self.get_value_by_field(row, 'vin'),
            'driver_name': self.get_value_by_field(row, 'driver_name'),
            'latitude': self.get_value_by_field(row, 'latitude'),
            'longitude': self.get_value_by_field(row, 'longitude'),
            'address': self.get_value_by_field(row, 'address'),
            'speed_mph': self.get_value_by_field(row, 'speed_mph'),
            'status': self.get_value_by_field(row, 'status'),
            'movement_status': self.get_value_by_field(row, 'movement_status'),
            'last_updated': self.get_value_by_field(row, 'last_updated')
        }


# Convenience instances (will be initialized with config when needed)
assets_mapper = None
groups_mapper = None
fleet_status_mapper = None

def initialize_mappers(config=None):
    """Initialize convenience mapper instances with config"""
    global assets_mapper, groups_mapper, fleet_status_mapper
    assets_mapper = AssetsColumnMapper(config=config)
    groups_mapper = GroupsColumnMapper(config=config)
    fleet_status_mapper = FleetStatusColumnMapper(config=config)
    return assets_mapper, groups_mapper, fleet_status_mapper

# Utility functions
def get_assets_driver_name(row: List[Any]) -> Optional[str]:
    """Get driver name from assets row"""
    return assets_mapper.get_driver_name(row)

def get_assets_vin(row: List[Any]) -> Optional[str]:
    """Get VIN from assets row"""
    return assets_mapper.get_vin(row)

def validate_assets_row(row: List[Any]) -> Tuple[bool, List[str]]:
    """Validate assets row"""
    return assets_mapper.validate_row(row)

def create_assets_headers() -> List[str]:
    """Create headers for assets worksheet"""
    return assets_mapper.get_headers_row()

def debug_column_mappings() -> Dict[str, Any]:
    """Debug all column mappings"""
    return {
        'assets': assets_mapper.debug_info(),
        'groups': groups_mapper.debug_info(),
        'fleet_status': fleet_status_mapper.debug_info()
    }
