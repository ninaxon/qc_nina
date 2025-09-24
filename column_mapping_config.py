#!/usr/bin/env python3
"""
Column Mapping Configuration for Google Sheets
Provides robust column mapping using A,B,C notation instead of relying on header names
"""

import logging
from typing import Dict, Optional, List, Any
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class WorksheetType(Enum):
    """Supported worksheet types"""
    ASSETS = "assets"
    GROUPS = "groups"
    FLEET_STATUS = "fleet_status"
    ELD_TRACKER = "ELD_tracker"
    QC_PANEL = "qc_panel"


@dataclass
class ColumnMapping:
    """Column mapping definition for a worksheet"""
    column_letter: str  # A, B, C, etc.
    column_index: int   # 0-based index
    field_name: str     # Internal field name
    display_name: str   # Human readable name
    data_type: str      # string, int, float, datetime
    required: bool = False
    validation_regex: Optional[str] = None
    description: str = ""


class ColumnMappingManager:
    """
    Manages column mappings for all worksheets using A,B,C notation
    This eliminates issues with header name variations and provides consistent access
    """

    def __init__(self, config=None):
        self.config = config
        self.mappings = self._initialize_mappings_from_config()

    def _initialize_mappings_from_config(
            self) -> Dict[WorksheetType, Dict[str, ColumnMapping]]:
        """Initialize column mappings from config or use defaults"""

        # Get column positions from config or use defaults
        if self.config:
            driver_col = getattr(self.config, 'ASSETS_DRIVER_NAME_COL', 'D')
            vin_col = getattr(self.config, 'ASSETS_VIN_COL', 'E')
            location_col = getattr(self.config, 'ASSETS_LOCATION_COL', 'F')
            latitude_col = getattr(self.config, 'ASSETS_LATITUDE_COL', 'G')
            longitude_col = getattr(self.config, 'ASSETS_LONGITUDE_COL', 'H')
            phone_col = getattr(self.config, 'ASSETS_PHONE_COL', 'L')
        else:
            # Default positions
            driver_col = 'D'
            vin_col = 'E'
            location_col = 'F'
            latitude_col = 'G'
            longitude_col = 'H'
            phone_col = 'L'

        return self._create_mappings_with_positions(
            driver_col, vin_col, location_col, latitude_col, longitude_col, phone_col)

    def _create_mappings_with_positions(self,
                                        driver_col: str,
                                        vin_col: str,
                                        location_col: str,
                                        latitude_col: str,
                                        longitude_col: str,
                                        phone_col: str) -> Dict[WorksheetType,
                                                                Dict[str,
                                                                     ColumnMapping]]:
        """Initialize default column mappings for all worksheets"""

        return {
            # ASSETS WORKSHEET - Main fleet data (configurable positions)
            WorksheetType.ASSETS: {
                'driver_name': ColumnMapping(driver_col, self.letter_to_index(driver_col), 'driver_name', 'Driver Name', 'string', required=True, description='Primary driver name'),
                'vin': ColumnMapping(vin_col, self.letter_to_index(vin_col), 'vin', 'VIN', 'string', required=True, validation_regex=r'^[A-Z0-9]{17}$', description='Vehicle identification number'),
                'last_known_location': ColumnMapping(location_col, self.letter_to_index(location_col), 'last_known_location', 'Last Known Location', 'string', description='Auto-updated from TMS'),
                'latitude': ColumnMapping(latitude_col, self.letter_to_index(latitude_col), 'latitude', 'Latitude', 'float', description='GPS latitude'),
                'longitude': ColumnMapping(longitude_col, self.letter_to_index(longitude_col), 'longitude', 'Longitude', 'float', description='GPS longitude'),
                'phone': ColumnMapping(phone_col, self.letter_to_index(phone_col), 'phone', 'Phone', 'string', description='Driver phone number'),
                # Additional fixed columns that are less likely to change
                'status': ColumnMapping('I', 8, 'status', 'Status', 'string', description='Vehicle status'),
                'update_time': ColumnMapping('J', 9, 'update_time', 'Update Time', 'datetime', description='TMS sync timestamp'),
                'source': ColumnMapping('K', 10, 'source', 'Source', 'string', description='Data source (TMS)'),
                'load_id': ColumnMapping('T', 19, 'load_id', 'Load id', 'string', description='Load identifier'),
                'pu_address': ColumnMapping('U', 20, 'pu_address', 'PU address', 'string', description='Pickup address'),
                'pu_appt': ColumnMapping('V', 21, 'pu_appt', 'PU appt', 'string', description='Pickup appointment'),
                'del_address': ColumnMapping('W', 22, 'del_address', 'DEL address', 'string', description='Delivery address'),
                'del_appt': ColumnMapping('X', 23, 'del_appt', 'DEL appt', 'string', description='Delivery appointment'),
            },

            # GROUPS WORKSHEET - Telegram group registrations
            WorksheetType.GROUPS: {
                'group_id': ColumnMapping('A', 0, 'group_id', 'Group ID', 'int', required=True, description='Telegram group ID'),
                'group_title': ColumnMapping('B', 1, 'group_title', 'Group Title', 'string', required=True, description='Group chat title'),
                'vin': ColumnMapping('C', 2, 'vin', 'VIN', 'string', required=True, validation_regex=r'^[A-Z0-9]{17}$', description='Associated VIN'),
                'driver_name': ColumnMapping('D', 3, 'driver_name', 'Driver Name', 'string', description='Driver name'),
                'status': ColumnMapping('E', 4, 'status', 'Status', 'string', required=True, description='Registration status'),
                'last_updated': ColumnMapping('F', 5, 'last_updated', 'Last Updated', 'datetime', description='Last update timestamp'),
                'error_count': ColumnMapping('G', 6, 'error_count', 'Error Count', 'int', description='Number of errors'),
                'created_at': ColumnMapping('H', 7, 'created_at', 'Created At', 'datetime', description='Creation timestamp'),
                'updated_at': ColumnMapping('I', 8, 'updated_at', 'Updated At', 'datetime', description='Last modification'),
                'notes': ColumnMapping('J', 9, 'notes', 'Notes', 'string', description='Additional notes'),
            },

            # FLEET_STATUS WORKSHEET - Real-time tracking
            WorksheetType.FLEET_STATUS: {
                'vin': ColumnMapping('A', 0, 'vin', 'VIN', 'string', required=True, validation_regex=r'^[A-Z0-9]{17}$', description='Vehicle identification'),
                'driver_name': ColumnMapping('B', 1, 'driver_name', 'Driver Name', 'string', description='Driver name'),
                'last_updated': ColumnMapping('C', 2, 'last_updated', 'Last Updated', 'datetime', description='Last update time'),
                'latitude': ColumnMapping('D', 3, 'latitude', 'Latitude', 'float', description='Current latitude'),
                'longitude': ColumnMapping('E', 4, 'longitude', 'Longitude', 'float', description='Current longitude'),
                'address': ColumnMapping('F', 5, 'address', 'Address', 'string', description='Current address'),
                'speed_mph': ColumnMapping('G', 6, 'speed_mph', 'Speed mph', 'float', description='Current speed'),
                'status': ColumnMapping('H', 7, 'status', 'Status', 'string', description='Vehicle status'),
                'movement_status': ColumnMapping('I', 8, 'movement_status', 'Movement Status', 'string', description='Moving/Stopped'),
                'risk_level': ColumnMapping('J', 9, 'risk_level', 'Risk Level', 'string', description='Risk assessment'),
                'group_chat_id': ColumnMapping('K', 10, 'group_chat_id', 'Group Chat ID', 'int', description='Associated chat'),
                'last_contact': ColumnMapping('L', 11, 'last_contact', 'Last Contact', 'datetime', description='Last communication'),
            },

            # ELD_TRACKER WORKSHEET - Location history
            WorksheetType.ELD_TRACKER: {
                'vin': ColumnMapping('A', 0, 'vin', 'VIN', 'string', required=True, validation_regex=r'^[A-Z0-9]{17}$', description='Vehicle identification'),
                'field_a': ColumnMapping('B', 1, 'field_a', 'A', 'string', description='Field A'),
                'field_b': ColumnMapping('C', 2, 'field_b', 'B', 'string', description='Field B'),
                'field_c': ColumnMapping('D', 3, 'field_c', 'C', 'string', description='Field C'),
                'field_d': ColumnMapping('E', 4, 'field_d', 'D', 'string', description='Field D'),
                'last_known_location': ColumnMapping('F', 5, 'last_known_location', 'Last Known Location', 'string', description='Location description'),
                'latitude': ColumnMapping('G', 6, 'latitude', 'Latitude', 'float', description='GPS latitude'),
                'longitude': ColumnMapping('H', 7, 'longitude', 'Longitude', 'float', description='GPS longitude'),
                'status': ColumnMapping('I', 8, 'status', 'Status', 'string', description='Status'),
                'update_time': ColumnMapping('J', 9, 'update_time', 'Update Time', 'datetime', description='Update timestamp'),
                'source': ColumnMapping('K', 10, 'source', 'Source', 'string', description='Data source'),
            },

            # QC_PANEL WORKSHEET - Load management
            WorksheetType.QC_PANEL: {
                'driver': ColumnMapping('A', 0, 'driver', 'DRIVER', 'string', description='Driver name'),
                'vin': ColumnMapping('B', 1, 'vin', 'VIN', 'string', validation_regex=r'^[A-Z0-9]{17}$', description='Vehicle identification'),
                'unit': ColumnMapping('C', 2, 'unit', 'UNIT', 'string', description='Unit number'),
                'load_id': ColumnMapping('D', 3, 'load_id', '#', 'string', description='Load identifier'),
                'pu_status': ColumnMapping('R', 17, 'pu_status', 'STS OF PU', 'string', description='Pickup status'),
                'del_status': ColumnMapping('S', 18, 'del_status', 'STS OF DEL', 'string', description='Delivery status'),
                'pu_appt': ColumnMapping('T', 19, 'pu_appt', 'PU APT', 'string', description='Pickup appointment'),
                'pu_address': ColumnMapping('U', 20, 'pu_address', 'PU ADDRESS', 'string', description='Pickup address'),
                'del_appt': ColumnMapping('V', 21, 'del_appt', 'DEL APT', 'string', description='Delivery appointment'),
                'del_address': ColumnMapping('W', 22, 'del_address', 'DEL ADDRESS', 'string', description='Delivery address'),
            }
        }

    def get_mapping(self, worksheet_type: WorksheetType,
                    field_name: str) -> Optional[ColumnMapping]:
        """Get column mapping for a specific field"""
        worksheet_mappings = self.mappings.get(worksheet_type, {})
        return worksheet_mappings.get(field_name)

    def get_all_mappings(
            self, worksheet_type: WorksheetType) -> Dict[str, ColumnMapping]:
        """Get all column mappings for a worksheet"""
        return self.mappings.get(worksheet_type, {})

    def get_column_by_letter(
            self,
            worksheet_type: WorksheetType,
            column_letter: str) -> Optional[ColumnMapping]:
        """Get mapping by column letter (A, B, C, etc.)"""
        worksheet_mappings = self.mappings.get(worksheet_type, {})
        for mapping in worksheet_mappings.values():
            if mapping.column_letter.upper() == column_letter.upper():
                return mapping
        return None

    def get_column_by_index(
            self,
            worksheet_type: WorksheetType,
            column_index: int) -> Optional[ColumnMapping]:
        """Get mapping by column index (0-based)"""
        worksheet_mappings = self.mappings.get(worksheet_type, {})
        for mapping in worksheet_mappings.values():
            if mapping.column_index == column_index:
                return mapping
        return None

    def letter_to_index(self, column_letter: str) -> int:
        """Convert column letter to 0-based index (A=0, B=1, etc.)"""
        column_letter = column_letter.upper()
        result = 0
        for char in column_letter:
            result = result * 26 + (ord(char) - ord('A') + 1)
        return result - 1

    def index_to_letter(self, column_index: int) -> str:
        """Convert 0-based index to column letter (0=A, 1=B, etc.)"""
        result = ""
        while column_index >= 0:
            result = chr(column_index % 26 + ord('A')) + result
            column_index = column_index // 26 - 1
        return result

    def validate_data(self, worksheet_type: WorksheetType,
                      field_name: str, value: Any) -> tuple[bool, str]:
        """Validate data against column mapping rules"""
        mapping = self.get_mapping(worksheet_type, field_name)
        if not mapping:
            return False, f"Unknown field: {field_name}"

        # Check required fields
        if mapping.required and (value is None or str(value).strip() == ""):
            return False, f"Required field {mapping.display_name} is empty"

        # Check regex validation
        if mapping.validation_regex and value:
            import re
            if not re.match(mapping.validation_regex, str(value)):
                return False, f"Field {mapping.display_name} does not match required format"

        # Type validation
        if value and mapping.data_type != 'string':
            try:
                if mapping.data_type == 'int':
                    int(value)
                elif mapping.data_type == 'float':
                    float(value)
                elif mapping.data_type == 'datetime':
                    # Basic datetime validation - could be enhanced
                    pass
            except ValueError:
                return False, f"Field {mapping.display_name} has invalid {mapping.data_type} value"

        return True, ""

    def get_headers_list(self, worksheet_type: WorksheetType) -> List[str]:
        """Get ordered list of headers for a worksheet"""
        worksheet_mappings = self.mappings.get(worksheet_type, {})
        # Sort by column index
        sorted_mappings = sorted(
            worksheet_mappings.values(),
            key=lambda x: x.column_index)
        return [mapping.display_name for mapping in sorted_mappings]

    def create_row_from_dict(self,
                             worksheet_type: WorksheetType,
                             data: Dict[str,
                                        Any]) -> List[Any]:
        """Create a row list from a dictionary using column mapping"""
        worksheet_mappings = self.mappings.get(worksheet_type, {})
        if not worksheet_mappings:
            return []

        # Find the maximum column index to determine row length
        max_index = max(
            mapping.column_index for mapping in worksheet_mappings.values())
        row = [''] * (max_index + 1)

        # Fill in the data
        for field_name, value in data.items():
            mapping = self.get_mapping(worksheet_type, field_name)
            if mapping:
                row[mapping.column_index] = value if value is not None else ''

        return row

    def create_dict_from_row(
            self, worksheet_type: WorksheetType, row: List[Any]) -> Dict[str, Any]:
        """Create a dictionary from a row list using column mapping"""
        worksheet_mappings = self.mappings.get(worksheet_type, {})
        result = {}

        for field_name, mapping in worksheet_mappings.items():
            if mapping.column_index < len(row):
                value = row[mapping.column_index]
                # Convert empty strings to None for cleaner data
                result[field_name] = value if value != '' else None
            else:
                result[field_name] = None

        return result

    def get_column_range(
            self,
            worksheet_type: WorksheetType,
            field_names: List[str]) -> str:
        """Get A1 notation range for specified fields"""
        if not field_names:
            return ""

        column_letters = []
        for field_name in field_names:
            mapping = self.get_mapping(worksheet_type, field_name)
            if mapping:
                column_letters.append(mapping.column_letter)

        if not column_letters:
            return ""

        # Sort and get range
        column_letters.sort()
        if len(column_letters) == 1:
            return f"{column_letters[0]}:{column_letters[0]}"
        else:
            return f"{column_letters[0]}:{column_letters[-1]}"

    def debug_worksheet_mapping(
            self, worksheet_type: WorksheetType) -> Dict[str, Any]:
        """Get debug information about a worksheet mapping"""
        worksheet_mappings = self.mappings.get(worksheet_type, {})

        debug_info = {
            'worksheet_type': worksheet_type.value,
            'total_columns': len(worksheet_mappings),
            'required_fields': [],
            'validated_fields': [],
            'column_details': []
        }

        for field_name, mapping in worksheet_mappings.items():
            debug_info['column_details'].append({
                'field_name': field_name,
                'column_letter': mapping.column_letter,
                'column_index': mapping.column_index,
                'display_name': mapping.display_name,
                'data_type': mapping.data_type,
                'required': mapping.required,
                'has_validation': bool(mapping.validation_regex),
                'description': mapping.description
            })

            if mapping.required:
                debug_info['required_fields'].append(field_name)

            if mapping.validation_regex:
                debug_info['validated_fields'].append(field_name)

        # Sort by column index
        debug_info['column_details'].sort(key=lambda x: x['column_index'])

        return debug_info


# Global instance (will be initialized with config when available)
column_mapper = None


def initialize_column_mapper(config=None):
    """Initialize the global column mapper with config"""
    global column_mapper
    column_mapper = ColumnMappingManager(config)
    return column_mapper

# Convenience functions


def get_assets_mapping(field_name: str) -> Optional[ColumnMapping]:
    """Get assets worksheet column mapping"""
    return column_mapper.get_mapping(WorksheetType.ASSETS, field_name)


def get_groups_mapping(field_name: str) -> Optional[ColumnMapping]:
    """Get groups worksheet column mapping"""
    return column_mapper.get_mapping(WorksheetType.GROUPS, field_name)


def get_fleet_status_mapping(field_name: str) -> Optional[ColumnMapping]:
    """Get fleet status worksheet column mapping"""
    return column_mapper.get_mapping(WorksheetType.FLEET_STATUS, field_name)


def validate_vin(vin: str) -> tuple[bool, str]:
    """Validate VIN format"""
    return column_mapper.validate_data(WorksheetType.ASSETS, 'vin', vin)


def validate_driver_name(driver_name: str) -> tuple[bool, str]:
    """Validate driver name"""
    return column_mapper.validate_data(
        WorksheetType.ASSETS, 'driver_name', driver_name)
