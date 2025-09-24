#!/usr/bin/env python3
"""
Robust Google Sheets Writer
Implements dynamic resize, chunked writes, and upsert-by-VIN logic
"""

import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from gspread.utils import rowcol_to_a1

logger = logging.getLogger(__name__)


def chunked(seq: List, n: int):
    """Yield successive n-sized chunks from seq"""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def a1_range(
        start_row: int,
        start_col: int,
        end_row: int,
        end_col: int) -> str:
    """Convert row/col to A1 notation range"""
    return f"{rowcol_to_a1(start_row, start_col)}:{rowcol_to_a1(end_row, end_col)}"


class RobustSheetsWriter:
    """Robust writer for Google Sheets with dynamic resize and chunked operations"""

    def __init__(
            self,
            worksheet,
            chunk_size: int = 300,
            allow_new_trucks: bool = False):
        self.worksheet = worksheet
        self.chunk_size = chunk_size
        # Safety flag to prevent automatic VIN addition
        self.allow_new_trucks = allow_new_trucks

    def unmerge_all_cells(self):
        """Remove all merged cells from the worksheet"""
        try:
            from googleapiclient.discovery import build
            from google.oauth2.service_account import Credentials

            # This would need proper credentials setup
            # For now, we'll skip this and assume it's done by
            # fix_sheets_merges.py
            logger.debug(
                "Merged cells should be cleared by fix_sheets_merges.py")
        except ImportError:
            logger.debug(
                "Google API client not available for unmerge operation")

    def write_tms_data_to_assets(self,
                                 trucks: List[Dict],
                                 existing_records: List[Dict],
                                 headers: List[str]) -> Dict[str,
                                                             Any]:
        """
        Robust write operation: upsert TMS truck data to assets sheet

        Args:
            trucks: List of truck data from TMS
            existing_records: Current records from assets sheet
            headers: Header row for the sheet

        Returns:
            Statistics about the operation
        """
        start_time = time.time()
        stats = {
            'trucks_processed': 0,
            'assets_updated': 0,
            'field_updates_made': 0,
            'new_trucks_found': 0,
            'new_trucks_skipped': 0,  # Track skipped new trucks
            'errors': []
        }

        try:
            logger.info(
                f"Starting robust TMS → assets update for {len(trucks)} trucks")
            logger.info(
                f"New truck addition is {'ENABLED' if self.allow_new_trucks else 'DISABLED'} (safety mode)")

            # Build VIN-to-row index from existing data
            vin_to_row = {}
            for i, record in enumerate(existing_records):
                vin = str(record.get('VIN', '')).strip().upper()
                if vin:
                    # +2 for header row and 1-based indexing
                    vin_to_row[vin] = i + 2

            logger.info(
                f"Found {len(vin_to_row)} existing VINs in assets sheet")

            # Prepare updates and new rows
            batch_updates = []
            new_rows = []
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Process all trucks (no artificial limit)
            for truck in trucks:
                stats['trucks_processed'] += 1

                try:
                    vin = str(truck.get('vin', '')).strip().upper()
                    if not vin:
                        continue

                    # Extract truck data
                    location = truck.get('address', '')
                    lat = truck.get('latitude', '')
                    lon = truck.get('longitude', '')
                    status = truck.get('status', 'Unknown')

                    # Convert coordinates to strings
                    lat_str = str(lat) if lat else ""
                    lon_str = str(lon) if lon else ""

                    if vin in vin_to_row:
                        # Update existing row
                        row_num = vin_to_row[vin]

                        # Find column indices
                        location_col = self._find_header_col(
                            headers, 'Last Known Location')
                        lat_col = self._find_header_col(headers, 'Latitude')
                        lon_col = self._find_header_col(headers, 'Longitude')
                        status_col = self._find_header_col(headers, 'Status')
                        update_col = self._find_header_col(
                            headers, 'Update Time')

                        # Prepare individual cell updates
                        if location and location_col:
                            batch_updates.append({
                                'range': f'{self._col_letter(location_col)}{row_num}',
                                'values': [[location]]
                            })
                            stats['field_updates_made'] += 1

                        if lat_str and lat_col:
                            batch_updates.append({
                                'range': f'{self._col_letter(lat_col)}{row_num}',
                                'values': [[lat_str]]
                            })
                            stats['field_updates_made'] += 1

                        if lon_str and lon_col:
                            batch_updates.append({
                                'range': f'{self._col_letter(lon_col)}{row_num}',
                                'values': [[lon_str]]
                            })
                            stats['field_updates_made'] += 1

                        if status and status_col:
                            batch_updates.append({
                                'range': f'{self._col_letter(status_col)}{row_num}',
                                'values': [[status]]
                            })
                            stats['field_updates_made'] += 1

                        if update_col:
                            batch_updates.append({
                                'range': f'{self._col_letter(update_col)}{row_num}',
                                'values': [[current_time]]
                            })
                            stats['field_updates_made'] += 1

                        stats['assets_updated'] += 1

                    else:
                        # New truck found - check if we should add it
                        if self.allow_new_trucks:
                            # Add to new_rows list (only if explicitly allowed)
                            # Initialize with empty strings
                            new_row = [''] * len(headers)

                            # Fill in known data
                            self._set_row_value(new_row, headers, 'VIN', vin)
                            self._set_row_value(
                                new_row, headers, 'Last Known Location', location)
                            self._set_row_value(
                                new_row, headers, 'Latitude', lat_str)
                            self._set_row_value(
                                new_row, headers, 'Longitude', lon_str)
                            self._set_row_value(
                                new_row, headers, 'Status', status)
                            self._set_row_value(
                                new_row, headers, 'Update Time', current_time)
                            self._set_row_value(
                                new_row, headers, 'Source', 'TMS')

                            new_rows.append(new_row)
                            stats['new_trucks_found'] += 1
                            logger.info(
                                f"Will add new truck: {vin} (auto-addition enabled)")
                        else:
                            # Skip new truck (safety mode)
                            stats['new_trucks_skipped'] += 1
                            logger.debug(
                                f"Skipped new truck: {vin} (auto-addition disabled for safety)")

                except Exception as e:
                    stats['errors'].append(
                        f"Error processing truck {vin}: {e}")
                    logger.debug(f"Error processing truck {vin}: {e}")
                    continue

            # Execute batch updates in chunks
            if batch_updates:
                logger.info(
                    f"Executing {len(batch_updates)} cell updates in chunks of {self.chunk_size}")

                for i, chunk in enumerate(
                        chunked(batch_updates, self.chunk_size)):
                    try:
                        # Execute chunk with retry
                        self._execute_batch_update_chunk(
                            chunk, i + 1, len(list(chunked(batch_updates, self.chunk_size))))
                        time.sleep(0.1)  # Small delay between chunks

                    except Exception as e:
                        stats['errors'].append(
                            f"Batch update chunk {i+1} failed: {e}")
                        logger.error(f"Batch update chunk {i+1} failed: {e}")

            # Add new rows if any (and if allowed)
            if new_rows and self.allow_new_trucks:
                logger.info(
                    f"Adding {len(new_rows)} new trucks to assets sheet")
                try:
                    # Ensure sheet is large enough
                    current_rows = self.worksheet.row_count
                    needed_rows = len(existing_records) + \
                        len(new_rows) + 1  # +1 for header

                    if needed_rows > current_rows:
                        self.worksheet.resize(needed_rows)
                        logger.info(f"Resized sheet to {needed_rows} rows")

                    # Append new rows in chunks
                    # +2 for header and 1-based indexing
                    start_row = len(existing_records) + 2

                    for chunk in chunked(new_rows, self.chunk_size):
                        end_row = start_row + len(chunk) - 1
                        range_name = a1_range(
                            start_row, 1, end_row, len(headers))

                        self.worksheet.update(
                            range_name, chunk, value_input_option="RAW")
                        logger.debug(
                            f"Added chunk: rows {start_row}-{end_row}")

                        start_row = end_row + 1
                        time.sleep(0.1)  # Small delay between chunks

                except Exception as e:
                    stats['errors'].append(f"Failed to add new rows: {e}")
                    logger.error(f"Failed to add new rows: {e}")
            elif new_rows and not self.allow_new_trucks:
                logger.warning(
                    f"Skipped adding {len(new_rows)} new trucks (auto-addition disabled for safety)")

            duration = time.time() - start_time
            logger.info(f"TMS → assets update completed in {duration:.1f}s")
            logger.info(
                f"Stats: {stats['trucks_processed']} processed, {stats['assets_updated']} updated, "
                f"{stats['field_updates_made']} field updates, {stats['new_trucks_found']} new trucks found, "
                f"{stats['new_trucks_skipped']} new trucks skipped")

            if stats['errors']:
                logger.warning(
                    f"Encountered {len(stats['errors'])} errors during update")

            return stats

        except Exception as e:
            stats['errors'].append(f"Critical error in write operation: {e}")
            logger.error(
                f"Critical error in robust write operation: {e}",
                exc_info=True)
            return stats

    def _execute_batch_update_chunk(
            self,
            chunk: List[Dict],
            chunk_num: int,
            total_chunks: int):
        """Execute a chunk of batch updates with retry logic"""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                # Use batch_update for efficiency
                self.worksheet.batch_update(chunk, value_input_option="RAW")
                logger.debug(
                    f"Batch chunk {chunk_num}/{total_chunks} completed ({len(chunk)} updates)")
                return

            except Exception as e:
                if attempt == max_retries - 1:
                    raise e

                wait_time = (attempt + 1) * 2  # Exponential backoff
                logger.warning(
                    f"Batch chunk {chunk_num} attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)

    def _find_header_col(
            self,
            headers: List[str],
            target: str) -> Optional[int]:
        """Find column index for header (1-based)"""
        try:
            return headers.index(target) + 1  # 1-based for sheets
        except ValueError:
            return None

    def _col_letter(self, col_num: int) -> str:
        """Convert column number to letter (1-based)"""
        return rowcol_to_a1(1, col_num)[:-1]  # Remove row number

    def _set_row_value(
            self,
            row: List,
            headers: List[str],
            header: str,
            value: str):
        """Set value in row by header name"""
        try:
            col_idx = headers.index(header)
            row[col_idx] = value or ""  # Ensure string, never None
        except (ValueError, IndexError):
            pass  # Header not found or index error
