"""
Comprehensive tests for the Asset Tracking Bot.
Tests FleetPoint data contracts, scheduling, ELD_tracker updates, and timezone handling.
"""
import asyncio
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

from data_contracts import FleetPoint
from tms_service import TMSService
from group_update_scheduler import GroupUpdateScheduler
from config import Config
from google_integration import GoogleSheetsIntegration


class TestFleetPoint:
    """Test FleetPoint data contract compliance"""
    
    def test_fleet_point_vin_normalization(self):
        """Test VIN is normalized to uppercase"""
        fp = FleetPoint(
            vin="  4v4nc9eh7pn336858  ",
            driver_name="John Doe",
            location_str="I-80, Tiffin, IA",
            lat=41.5128,
            lon=-91.4315,
            status="Moving (65 mph)",
            updated_at_utc=datetime.now(ZoneInfo('UTC')),
            source="TMS Auto-Update"
        )
        assert fp.vin == "4V4NC9EH7PN336858"
    
    def test_fleet_point_timezone_conversion(self):
        """Test UTC to NY timezone conversion with EDT/EST handling"""
        # Test EDT (summer)
        utc_summer = datetime(2023, 7, 15, 20, 30, 0, tzinfo=ZoneInfo('UTC'))
        fp = FleetPoint(
            vin="TEST123",
            driver_name=None,
            location_str=None,
            lat=None,
            lon=None,
            status=None,
            updated_at_utc=utc_summer,
            source="test"
        )
        
        ny_time = fp.to_ny_time()
        assert ny_time is not None
        assert ny_time.strftime('%Z') == 'EDT'
        assert ny_time.hour == 16  # 20 UTC = 16 EDT
        
        # Test EST (winter)
        utc_winter = datetime(2023, 12, 15, 20, 30, 0, tzinfo=ZoneInfo('UTC'))
        fp_winter = FleetPoint(
            vin="TEST123",
            driver_name=None,
            location_str=None,
            lat=None,
            lon=None,
            status=None,
            updated_at_utc=utc_winter,
            source="test"
        )
        
        ny_winter = fp_winter.to_ny_time()
        assert ny_winter is not None
        assert ny_winter.strftime('%Z') == 'EST'
        assert ny_winter.hour == 15  # 20 UTC = 15 EST
    
    def test_fleet_point_speed_extraction(self):
        """Test speed extraction from status"""
        test_cases = [
            ("Moving (65 mph)", 65),
            ("Idle", 0),
            ("Stopped", 0),
            ("Highway (80 mph)", 80),
            ("Unknown Status", 0),
            ("Moving Slowly (5 mph)", 5),
        ]
        
        for status, expected_speed in test_cases:
            fp = FleetPoint(
                vin="TEST123",
                driver_name=None,
                location_str=None,
                lat=None,
                lon=None,
                status=status,
                updated_at_utc=None,
                source="test"
            )
            assert fp.speed_mph() == expected_speed


class TestGroupUpdateScheduler:
    """Test Group Update Scheduler with jitter and semaphore"""
    
    @pytest.fixture
    def mock_config(self):
        config = MagicMock()
        config.MAX_CONCURRENT_TELEGRAM_SENDS = 12
        config.SCHEDULER_JITTER_MAX_SECONDS = 15
        config.GROUP_LOCATION_INTERVAL = 3600
        return config
    
    @pytest.fixture
    def mock_bot(self):
        return AsyncMock()
    
    @pytest.fixture
    def mock_google(self):
        google = MagicMock()
        google._get_groups_records_safe.return_value = [
            {'group_id': '12345', 'vin': 'TEST123', 'status': 'ACTIVE', 'group_title': 'Test Group'}
        ]
        return google
    
    def test_html_message_formatting(self, mock_config, mock_bot, mock_google):
        """Test HTML message formatting per template spec"""
        scheduler = GroupUpdateScheduler(mock_config, mock_bot, mock_google)
        
        fleet_point = FleetPoint(
            vin="TEST123",
            driver_name="John Doe <script>alert('xss')</script>",  # Test HTML escaping
            location_str="I-80, Tiffin, IA & Co",  # Test HTML escaping
            lat=41.5128,
            lon=-91.4315,
            status="Moving (65 mph)",
            updated_at_utc=datetime(2023, 7, 15, 16, 30, 0, tzinfo=ZoneInfo('UTC')),
            source="TMS Auto-Update"
        )
        
        message = scheduler._build_location_message(fleet_point)
        
        # Check HTML formatting
        assert message.startswith("🚛 <b>Location Update</b>")
        assert "<b>Driver:</b>" in message
        assert "<b>Status:</b>" in message
        assert "<b>Location:</b>" in message
        assert "<b>Speed:</b> 65 mph" in message
        assert "<b>Updated:</b>" in message
        
        # Check HTML escaping
        assert "&lt;script&gt;" in message  # XSS should be escaped
        assert "&amp;" in message  # Ampersand should be escaped
        
        # Check map link
        assert "<a href='https://maps.google.com/?q=41.5128,-91.4315'>View on Map</a>" in message
        
        # Check timezone formatting (should be EDT for July)
        assert "EDT" in message


class TestSheetsModel:
    """Test comprehensive sheets model operations"""
    
    @pytest.fixture
    def mock_config(self):
        config = MagicMock()
        return config
    
    @pytest.fixture 
    def mock_google_integration(self):
        google = MagicMock()
        google.spreadsheet = MagicMock()
        return google
    
    def test_sheet_schema_validation(self, mock_config, mock_google_integration):
        """Test sheet schemas are properly defined"""
        from sheets_model import SHEET_SCHEMAS, SheetsModelManager
        
        # Test all required sheets are defined
        required_sheets = [
            'assets', 'groups', 'ELD_tracker', 'fleet_status', 
            'location_logs', 'dashboard_logs', 'ack_audit', 'errors'
        ]
        
        for sheet_name in required_sheets:
            assert sheet_name in SHEET_SCHEMAS
            schema = SHEET_SCHEMAS[sheet_name]
            assert len(schema.headers) > 0
            assert len(schema.required_columns) > 0
    
    def test_eld_tracker_f_k_update(self, mock_config, mock_google_integration):
        """Test ELD_tracker F:K column batch update"""
        from sheets_model import SheetsModelManager
        from data_contracts import FleetPoint
        from datetime import datetime
        from zoneinfo import ZoneInfo
        
        # Mock worksheet
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [
            ['VIN', 'A', 'B', 'C', 'D', 'Last Known Location', 'Latitude', 'Longitude', 'Status', 'Update Time', 'Source'],
            ['TEST123', '', '', '', '', 'Old Location', '0', '0', 'Old Status', 'Old Time', 'Old Source'],
        ]
        mock_worksheet.batch_update = MagicMock()
        
        mock_google_integration.spreadsheet.worksheet.return_value = mock_worksheet
        
        sheets_model = SheetsModelManager(mock_google_integration, mock_config)
        
        # Test fleet points
        fleet_points = [
            FleetPoint(
                vin="TEST123",
                driver_name="John Doe",
                location_str="I-80, Tiffin, IA",
                lat=41.5128,
                lon=-91.4315,
                status="Moving (65 mph)",
                updated_at_utc=datetime(2023, 7, 15, 16, 30, 0, tzinfo=ZoneInfo('UTC')),
                source="TMS Auto-Update"
            )
        ]
        
        # Test batch update
        updated_count = sheets_model.batch_update_eld_tracker(fleet_points)
        
        assert updated_count == 1
        mock_worksheet.batch_update.assert_called_once()
        
        # Verify F:K update structure
        call_args = mock_worksheet.batch_update.call_args[0][0]
        assert len(call_args) == 1
        update = call_args[0]
        assert update['range'] == 'F2:K2'  # Row 2, F:K columns
        assert update['values'][0][0] == 'I-80, Tiffin, IA'  # F: Location
        assert update['values'][0][1] == 41.5128  # G: Latitude
        assert update['values'][0][2] == -91.4315  # H: Longitude
        assert update['values'][0][3] == 'Moving (65 mph)'  # I: Status
        assert 'EDT' in update['values'][0][4]  # J: NY timezone
        assert update['values'][0][5] == 'TMS Auto-Update'  # K: Source
    
    def test_location_logs_deduplication(self, mock_config, mock_google_integration):
        """Test location logs append with deduplication"""
        from sheets_model import SheetsModelManager
        
        # Mock worksheet
        mock_worksheet = MagicMock()
        mock_worksheet.append_row = MagicMock()
        mock_google_integration.spreadsheet.worksheet.return_value = mock_worksheet
        
        sheets_model = SheetsModelManager(mock_google_integration, mock_config)
        
        # Test events
        events = [
            {
                'ts_utc': '2023-07-15T16:30:00Z',
                'ts_ny': '2023-07-15 12:30:00 EDT',
                'event_type': 'hourly_update',
                'VIN': 'TEST123',
                'Driver': 'John Doe',
                'ts_utc_timestamp': 1689436200  # Fixed timestamp for dedup key
            },
            {
                'ts_utc': '2023-07-15T16:30:00Z',
                'ts_ny': '2023-07-15 12:30:00 EDT', 
                'event_type': 'hourly_update',
                'VIN': 'TEST123',
                'Driver': 'John Doe',
                'ts_utc_timestamp': 1689436200  # Same timestamp - should be deduplicated
            }
        ]
        
        # First append should work
        count1 = sheets_model.append_location_logs([events[0]])
        assert count1 == 1
        
        # Second append with same dedup key should be skipped
        count2 = sheets_model.append_location_logs([events[1]])
        assert count2 == 0  # Deduplicated
        
        # Verify only one append_row call
        assert mock_worksheet.append_row.call_count == 1
    
    def test_fleet_status_upsert_by_vin(self, mock_config, mock_google_integration):
        """Test fleet_status upsert by VIN key"""
        from sheets_model import SheetsModelManager
        
        # Mock worksheet with existing data
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [
            ['VIN', 'Driver', 'On Load', 'Load ID', 'Speed mph', 'Last Refresh'],
            ['TEST123', 'John Doe', 'Y', 'LOAD001', '65', '2023-07-15 11:00:00 EDT'],
        ]
        mock_worksheet.batch_update = MagicMock()
        mock_worksheet.append_row = MagicMock()
        mock_google_integration.spreadsheet.worksheet.return_value = mock_worksheet
        
        sheets_model = SheetsModelManager(mock_google_integration, mock_config)
        
        # Test upsert data
        rows = [
            {
                'vin': 'TEST123',  # Existing VIN - should update
                'driver': 'John Doe Updated',
                'on_load': True,
                'speed_mph': 70
            },
            {
                'vin': 'TEST456',  # New VIN - should append
                'driver': 'Jane Doe',
                'on_load': False,
                'speed_mph': 0
            }
        ]
        
        updated_count = sheets_model.upsert_fleet_status(rows)
        
        assert updated_count == 2
        # Should have one update and one append
        mock_worksheet.batch_update.assert_called_once()
        mock_worksheet.append_row.assert_called_once()
    
    def test_retention_pruning(self, mock_config, mock_google_integration):
        """Test location logs retention pruning"""
        from sheets_model import SheetsModelManager
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        
        # Mock worksheet with old and new data
        old_time = (datetime.now(ZoneInfo('UTC')) - timedelta(days=90)).isoformat()
        new_time = (datetime.now(ZoneInfo('UTC')) - timedelta(days=30)).isoformat()
        
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [
            ['ts_utc', 'event_type', 'VIN', 'Driver'],
            [old_time, 'hourly_update', 'TEST123', 'John Doe'],    # Should be pruned
            [new_time, 'hourly_update', 'TEST456', 'Jane Doe'],    # Should be kept
            [old_time, 'risk_alert', 'TEST789', 'Bob Smith'],      # Should be pruned
        ]
        mock_worksheet.delete_rows = MagicMock()
        mock_google_integration.spreadsheet.worksheet.return_value = mock_worksheet
        
        sheets_model = SheetsModelManager(mock_google_integration, mock_config)
        
        # Test pruning (older than 60 days)
        pruned_count = sheets_model.prune_location_logs_older_than(60)
        
        assert pruned_count == 2  # Two old rows should be pruned
        assert mock_worksheet.delete_rows.call_count == 2
    
    def test_header_drift_tolerance(self, mock_config, mock_google_integration):
        """Test header drift tolerance in worksheets"""
        from sheets_model import SheetsModelManager
        
        # Mock worksheet with slightly different headers
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [
            ['  VIN  ', 'DRIVER NAME', 'lat', 'lng', 'STATUS', 'update_time', 'data_source'],  # Variations
            ['TEST123', 'John Doe', '41.5', '-91.4', 'Moving', '2023-07-15', 'TMS'],
        ]
        mock_worksheet.batch_update = MagicMock()
        mock_google_integration.spreadsheet.worksheet.return_value = mock_worksheet
        
        sheets_model = SheetsModelManager(mock_google_integration, mock_config)
        
        # Test that header normalization works
        header_map = sheets_model._normalize_headers([
            '  VIN  ', 'Driver Name', 'Latitude', 'Longitude'
        ])
        
        # Check normalized keys
        assert 'vin' in header_map
        assert 'driver_name' in header_map
        assert 'latitude' in header_map
        assert 'longitude' in header_map
        
        # Test flexible header matching
        vin_col = sheets_model._find_header_column(header_map, 'VIN')
        driver_col = sheets_model._find_header_column(header_map, 'Driver Name')
        
        assert vin_col is not None
        assert driver_col is not None


class TestConfigValidation:
    """Test configuration validation and environment variable parsing"""
    
    def test_config_required_variables(self):
        """Test that missing required variables raise errors"""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError, match="TELEGRAM_BOT_TOKEN"):
                Config()
    
    def test_config_optional_variables(self):
        """Test optional variables with defaults"""
        required_env = {
            'TELEGRAM_BOT_TOKEN': 'test_token',
            'SHEETS_SERVICE_ACCOUNT_FILE': 'test_file.json',
            'SPREADSHEET_ID': 'test_sheet_id',
            'TMS_API_URL': 'https://test.com',
            'TMS_API_KEY': 'test_key',
            'TMS_API_HASH': 'test_hash',
            'ORS_API_KEY': 'test_ors_key'
        }
        
        with patch.dict('os.environ', required_env, clear=True):
            config = Config()
            
            # Test defaults
            assert config.GROUP_LOCATION_INTERVAL == 3600
            assert config.LIVE_TRACKING_INTERVAL == 300
            assert config.MAX_CONCURRENT_TELEGRAM_SENDS == 12
            assert config.ENABLE_RISK_MONITORING is True
    
    def test_config_feature_flags(self):
        """Test feature flag parsing"""
        env_vars = {
            'TELEGRAM_BOT_TOKEN': 'test_token',
            'SHEETS_SERVICE_ACCOUNT_FILE': 'test_file.json',
            'SPREADSHEET_ID': 'test_sheet_id',
            'TMS_API_URL': 'https://test.com',
            'TMS_API_KEY': 'test_key',
            'TMS_API_HASH': 'test_hash',
            'ORS_API_KEY': 'test_ors_key',
            'ENABLE_RISK_MONITORING': 'false',
            'ENABLE_LIVE_TRACKING': 'true'
        }
        
        with patch.dict('os.environ', env_vars, clear=True):
            config = Config()
            
            assert config.ENABLE_RISK_MONITORING is False
            assert config.ENABLE_LIVE_TRACKING is True


class TestLocationRenderer:
    """Test location renderer with coordinate detection and timezone handling"""
    
    def test_is_latlon_like_detection(self):
        """Test coordinate detection rejects lat/lon strings"""
        from location_renderer import is_latlon_like
        
        # Should detect as coordinates (TRUE)
        assert is_latlon_like("40.72734708, -111.94709302") == True
        assert is_latlon_like("40.7273,-111.9471") == True
        assert is_latlon_like("40.72734708") == True
        assert is_latlon_like("-111.94709302") == True
        assert is_latlon_like("(40.7273, -111.9471)") == True
        assert is_latlon_like("[40.7273,-111.9471]") == True
        assert is_latlon_like("40.7273, -111.9471, 40.72734708, -111.94709302") == True
        
        # Should NOT detect as coordinates (FALSE)
        assert is_latlon_like("I-80, Tiffin, IA 52340") == False
        assert is_latlon_like("290 NW Ironwood Rd, Troutdale, OR 97060") == False
        assert is_latlon_like("Near Troutdale, OR") == False
        assert is_latlon_like("Highway 101, San Francisco") == False
        assert is_latlon_like("") == False
        assert is_latlon_like(None) == False
        assert is_latlon_like("123 Main St") == False  # Short number
        
        print("   ✅ Coordinate detection working correctly")
    
    def test_timezone_rendering_summer_winter(self):
        """Test EDT/EST timezone rendering with DST transitions"""
        from location_renderer import render_location_update
        from datetime import datetime
        from zoneinfo import ZoneInfo
        
        # Summer timestamp (should be EDT)
        summer_utc = datetime(2025, 8, 9, 3, 18, 11, tzinfo=ZoneInfo('UTC'))
        summer_message = render_location_update(
            driver="Rafael Suarez / Gretzin Sanchez",
            status="Idle",
            lat=40.72734708,
            lon=-111.94709302,
            speed_mph=0,
            updated_at_utc=summer_utc,
            location_str="I-80, Tiffin, IA 52340",
            map_source="TMS Auto-Update"
        )
        
        assert "2025-08-08 23:18:11 EDT" in summer_message
        print("   ✅ Summer time renders EDT correctly")
        
        # Winter timestamp (should be EST)  
        winter_utc = datetime(2025, 12, 15, 18, 5, 0, tzinfo=ZoneInfo('UTC'))
        winter_message = render_location_update(
            driver="Test Driver",
            status="Moving",
            lat=40.72734708,
            lon=-111.94709302,
            speed_mph=65,
            updated_at_utc=winter_utc,
            location_str="I-80, Tiffin, IA 52340",
            map_source="TMS Auto-Update"
        )
        
        assert "2025-12-15 13:05:00 EST" in winter_message
        print("   ✅ Winter time renders EST correctly")
    
    def test_coordinate_formatting_precision(self):
        """Test coordinates are formatted to exactly 5 decimal places"""
        from location_renderer import render_location_update
        from datetime import datetime
        from zoneinfo import ZoneInfo
        
        message = render_location_update(
            driver="Test Driver",
            status="Moving",
            lat=40.72734708123456,  # Extra precision
            lon=-111.94709302987654,  # Extra precision
            speed_mph=65,
            updated_at_utc=datetime(2025, 8, 9, 3, 18, 11, tzinfo=ZoneInfo('UTC')),
            location_str="I-80, Tiffin, IA 52340",
            map_source="TMS Auto-Update"
        )
        
        # Should show exactly 5 decimal places
        assert "🗺️ <b>Coordinates:</b> 40.72735, -111.94709" in message
        assert "https://maps.google.com/?q=40.72735,-111.94709" in message
        
        # Should NOT have duplicate coordinates
        assert message.count("40.72735") == 2  # Once in coordinates line, once in map URL
        assert message.count("-111.94709") == 2  # Once in coordinates line, once in map URL
        
        print("   ✅ Coordinates formatted to 5 decimals exactly")
    
    def test_junk_location_str_handling(self):
        """Test that coordinate-like location_str triggers fallback"""
        from location_renderer import render_location_update
        from datetime import datetime
        from zoneinfo import ZoneInfo
        
        # When location_str is coordinates, should use fallback
        message = render_location_update(
            driver="Test Driver", 
            status="Moving",
            lat=40.72734708,
            lon=-111.94709302,
            speed_mph=0,
            updated_at_utc=datetime(2025, 8, 9, 3, 18, 11, tzinfo=ZoneInfo('UTC')),
            location_str="40.72734708, -111.94709302",  # Junk coordinates
            map_source="TMS Auto-Update"
        )
        
        # Should NOT show the coordinate string in location line
        assert "📍 <b>Location:</b> 40.72734708, -111.94709302" not in message
        
        # Should show fallback location
        assert "📍 <b>Location:</b> Near" in message or "📍 <b>Location:</b> Remote" in message
        
        # Coordinates should still be shown separately
        assert "🗺️ <b>Coordinates:</b> 40.72735, -111.94709" in message
        
        print("   ✅ Junk location_str triggers fallback correctly")
    
    def test_message_format_compliance(self):
        """Test message follows exact format specification"""
        from location_renderer import render_location_update
        from datetime import datetime
        from zoneinfo import ZoneInfo
        
        message = render_location_update(
            driver="Rafael Suarez / Gretzin Sanchez",
            status="Idle",
            lat=40.72734708,
            lon=-111.94709302,
            speed_mph=0,
            updated_at_utc=datetime(2025, 8, 9, 3, 18, 11, tzinfo=ZoneInfo('UTC')),
            location_str="I-80, Tiffin, IA 52340",
            map_source="TMS Auto-Update"
        )
        
        lines = message.split('\n')
        
        # Check exact format
        assert lines[0] == "🚛 <b>Location Update</b>"
        assert lines[1] == ""  # Empty line
        assert "👤 <b>Driver:</b>" in lines[2]
        assert "🛑 <b>Status:</b>" in lines[3]
        assert "📍 <b>Location:</b>" in lines[4]
        assert "🏃 <b>Speed:</b>" in lines[5]
        assert "📡 <b>Updated:</b>" in lines[6]
        assert lines[7] == ""  # Empty line
        assert "🗺️ <b>Coordinates:</b>" in lines[8]
        assert "🔗 <b>Map:</b>" in lines[9]
        
        # Verify no coordinates in location line
        location_line = [line for line in lines if "📍 <b>Location:</b>" in line][0]
        assert "40.727" not in location_line
        assert "-111.947" not in location_line
        
        print("   ✅ Message format follows specification exactly")
    
    def test_html_escaping(self):
        """Test HTML escaping prevents XSS"""
        from location_renderer import render_location_update
        from datetime import datetime
        from zoneinfo import ZoneInfo
        
        message = render_location_update(
            driver="<script>alert('xss')</script>",
            status="Moving & Dangerous",
            lat=40.72734708,
            lon=-111.94709302,
            speed_mph=65,
            updated_at_utc=datetime(2025, 8, 9, 3, 18, 11, tzinfo=ZoneInfo('UTC')),
            location_str="I-80 & Highway 101",
            map_source="TMS Auto-Update"
        )
        
        # Check HTML escaping
        assert "&lt;script&gt;" in message
        assert "&amp;" in message
        assert "<script>" not in message
        
        print("   ✅ HTML escaping prevents XSS")
    
    def test_speed_conversion(self):
        """Test speed conversion to integer mph"""
        from location_renderer import render_location_update
        from datetime import datetime
        from zoneinfo import ZoneInfo
        
        test_cases = [
            (65.7, "65 mph"),    # Round down
            (65.9, "66 mph"),    # Round up
            (0.0, "0 mph"),      # Zero
            (0.4, "0 mph"),      # Very low speed
        ]
        
        for input_speed, expected in test_cases:
            message = render_location_update(
                driver="Test Driver",
                status="Moving",
                lat=40.0,
                lon=-111.0,
                speed_mph=input_speed,
                updated_at_utc=datetime(2025, 8, 9, 12, 0, 0, tzinfo=ZoneInfo('UTC')),
                location_str="Test Location",
                map_source="TMS Auto-Update"
            )
            
            assert f"🏃 <b>Speed:</b> {expected}" in message
        
        print("   ✅ Speed conversion to integer mph works correctly")


if __name__ == "__main__":
    # Simple test runner for basic validation
    print("🔍 Running Asset Tracking Bot Tests...")
    
    # Test FleetPoint
    print("1. Testing FleetPoint data contract...")
    test_fp = TestFleetPoint()
    test_fp.test_fleet_point_vin_normalization()
    test_fp.test_fleet_point_timezone_conversion()
    test_fp.test_fleet_point_speed_extraction()
    print("   ✅ FleetPoint tests passed")
    
    # Test Location Renderer
    print("2. Testing Location Renderer...")
    test_renderer = TestLocationRenderer()
    test_renderer.test_is_latlon_like_detection()
    test_renderer.test_timezone_rendering_summer_winter()
    test_renderer.test_coordinate_formatting_precision()
    test_renderer.test_junk_location_str_handling()
    test_renderer.test_message_format_compliance()
    test_renderer.test_html_escaping()
    test_renderer.test_speed_conversion()
    print("   ✅ Location Renderer tests passed")
    
    # Test Sheets Model
    print("3. Testing Sheets Model...")
    from sheets_model import SHEET_SCHEMAS
    assert len(SHEET_SCHEMAS) == 8
    print("   ✅ All 8 worksheets defined")
    
    # Test required schemas
    required_schemas = ['assets', 'groups', 'ELD_tracker', 'fleet_status']
    for schema_name in required_schemas:
        assert schema_name in SHEET_SCHEMAS
        schema = SHEET_SCHEMAS[schema_name]
        assert len(schema.headers) > 0
        assert len(schema.required_columns) > 0
    print("   ✅ Core worksheet schemas validated")
    
    print("🎉 All basic tests passed!")
    print("📝 For full test suite, run: pytest test_bot.py -v")