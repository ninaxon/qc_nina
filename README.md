# Asset Tracking Telegram Bot

A comprehensive Telegram bot for tracking assets, managing deliveries, and calculating routes using Google Sheets integration, TMS API, and OpenRouteService.

## Features

- 🚛 **Asset Tracking**: Track vehicles and drivers using VIN and driver mappings
- 📍 **Location Services**: Real-time location tracking with geocoding
- 🗺️ **Route Calculation**: Calculate routes and ETAs using OpenRouteService
- 📊 **Google Sheets Integration**: Store and manage data in Google Sheets
- 🤖 **Telegram Interface**: Easy-to-use Telegram bot commands
- 🔄 **Service Management**: Daemon support with start/stop/status controls
- 📝 **Comprehensive Logging**: Detailed logging with multiple log levels

## Requirements

- Python 3.8+
- Google Service Account with Sheets API access
- Telegram Bot Token
- OpenRouteService API Key
- TMS API access

## Quick Setup

1. **Clone and setup**:
   ```bash
   git clone <repository>
   cd asset-tracking-bot
   python setup.py
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Add Google credentials**:
   - Download Google service account JSON file
   - Place in `credentials/` directory

4. **Test configuration**:
   ```bash
   python main.py --test
   ```

5. **Start the bot**:
   ```bash
   python main.py
   ```

## Configuration

Create a `.env` file with the following configuration:

```bash
# Google Sheets
SHEETS_SERVICE_ACCOUNT_FILE=credentials/your-service-account.json
SPREADSHEET_ID=your_spreadsheet_id
SPREADSHEET_ASSETS=assets

# Telegram
TELEGRAM_TOKEN=your_bot_token

# APIs
ORS_API_KEY=your_openrouteservice_key
TMS_API_URL=your_tms_api_url
TMS_API_KEY=your_tms_key
TMS_API_HASH=your_tms_hash

# Application Settings
VERBOSE_OUTPUT=true
LOG_LEVEL=INFO
```

## Usage

### Telegram Commands

- `/start` - Welcome message and overview
- `/help` - Show available commands
- `/update <driver name>` - Start delivery update process
- `/status` - Show system status and statistics
- `/refresh` - Refresh data from TMS and Google Sheets

### Service Management

```bash
# Start as daemon
python service_manager.py start

# Stop daemon
python service_manager.py stop

# Check status
python service_manager.py status

# View logs
python service_manager.py logs

# Follow logs in real-time
python service_manager.py logs --follow

# Restart service
python service_manager.py restart

# Force stop
python service_manager.py stop --force

# Cleanup service files
python service_manager.py cleanup
```

### Development Mode

```bash
# Run in foreground
python main.py

# Run tests
python main.py --test

# Start daemon in foreground
python service_manager.py start --foreground
```

## Google Sheets Structure

The bot expects a Google Sheet with the following columns:

| Column | Field | Description |
|--------|-------|-------------|
| A | Timestamp | Last update timestamp |
| B | Name Gateway | Asset/gateway name |
| C | Serial Current | Current serial number |
| D | Driver Name | Driver name |
| E | VIN | Vehicle identification number |
| F | Last Known Location | Address of last known location |
| G | Latitude | GPS latitude |
| H | Longitude | GPS longitude |
| I | Status | Asset status |
| J | Update Time | Last update from TMS |
| K | Source | Data source (e.g., "samsara") |

## Project Structure

```
asset-tracking-bot/
├── main.py                 # Main application entry point
├── config.py              # Configuration management
├── telegram_integration.py # Telegram bot implementation
├── tms_integration.py      # TMS API integration
├── google_integration.py   # Google Sheets integration
├── service_manager.py      # Service lifecycle management
├── setup.py               # Setup and installation script
├── requirements.txt       # Python dependencies
├── .env.example          # Environment configuration template
├── logs/                 # Log files directory
├── credentials/          # Google service account files
└── README.md            # This file
```

## Workflow

1. **Driver Lookup**: User sends `/update driver_name`
2. **Asset Matching**: Bot finds driver in Google Sheets and matches to VIN
3. **Location Retrieval**: Current location fetched from TMS API
4. **Address Input**: User provides delivery address
5. **Time Input**: User provides appointment time
6. **Route Calculation**: Bot calculates route using OpenRouteService
7. **ETA Analysis**: Compares ETA to appointment time
8. **Results Display**: Shows comprehensive delivery information

## Logging

The bot provides comprehensive logging:

- **Console Output**: Real-time status and errors
- **Main Log** (`logs/bot.log`): All application events
- **Error Log** (`logs/error.log`): Errors and exceptions only

Log levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`

## Error Handling

- **Graceful Degradation**: Bot continues working even if some services fail
- **Retry Logic**: Automatic retries for API calls
- **User Feedback**: Clear error messages for users
- **Comprehensive Logging**: All errors logged for debugging

## API Integration

### TMS API
- Fetches real-time vehicle locations
- Filters by source (Samsara)
- Validates data quality

### OpenRouteService
- Geocoding addresses
- Route calculation
- ETA estimation

### Google Sheets API
- Read/write asset data
- Driver-VIN mapping
- Location updates

## Security Considerations

- **Environment Variables**: All sensitive data in `.env` file
- **Service Account**: Google API access via service account
- **PID Files**: Process management with PID tracking
- **Signal Handling**: Graceful shutdown on system signals

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## Troubleshooting

### Common Issues

1. **Google Sheets Access Denied**:
   - Check service account permissions
   - Verify spreadsheet sharing settings

2. **TMS API Connection Failed**:
   - Verify API credentials in `.env`
   - Check network connectivity

3. **Bot Not Responding**:
   - Check Telegram token validity
   - Verify bot is running: `python service_manager.py status`

4. **Geocoding Failures**:
   - Check OpenRouteService API key
   - Verify API quota limits

### Getting Help

- Check logs: `python service_manager.py logs`
- Run tests: `python main.py --test`
- Check status: `python service_manager.py status`
- Review configuration: verify `.env` file

