# Railway Deployment Guide - Enhanced Asset Tracking Bot

## 🚀 Quick Deploy to Railway

### 1. Prepare Your Repository

First, make sure all files are in your repository:

```bash
# Add all new files to git
git add .
git commit -m "Add Railway deployment configuration"
git push origin main
```

Required files for deployment:
- `Dockerfile` - Container configuration
- `requirements.txt` - Python dependencies
- `railway.json` - Railway-specific config
- `Procfile` - Process definition
- `health_check.py` - Health monitoring
- Your updated `main.py`
- All other bot files

### 2. Deploy to Railway

#### Option A: GitHub Integration (Recommended)
1. Go to [Railway.app](https://railway.app)
2. Click "Deploy from GitHub repo"
3. Select your repository
4. Railway will auto-detect the configuration

#### Option B: Railway CLI
```bash
# Install Railway CLI
npm install -g @railway/cli

# Login to Railway
railway login

# Initialize project
railway init

# Deploy
railway up
```

### 3. Configure Environment Variables

In Railway dashboard, add these environment variables:

#### **Required Variables:**
```bash
# Telegram Bot
TELEGRAM_BOT_TOKEN=your_bot_token_here
OWNER_TELEGRAM_ID=your_telegram_user_id

# Google Sheets
SPREADSHEET_ID=your_google_sheet_id
SHEETS_SERVICE_ACCOUNT_FILE=service_account.json
SPREADSHEET_ASSETS=assets
SPREADSHEET_GROUPS=groups

# TMS API
TMS_API_URL=your_tms_api_url
TMS_API_KEY=your_tms_api_key
TMS_API_HASH=your_tms_api_hash

# OpenRouteService
ORS_API_KEY=your_openrouteservice_key
```

#### **Optional Variables (with defaults):**
```bash
# Dual-Mode Scheduling
GROUP_LOCATION_INTERVAL=3600          # 1 hour group messages
LIVE_TRACKING_INTERVAL=300            # 5 minute silent refresh
AUTO_START_LOCATION_UPDATES=true

# Session Management
MAX_LIVE_SESSIONS=100
MAX_GROUP_SESSIONS=50
SESSION_TIMEOUT_HOURS=24

# Logging
LOG_LEVEL=INFO
ENABLE_DEBUG_LOGGING=false
LOG_FILE_MAX_MB=10
LOG_BACKUP_COUNT=5

# Features
ENABLE_LIVE_TRACKING=true
ENABLE_GROUP_AUTO_UPDATES=true
ENABLE_PM_TRACKING=true

# Admin
ADMIN_USERNAME=your_telegram_username
```

### 4. Upload Service Account File

For Google Sheets integration, you'll need to upload your service account JSON file:

#### Method 1: Base64 Environment Variable
```bash
# Encode your service account file
base64 service_account.json > service_account_b64.txt

# Add to Railway as environment variable
GOOGLE_SERVICE_ACCOUNT_B64=paste_base64_content_here
```

Then update your `config.py` to decode it:
```python
import base64
import json
import tempfile

# In Config.__init__:
if os.getenv('GOOGLE_SERVICE_ACCOUNT_B64'):
    # Decode base64 service account
    service_account_b64 = os.getenv('GOOGLE_SERVICE_ACCOUNT_B64')
    service_account_json = base64.b64decode(service_account_b64).decode('utf-8')
    
    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(service_account_json)
        self.SHEETS_SERVICE_ACCOUNT_FILE = f.name
```

#### Method 2: Railway Volumes (if available)
Upload the file directly using Railway's file upload feature.

### 5. Monitor Deployment

#### Health Check Endpoints:
- `https://your-app.railway.app/health` - Basic health check
- `https://your-app.railway.app/status` - Detailed status

#### Logs:
```bash
# View logs via CLI
railway logs

# Or check Railway dashboard
```

### 6. Post-Deployment Setup

1. **Test the bot:**
   ```bash
   # Send /start to your bot
   # Check health endpoint
   curl https://your-app.railway.app/health
   ```

2. **Register your first group:**
   - Add bot to a Telegram group
   - Send `/start`
   - Use "🛠 Set VIN" button
   - Verify hourly updates start

3. **Monitor logs:**
   - Check Railway dashboard logs
   - Look for "Dual-mode scheduling system active"

## 🔧 Troubleshooting

### Common Issues:

1. **Service Account Error:**
   ```
   Error: Service account file not found
   ```
   - Make sure `GOOGLE_SERVICE_ACCOUNT_B64` is set
   - Verify base64 encoding is correct

2. **Port Binding Error:**
   ```
   Error: Port already in use
   ```
   - Railway automatically assigns ports
   - Remove any hardcoded port numbers

3. **Memory Issues:**
   ```
   Error: Out of memory
   ```
   - Reduce `MAX_LIVE_SESSIONS`
   - Set `LOG_LEVEL=WARNING`

4. **Import Errors:**
   ```
   ModuleNotFoundError: No module named 'xyz'
   ```
   - Check `requirements.txt`
   - Verify all dependencies are listed

### Debug Commands:

```bash
# Check Railway service status
railway status

# View environment variables
railway variables

# Check logs
railway logs --tail

# Connect to container
railway shell
```

## 🎯 Verification Checklist

- [ ] All files committed and pushed to GitHub
- [ ] Railway project created and connected
- [ ] All environment variables configured
- [ ] Service account file uploaded/encoded
- [ ] Health check responds at `/health`
- [ ] Bot responds to `/start` command
- [ ] Group VIN registration works
- [ ] Hourly location updates functioning
- [ ] ETA calculation with silent refresh works
- [ ] Logs show "Dual-mode scheduling system active"

## 📊 Monitoring

Railway provides built-in monitoring:
- CPU usage
- Memory usage
- Request metrics
- Health check status

Additional monitoring endpoints:
- `/health` - Service health
- `/status` - Detailed bot status

## 🔄 Updates

To update your deployed bot:

```bash
# Make changes locally
git add .
git commit -m "Update bot features"
git push origin main

# Railway will auto-deploy
# Or trigger manual deploy in dashboard
```

Your enhanced bot with dual-mode scheduling is now ready for Railway! 🚀