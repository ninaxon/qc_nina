# 🚀 Enhanced Asset Tracking Bot - Live Updates Setup Guide

## 🔴 Live Update Features

Your bot now includes powerful live update capabilities:

* **🕐 5-Minute Auto-Refresh** : Fresh location data every 5 minutes
* **📡 Live TMS Integration** : Real-time data from your TMS system
* **🛑 User Controls** : Start/stop live tracking per chat
* **📊 Owner Dashboard** : System diagnostics and controls
* **⚡ Performance Optimized** : Efficient job scheduling and caching

## 🚁 Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Telegram      │    │   Live Update    │    │   TMS/Samsara   │
│   Users/Groups  │◄──►│   Bot Engine     │◄──►│   API System    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │ Google Sheets    │
                       │ or SQLite DB     │
                       └──────────────────┘
```

## 📦 Installation & Setup

### 1. Clone and Install Dependencies

```bash
# Clone your repository
git clone <your-repo>
cd asset-tracking-bot

# Install enhanced dependencies
pip install -r requirements.txt

# Alternative: Create virtual environment first
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configuration Setup

Create your `.env` file with the live update settings:

```bash
# Copy example and edit
cp .env.example .env
nano .env  # or your preferred editor
```

**Key Live Update Settings:**

```env
# Live Update Configuration
LIVE_UPDATE_INTERVAL=300          # 5 minutes (300 seconds)
GROUP_UPDATE_INTERVAL=300         # Also set groups to 5 minutes
MAX_LIVE_SESSIONS=100             # Max concurrent live tracking sessions
ENABLE_LIVE_TRACKING=true         # Enable live updates
AUTO_REFRESH_ENABLED=true         # Auto-start refresh after ETA calc

# Performance Settings
MAX_CONCURRENT_JOBS=50            # Max background jobs
TMS_REQUEST_DELAY=1.0             # Rate limiting for TMS API
ORS_REQUEST_DELAY=0.5             # Rate limiting for ORS API

# Session Management
SESSION_TIMEOUT_HOURS=24          # Auto-cleanup old sessions
NOTIFICATION_COOLDOWN_MINUTES=15  # Error notification frequency
```

### 3. Test the Enhanced System

```bash
# Run enhanced integration tests
python main.py --test

# Expected output:
# ✅ Google Sheets Integration: PASS
# ✅ TMS Integration: PASS  
# ✅ Telegram Integration: PASS
# ✅ Live Update Configuration: PASS
# 🎉 All integration tests passed!
# 🚀 Bot is ready for live operation with 5-minute updates!
```

### 4. Start the Enhanced Bot

```bash
# Development mode (foreground)
python main.py

# Production mode (daemon)
python service_manager.py start --daemon

# Check status
python service_manager.py status

# View live logs
python service_manager.py logs --follow
```

## 🔘 User Interface & Commands

### Chat Mode Differences

| **Private Chat** | **Group Chat**      |
| ---------------------- | ------------------------- |
| Manual driver lookup   | VIN set once per group    |
| Temporary sessions     | Persistent group settings |
| Individual tracking    | Shared group updates      |

### Button Flow for Live Updates

```
🛰 Get an Update
    ↓
📍 Send Stop Location  
    ↓
⏰ Send Appointment (optional)
    ↓
↪️ Calculate ETA  ← 🔴 ACTIVATES LIVE UPDATES
    ↓
🔄 Live updates every 5 minutes
    │
    ├── Fresh TMS location data
    ├── Recalculated ETA
    ├── Updated status (On Time/Late)
    └── 🛑 Stop Auto-Updates (user control)
```

### Owner-Only Controls

If you set `OWNER_TELEGRAM_ID` in your config:

* **🔁 Reload** : Refresh all groups and restart job queue
* **📊 Status** : System diagnostics with live session counts
* **🚨 Error Notifications** : Automatic alerts for system issues

## 🔧 System Administration

### Service Management

```bash
# Start as daemon
python service_manager.py start

# Stop gracefully
python service_manager.py stop

# Force stop if needed
python service_manager.py stop --force

# Restart (stop + start)
python service_manager.py restart

# Check detailed status
python service_manager.py status

# Monitor logs in real-time
python service_manager.py logs --follow --lines 100
```

### Log Files Structure

```
logs/
├── bot.log              # Main application logs
├── error.log            # Error-only logs
├── live_updates.log     # Live update system logs
└── (rotated backups)    # .1, .2, .3 etc.
```

### Monitoring Live Updates

**Key metrics to watch:**

```bash
# Active live sessions
grep "auto-refresh" logs/live_updates.log | tail -20

# TMS API performance  
grep "TMS" logs/bot.log | grep -E "(success|failed)" | tail -10

# Job queue status
grep "job_queue" logs/bot.log | tail -10

# Memory usage
python -c "
import psutil
p = psutil.Process()
print(f'Memory: {p.memory_info().rss / 1024 / 1024:.1f} MB')
print(f'CPU: {p.cpu_percent()}%')
"
```

## 🚀 Production Deployment

### Option 1: Systemd Service (Linux)

Create `/etc/systemd/system/asset-tracking-bot.service`:

```ini
[Unit]
Description=Enhanced Asset Tracking Bot with Live Updates
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/asset-tracking-bot
Environment=PATH=/path/to/asset-tracking-bot/venv/bin
ExecStart=/path/to/asset-tracking-bot/venv/bin/python main.py
Restart=always
RestartSec=10

# Performance settings
LimitNOFILE=65536
MemoryLimit=1G

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start
sudo systemctl enable asset-tracking-bot
sudo systemctl start asset-tracking-bot

# Check status
sudo systemctl status asset-tracking-bot

# View logs
sudo journalctl -u asset-tracking-bot -f
```

### Option 2: Docker Deployment

Create `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create logs directory
RUN mkdir -p logs

# Run as non-root user
RUN useradd -m botuser && chown -R botuser:botuser /app
USER botuser

# Expose health check port (optional)
EXPOSE 8080

CMD ["python", "main.py"]
```

```bash
# Build and run
docker build -t asset-tracking-bot .
docker run -d --name tracking-bot \
  --env-file .env \
  -v $(pwd)/logs:/app/logs \
  --restart unless-stopped \
  asset-tracking-bot

# Check logs
docker logs -f tracking-bot
```

### Option 3: Docker Compose (Recommended)

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  bot:
    build: .
    container_name: asset-tracking-bot
    env_file: .env
    volumes:
      - ./logs:/app/logs
      - ./service_account.json:/app/service_account.json:ro
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "-c", "import requests; requests.get('http://localhost:8080/health')"]
      interval: 30s
      timeout: 10s
      retries: 3
  
  # Optional: Redis for session storage (production)
  redis:
    image: redis:7-alpine
    container_name: tracking-bot-redis
    volumes:
      - redis_data:/data
    restart: unless-stopped
  
volumes:
  redis_data:
```

```bash
# Start stack
docker-compose up -d

# Scale if needed
docker-compose up -d --scale bot=2

# Monitor
docker-compose logs -f bot
```

## 🔧 Troubleshooting

### Common Issues

**1. Live Updates Not Starting**

```bash
# Check configuration
grep -E "(LIVE_UPDATE|ENABLE_LIVE)" .env

# Check logs
grep "auto-refresh" logs/live_updates.log

# Verify TMS connectivity
python -c "
from tms_integration import TMSIntegration
from config import Config
tms = TMSIntegration(Config())
trucks = tms.load_truck_list()
print(f'TMS returned {len(trucks)} trucks')
"
```

**2. High Memory Usage**

```bash
# Check active sessions
python -c "
import json, glob
for f in glob.glob('*.pid'):
    print(f'Active PID file: {f}')
"

# Monitor with htop or similar
htop -p $(cat asset_tracking_bot.pid)
```

**3. API Rate Limiting**

```bash
# Increase delays in .env
TMS_REQUEST_DELAY=2.0
ORS_REQUEST_DELAY=1.0

# Restart bot
python service_manager.py restart
```

**4. Job Queue Overload**

```bash
# Check job queue status via owner commands
# Send 📊 Status button in Telegram to owner

# Or check logs
grep "job_queue" logs/bot.log | tail -20
```

### Performance Optimization

**For High-Volume Deployments:**

```env
# Reduce update frequency if needed
LIVE_UPDATE_INTERVAL=600  # 10 minutes instead of 5

# Limit concurrent sessions
MAX_LIVE_SESSIONS=50

# Enable caching
ENABLE_CACHING=true
CACHE_TTL_MINUTES=5
```

**For Multiple Bot Instances:**

1. Use Redis for shared session storage
2. Load balance with nginx
3. Use separate databases per instance
4. Monitor with Prometheus + Grafana

## 📊 Monitoring & Analytics

### Health Checks

The bot includes built-in health monitoring:

```python
# Add to your monitoring system
import requests
health = requests.get('http://bot-server:8080/health').json()
print(f"Active sessions: {health['active_sessions']}")
print(f"Jobs running: {health['jobs_running']}")
print(f"TMS status: {health['tms_status']}")
```

### Key Metrics to Track

* **Active live sessions** : Number of chats with auto-refresh enabled
* **TMS API success rate** : Percentage of successful location fetches
* **Average response time** : From button click to live update
* **Memory usage** : Should stay under 512MB for normal loads
* **Job queue length** : Should rarely exceed 10 pending jobs

## 🎯 Next Steps

1. **Deploy** using your preferred method above
2. **Test** live updates with a few pilot groups
3. **Monitor** performance and adjust intervals as needed
4. **Scale** by adding more bot instances if needed
5. **Enhance** with custom features for your specific use case

Your bot now provides **real-time asset tracking** with minimal user interaction - just set it up once and get fresh updates every 5 minutes! 🚀
