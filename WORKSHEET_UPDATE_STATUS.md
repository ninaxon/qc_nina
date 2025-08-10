# Worksheet Update Status Report

## Overview
This report documents the current status and update mechanisms for all active Google Sheets worksheets in the QC Assets Tracker Service.

## Active Worksheets and Update Schedules

### 1. **Assets Worksheet** ✅ ACTIVE
- **Purpose**: Main driver and truck data storage
- **Update Method**: QC Panel sync + Manual updates
- **Frequency**: Every 5 minutes (300 seconds) via risk monitoring callback
- **Primary Update Source**: `sync_active_loads_to_assets()` in google_integration.py
- **Status**: **HEALTHY** - Consistently showing 400-500+ sync updates every 5 minutes
- **Last Verified**: QC Panel sync logs show regular updates
- **Key Fields Updated**: 
  - Driver names, Load IDs, DEL/PU addresses and appointments
  - Sync from QC Panel spreadsheet to assets sheet
- **Manual Commands**: `/updateassets` command available

### 2. **Fleet Status Worksheet** ✅ ACTIVE  
- **Purpose**: Real-time truck location and status tracking
- **Update Method**: TMS integration via risk monitoring
- **Frequency**: Every 5 minutes (300 seconds) 
- **Primary Update Source**: `update_asset_tracking_sheet_fixed()` in google_integration.py
- **Status**: **HEALTHY** - Updates existing records only (no new rows due to 10M cell limit)
- **Key Features**:
  - Tracks VIN, driver, lat/lng, address, speed, status
  - Uses batch updates for efficiency
  - Fixed duplicate prevention system
- **Manual Commands**: Cleanup via `cleanup_fleet_status_duplicates()` if needed

### 3. **Groups Worksheet** ✅ ACTIVE
- **Purpose**: Group chat registrations and VIN tracking
- **Update Method**: Real-time on group interactions  
- **Frequency**: On-demand (when groups register/update VINs)
- **Primary Update Source**: `save_group_vin()` in google_integration.py
- **Status**: **HEALTHY** - Updates when groups set VINs or register
- **Key Features**:
  - Tracks group_id, group_title, vin, driver_name
  - Updates last_updated timestamps
  - Safe header duplication handling implemented
- **Issues**: Header duplication warnings (handled gracefully)

### 4. **Dashboard Logs Worksheet** ✅ ACTIVE
- **Purpose**: User interaction analytics and command logging
- **Update Method**: Real-time logging on all user interactions
- **Frequency**: Every user interaction (~1-60 seconds depending on usage)
- **Primary Update Source**: `log_dashboard_event()` in google_integration.py
- **Status**: **HEALTHY** - Logs all commands, interactions, button clicks
- **Key Features**:
  - Comprehensive user analytics
  - Command execution tracking
  - Error logging and success rates
  - Session data tracking

## Update Mechanisms Summary

### Automated Updates (Background Jobs)
1. **Risk Monitoring Job** (Every 5 minutes):
   - Updates QC Panel → Assets sync
   - Updates Fleet Status from TMS data
   - Runs cargo theft detection
   - Runs ETA alerting

2. **Group Location Updates** (Every 1 hour with jitter):
   - Sends location messages to registered groups
   - Updates group interaction logs

### Real-time Updates
1. **User Interactions**: Dashboard logs updated immediately
2. **Group Registration**: Groups worksheet updated when VINs are set
3. **Manual Commands**: Various worksheets updated on demand

### Manual Commands Available
- `/updateall` - Update all group locations immediately
- `/updateassets` - Manual asset updates from TMS
- `/workshealth` - Check worksheet health status (NEW)
- `/listnewtrucks` - List new trucks not in assets
- `/addtruck` - Manually add truck to assets

## Health Monitoring

### New Worksheet Monitor System ✅ IMPLEMENTED
- **Command**: `/workshealth` (owner only)
- **Features**:
  - Checks all worksheets for recent updates
  - Monitors expected update intervals
  - Identifies stale data and issues
  - Provides actionable recommendations
- **Alert Thresholds**:
  - Assets: 10 minutes (2x 5-minute interval)
  - Fleet Status: 10 minutes (2x 5-minute interval)  
  - Groups: 2 hours (2x 1-hour interval)
  - Dashboard Logs: 2 minutes (2x 1-minute interval)

## Current Status: ✅ ALL SYSTEMS HEALTHY

### Key Performance Metrics
- **QC Panel Sync**: 400-500 updates every 5 minutes
- **Fleet Status**: Tracking 50+ active trucks
- **Groups**: 10+ registered groups receiving updates
- **Dashboard Logs**: Comprehensive interaction tracking

### Recent Improvements
1. Fixed Google Sheets 10M cell limit issues
2. Implemented header duplication workarounds
3. Added batch update optimization
4. Created comprehensive worksheet health monitoring
5. Added manual asset addition capabilities

## Troubleshooting Guide

### If Worksheets Stop Updating
1. Check `/workshealth` command for specific issues
2. Verify Google Sheets API quotas and permissions
3. Check TMS integration connectivity
4. Review QC Panel spreadsheet access
5. Monitor bot logs for error patterns

### Common Issues and Solutions
- **Header Duplication**: Handled automatically with fallback methods
- **Cell Limit**: Fleet status only updates existing records (no new rows)
- **Rate Limiting**: Batch updates and delays implemented
- **Stale Data**: Health monitor will identify and alert

## Configuration
- **Risk Check Interval**: 300 seconds (5 minutes)
- **Group Location Interval**: 3600 seconds (1 hour)
- **QC Panel Sync**: Embedded in risk monitoring cycle
- **Health Check**: Available on-demand via `/workshealth`

---
**Last Updated**: August 7, 2025
**Report Generated**: Automated worksheet audit system
**Status**: All systems operational and healthy ✅