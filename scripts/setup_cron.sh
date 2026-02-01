#!/bin/bash

################################################################################
# Setup Cron Job for IMDB Ingestion
#
# This script configures a cron job to run the ingestion pipeline daily.
#
# Usage:
#   ./setup_cron.sh
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "========================================="
echo "IMDB Ingestion - Cron Setup"
echo "========================================="

# Check if run_ingestion.sh exists
if [ ! -f "$SCRIPT_DIR/run_ingestion.sh" ]; then
    echo "ERROR: run_ingestion.sh not found"
    exit 1
fi

# Make sure it's executable
chmod +x "$SCRIPT_DIR/run_ingestion.sh"

echo ""
echo "Project root: $PROJECT_ROOT"
echo "Script: $SCRIPT_DIR/run_ingestion.sh"
echo ""

# Create cron job entry
CRON_JOB="0 2 * * * cd $PROJECT_ROOT && $SCRIPT_DIR/run_ingestion.sh >> $PROJECT_ROOT/logs/cron.log 2>&1"

echo "Proposed cron job:"
echo "  Schedule: Daily at 2:00 AM"
echo "  Command: $CRON_JOB"
echo ""

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "run_ingestion.sh"; then
    echo "WARNING: Cron job already exists!"
    echo ""
    echo "Current crontab:"
    crontab -l | grep "run_ingestion.sh"
    echo ""
    read -p "Remove existing and add new? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
    
    # Remove old entry
    crontab -l | grep -v "run_ingestion.sh" | crontab -
    echo "Removed old cron job"
fi

# Add new cron job
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo ""
echo " Cron job installed successfully!"
echo ""
echo "Current crontab:"
crontab -l
echo ""
echo "========================================="
echo "Configuration:"
echo "  - Runs daily at 2:00 AM"
echo "  - Logs to: $PROJECT_ROOT/logs/cron.log"
echo "  - Detailed logs: $PROJECT_ROOT/logs/ingestion_*.log"
echo ""
echo "Useful commands:"
echo "  - View cron log:    tail -f $PROJECT_ROOT/logs/cron.log"
echo "  - List cron jobs:   crontab -l"
echo "  - Edit cron jobs:   crontab -e"
echo "  - Remove cron job:  crontab -l | grep -v run_ingestion.sh | crontab -"
echo "  - Test manually:    $SCRIPT_DIR/run_ingestion.sh"
echo "========================================="
