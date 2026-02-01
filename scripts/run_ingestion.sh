#!/bin/bash

################################################################################
# IMDB Data Ingestion Pipeline
# 
# This script automates the daily ingestion of IMDB data:
# 1. Downloads latest datasets from IMDB
# 2. Loads data into BigQuery (bronze layer)
# 3. Runs dbt transformations (silver layer)
# 4. Logs all operations
#
# Usage:
#   ./run_ingestion.sh [--force-download]
#
# Environment:
#   Requires .env file with:
#   - GOOGLE_APPLICATION_CREDENTIALS
#   - GCP_PROJECT_ID
#   - OPENAI_API_KEY (for app, not ingestion)
################################################################################

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_ROOT/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/ingestion_${TIMESTAMP}.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

################################################################################
# Functions
################################################################################

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

cleanup() {
    if [ $? -eq 0 ]; then
        success "Pipeline completed successfully"
    else
        error "Pipeline failed - check logs at $LOG_FILE"
    fi
}

trap cleanup EXIT

################################################################################
# Main Pipeline
################################################################################

log "========================================="
log "IMDB Data Ingestion Pipeline"
log "========================================="
log "Log file: $LOG_FILE"

# Change to project root
cd "$PROJECT_ROOT"
log "Working directory: $(pwd)"

# Check if .env exists
if [ ! -f .env ]; then
    error ".env file not found in $PROJECT_ROOT"
    exit 1
fi

# Load environment variables
set -a
source .env
set +a
log "Environment variables loaded"

# Check credentials
if [ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
    error "GOOGLE_APPLICATION_CREDENTIALS not set in .env"
    exit 1
fi

if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    error "Credentials file not found: $GOOGLE_APPLICATION_CREDENTIALS"
    exit 1
fi

log "Credentials verified: $GOOGLE_APPLICATION_CREDENTIALS"

################################################################################
# Step 1: Data Ingestion
################################################################################

log "---"
log "Step 1: Running data ingestion..."

INGESTION_START=$(date +%s)

# Check for --force-download flag
FORCE_FLAG=""
if [ "${1:-}" = "--force-download" ]; then
    FORCE_FLAG="--force-download"
    warning "Force download enabled - will re-download all files"
fi

# Run ingestion
if uv run python ingestion/ingest.py $FORCE_FLAG >> "$LOG_FILE" 2>&1; then
    success "Data ingestion completed"
else
    error "Data ingestion failed"
    exit 1
fi

INGESTION_END=$(date +%s)
INGESTION_TIME=$((INGESTION_END - INGESTION_START))
log "Ingestion took ${INGESTION_TIME}s"

################################################################################
# Step 2: dbt Transformations
################################################################################

log "---"
log "Step 2: Running dbt transformations..."

DBT_START=$(date +%s)

cd dbt/transform

# Run dbt with logging
if uv run dbt run --profiles-dir . >> "$LOG_FILE" 2>&1; then
    success "dbt transformations completed"
else
    error "dbt transformations failed"
    cd "$PROJECT_ROOT"
    exit 1
fi

# Optional: Run tests
log "Running dbt tests..."
if uv run dbt test --profiles-dir . >> "$LOG_FILE" 2>&1; then
    success "dbt tests passed"
else
    warning "Some dbt tests failed - check logs"
fi

cd "$PROJECT_ROOT"

DBT_END=$(date +%s)
DBT_TIME=$((DBT_END - DBT_START))
log "dbt took ${DBT_TIME}s"

################################################################################
# Summary
################################################################################

TOTAL_TIME=$((DBT_END - INGESTION_START))
log "---"
log "Pipeline Summary:"
log "  - Ingestion: ${INGESTION_TIME}s"
log "  - dbt:       ${DBT_TIME}s"
log "  - Total:     ${TOTAL_TIME}s (~$((TOTAL_TIME / 60))m)"
log "========================================="

# Keep only last 30 days of logs
find "$LOG_DIR" -name "ingestion_*.log" -mtime +30 -delete 2>/dev/null || true

exit 0
