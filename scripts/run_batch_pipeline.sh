#!/bin/bash
set -e  # Exit immediately if any command fails

# Detect TTY for manual runs vs cron
INTERACTIVE=""
if [ -t 0 ]; then
    INTERACTIVE="-it"
else
    INTERACTIVE="-i"
fi

echo "[$(date)] === Starting TransitFlow Daily Pipeline ==="

# --- Phase 1: Ingestion & Storage ---
echo "[$(date)] 1. Running Bronze Ingestion (Batch Mode)..."
# We use the --once flag (added to Makefile spark-bronze) to process backlog and exit.
make spark-bronze

echo "[$(date)] 2. Syncing to Postgres..."
# Required for Drift Monitor (Postgres Bronze table)
make spark-sync

echo "[$(date)] 3. Running Silver Transformation..."
# Cleans and deduplicates data
make spark-silver

echo "[$(date)] 4. Verifying Data Integrity (Reconciliation)..."
# Checks count(Bronze) vs count(Silver). Logs results to Postgres.
make spark-reconcile

# --- Phase 2: Analytics & Metadata ---
echo "[$(date)] 5. Initializing Gold Metadata..."
# Ensures Gold/Stops tables exist before we try to join them
make metadata-init

echo "[$(date)] 6. Running Gold Aggregation..."
make spark-gold

# --- Phase 3: Maintenance ---
echo "[$(date)] 7. Running Lakehouse Maintenance..."
# Vacuums Delta files and Cleans Postgres history (>35 days)
make spark-maintenance

# --- Phase 4: MLOps ---
echo "[$(date)] 8. Checking for Data Drift..."
# Calculates PSI. Uses fallback if <30 days history.
docker exec $INTERACTIVE serving-api python scripts/monitor_drift.py

echo "[$(date)] 9. Evaluating Retraining Rules..."
# Checks drift.json. Triggers model-trainer if drift detected + sufficient data exists.
docker exec $INTERACTIVE serving-api python mlops/retraining.py --drift-file drift.json

echo "[$(date)] === Pipeline Complete ==="