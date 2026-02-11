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

# 1. Processing (Bronze -> Silver -> Gold)
echo "[$(date)] 1. Running Bronze Ingestion..."
make spark-bronze

echo "[$(date)] 2. Syncing to Postgres..."
make spark-sync

echo "[$(date)] 3. Running Silver Transformation..."
make spark-silver

echo "[$(date)] 4. Running Gold Aggregation..."
make spark-gold

# 2. Maintenance (Vacuum & Optimization)
echo "[$(date)] 5. Running Lakehouse Maintenance..."
make spark-maintenance

# 3. Observability (Drift Detection)
echo "[$(date)] 6. Checking for Data Drift..."
# We run this inside the serving-api container because it has the MLOps dependencies
docker exec $INTERACTIVE serving-api python scripts/monitor_drift.py

# 4. Decision (Retraining Trigger)
echo "[$(date)] 7. Evaluating Retraining Rules..."
docker exec $INTERACTIVE serving-api python mlops/retraining.py --drift-file drift.json

echo "[$(date)] === Pipeline Complete ==="