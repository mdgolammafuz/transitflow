#!/usr/bin/env bash

# ==========================================
# Fleet Telemetry Topic Setup
# ==========================================

set -e

# Default to localhost, but allow override via env var
REDPANDA_HOST="${REDPANDA_HOST:-localhost:9092}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-3}"     # Default to 3 for high availability
MIN_INSYNC_REPLICAS="${MIN_INSYNC_REPLICAS:-2}"   # Ensure data is written to at least 2 brokers

# --- Time Constants (ms) ---
ONE_HOUR=$((60 * 60 * 1000))
ONE_DAY=$((24 * ONE_HOUR))
ONE_WEEK=$((7 * ONE_DAY))
ONE_MONTH=$((30 * ONE_DAY))

echo "Configuring Kafka topics on $REDPANDA_HOST..."
echo "Replication Factor: $REPLICATION_FACTOR"
echo "Min Insync Replicas: $MIN_INSYNC_REPLICAS"

# Function to safely create topics (DRY Principle)
create_topic() {
    local topic_name=$1
    local partitions=$2
    local retention_ms=$3
    local compression=$4 # Optional argument

    echo "Ensuring topic: $topic_name"

    # Construct the base command
    cmd="rpk topic create $topic_name \
        --brokers \"$REDPANDA_HOST\" \
        --partitions $partitions \
        --replicas $REPLICATION_FACTOR \
        --config retention.ms=$retention_ms \
        --config min.insync.replicas=$MIN_INSYNC_REPLICAS"

    # Append compression if specified
    if [ -n "$compression" ]; then
        cmd="$cmd --config compression.type=$compression"
    fi

    # Execute. 
    # We suppress standard error (2>/dev/null) to hide "topic exists" errors,
    # but we allow the script to continue (|| true) to maintain idempotency.
    # A failure to create due to connection issues will still be caught if we remove the silence,
    # For stricter environments, check `rpk topic list` first.
    eval "$cmd" 2>/dev/null || echo "  - Topic $topic_name likely exists (skipping)."
}

# --- Main Telemetry Topics ---

# Raw Telemetry: High throughput, short retention, ZSTD compression for cost/performance
create_topic "fleet.telemetry.raw" 8 $ONE_HOUR "zstd"
create_topic "fleet.telemetry.dlq" 1 $ONE_WEEK

# Enriched Events: Processed data
create_topic "fleet.enriched" 8 $ONE_DAY
create_topic "fleet.enriched.dlq" 1 $ONE_WEEK

# --- Critical Events ---

# Stop Events: Critical state changes.
# SECURITY NOTE: Includes DLQ to prevent blocking the queue on poison pills.
create_topic "fleet.stop_events" 8 $ONE_WEEK
create_topic "fleet.stop_events.dlq" 1 $ONE_MONTH

# --- Analytics & ML ---

# Predictions
create_topic "fleet.predictions" 8 $ONE_WEEK
create_topic "fleet.predictions.dlq" 1 $ONE_MONTH

# Anomalies: Longer retention for investigation
create_topic "fleet.anomalies" 4 $ONE_MONTH

# Metrics: Standard operational metrics
create_topic "fleet.metrics" 4 $ONE_WEEK

echo "----------------------------------------"
echo "Topic setup complete. Current list:"
rpk topic list --brokers "$REDPANDA_HOST"