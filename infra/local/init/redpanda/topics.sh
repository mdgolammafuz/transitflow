#!/usr/bin/env bash

# ==========================================
# Fleet Telemetry Topic Setup (Local Dev)
# ==========================================

set -e

# Default to localhost
REDPANDA_HOST="${REDPANDA_HOST:-localhost:9092}"

# FIXED: Default to 1 for local development (single broker)
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
MIN_INSYNC_REPLICAS="${MIN_INSYNC_REPLICAS:-1}"

# --- Time Constants (ms) ---
ONE_HOUR=$((60 * 60 * 1000))
ONE_DAY=$((24 * ONE_HOUR))
ONE_WEEK=$((7 * ONE_DAY))
ONE_MONTH=$((30 * ONE_DAY))

echo "Configuring Kafka topics on $REDPANDA_HOST..."
echo "Replication Factor: $REPLICATION_FACTOR"
echo "Min Insync Replicas: $MIN_INSYNC_REPLICAS"

# Function to safely create topics
create_topic() {
    local topic_name=$1
    local partitions=$2
    local retention_ms=$3
    local compression=$4 

    echo "Ensuring topic: $topic_name"

    cmd="rpk topic create $topic_name \
        --brokers \"$REDPANDA_HOST\" \
        --partitions $partitions \
        --replicas $REPLICATION_FACTOR \
        --config retention.ms=$retention_ms \
        --config min.insync.replicas=$MIN_INSYNC_REPLICAS"

    if [ -n "$compression" ]; then
        cmd="$cmd --config compression.type=$compression"
    fi

    # Suppress error if topic exists
    eval "$cmd" 2>/dev/null || echo "  - Topic $topic_name likely exists (skipping)."
}

# --- Main Telemetry Topics ---

# Raw Telemetry
create_topic "fleet.telemetry.raw" 8 $ONE_HOUR "zstd"
create_topic "fleet.telemetry.dlq" 1 $ONE_WEEK

# Enriched Events
create_topic "fleet.enriched" 8 $ONE_DAY
create_topic "fleet.enriched.dlq" 1 $ONE_WEEK

# --- Critical Events ---

# Stop Events
create_topic "fleet.stop_events" 8 $ONE_WEEK
create_topic "fleet.stop_events.dlq" 1 $ONE_MONTH

# --- Analytics & ML ---

# Predictions
create_topic "fleet.predictions" 8 $ONE_WEEK
create_topic "fleet.predictions.dlq" 1 $ONE_MONTH

# Anomalies
create_topic "fleet.anomalies" 4 $ONE_MONTH

# Metrics
create_topic "fleet.metrics" 4 $ONE_WEEK

echo "----------------------------------------"
echo "Topic setup complete. Current list:"
rpk topic list --brokers "$REDPANDA_HOST"