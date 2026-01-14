#!/bin/sh
set -e # Exit on any error

# Configure the mc client to talk to the minio service
mc alias set myminio http://minio:9000 "${MINIO_USER}" "${MINIO_PASSWORD}"

# Helper function for idempotent bucket creation
create_bucket() {
    BUCKET=$1
    if ! mc ls myminio/$BUCKET > /dev/null 2>&1; then
        echo "Creating bucket: $BUCKET"
        mc mb myminio/$BUCKET
    else
        echo "Bucket $BUCKET already exists."
    fi
}

# Initialize required buckets
create_bucket "flink-checkpoints"
create_bucket "transitflow-lakehouse"

# Enable versioning for data integrity
mc version enable myminio/transitflow-lakehouse

echo "MinIO Infrastructure Initialization: SUCCESS"