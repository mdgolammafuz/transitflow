#!/usr/bin/env python3
"""
Lakehouse Verification Script
Checks the integrity of the Medallion Architecture (Bronze/Silver/Gold)
and infrastructure health for Spark, MinIO, and PostgreSQL.
"""

import argparse
import json
import os
import subprocess
import sys
from urllib.request import urlopen

def run_pg_query(query):
    pg_user = os.environ.get("POSTGRES_USER", "transit")
    pg_db = os.environ.get("POSTGRES_DB", "transit")
    cmd = ["docker", "exec", "postgres", "psql", "-U", pg_user, "-d", pg_db, "-t", "-c", query]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout.strip()

def check_counts(layer_path):
    # Queries the latest record count via a lightweight Spark SQL check or checking Parquet file presence
    # For high robustness without triggering full Spark sessions, we check the Postgres Audit logs for that table
    print(f"  Table: {layer_path}")
    count = run_pg_query(f"SELECT batch_count FROM reconciliation_results WHERE table_name='enriched' ORDER BY checked_at DESC LIMIT 1;")
    print(f"    Last Verified Records: {count}")
    return int(count) if count else 0

def verify_reconciliation():
    print("\n=== Check 4: Reconciliation ===")
    query = "SELECT stream_count, batch_count, diff_percentage FROM reconciliation_results ORDER BY checked_at DESC LIMIT 1;"
    raw = run_pg_query(query)
    if raw:
        s, b, diff = raw.split('|')
        print(f"  Stream count: {s.strip()}")
        print(f"  Batch count: {b.strip()}")
        print(f"  Difference: {diff.strip()}%")
        if float(diff.strip()) < 20.0:
            print("PASS: Stream and batch agree")
            return True
    print("FAIL: Reconciliation mismatch")
    return False

def check_minio_connection():
    print("\n=== Check 0: MinIO Connection ===")
    try:
        # Checking MinIO API health
        response = urlopen("http://localhost:9000/minio/health/live", timeout=5)
        if response.status == 200:
            print("  PASS: MinIO health is OK")
            return True
    except Exception as e:
        print(f"  FAIL: MinIO unreachable at localhost:9000. {e}")
        return False

def check_spark_master():
    print("\n=== Check 1: Spark Master ===")
    try:
        # User Docker config maps host 8083 to container 8080
        response = urlopen("http://localhost:8083/json/", timeout=5)
        data = json.loads(response.read().decode())
        print(f"  Status: {data.get('status')} | Workers: {len(data.get('workers', []))}")
        if data.get('status') == 'ALIVE':
            print("  PASS: Spark Master is ALIVE")
            return True
        return False
    except Exception as e:
        print(f"  FAIL: Spark Master UI (localhost:8083) unreachable. {e}")
        return False

def check_delta_path(path):
    """Checks if Delta logs exist in the container."""
    bucket = "transitflow-lakehouse"
    # Using 'ls -R' to find the _delta_log specifically in the MinIO data volume
    cmd = ["docker", "exec", "minio", "ls", "-R", f"/data/{bucket}/{path}"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return "_delta_log" in result.stdout

def check_medallion_layers():
    print("\n=== Check 2: Medallion Layers ===")
    # Explicitly including stop_events checks as requested
    layers = {
        "Bronze": ["bronze/enriched", "bronze/stop_events"],
        "Silver": ["silver/enriched", "silver/stop_events"],
        "Gold": ["gold/daily_metrics", "gold/hourly_metrics", "gold/stop_performance"]
    }
    
    all_passed = True
    for layer, tables in layers.items():
        print(f"  {layer} Layer:")
        for t in tables:
            exists = check_delta_path(t)
            status_icon = "[PASS]" if exists else "[MISSING]"
            print(f"    {status_icon} {t}")
            if not exists:
                all_passed = False
    
    if all_passed:
        print("  PASS: All Medallion layers and stop_events are present.")
    return all_passed

def check_reconciliation():
    print("\n=== Check 3: PostgreSQL Audit Trail ===")
    pg_user = os.environ.get("POSTGRES_USER", "transit")
    pg_db = os.environ.get("POSTGRES_DB", "transit")
    
    cmd = [
        "docker", "exec", "postgres", "psql", "-U", pg_user, "-d", pg_db,
        "-t", "-c", "SELECT count(*) FROM reconciliation_results;"
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            count = int(result.stdout.strip())
            print(f"  Found {count} audit records in PostgreSQL.")
            if count > 0:
                print("  PASS: Reconciliation table is populated.")
                return True
        print("  WARN: Reconciliation table exists but is empty.")
        return False
    except Exception:
        print("  FAIL: Cannot query PostgreSQL. Check if 'reconciliation_results' table exists.")
        return False

def main():
    print("Starting Lakehouse Verification...")
    c0 = check_minio_connection()
    c1 = check_spark_master()
    c2 = check_medallion_layers()
    c3 = check_reconciliation()
    
    print("\nVerification Complete.")
    if not all([c0, c1, c2, c3]):
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()