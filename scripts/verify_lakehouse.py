#!/usr/bin/env python3
"""
Lakehouse Verification Script
Checks the integrity of the Medallion Architecture (Bronze/Silver/Gold)
and infrastructure health for Spark, MinIO, and PostgreSQL.
Standard: Requires --date to verify specific partition existence for OCI stability.
"""

import argparse
import json
import os
import subprocess
import sys
from urllib.request import urlopen

def run_pg_query(query):
    """Executes a query directly against the Postgres container."""
    pg_user = os.environ.get("POSTGRES_USER", "transit")
    pg_db = os.environ.get("POSTGRES_DB", "transit")
    cmd = ["docker", "exec", "postgres", "psql", "-U", pg_user, "-d", pg_db, "-t", "-c", query]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        return f"ERROR: {e.stderr}"

def check_minio_connection():
    print("\n=== Check 0: MinIO Connection ===")
    try:
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
        response = urlopen("http://localhost:8083/json/", timeout=5)
        data = json.loads(response.read().decode())
        print(f"  Status: {data.get('status')} | Workers: {len(data.get('workers', []))}")
        if data.get("status") == "ALIVE":
            print("  PASS: Spark Master is ALIVE")
            return True
        return False
    except Exception as e:
        print(f"  FAIL: Spark Master UI unreachable at localhost:8083. {e}")
        return False

def check_delta_partition(path, date):
    """Checks if a specific date partition exists inside the Delta table."""
    bucket = "transitflow-lakehouse"
    # Search for the specific partition folder within the Delta path
    partition_path = f"/data/{bucket}/{path}/date={date}"
    cmd = ["docker", "exec", "minio", "ls", "-R", partition_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return "part-" in result.stdout  # If part files exist, the partition is populated

def check_medallion_layers(target_date):
    print(f"\n=== Check 2: Medallion Layers (Partition: {target_date}) ===")
    layers = {
        "Bronze": ["bronze/enriched", "bronze/stop_events"],
        "Silver": ["silver/enriched", "silver/stop_events"],
        "Gold": ["gold/daily_metrics", "gold/hourly_metrics"],
    }

    all_passed = True
    for layer, tables in layers.items():
        print(f"  {layer} Layer:")
        for t in tables:
            exists = check_delta_partition(t, target_date)
            status_icon = "[PASS]" if exists else "[MISSING]"
            print(f"    {status_icon} {t}")
            if not exists:
                all_passed = False

    return all_passed

def check_reconciliation_audit(target_date):
    print(f"\n=== Check 3: PostgreSQL Audit for {target_date} ===")
    query = f"SELECT count(*) FROM reconciliation_results WHERE date = '{target_date}';"
    count_str = run_pg_query(query)
    if "ERROR" in count_str:
        print(f"  FAIL: Database error. {count_str}")
        return False
    
    try:
        count = int(count_str)
        print(f"  Found {count} audit records for {target_date}.")
        if count > 0:
            print("  PASS: Audit trail entry exists.")
            return True
        print(f"  WARN: No audit record for {target_date}. Run 'make spark-reconcile DATE={target_date}'.")
        return False
    except ValueError:
        print("  FAIL: Unexpected response from Postgres.")
        return False

def main():
    parser = argparse.ArgumentParser(description="TransitFlow Lakehouse Verification")
    parser.add_argument("--date", type=str, required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()

    print(f"Starting Lakehouse Verification for DATE: {args.date}...")
    c0 = check_minio_connection()
    c1 = check_spark_master()
    c2 = check_medallion_layers(args.date)
    c3 = check_reconciliation_audit(args.date)

    print("\nVerification Complete.")
    if not all([c0, c1, c2, c3]):
        print("!!! PIPELINE INCOMPLETE: One or more stages missing for this date. !!!")
        sys.exit(1)
    
    print(f"!!! PHASE 3 CERTIFIED: End-to-End Lakehouse is Functional for {args.date}. !!!")
    sys.exit(0)

if __name__ == "__main__":
    main()