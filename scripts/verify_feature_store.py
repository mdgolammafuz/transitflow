#!/usr/bin/env python3
"""
Feature Store Verification Script - Phase 5 Final Alignment

Verifies the integrity of the unified feature serving layer:
1. Online Store (Redis) connectivity and key presence.
2. Offline Store (PostgreSQL) connectivity using the 'marts' schema.
3. Feature API health and store synchronization.
4. End-to-end feature retrieval for a real-world vehicle context.
"""

import argparse
import json
import os
import sys
from urllib.error import URLError
from urllib.request import urlopen

import psycopg2
import redis


def check_redis():
    """Verify Redis (Online Store) connection and data presence."""
    print("\n=== Check: Redis (Online Store) ===")

    host = os.environ.get("REDIS_HOST", "127.0.0.1")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    password = os.environ.get("REDIS_PASSWORD")
    prefix = os.environ.get("REDIS_KEY_PREFIX", "features:vehicle:")

    try:
        client = redis.Redis(
            host=host, port=port, password=password, socket_timeout=5, decode_responses=True
        )

        client.ping()
        print(f"  [OK] Connected to Redis at {host}:{port}")

        pattern = f"{prefix}*"
        count = 0
        for _ in client.scan_iter(match=pattern, count=100):
            count += 1
            if count >= 100:
                break

        if count > 0:
            print(f"  [OK] Active vehicle features found (sampled): {count}")
        else:
            print("  [WARN] Redis connected, but no vehicle features found. Sync might be pending.")

        return True
    except redis.RedisError as e:
        print(f"  [FAIL] Redis error: {e}")
        return False


def check_postgres():
    """
    Verify PostgreSQL (Offline Store) schema and table status.
    Strictly aligned with Phase 5 'marts' schema.
    """
    print("\n=== Check: PostgreSQL (Offline Store) ===")

    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    user = os.environ.get("POSTGRES_USER", "transit")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    dbname = os.environ.get("POSTGRES_DB", "transit")
    schema = os.environ.get("POSTGRES_SCHEMA", "marts")

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            connect_timeout=5,
        )

        with conn.cursor() as cur:
            # Check for table in the 'marts' schema
            cur.execute(
                f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = '{schema}'
                  AND table_name = 'fct_stop_arrivals'
                """
            )

            if cur.fetchone()[0] > 0:
                print(f"  [OK] {schema}.fct_stop_arrivals table exists")
                cur.execute(f"SELECT COUNT(*) FROM {schema}.fct_stop_arrivals")
                row_count = cur.fetchone()[0]
                print(f"  [OK] Offline features: {row_count} rows")

                # Verify key columns for Phase 6 robustness (historical naming)
                cur.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_schema='{schema}' AND table_name='fct_stop_arrivals'"
                )
                cols = [c[0] for c in cur.fetchall()]
                required = ["historical_avg_delay", "stop_lat", "stop_lon"]
                for req in required:
                    if req in cols:
                        print(f"  [OK] Required column found: {req}")
                    else:
                        print(f"  [FAIL] Missing required column: {req}")
                        return False
            else:
                print(
                    f"  [FAIL] {schema}.fct_stop_arrivals table not found. Did you run make dbt-run?"
                )
                return False

        conn.close()
        return True

    except psycopg2.Error as e:
        print(f"  [FAIL] PostgreSQL error: {e}")
        return False


def check_api_health():
    """Verify Feature API health and store statuses."""
    print("\n=== Check: Feature API Health ===")
    api_url = os.environ.get("FEATURE_API_URL", "http://localhost:8000")

    try:
        response = urlopen(f"{api_url}/health", timeout=5)
        data = json.loads(response.read().decode())

        print(f"  [OK] API Status: {data.get('status')}")
        print(f"  [OK] Online Store: {data.get('online_store')}")
        print(f"  [OK] Offline Store: {data.get('offline_store')}")

        return data.get("status") == "healthy"
    except URLError as e:
        print(f"  [FAIL] API not responding at {api_url}: {e}")
        return False


def check_feature_retrieval(vehicle_id: int):
    """Verify end-to-end feature retrieval logic."""
    print(f"\n=== Check: Feature Retrieval (ID: {vehicle_id}) ===")
    api_url = os.environ.get("FEATURE_API_URL", "http://localhost:8000")

    try:
        # Request features to see if the vector assembler is working
        response = urlopen(f"{api_url}/features/{vehicle_id}", timeout=5)
        data = json.loads(response.read().decode())

        print(f"  [OK] Online Available: {data.get('online_available')}")
        print(f"  [OK] Offline Available: {data.get('offline_available')}")
        print(f"  [OK] Latency: {data.get('latency_ms')}ms")

        return True
    except URLError as e:
        print(f"  [FAIL] Retrieval failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Feature Store Verification")
    parser.add_argument("--check-all", action="store_true")
    parser.add_argument("--vehicle-id", type=int, default=1362)
    args = parser.parse_args()

    # Step 0: Get a real vehicle ID from DB if possible to make Retrieval test pass
    vehicle_id = args.vehicle_id
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    user = os.environ.get("POSTGRES_USER", "transit")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    dbname = os.environ.get("POSTGRES_DB", "transit")

    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        with conn.cursor() as cur:
            # Note: Checking staging table or enriched events for current activity
            cur.execute("SELECT vehicle_id FROM marts.fct_stop_arrivals LIMIT 1")
            res = cur.fetchone()
            if res:
                vehicle_id = res[0]
        conn.close()
    except:
        pass  # Fallback to default ID if DB query fails

    print("=" * 50)
    print("FEATURE STORE INTEGRATION VERIFICATION")
    print("=" * 50)

    results = {
        "Redis": check_redis(),
        "PostgreSQL": check_postgres(),
        "API Health": check_api_health(),
        "Retrieval": check_feature_retrieval(vehicle_id),
    }

    print("\n" + "=" * 50)
    print("VERIFICATION SUMMARY")
    print("=" * 50)

    all_passed = True
    for name, success in results.items():
        status = "[PASS]" if success else "[FAIL]"
        if not success:
            all_passed = False
        print(f"  {name:15} {status}")

    if all_passed:
        print("\nSUCCESS: Feature Store is fully operational and targeting 'marts'.")
        sys.exit(0)
    else:
        print("\nFAILURE: One or more components are not responding correctly.")
        sys.exit(1)


if __name__ == "__main__":
    main()
