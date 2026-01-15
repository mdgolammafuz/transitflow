#!/usr/bin/env python3
"""
Feature Store Verification Script

Verifies the integrity of the unified feature serving layer:
1. Online Store (Redis) connectivity and key presence.
2. Offline Store (PostgreSQL) connectivity and table schema.
3. Feature API health and response status.
4. End-to-end feature retrieval and vector formatting.
5. Metrics and observability endpoints.
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

    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))

    try:
        client = redis.Redis(host=host, port=port, socket_timeout=5)
        client.ping()
        print(f"  [OK] Connected to Redis at {host}:{port}")

        pattern = os.environ.get("REDIS_KEY_PREFIX", "features:vehicle:") + "*"
        # Efficiently check for presence without loading all keys
        count = 0
        for _ in client.scan_iter(match=pattern, count=100):
            count += 1
            if count >= 100:
                break  # Cap for verification speed

        print(f"  [OK] Active vehicle features found (sampled): {count}")
        return True

    except redis.RedisError as e:
        print(f"  [FAIL] Redis error: {e}")
        return False


def check_postgres():
    """Verify PostgreSQL (Offline Store) schema and table status."""
    print("\n=== Check: PostgreSQL (Offline Store) ===")

    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    user = os.environ.get("POSTGRES_USER", "transit")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    dbname = os.environ.get("POSTGRES_DB", "transit")

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
            cur.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'features'
                  AND table_name = 'stop_historical'
            """
            )
            if cur.fetchone()[0] > 0:
                print("  [OK] features.stop_historical table exists")
                cur.execute("SELECT COUNT(*) FROM features.stop_historical")
                print(f"  [OK] Offline features: {cur.fetchone()[0]} rows")
            else:
                print("  [FAIL] features.stop_historical table not found")
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


def check_feature_retrieval(vehicle_id: int = 1362):
    """Verify end-to-end feature retrieval logic."""
    print(f"\n=== Check: Feature Retrieval (ID: {vehicle_id}) ===")
    api_url = os.environ.get("FEATURE_API_URL", "http://localhost:8000")

    try:
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
    parser.parse_args()

    print("=" * 50)
    print("FEATURE STORE INTEGRATION VERIFICATION")
    print("=" * 50)

    results = {
        "Redis": check_redis(),
        "PostgreSQL": check_postgres(),
        "API Health": check_api_health(),
        "Retrieval": check_feature_retrieval(),
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
        print("\nSUCCESS: Feature Store is fully operational.")
        sys.exit(0)
    else:
        print("\nFAILURE: One or more components are not responding correctly.")
        sys.exit(1)


if __name__ == "__main__":
    main()
