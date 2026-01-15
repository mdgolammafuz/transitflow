#!/usr/bin/env python3
"""
Warehouse Readiness Check
Verifies connectivity to the correct PostgreSQL instance.
"""

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# Load env from infra/local/.env
base_path = Path(__file__).parent.parent
env_path = base_path / "infra" / "local" / ".env"
load_dotenv(dotenv_path=env_path)


def check_upstream_data():
    conn = None
    try:
        # Pulling from your confirmed .env
        db_user = os.environ.get("POSTGRES_USER")
        db_pass = os.environ.get("POSTGRES_PASSWORD")
        db_name = os.environ.get("POSTGRES_DB")
        # Use IPv4 loopback to ensure we hit the Docker-mapped port
        db_host = "127.0.0.1"
        db_port = os.environ.get("POSTGRES_PORT", 5432)

        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_pass,
            connect_timeout=5,
        )

        print("-" * 40)
        print("WAREHOUSE INTEGRITY CHECK")
        print("-" * 40)
        print(f"  [PASS] Connected to {db_name} as {db_user}")
        return True

    except Exception as e:
        print(f"  [FAIL] Connection failed: {e}")
        print("  [TIP] Check if a local Postgres service is blocking port 5432.")
        return False
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    import sys

    sys.exit(0 if check_upstream_data() else 1)
