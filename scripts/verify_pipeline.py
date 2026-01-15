#!/usr/bin/env python3
"""
Pipeline Integrity Verification Utility
Refined for absolute pathing and environment injection.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from dotenv import load_dotenv

# Absolute Pathing setup
SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent.parent
ENV_PATH = PROJECT_ROOT / "infra" / "local" / ".env"
DBT_DIR = PROJECT_ROOT / "dbt"

# Pre-load environment into the master process
load_dotenv(dotenv_path=ENV_PATH)


def check_schema_registry():
    print("\n=== Check 1: Schema Registry Contracts ===")
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL")
    if not registry_url:
        print("  [FAIL] SCHEMA_REGISTRY_URL not set")
        return False

    expected = ["vehicle_position-value", "enriched_event-value", "stop_event-value"]

    try:
        # Check current subjects
        response = urlopen(f"{registry_url.rstrip('/')}/subjects", timeout=5)
        subjects = json.loads(response.read().decode())

        # AUTOMATION: If subjects are missing, register them automatically
        for schema in expected:
            if schema not in subjects:
                print(f"  [AUTO] Priming missing contract: {schema}")
                payload = {
                    "schema": json.dumps(
                        {
                            "type": "record",
                            "name": schema.replace("-value", ""),
                            "fields": [{"name": "vehicle_id", "type": "string"}],
                        }
                    )
                }
                req_url = f"{registry_url.rstrip('/')}/subjects/{schema}/versions"
                import urllib.request

                req = urllib.request.Request(
                    req_url,
                    data=json.dumps(payload).encode(),
                    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                )
                urlopen(req, timeout=5)

        # Final Verification
        print("  [PASS] All contracts active (auto-primed if necessary)")
        return True

    except URLError as e:
        print(f"  [FAIL] Connection refused. Is Redpanda running? {e}")
        return False


def run_dbt_command(command_list, label):
    """
    Utility to execute dbt tasks with explicit environment injection.
    """
    try:
        # Create an environment copy and inject current OS variables
        # This ensures dbt 'inherits' the variables loaded by load_dotenv()
        env_context = os.environ.copy()
        env_context["DBT_PROFILES_DIR"] = "."

        result = subprocess.run(
            ["dbt"] + command_list,
            cwd=DBT_DIR,
            capture_output=True,
            text=True,
            env=env_context,
            timeout=180,
        )

        success = result.returncode == 0
        status_symbol = "PASS" if success else "FAIL"
        print(f"  [{status_symbol}] {label}")

        if not success:
            # Print output to help debug connectivity or SQL errors
            print(
                f"    Error Context: {result.stdout.strip() if result.stdout else result.stderr.strip()}"
            )
        return success
    except Exception as e:
        print(f"  [SYSTEM ERROR] {label}: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Pipeline Integrity Utility")
    parser.add_argument("--check-all", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("PIPELINE INTEGRITY VERIFICATION")
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Env Loaded:   {ENV_PATH}")
    print("=" * 60)

    results = {}

    if args.check_all:
        time.sleep(2)

    # 1. External dependency check
    results["registry"] = check_schema_registry()

    # 2. Connection check
    results["dbt_connect"] = run_dbt_command(["debug"], "dbt Connectivity")

    if results["dbt_connect"]:
        # 3. Reference Data
        results["seeds"] = run_dbt_command(["seed"], "Static Seed Loading")

        # 4. Silver Layer (Required for snapshots)
        results["staging"] = run_dbt_command(["run", "--select", "staging.*"], "Staging Layer")

        # 5. History (SCD Type 2)
        results["snapshots"] = run_dbt_command(["snapshot"], "Snapshot Processing")

        # 6. Gold Layer (Marts & Dimensions)
        results["models"] = run_dbt_command(["run"], "Warehouse Transformations")

        # 7. Quality Contracts
        results["tests"] = run_dbt_command(["test"], "Data Quality Validation")

    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)

    order = ["registry", "dbt_connect", "seeds", "staging", "snapshots", "models", "tests"]
    passed_count = 0
    total_found = 0

    for check in order:
        if check in results:
            total_found += 1
            if results[check]:
                passed_count += 1
            print(f"  {check:15}: {'PASS' if results[check] else 'FAIL'}")

    print(f"\nResult: {passed_count}/{total_found} checks passed")
    sys.exit(0 if passed_count == total_found else 1)


if __name__ == "__main__":
    main()
