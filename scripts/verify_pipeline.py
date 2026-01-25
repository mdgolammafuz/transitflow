#!/usr/bin/env python3
"""
Pipeline Integrity Verification Utility
Hardened for Phase 4: No Renaming & UTC Lock Enforcement.
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

# Pre-load environment
load_dotenv(dotenv_path=ENV_PATH)

def check_schema_registry():
    print("\n=== Check 1: Schema Registry Contracts ===")
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL")
    if not registry_url:
        print("  [FAIL] SCHEMA_REGISTRY_URL not set")
        return False

    expected = ["vehicle_position-value", "enriched_event-value", "stop_event-value"]

    try:
        response = urlopen(f"{registry_url.rstrip('/')}/subjects", timeout=5)
        subjects = json.loads(response.read().decode())

        all_found = True
        for schema in expected:
            if schema not in subjects:
                print(f"  [FAIL] Missing contract: {schema}")
                all_found = False
            else:
                print(f"  [PASS] Contract active: {schema}")
        
        if not all_found:
            print("  [INFO] Tip: Run 'python scripts/register_schemas.py' to fix.")
        
        return all_found

    except URLError as e:
        print(f"  [FAIL] Connection refused. Is Redpanda running? {e}")
        return False

def run_dbt_command(command_list, label):
    try:
        env_context = os.environ.copy()
        env_context["DBT_PROFILES_DIR"] = str(DBT_DIR)

        result = subprocess.run(
            ["dbt"] + command_list,
            cwd=DBT_DIR,
            capture_output=True,
            text=True,
            env=env_context,
            timeout=300,
        )

        success = result.returncode == 0
        status_symbol = "PASS" if success else "FAIL"
        print(f"  [{status_symbol}] {label}")

        if not success:
            print(f"    Error Context: {result.stdout.strip() if result.stdout else result.stderr.strip()}")
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
    print("=" * 60)

    results = {}

    if args.check_all:
        time.sleep(2)

    results["registry"] = check_schema_registry()
    results["dbt_connect"] = run_dbt_command(["debug"], "dbt Connectivity")

    if results["dbt_connect"]:
        results["seeds"] = run_dbt_command(["seed"], "Static Seed Loading")
        results["staging"] = run_dbt_command(["run", "--select", "staging.*"], "Staging Layer")
        results["snapshots"] = run_dbt_command(["snapshot"], "Snapshot Processing")
        results["models"] = run_dbt_command(["run", "--exclude", "staging.*"], "Warehouse Transformations")
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