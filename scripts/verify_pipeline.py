#!/usr/bin/env python3
"""
Pipeline Integrity Verification Utility

Verifies:
1. Schema Registry - Contracts are registered
2. dbt connection - Profiles and environment variables are valid
3. Seed data - Reference tables are loaded
4. Snapshots - SCD Type 2 history capture is active
5. Transformations - Lineage from Staging to Marts
6. Data contracts - All YAML-defined tests pass
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.request import urlopen
from urllib.error import URLError

def check_schema_registry():
    print("\n=== Check 1: Schema Registry Contracts ===")
    # Security: No fallback. Fail if not configured in the environment.
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL")
    if not registry_url:
        print("  ✗ FAIL: SCHEMA_REGISTRY_URL environment variable is not set")
        return False
    
    try:
        response = urlopen(f"{registry_url.rstrip('/')}/subjects", timeout=5)
        subjects = json.loads(response.read().decode())
        
        expected = ["vehicle_position-value", "enriched_event-value", "stop_event-value"]
        all_found = True
        for schema in expected:
            found = schema in subjects
            print(f"  {'✓' if found else '✗'} {schema}")
            if not found: all_found = False
            
        return all_found
    except URLError as e:
        print(f"  ✗ FAIL: Cannot connect to Registry: {e}")
        return False

def run_dbt_command(command_list, label):
    """Utility to execute dbt tasks and capture status."""
    dbt_dir = Path(__file__).parent.parent / "dbt"
    try:
        result = subprocess.run(
            ["dbt"] + command_list + ["--profiles-dir", "."],
            cwd=dbt_dir, capture_output=True, text=True, timeout=180
        )
        success = result.returncode == 0
        print(f"  {'✓' if success else '✗'} {label}")
        if not success:
            print(f"    Error: {result.stderr[-250:] if result.stderr else 'Check dbt logs'}")
        return success
    except Exception as e:
        print(f"  ✗ {label} System Error: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Pipeline Integrity Utility")
    parser.add_argument("--check-all", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("PIPELINE INTEGRITY VERIFICATION")
    print("=" * 60)

    results = {}
    
    # 1. Schema Registry
    results['registry'] = check_schema_registry()
    
    # 2. dbt Connectivity
    results['dbt_connect'] = run_dbt_command(["debug"], "dbt Connectivity")
    
    if results['dbt_connect']:
        # 3. Static Reference Data
        results['seeds'] = run_dbt_command(["seed"], "Static Seed Loading")
        
        # 4. Snapshots (Required for Dimension History)
        results['snapshots'] = run_dbt_command(["snapshot"], "SCD Type 2 History Capture")
        
        # 5. Full Pipeline Run (Lineage: Staging -> Intermediate -> Marts)
        results['models'] = run_dbt_command(["run"], "Transformation Lineage")
        
        # 6. Contract Verification
        results['tests'] = run_dbt_command(["test"], "Data Contract Validation")

    # Summary
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    passed = sum(1 for v in results.values() if v is True)
    total = len(results)
    
    for check, status in results.items():
        print(f"  {check:15}: {'PASS ✓' if status else 'FAIL ✗'}")
    
    print(f"\nResult: {passed}/{total} checks passed")
    sys.exit(0 if passed == total else 1)

if __name__ == "__main__":
    main()