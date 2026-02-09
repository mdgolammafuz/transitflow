#!/usr/bin/env python3
"""
Register Avro schemas with Schema Registry.
Loads .avsc files from a configurable directory.
"""

import argparse
import logging
import sys
import json
import os
from pathlib import Path

# PYTHONPATH=. must be set in the Makefile for this import to work
from schema_registry.client import SchemaRegistryClient, SchemaRegistryError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_schemas_from_disk(schema_dir_path):
    """Reads all .avsc files from the provided directory."""
    schemas = {}
    path_obj = Path(schema_dir_path)
    
    if not path_obj.exists():
        logger.error(f"[FAIL] Schema directory not found: {path_obj.absolute()}")
        return {}

    logger.info(f"Loading schemas from: {path_obj.absolute()}")

    for file_path in path_obj.glob("*.avsc"):
        subject_name = f"{file_path.stem}-value"
        try:
            with open(file_path, 'r') as f:
                schema_content = json.load(f)
                schemas[subject_name] = schema_content
                logger.info(f"Loaded definition for {subject_name}")
        except Exception as e:
            logger.error(f"Failed to load {file_path.name}: {e}")
    
    return schemas

def check_schemas(client: SchemaRegistryClient, local_schemas: dict) -> bool:
    """Check if the Registry matches our local source of truth."""
    try:
        remote_subjects = client.list_subjects()
    except SchemaRegistryError as e:
        logger.error(f"[FAIL] Could not list subjects: {e}")
        return False

    all_consistent = True
    for subject, schema_def in local_schemas.items():
        if subject not in remote_subjects:
            logger.error(f"[FAIL] Subject missing from Registry: {subject}")
            all_consistent = False
            continue

        try:
            is_compatible = client.is_compatible(subject, schema_def)
            if is_compatible:
                logger.info(f"[PASS] {subject} is present and compatible.")
            else:
                logger.warning(f"[FAIL] {subject} exists but definition is NOT compatible.")
                all_consistent = False
        except SchemaRegistryError as e:
            logger.error(f"[ERROR] Verification failed for {subject}: {e}")
            all_consistent = False

    return all_consistent

def register_all_schemas(client: SchemaRegistryClient, local_schemas: dict, dry_run: bool = False) -> bool:
    success = True
    for subject, schema in local_schemas.items():
        try:
            if dry_run:
                logger.info(f"[DRY RUN] Target: {subject}")
                continue

            try:
                schema_id = client.register_schema(subject, schema)
                logger.info(f"[PASS] Registered {subject} (ID: {schema_id})")
            except SchemaRegistryError as e:
                if "409" in str(e):
                    if client.is_compatible(subject, schema):
                        logger.info(f"[INFO] {subject} already exists and is compatible. Skipping.")
                    else:
                        logger.error(f"[FAIL] {subject} has an incompatible schema change.")
                        success = False
                        continue
                else:
                    raise e

            if hasattr(client, "set_compatibility"):
                client.set_compatibility(subject, "BACKWARD")

        except Exception as e:
            logger.error(f"[FAIL] Registration failed for {subject}: {e}")
            success = False
    return success

def main():
    parser = argparse.ArgumentParser(description="TransitFlow Schema Registry Utility")
    parser.add_argument("--url", default="http://localhost:8081")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    # Allows Makefile to control the location
    parser.add_argument("--schema-dir", default="src/schema_registry/schemas", help="Path to .avsc files")
    args = parser.parse_args()

    client = SchemaRegistryClient(args.url)

    if not client.is_healthy():
        logger.error(f"[FAIL] Connection to Schema Registry refused at {args.url}")
        sys.exit(1)

    # Load from the configured directory
    local_schemas = load_schemas_from_disk(args.schema_dir)
    if not local_schemas:
        logger.error("[FAIL] No schemas found to register.")
        sys.exit(1)

    if args.check_only:
        success = check_schemas(client, local_schemas)
        if success:
            logger.info("Integrity Check: Registry matches local definitions.")
    else:
        success = register_all_schemas(client, local_schemas, args.dry_run)
        if success and not args.dry_run:
            logger.info("Registry synchronization successful.")

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()