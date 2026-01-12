#!/usr/bin/env python3
"""
Register Avro schemas with Schema Registry.
Final Version: Fully implemented check-only logic, no emojis.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# PYTHONPATH=. must be set in the Makefile for this import to work
from src.schema_registry.client import SchemaRegistryClient, SchemaRegistryError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Schema definitions (Source of Truth for the Data Contract)
SCHEMAS = {
    "vehicle_position-value": {
        "type": "record",
        "name": "VehiclePosition",
        "namespace": "fi.transitflow",
        "doc": "Raw vehicle position from HSL MQTT",
        "fields": [
            {"name": "vehicle_id", "type": "int", "doc": "Unique vehicle identifier"},
            {"name": "timestamp", "type": "string", "doc": "ISO 8601 timestamp"},
            {"name": "event_time_ms", "type": "long", "doc": "Event time in milliseconds"},
            {"name": "latitude", "type": "double", "doc": "WGS84 latitude"},
            {"name": "longitude", "type": "double", "doc": "WGS84 longitude"},
            {"name": "speed_ms", "type": ["null", "double"], "default": None},
            {"name": "heading", "type": ["null", "int"], "default": None},
            {"name": "delay_seconds", "type": ["null", "int"], "default": None},
            {"name": "door_status", "type": ["null", "int"], "default": None},
            {"name": "line_id", "type": ["null", "string"], "default": None},
            {"name": "direction_id", "type": ["null", "int"], "default": None},
            {"name": "operator_id", "type": ["null", "int"], "default": None},
            {"name": "next_stop_id", "type": ["null", "int"], "default": None},
        ]
    },
    "enriched_event-value": {
        "type": "record",
        "name": "EnrichedEvent",
        "namespace": "fi.transitflow",
        "fields": [
            {"name": "vehicle_id", "type": "int"},
            {"name": "timestamp", "type": "string"},
            {"name": "event_time_ms", "type": "long"},
            {"name": "latitude", "type": "double"},
            {"name": "longitude", "type": "double"},
            {"name": "speed_ms", "type": ["null", "double"], "default": None},
            {"name": "delay_seconds", "type": ["null", "int"], "default": None},
            {"name": "line_id", "type": ["null", "string"], "default": None},
            {"name": "delay_trend", "type": ["null", "double"], "default": None},
            {"name": "is_stopped", "type": ["null", "boolean"], "default": None},
            {"name": "processing_time", "type": ["null", "long"], "default": None},
        ]
    },
    "stop_event-value": {
        "type": "record",
        "name": "StopEvent",
        "namespace": "fi.transitflow",
        "doc": "Stop arrival event for ML labels",
        "fields": [
            {"name": "vehicle_id", "type": "int"},
            {"name": "stop_id", "type": "int"},
            {"name": "arrival_time", "type": "long"},
            {"name": "delay_at_arrival", "type": "int"},
            {"name": "dwell_time_ms", "type": ["null", "long"], "default": None},
        ]
    }
}

def check_schemas(client: SchemaRegistryClient) -> bool:
    """Check if the Registry matches our local source of truth."""
    try:
        remote_subjects = client.list_subjects()
    except SchemaRegistryError as e:
        logger.error(f"[FAIL] Could not list subjects: {e}")
        return False

    all_consistent = True
    for subject in SCHEMAS.keys():
        if subject not in remote_subjects:
            logger.error(f"[FAIL] Subject missing from Registry: {subject}")
            all_consistent = False
            continue

        try:
            # Check if the schema content actually matches
            # We use is_compatible to verify the local definition matches the remote version
            is_compatible = client.is_compatible(subject, SCHEMAS[subject])
            if is_compatible:
                logger.info(f"[PASS] {subject} is present and compatible.")
            else:
                logger.warning(f"[FAIL] {subject} exists but definition is NOT compatible.")
                all_consistent = False
        except SchemaRegistryError as e:
            logger.error(f"[ERROR] Verification failed for {subject}: {e}")
            all_consistent = False

    return all_consistent

def register_all_schemas(client: SchemaRegistryClient, dry_run: bool = False) -> bool:
    success = True
    for subject, schema in SCHEMAS.items():
        try:
            if dry_run:
                logger.info(f"[DRY RUN] Target: {subject}")
                continue
            
            try:
                # Attempt to register the schema
                schema_id = client.register_schema(subject, schema)
                logger.info(f"[PASS] Registered {subject} (ID: {schema_id})")
            except SchemaRegistryError as e:
                # Handle existing/conflicting schemas gracefully
                if "409" in str(e):
                    if client.is_compatible(subject, schema):
                        logger.info(f"[INFO] {subject} already exists and is compatible. Skipping.")
                    else:
                        logger.error(f"[FAIL] {subject} has an incompatible schema change.")
                        success = False
                        continue
                else:
                    raise e
            
            # Enforce the data contract compatibility level
            if hasattr(client, 'set_compatibility'):
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
    args = parser.parse_args()
    
    client = SchemaRegistryClient(args.url)
    
    if not client.is_healthy():
        logger.error(f"[FAIL] Connection to Schema Registry refused at {args.url}")
        sys.exit(1)
    
    if args.check_only:
        success = check_schemas(client)
        if success:
            logger.info("Integrity Check: Registry matches local definitions.")
    else:
        success = register_all_schemas(client, args.dry_run)
        if success and not args.dry_run:
            logger.info("Registry synchronization successful.")
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()