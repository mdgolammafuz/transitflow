"""
Maintenance Job: VACUUM, Retention, Optimization
Pattern: Cloud Lifecycle & FinOps (DE#2 & Cloud#5)
Aligned: Uses UTC cutoff to match Delta Lake storage and event telemetry.
Professional: Forces explicit date targeting to ensure retention windows are deterministic.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone

from delta.tables import DeltaTable

# Project-wide absolute import consistency
from spark.config import create_spark_session, load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def apply_retention(spark, table_path, retention_days, table_name, base_date: str):
    """
    Purge old partitions to manage costs and storage.
    Aligned: Uses UTC to prevent timezone-drift from deleting fresh data.
    Deterministic: Uses base_date to calculate the cutoff.
    """
    try:
        if not DeltaTable.isDeltaTable(spark, table_path):
            logger.warning(f"Skipping retention: {table_name} table not found at {table_path}")
            return
            
        # Aligned: Cutoff calculation based on the mandatory processing date
        base_dt = datetime.strptime(base_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        cutoff_date = (base_dt - timedelta(days=retention_days)).strftime("%Y-%m-%d")
        
        delta_table = DeltaTable.forPath(spark, table_path)
        # Delete only old partitions to keep the storage clean and costs low
        delta_table.delete(f"date < '{cutoff_date}'")
        
        logger.info(f"Retention applied to {table_name}: Removed data older than {cutoff_date}")
    except Exception as e:
        logger.error(f"Retention failed for {table_name}: {e}")


def vacuum_table(spark, table_path, table_name, retention_hours=168):
    """
    Remove files no longer in the latest state of the transaction log.
    Default 168h (7 days) for safety to support Time Travel.
    """
    try:
        if not DeltaTable.isDeltaTable(spark, table_path):
            return
            
        logger.info(f"Vacuuming {table_name} (Retention: {retention_hours}h)...")
        DeltaTable.forPath(spark, table_path).vacuum(retention_hours)
        logger.info(f"Vacuum complete for {table_name}")
    except Exception as e:
        logger.error(f"Vacuum failed for {table_name}: {e}")


def optimize_table(spark, table_path, table_name):
    """
    Compact small files (Bin-packing) to improve read performance.
    Essential for high-velocity streaming data that creates many small Parquet files.
    """
    try:
        if not DeltaTable.isDeltaTable(spark, table_path):
            return
            
        logger.info(f"Optimizing layout for {table_name}...")
        DeltaTable.forPath(spark, table_path).optimize().executeCompaction()
        logger.info(f"Optimize complete for {table_name}")
    except Exception as e:
        logger.error(f"Optimize failed for {table_name}: {e}")


def main():
    parser = argparse.ArgumentParser(description="TransitFlow Lakehouse Maintenance")
    parser.add_argument(
        "--action", choices=["retention", "vacuum", "optimize", "all"], default="all"
    )
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all")
    # Professional: Date is mandatory to ensure retention windows are deterministic
    parser.add_argument("--date", type=str, required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()

    config = load_config()
    # create_spark_session handles the UTC lock and circuit breakers
    spark = create_spark_session("TransitFlow-Maintenance")

    # Disable safety check to allow vacuuming with short retention if needed for local dev
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    # Map table paths and retention policies from config
    tables = {
        "bronze": [
            (f"{config.bronze_path}/enriched", "bronze.enriched", config.bronze_retention_days),
            (f"{config.bronze_path}/stop_events", "bronze.stop_events", config.bronze_retention_days),
        ],
        "silver": [
            (f"{config.silver_path}/enriched", "silver.enriched", config.silver_retention_days),
            (f"{config.silver_path}/stop_events", "silver.stop_events", config.silver_retention_days),
        ],
        "gold": [
            (f"{config.gold_path}/daily_metrics", "gold.daily_metrics", config.gold_retention_days),
            (f"{config.gold_path}/stop_performance", "gold.stop_performance", config.gold_retention_days),
        ],
    }

    layers = ["bronze", "silver", "gold"] if args.layer == "all" else [args.layer]

    for layer in layers:
        for path, name, days in tables.get(layer, []):
            if args.action in ["retention", "all"]:
                apply_retention(spark, path, days, name, args.date)
            if args.action in ["vacuum", "all"]:
                vacuum_table(spark, path, name)
            if args.action in ["optimize", "all"]:
                optimize_table(spark, path, name)

    spark.stop()


if __name__ == "__main__":
    main()