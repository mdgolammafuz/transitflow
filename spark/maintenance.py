"""
Maintenance Job: VACUUM, Retention, Optimization
"""

import argparse
import logging
from datetime import datetime, timedelta

from delta.tables import DeltaTable

from spark.config import create_spark_session, load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def apply_retention(spark, table_path, retention_days, table_name):
    try:
        if not DeltaTable.isDeltaTable(spark, table_path):
            return
        cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime("%Y-%m-%d")
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.delete(f"date < '{cutoff_date}'")
        logger.info(f"Retention applied to {table_name}")
    except Exception as e:
        logger.error(f"Retention failed for {table_name}: {e}")


def vacuum_table(spark, table_path, table_name, retention_hours=168):
    try:
        if not DeltaTable.isDeltaTable(spark, table_path):
            return
        DeltaTable.forPath(spark, table_path).vacuum(retention_hours)
        logger.info(f"Vacuum complete for {table_name}")
    except Exception as e:
        logger.error(f"Vacuum failed for {table_name}: {e}")


def optimize_table(spark, table_path, table_name):
    try:
        if not DeltaTable.isDeltaTable(spark, table_path):
            return
        DeltaTable.forPath(spark, table_path).optimize().executeCompaction()
        logger.info(f"Optimize complete for {table_name}")
    except Exception as e:
        logger.error(f"Optimize failed for {table_name}: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--action", choices=["retention", "vacuum", "optimize", "all"], default="all"
    )
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all")
    args = parser.parse_args()

    config = load_config()
    spark = create_spark_session("TransitFlow-Maintenance")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    tables = {
        "bronze": [
            (f"{config.bronze_path}/enriched", "bronze.enriched", config.bronze_retention_days)
        ],
        "silver": [
            (f"{config.silver_path}/enriched", "silver.enriched", config.silver_retention_days)
        ],
        "gold": [
            (f"{config.gold_path}/daily_metrics", "gold.daily_metrics", config.gold_retention_days)
        ],
    }

    layers = ["bronze", "silver", "gold"] if args.layer == "all" else [args.layer]

    for layer in layers:
        for path, name, days in tables.get(layer, []):
            if args.action in ["retention", "all"]:
                apply_retention(spark, path, days, name)
            if args.action in ["vacuum", "all"]:
                vacuum_table(spark, path, name)
            if args.action in ["optimize", "all"]:
                optimize_table(spark, path, name)

    spark.stop()


if __name__ == "__main__":
    main()
