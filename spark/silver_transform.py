"""
Silver Transform: Bronze -> Silver (Batch)
Transforms raw Bronze data into cleaned, deduplicated Silver layer.

Optimized for 8GB RAM environments:
- Removed expensive RDD isEmpty() calls
- Streamlined window functions
- Uses inter-batch deduplication via Delta Merge
- Aligned: Explicit UTC timestamp casting to prevent timezone leaks.
"""

import argparse
import logging
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window

# Absolute import for package consistency
from spark.config import create_spark_session, load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_enriched(spark: SparkSession, config, process_date: str):
    """Transform bronze.enriched -> silver.enriched"""
    bronze_path = f"{config.bronze_path}/enriched"
    silver_path = f"{config.silver_path}/enriched"

    logger.info(f"Transforming enriched partition {process_date}: {bronze_path} -> {silver_path}")

    try:
        if not DeltaTable.isDeltaTable(spark, bronze_path):
            logger.warning(f"Bronze table not found at {bronze_path}. Skipping.")
            return

        # 1. Read raw bronze data - STRICTLY filter by date for RAM stability
        bronze_df = spark.read.format("delta").load(bronze_path).filter(col("date") == process_date)

        # 2. Intra-batch Deduplication: keep latest record per (vehicle_id, event_time_ms)
        # Aligned: vehicle_id is String to match Phase 1 & 2
        window = Window.partitionBy("vehicle_id", "event_time_ms").orderBy(
            col("kafka_offset").desc()
        )

        silver_df = (
            bronze_df.withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            # Safety: Drop pings with missing spatial data
            .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
            .withColumn("door_status", col("door_status").cast("boolean"))
            # CRITICAL: Force UTC by casting long to timestamp directly (avoids OS TZ drift)
            .withColumn("event_timestamp", (col("event_time_ms") / 1000).cast("timestamp"))
            .withColumn("speed_kmh", col("speed_ms") * 3.6)
            .withColumn(
                "delay_category",
                when(spark_abs(col("delay_seconds")) <= 60, "on_time")
                .when(col("delay_seconds") > 300, "delayed")
                .when(col("delay_seconds") < -300, "early")
                .otherwise("slight_delay"),
            )
        )

        # 3. Inter-batch Deduplication (Merge)
        # Prevents duplicates if the batch is re-run
        if DeltaTable.isDeltaTable(spark, silver_path):
            delta_table = DeltaTable.forPath(spark, silver_path)
            (
                delta_table.alias("target")
                .merge(
                    silver_df.alias("source"),
                    "target.vehicle_id = source.vehicle_id AND target.event_time_ms = source.event_time_ms AND target.date = source.date",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            silver_df.write.format("delta").mode("overwrite").partitionBy("date").save(silver_path)

    except Exception as e:
        logger.error(f"Silver Enriched Transformation failed for {process_date}: {str(e)}")


def transform_stop_events(spark: SparkSession, config, process_date: str):
    """Transform bronze.stop_events -> silver.stop_events"""
    bronze_path = f"{config.bronze_path}/stop_events"
    silver_path = f"{config.silver_path}/stop_events"

    try:
        if not DeltaTable.isDeltaTable(spark, bronze_path):
            logger.warning(f"Bronze table not found at {bronze_path}. Skipping.")
            return

        # STRICTLY filter by date
        bronze_df = spark.read.format("delta").load(bronze_path).filter(col("date") == process_date)

        # Aligned: stop_id is String to match Metadata
        window = Window.partitionBy("vehicle_id", "stop_id", "arrival_time").orderBy(
            col("kafka_offset").desc()
        )

        silver_df = (
            bronze_df.withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            # CRITICAL: Force UTC conversion
            .withColumn("arrival_timestamp", (col("arrival_time") / 1000).cast("timestamp"))
        )

        if DeltaTable.isDeltaTable(spark, silver_path):
            dt = DeltaTable.forPath(spark, silver_path)
            dt.alias("t").merge(
                silver_df.alias("s"),
                "t.vehicle_id = s.vehicle_id AND t.stop_id = s.stop_id AND t.arrival_time = s.arrival_time AND t.date = s.date",
            ).whenNotMatchedInsertAll().execute()
        else:
            silver_df.write.format("delta").mode("overwrite").partitionBy("date").save(silver_path)

    except Exception as e:
        logger.warning(f"Stop events processing failed for {process_date}: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=["enriched", "stop_events", "all"], default="all")
    # Robustness: date is now required for OCI/Cron stability
    parser.add_argument("--date", type=str, required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    config = load_config()
    spark = create_spark_session("TransitFlow-SilverTransform")

    if args.table in ["enriched", "all"]:
        transform_enriched(spark, config, args.date)
    if args.table in ["stop_events", "all"]:
        transform_stop_events(spark, config, args.date)

    spark.stop()


if __name__ == "__main__":
    main()