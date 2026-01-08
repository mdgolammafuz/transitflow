"""
Silver Transform: Bronze -> Silver (Batch)
Transforms raw Bronze data into cleaned, deduplicated Silver layer.

Optimized for 8GB RAM environments:
- Removed expensive RDD isEmpty() calls
- Streamlined window functions
- Uses inter-batch deduplication via Delta Merge
"""

import argparse
import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import col, from_unixtime, row_number, to_timestamp, when
from pyspark.sql.window import Window

# Absolute import for package consistency
from spark.config import create_spark_session, load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_enriched(spark: SparkSession, config, process_date: str = None):
    """Transform bronze.enriched -> silver.enriched"""
    bronze_path = f"{config.bronze_path}/enriched"
    silver_path = f"{config.silver_path}/enriched"

    logger.info(f"Transforming enriched: {bronze_path} -> {silver_path}")

    try:
        # 1. Read raw bronze data
        bronze_df = spark.read.format("delta").load(bronze_path)

        if process_date:
            bronze_df = bronze_df.filter(col("date") == process_date)

        # 2. Intra-batch Deduplication: keep latest record per (vehicle_id, event_time_ms)
        window = Window.partitionBy("vehicle_id", "event_time_ms").orderBy(
            col("kafka_offset").desc()
        )

        silver_df = (
            bronze_df.withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            .withColumn("door_status", col("door_status").cast("boolean"))
            .withColumn("event_timestamp", to_timestamp(from_unixtime(col("event_time_ms") / 1000)))
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
        if DeltaTable.isDeltaTable(spark, silver_path):
            delta_table = DeltaTable.forPath(spark, silver_path)
            (
                delta_table.alias("target")
                .merge(
                    silver_df.alias("source"),
                    "target.vehicle_id = source.vehicle_id AND target.event_time_ms = source.event_time_ms",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            silver_df.write.format("delta").mode("overwrite").partitionBy("date").save(silver_path)

    except Exception as e:
        logger.error(f"Silver Enriched Transformation failed: {str(e)}")


def transform_stop_events(spark: SparkSession, config, process_date: str = None):
    """Transform bronze.stop_events -> silver.stop_events"""
    bronze_path = f"{config.bronze_path}/stop_events"
    silver_path = f"{config.silver_path}/stop_events"

    try:
        # Check if Bronze exists before reading
        if not DeltaTable.isDeltaTable(spark, bronze_path):
            return

        bronze_df = spark.read.format("delta").load(bronze_path)
        if process_date:
            bronze_df = bronze_df.filter(col("date") == process_date)

        window = Window.partitionBy("vehicle_id", "stop_id", "arrival_time").orderBy(
            col("kafka_offset").desc()
        )

        silver_df = (
            bronze_df.withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            .withColumn(
                "arrival_timestamp", to_timestamp(from_unixtime(col("arrival_time") / 1000))
            )
        )

        if DeltaTable.isDeltaTable(spark, silver_path):
            dt = DeltaTable.forPath(spark, silver_path)
            dt.alias("t").merge(
                silver_df.alias("s"),
                "t.vehicle_id = s.vehicle_id AND t.stop_id = s.stop_id AND t.arrival_time = s.arrival_time",
            ).whenNotMatchedInsertAll().execute()
        else:
            silver_df.write.format("delta").mode("overwrite").partitionBy("date").save(silver_path)

    except Exception as e:
        logger.warning(f"Stop events processing skipped: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=["enriched", "stop_events", "all"], default="all")
    parser.add_argument("--date", type=str, default=None)
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
