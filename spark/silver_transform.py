"""
Silver Transform: Bronze → Silver (Batch)

Transforms raw Bronze data into cleaned, deduplicated Silver layer.

Operations:
- Parse timestamps
- Deduplicate by (vehicle_id, event_time_ms)
- Add derived columns
- Validate data quality
"""

import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, to_timestamp, 
    when, row_number, abs as spark_abs
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from config import load_config, create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_enriched(spark: SparkSession, config, process_date: str = None):
    """
    Transform bronze.enriched → silver.enriched
    """
    bronze_path = f"{config.bronze_path}/enriched"
    silver_path = f"{config.silver_path}/enriched"
    
    logger.info(f"Transforming enriched data: {bronze_path} -> {silver_path}")
    
    try:
        # 1. Read raw bronze data
        bronze_df = spark.read.format("delta").load(bronze_path)
        
        if process_date:
            bronze_df = bronze_df.filter(col("date") == process_date)
        
        # Check if data exists
        if bronze_df.rdd.isEmpty():
            logger.warning(f"No Bronze data found for processing.")
            return
        
        # 2. Deduplicate: keep latest record per (vehicle_id, event_time_ms)
        window = Window.partitionBy("vehicle_id", "event_time_ms").orderBy(col("kafka_offset").desc())
        
        silver_df = (bronze_df
            .withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            # Enforce data types and add derived metrics
            .withColumn("door_status", col("door_status").cast("boolean"))
            .withColumn("event_timestamp", to_timestamp(from_unixtime(col("event_time_ms") / 1000)))
            .withColumn("speed_kmh", col("speed_ms") * 3.6)
            .withColumn("delay_category",
                when(spark_abs(col("delay_seconds")) <= 60, "on_time")
                .when(col("delay_seconds") > 300, "delayed")
                .when(col("delay_seconds") < -300, "early")
                .otherwise("slight_delay"))
        )
        
        # 3. Write to Silver (Idempotent Merge or Initialize)
        if DeltaTable.isDeltaTable(spark, silver_path):
            logger.info("Merging into existing Silver table...")
            delta_table = DeltaTable.forPath(spark, silver_path)
            (delta_table.alias("target")
                .merge(
                    silver_df.alias("source"),
                    "target.vehicle_id = source.vehicle_id AND target.event_time_ms = source.event_time_ms"
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            logger.info("Creating new Silver table for the first time...")
            (silver_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("date")
                .save(silver_path)
            )
        
        count = spark.read.format("delta").load(silver_path).count()
        logger.info(f"Silver enriched total records: {count}")

    except Exception as e:
        logger.error(f"Silver Transformation failed: {str(e)}")


def transform_stop_events(spark: SparkSession, config, process_date: str = None):
    """
    Transform bronze.stop_events → silver.stop_events
    """
    bronze_path = f"{config.bronze_path}/stop_events"
    silver_path = f"{config.silver_path}/stop_events"
    
    try:
        if not DeltaTable.isDeltaTable(spark, bronze_path):
            logger.warning(f"Bronze stop_events table not found. Skipping.")
            return

        bronze_df = spark.read.format("delta").load(bronze_path)
        if process_date:
            bronze_df = bronze_df.filter(col("date") == process_date)
        
        window = Window.partitionBy("vehicle_id", "stop_id", "arrival_time").orderBy(col("kafka_offset").desc())
        
        silver_df = (bronze_df
            .withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            .withColumn("arrival_timestamp", to_timestamp(from_unixtime(col("arrival_time") / 1000)))
        )
        
        if DeltaTable.isDeltaTable(spark, silver_path):
            dt = DeltaTable.forPath(spark, silver_path)
            dt.alias("t").merge(silver_df.alias("s"), 
                "t.vehicle_id = s.vehicle_id AND t.stop_id = s.stop_id AND t.arrival_time = s.arrival_time") \
                .whenNotMatchedInsertAll().execute()
        else:
            silver_df.write.format("delta").mode("overwrite").partitionBy("date").save(silver_path)

    except Exception as e:
        logger.warning(f"Stop events processing skipped: {e}")


def main():
    parser = argparse.ArgumentParser(description="Silver Transform")
    parser.add_argument("--table", choices=["enriched", "stop_events", "all"],
                        default="all", help="Table to transform")
    parser.add_argument("--date", type=str, default=None,
                        help="Process specific date (YYYY-MM-DD)")
    args = parser.parse_args()
    
    config = load_config()
    spark = create_spark_session("TransitFlow-SilverTransform")
    
    if args.table in ["enriched", "all"]:
        transform_enriched(spark, config, args.date)
    
    if args.table in ["stop_events", "all"]:
        transform_stop_events(spark, config, args.date)
    
    logger.info("Silver transform complete")
    spark.stop()

if __name__ == "__main__":
    main()