"""
Gold Aggregation: Silver → Gold (Batch)
Creates aggregated tables for analytics and Feature Store (Postgres).
Methodical: Fails fast if source data or metadata is missing.
Aligned: Forces Lakehouse schema to match the methodical 'historical_*' naming.
"""

import argparse
import logging
import sys
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, dayofweek, hour, when
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum

# Absolute import for package consistency
from spark.config import create_spark_session, load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def validate_source(spark: SparkSession, path: str, name: str):
    """Checks if a Delta table exists. If not, exits to prevent hangs."""
    if not DeltaTable.isDeltaTable(spark, path):
        logger.critical(f"DEPENDENCY MISSING: {name} table not found at {path}")
        logger.critical(f"Please ensure previous pipeline steps (Silver/Metadata) have completed.")
        sys.exit(1)
    return True


def write_to_postgres(df: DataFrame, table_name: str, config):
    """Sync Gold data to the Postgres Feature Store (Marts)."""
    logger.info(f"Syncing {table_name} to Postgres Marts...")
    df.write \
        .format("jdbc") \
        .option("url", config.postgres_jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config.postgres_user) \
        .option("password", config.postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()


def aggregate_daily_metrics(spark: SparkSession, config, process_date: Optional[str] = None):
    """Aggregate enriched data into daily KPIs."""
    silver_path = f"{config.silver_path}/enriched"
    gold_path = f"{config.gold_path}/daily_metrics"
    
    validate_source(spark, silver_path, "Silver Enriched")
    logger.info("Aggregating daily metrics")
    
    try:
        silver_df = spark.read.format("delta").load(silver_path)
        if process_date:
            silver_df = silver_df.filter(col("date") == process_date)

        daily_df = (
            silver_df.groupBy("line_id", "date")
            .agg(
                count("*").alias("total_events"),
                spark_round(avg("delay_seconds"), 2).alias("avg_delay_seconds"),
                spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
                spark_sum(when(col("delay_category") == "on_time", 1).otherwise(0)).alias(
                    "on_time_count"
                ),
            )
            .withColumn(
                "on_time_percentage",
                spark_round((col("on_time_count") / col("total_events")) * 100, 2),
            )
        )

        # 1. Update Lakehouse (MinIO)
        if DeltaTable.isDeltaTable(spark, gold_path):
            dt = DeltaTable.forPath(spark, gold_path)
            dt.alias("t").merge(
                daily_df.alias("s"), "t.line_id = s.line_id AND t.date = s.date"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            daily_df.write.format("delta").mode("overwrite").partitionBy("date").save(gold_path)

        # 2. Update Postgres (Source for dbt)
        write_to_postgres(daily_df, "public.fct_daily_performance", config)

    except Exception as e:
        logger.error(f"Daily aggregation failed: {e}")
        sys.exit(1)


def aggregate_hourly_metrics(spark: SparkSession, config, process_date: Optional[str] = None):
    """Aggregate enriched data into hourly snapshots."""
    silver_path = f"{config.silver_path}/enriched"
    gold_path = f"{config.gold_path}/hourly_metrics"
    
    validate_source(spark, silver_path, "Silver Enriched")
    logger.info("Aggregating hourly metrics")
    
    try:
        silver_df = spark.read.format("delta").load(silver_path)
        if process_date:
            silver_df = silver_df.filter(col("date") == process_date)

        hourly_df = (
            silver_df.withColumn("hour_val", hour("event_timestamp"))
            .groupBy("line_id", "date", "hour_val")
            .agg(
                count("*").alias("total_events"),
                spark_round(avg("delay_seconds"), 2).alias("avg_delay_seconds"),
                spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
            )
            .withColumnRenamed("hour_val", "hour")
        )

        # 1. Update Lakehouse (MinIO)
        if DeltaTable.isDeltaTable(spark, gold_path):
            dt = DeltaTable.forPath(spark, gold_path)
            dt.alias("t").merge(
                hourly_df.alias("s"),
                "t.line_id = s.line_id AND t.date = s.date AND t.hour = s.hour",
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            hourly_df.write.format("delta").mode("overwrite").partitionBy("date").save(gold_path)

    except Exception as e:
        logger.error(f"Hourly aggregation failed: {e}")
        sys.exit(1)

def write_to_postgres(df: DataFrame, table_name: str, config):
    """
    Syncs Gold data to Postgres Marts.
    Methodical: Uses truncate=true to empty the table without dropping it,
    preserving downstream dbt view dependencies.
    """
    logger.info(f"Syncing {table_name} to Postgres Marts (Truncate Mode)...")
    df.write \
        .format("jdbc") \
        .option("url", config.postgres_jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config.postgres_user) \
        .option("password", config.postgres_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .save()

def aggregate_stop_performance(spark: SparkSession, config, process_date: Optional[str] = None):
    """
    Aggregate stop events for ML feature engineering with coordinate enrichment.
    Methodical: Uses INNER JOIN to filter out stops missing Ingredient B (GPS),
    ensuring compliance with dbt 'not_null' contracts and avoiding NULL pollution.
    """
    silver_path = f"{config.silver_path}/stop_events"
    metadata_path = f"{config.gold_path}/stops" 
    gold_path = f"{config.gold_path}/stop_performance"
    
    # 1. Dependency Validation
    validate_source(spark, silver_path, "Silver Stop Events")
    validate_source(spark, metadata_path, "Gold Stop Metadata")
    
    logger.info("Aggregating stop performance and enforcing spatial integrity")
    try:
        # 2. Load Data
        silver_df = spark.read.format("delta").load(silver_path)
        if process_date:
            silver_df = silver_df.filter(col("date") == process_date)

        # 3. Load Metadata (Ingredient B - Coordinates)
        stops_metadata = spark.read.format("delta").load(metadata_path).select(
            col("stop_id"),
            col("stop_lat").alias("latitude"),
            col("stop_lon").alias("longitude")
        )

        # 4. Calculate Metrics with Methodical Naming
        stop_metrics = (
            silver_df.withColumn("hour_of_day", hour("arrival_timestamp"))
            .withColumn("day_of_week", dayofweek("arrival_timestamp"))
            .groupBy("stop_id", "line_id", "hour_of_day", "day_of_week")
            .agg(
                count("*").alias("historical_arrival_count"),
                spark_round(avg("delay_at_arrival"), 2).alias("historical_avg_delay"),
                spark_round(avg("dwell_time_ms"), 0).alias("avg_dwell_time_ms"),
            )
        )

        # 5. Join with Ingredient B (GPS coordinates)
        # INNER JOIN: Only promotes stops with valid GPS. 
        # This prevents 'not_null' test failures in dbt for the 13 problematic stations.
        enriched_stop_df = stop_metrics.join(stops_metadata, on="stop_id", how="inner")

        # 6. Update Lakehouse (MinIO)
        logger.info(f"Writing to Lakehouse: {gold_path}")
        enriched_stop_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(gold_path)

        # 7. Update Postgres (Marts for Feature API)
        # This uses the truncate-enabled write function to avoid DROP errors.
        write_to_postgres(enriched_stop_df, "public.fct_stop_arrivals", config)
        
        logger.info("SUCCESS: Gold stop performance updated. Spatial integrity enforced.")

    except Exception as e:
        logger.error(f"Stop performance aggregation failed: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=["daily", "hourly", "stops", "all"], default="all")
    parser.add_argument("--date", type=str, default=None)
    args = parser.parse_args()

    config = load_config()
    spark = create_spark_session("TransitFlow-GoldAggregation")

    if args.table in ["daily", "all"]:
        aggregate_daily_metrics(spark, config, args.date)
    if args.table in ["hourly", "all"]:
        aggregate_hourly_metrics(spark, config, args.date)
    if args.table in ["stops", "all"]:
        aggregate_stop_performance(spark, config, args.date)

    spark.stop()


if __name__ == "__main__":
    main()