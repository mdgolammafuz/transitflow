"""
Gold Aggregation: Silver → Gold (Batch)
Creates aggregated tables for analytics and Feature Store (Postgres).
"""

import argparse
import logging
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, col, count, dayofweek, hour
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when

# Absolute import for package consistency
from spark.config import create_spark_session, load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_to_postgres(df: DataFrame, table_name: str, config):
    """
    Helper to sync Gold data to the Postgres Feature Store.
    Note: We write to 'public' schema so dbt can pick it up as a raw source.
    """
    logger.info(f"Syncing {table_name} to Postgres")
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


def aggregate_hourly_metrics(spark: SparkSession, config, process_date: Optional[str] = None):
    """Aggregate enriched data into hourly snapshots."""
    silver_path = f"{config.silver_path}/enriched"
    gold_path = f"{config.gold_path}/hourly_metrics"
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


def aggregate_stop_performance(spark: SparkSession, config, process_date: Optional[str] = None):
    """Aggregate stop events for ML feature engineering."""
    silver_path = f"{config.silver_path}/stop_events"
    gold_path = f"{config.gold_path}/stop_performance"
    logger.info("Aggregating stop performance")
    try:
        silver_df = spark.read.format("delta").load(silver_path)
        if process_date:
            silver_df = silver_df.filter(col("date") == process_date)

        stop_df = (
            silver_df.withColumn("hour_of_day", hour("arrival_timestamp"))
            .withColumn("day_of_week", dayofweek("arrival_timestamp"))
            .groupBy("stop_id", "line_id", "hour_of_day", "day_of_week")
            .agg(
                count("*").alias("arrival_count"),
                spark_round(avg("delay_at_arrival"), 2).alias("avg_delay"),
                spark_round(avg("dwell_time_ms"), 0).alias("avg_dwell_time_ms"),
            )
        )

        # 1. Update Lakehouse (MinIO)
        if DeltaTable.isDeltaTable(spark, gold_path):
            dt = DeltaTable.forPath(spark, gold_path)
            dt.alias("t").merge(
                stop_df.alias("s"),
                "t.stop_id = s.stop_id AND t.line_id = s.line_id "
                "AND t.hour_of_day = s.hour_of_day AND t.day_of_week = s.day_of_week",
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            stop_df.write.format("delta").mode("overwrite").save(gold_path)

        # 2. Update Postgres (Source for dbt)
        write_to_postgres(stop_df, "public.fct_stop_arrivals", config)

    except Exception as e:
        logger.error(f"Stop performance aggregation failed: {e}")


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