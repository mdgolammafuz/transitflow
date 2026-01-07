"""
Gold Aggregation: Silver → Gold (Batch)
Creates aggregated tables for analytics:
- Daily metrics by line
- Hourly metrics by line
- Stop performance metrics
"""

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, avg, max as spark_max,
    min as spark_min, stddev, sum as spark_sum,
    hour, dayofweek, when
)
from delta.tables import DeltaTable
from config import load_config, create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def aggregate_daily_metrics(spark: SparkSession, config, process_date: str = None):
    silver_path = f"{config.silver_path}/enriched"
    gold_path = f"{config.gold_path}/daily_metrics"
    logger.info("Aggregating daily metrics")
    try:
        silver_df = spark.read.format("delta").load(silver_path)
        if process_date:
            silver_df = silver_df.filter(col("date") == process_date)
        
        daily_df = (silver_df
            .groupBy("line_id", "date")
            .agg(
                count("*").alias("total_events"),
                avg("delay_seconds").alias("avg_delay_seconds"),
                avg("speed_kmh").alias("avg_speed_kmh"),
                spark_sum(when(col("delay_category") == "on_time", 1).otherwise(0)).alias("on_time_count")
            )
            .withColumn("on_time_percentage", col("on_time_count") / col("total_events") * 100)
        )

        if DeltaTable.isDeltaTable(spark, gold_path):
            dt = DeltaTable.forPath(spark, gold_path)
            dt.alias("t").merge(daily_df.alias("s"), "t.line_id = s.line_id AND t.date = s.date") \
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            daily_df.write.format("delta").mode("overwrite").partitionBy("date").save(gold_path)
    except Exception as e:
        logger.error(f"Daily aggregation failed: {e}")

def aggregate_hourly_metrics(spark: SparkSession, config, process_date: str = None):
    silver_path = f"{config.silver_path}/enriched"
    gold_path = f"{config.gold_path}/hourly_metrics"
    logger.info("Aggregating hourly metrics")
    try:
        silver_df = spark.read.format("delta").load(silver_path)
        if process_date:
            silver_df = silver_df.filter(col("date") == process_date)
        
        hourly_df = (silver_df
            .withColumn("hour", hour("event_timestamp"))
            .groupBy("line_id", "date", "hour")
            .agg(
                count("*").alias("total_events"),
                avg("delay_seconds").alias("avg_delay_seconds"),
                avg("speed_kmh").alias("avg_speed_kmh")
            )
        )

        if DeltaTable.isDeltaTable(spark, gold_path):
            dt = DeltaTable.forPath(spark, gold_path)
            dt.alias("t").merge(hourly_df.alias("s"), 
                "t.line_id = s.line_id AND t.date = s.date AND t.hour = s.hour") \
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            hourly_df.write.format("delta").mode("overwrite").partitionBy("date").save(gold_path)
    except Exception as e:
        logger.error(f"Hourly aggregation failed: {e}")

def aggregate_stop_performance(spark: SparkSession, config, process_date: str = None):
    silver_path = f"{config.silver_path}/stop_events"
    gold_path = f"{config.gold_path}/stop_performance"
    logger.info("Aggregating stop performance")
    try:
        silver_df = spark.read.format("delta").load(silver_path)
        stop_df = (silver_df
            .withColumn("hour_of_day", hour("arrival_timestamp"))
            .withColumn("day_of_week", dayofweek("arrival_timestamp"))
            .groupBy("stop_id", "line_id", "hour_of_day", "day_of_week")
            .agg(
                count("*").alias("arrival_count"),
                avg("delay_at_arrival").alias("avg_delay"),
                avg("dwell_time_ms").alias("avg_dwell_time_ms")
            )
        )

        if DeltaTable.isDeltaTable(spark, gold_path):
            dt = DeltaTable.forPath(spark, gold_path)
            dt.alias("t").merge(stop_df.alias("s"), 
                "t.stop_id = s.stop_id AND t.line_id = s.line_id AND t.hour_of_day = s.hour_of_day AND t.day_of_week = s.day_of_week") \
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            stop_df.write.format("delta").mode("overwrite").save(gold_path)
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