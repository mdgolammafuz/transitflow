"""
Bronze Writer: Kafka → Delta Lake
Supports both Continuous Streaming and Batch Execution.
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_date
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark.config import create_spark_session, load_config

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeWriter")

# Schema Alignment
ENRICHED_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), False),
        StructField("timestamp", StringType(), True),
        StructField("event_time_ms", LongType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed_ms", DoubleType(), True),
        StructField("heading", IntegerType(), True),
        StructField("delay_seconds", IntegerType(), True),
        StructField("door_status", IntegerType(), True),
        StructField("line_id", StringType(), True),
        StructField("direction_id", IntegerType(), True),
        StructField("operator_id", IntegerType(), True),
        StructField("next_stop_id", StringType(), True),
        StructField("delay_trend", DoubleType(), True),
        StructField("speed_trend", DoubleType(), True),
        StructField("distance_since_last_m", DoubleType(), True),
        StructField("time_since_last_ms", LongType(), True),
        StructField("is_stopped", BooleanType(), True),
        StructField("stopped_duration_ms", LongType(), True),
        StructField("processing_time", LongType(), True),
    ]
)

STOP_EVENTS_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), False),
        StructField("stop_id", StringType(), True),
        StructField("line_id", StringType(), True),
        StructField("direction_id", IntegerType(), True),
        StructField("arrival_time", LongType(), True),
        StructField("delay_at_arrival", IntegerType(), True),
        StructField("dwell_time_ms", LongType(), True),
        StructField("door_status", IntegerType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]
)


def upsert_to_delta(micro_batch_df, batch_id, output_path):
    from delta.tables import DeltaTable
    
    # If the dataframe is empty, skip to save time
    if micro_batch_df.rdd.isEmpty():
        return

    if not DeltaTable.isDeltaTable(micro_batch_df.sparkSession, output_path):
        micro_batch_df.write.format("delta").mode("append").partitionBy("date").save(output_path)
    else:
        dt = DeltaTable.forPath(micro_batch_df.sparkSession, output_path)
        
        cols = micro_batch_df.columns
        if "event_time_ms" in cols:
            condition = "t.vehicle_id = s.vehicle_id AND t.date = s.date AND t.event_time_ms = s.event_time_ms"
        elif "arrival_time" in cols:
            condition = "t.vehicle_id = s.vehicle_id AND t.date = s.date AND t.arrival_time = s.arrival_time"
        else:
            condition = "t.vehicle_id = s.vehicle_id AND t.date = s.date"

        dt.alias("t").merge(
            micro_batch_df.alias("s"),
            condition
        ).whenNotMatchedInsertAll().execute()

def write_bronze_stream(
    spark: SparkSession,
    kafka_servers: str,
    topic: str,
    schema: StructType,
    output_path: str,
    checkpoint_path: str,
    table_name: str,
    run_once: bool = False
):
    logger.info(f"Initializing stream: {table_name} (Mode: {'BATCH/ONCE' if run_once else 'CONTINUOUS'})")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "data.*",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            current_timestamp().alias("ingestion_time"),
        )
        .withColumn("date", to_date(col("kafka_timestamp")))
    )

    writer = parsed_df.writeStream \
        .foreachBatch(lambda df, batch_id: upsert_to_delta(df, batch_id, output_path)) \
        .option("checkpointLocation", checkpoint_path)

    if run_once:
        # Trigger AvailableNow consumes all data available in Kafka then stops.
        # This is CRITICAL for the batch pipeline to proceed.
        return writer.trigger(availableNow=True).start()
    else:
        return writer.trigger(processingTime="10 seconds").start()


def main():
    parser = argparse.ArgumentParser(description="TransitFlow Bronze Writer")
    parser.add_argument("--table", choices=["enriched", "stop_events", "all"], default="all")
    # Essential for run_batch_pipeline
    parser.add_argument("--once", action="store_true", help="Process all available data and exit (Batch Mode)")
    args = parser.parse_args()

    config = load_config()
    spark = create_spark_session("TransitFlow-BronzeWriter")

    queries = []

    if args.table in ["enriched", "all"]:
        queries.append(
            write_bronze_stream(
                spark,
                config.kafka_bootstrap_servers,
                config.kafka_enriched_topic,
                ENRICHED_SCHEMA,
                f"{config.bronze_path}/enriched",
                config.get_checkpoint_path("bronze_enriched"),
                "bronze.enriched",
                run_once=args.once
            )
        )

    if args.table in ["stop_events", "all"]:
        queries.append(
            write_bronze_stream(
                spark,
                config.kafka_bootstrap_servers,
                config.kafka_stops_topic,
                STOP_EVENTS_SCHEMA,
                f"{config.bronze_path}/stop_events",
                config.get_checkpoint_path("bronze_stop_events"),
                "bronze.stop_events",
                run_once=args.once
            )
        )

    # Wait for completion
    for q in queries:
        q.awaitTermination()

if __name__ == "__main__":
    main()