"""
Bronze Writer: Kafka → Delta Lake (Streaming)
Pattern: Raw Data is Immutable
Pattern: Idempotent Operations (Exactly-once via Checkpointing)
"""

import argparse
import logging

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

# Schema for enriched events (Synchronized with EnrichedEvent.java)
ENRICHED_SCHEMA = StructType(
    [
        StructField("vehicle_id", IntegerType(), False),
        StructField("timestamp", StringType(), True),
        StructField("event_time_ms", LongType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed_ms", DoubleType(), True),
        StructField("heading", IntegerType(), True),
        StructField("delay_seconds", IntegerType(), True),
        StructField("door_status", BooleanType(), True),  # Aligned to Phase 2 Boolean fix
        StructField("line_id", StringType(), True),
        StructField("direction_id", IntegerType(), True),
        StructField("operator_id", IntegerType(), True),
        StructField("next_stop_id", IntegerType(), True),
        StructField("delay_trend", DoubleType(), True),
        StructField("speed_trend", DoubleType(), True),
        StructField("distance_since_last_m", DoubleType(), True),
        StructField("time_since_last_ms", LongType(), True),
        StructField("is_stopped", BooleanType(), True),
        StructField("stopped_duration_ms", LongType(), True),
        StructField("processing_time", LongType(), True),
    ]
)

# Schema for stop events (Synchronized with StopArrival.java)
STOP_EVENTS_SCHEMA = StructType(
    [
        StructField("vehicle_id", IntegerType(), False),
        StructField("stop_id", IntegerType(), True),
        StructField("line_id", StringType(), True),
        StructField("direction_id", IntegerType(), True),
        StructField("arrival_time", LongType(), True),
        StructField("delay_at_arrival", IntegerType(), True),
        StructField("dwell_time_ms", LongType(), True),
        StructField("door_opened", BooleanType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]
)


def write_bronze_stream(
    spark: SparkSession,
    kafka_servers: str,
    topic: str,
    schema: StructType,
    output_path: str,
    checkpoint_path: str,
    table_name: str,
):
    """
    Kafka to Delta Lake Bronze layer.
    DE#2: Append-only, never update or delete.
    DE#6: Exactly-once via Checkpointing.
    """
    logger.info(f"Initializing stream: {table_name}")

    # 1. Source: Kafka
    # failOnDataLoss=false is used for development flexibility
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Transform: Parse JSON and Add Audit Metadata
    parsed_df = (
        kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        ).select(
            "data.*",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            current_timestamp().alias("ingestion_time"),
        )
        # Partitioning by date is critical for VACUUM and performance
        .withColumn("date", to_date(col("kafka_timestamp")))
    )

    # 3. Sink: Delta Lake (Append Mode)
    # Using 'trigger' to balance between latency and file size
    return (
        parsed_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("date")
        .trigger(processingTime="10 seconds")
        .start(output_path)
    )


def main():
    parser = argparse.ArgumentParser(description="TransitFlow Bronze Writer")
    parser.add_argument(
        "--table",
        choices=["enriched", "stop_events", "all"],
        default="all",
        help="Specific table to process",
    )
    args = parser.parse_args()

    config = load_config()
    spark = create_spark_session("TransitFlow-BronzeWriter")

    queries = []

    # Processing Enriched Events
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
            )
        )

    # Processing Stop Events
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
            )
        )

    logger.info("Streams active. Waiting for termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
