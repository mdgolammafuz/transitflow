
"""
Spark Batch Processing for Bronze Layer (AVRO MODE).

Reads BINARY Avro from Kafka, fetches Schema from Registry,
and writes to Delta Lake.

Principle #2: Raw Data is Immutable Truth
Principle #5: Streaming and Batch Must Agree

Usage:
    spark-submit spark/bronze_ingestion.py --mode backfill
"""

import argparse
import os
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, to_timestamp, lit, window, avg, 
    max as spark_max, min as spark_min, count, countDistinct, current_timestamp
)
from pyspark.sql.avro.functions import from_avro

# =============================================================================
# Configuration
# =============================================================================

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
LAKEHOUSE_PATH = os.getenv("LAKEHOUSE_PATH", "data/lakehouse")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "data/checkpoints")

# =============================================================================
# Helper: Fetch Schema from Registry
# =============================================================================

def get_latest_schema_from_registry(subject: str) -> str:
    """
    Connects to Schema Registry and gets the latest Avro schema string.
    Spark needs the raw JSON schema string to decode the binary data.
    """
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    try:
        response = requests.get(url)
        response.raise_for_status()
        schema_info = response.json()
        print(f"   Fetched schema for {subject} (Version {schema_info['version']})")
        return schema_info['schema']
    except Exception as e:
        raise RuntimeError(f"Failed to fetch schema for {subject}: {e}")

# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session(app_name: str = "BerlinAirQuality") -> SparkSession:
    return (SparkSession.builder
        .appName(app_name)
        # Added spark-avro dependency
        .config("spark.jars.packages", 
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "org.apache.spark:spark-avro_2.12:3.4.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate())

# =============================================================================
# Ingestion Logic (Avro Decoder)
# =============================================================================

def ingest_stream(spark: SparkSession, topic: str, subject: str, bronze_table: str, mode: str):
    """
    Generic function to ingest Avro data.
    
    CRITICAL: Confluent Avro format adds 5 bytes (Magic Byte + Schema ID) 
    to the start of the message. We must skip them to let Spark decode it.
    """
    print(f"Ingesting {topic} (Avro Mode: {mode})...")
    
    # 1. Fetch Schema
    avro_schema_json = get_latest_schema_from_registry(subject)
    
    # 2. Read Kafka
    reader = spark.read if mode == "batch" else spark.readStream
    df = (reader
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest") 
        .load())

    # 3. Decode Avro (Skipping 5-byte Confluent Header)
    # The 'value' column is binary. We slice it from byte 6 to the end.
    parsed = (df
        .withColumn("fixed_value", expr("substring(value, 6, length(value)-5)"))
        .select(
            from_avro(col("fixed_value"), avro_schema_json).alias("data"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp")
        ))

    # 4. Flatten & Standardize
    if "pm25" in topic:
        # PM2.5 Schema
        final_df = parsed.select(
            col("data.station_id"),
            col("data.station_name"),
            # FIX: data.timestamp is already a TimestampType because of Avro logicalType
            col("data.timestamp").alias("measurement_timestamp"),
            col("data.value_ugm3"),
            col("data.is_valid"),
            col("kafka_topic"),
            lit("spark").alias("processing_engine"),
            current_timestamp().alias("processed_at")
        )
    else:
        # Weather Schema
        final_df = parsed.select(
            # FIX: data.timestamp is already a TimestampType
            col("data.timestamp").alias("observation_timestamp"),
            col("data.temperature_c"),
            col("data.wind_speed_ms"),
            col("data.humidity_pct"),
            col("data.is_forecast"),
            col("kafka_topic"),
            lit("spark").alias("processing_engine"),
            current_timestamp().alias("processed_at")
        )

    # 5. Write to Delta
    path = f"{LAKEHOUSE_PATH}/bronze/{bronze_table}"
    
    if mode == "batch":
        final_df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
        print(f"   Wrote batch to {path}")
    else:
        chk_path = f"{CHECKPOINT_PATH}/{bronze_table}"
        query = final_df.writeStream.format("delta").outputMode("append").option("checkpointLocation", chk_path).start(path)
        query.awaitTermination()

# =============================================================================
# Hourly Aggregation
# =============================================================================

def compute_aggregates(spark: SparkSession):
    print("Computing Aggregates...")
    bronze_path = f"{LAKEHOUSE_PATH}/bronze/pm25"
    hourly_path = f"{LAKEHOUSE_PATH}/bronze/hourly_pm25"
    
    df = spark.read.format("delta").load(bronze_path)
    
    hourly = (df
        .filter(col("value_ugm3") >= 0)
        .withColumn("hour", window(col("measurement_timestamp"), "1 hour"))
        .groupBy("hour")
        .agg(
            avg("value_ugm3").alias("pm25_mean"),
            spark_max("value_ugm3").alias("pm25_max"),
            count("*").alias("readings_count")
        )
        .select(
            col("hour.start").alias("window_start"),
            col("hour.end").alias("window_end"),
            col("pm25_mean"),
            col("pm25_max"),
            col("readings_count"),
            lit("spark").alias("processing_engine"),
            current_timestamp().alias("processed_at")
        ))
        
    hourly.write.format("delta").mode("overwrite").save(hourly_path)
    
    record_count = hourly.count()
    print(f"   Wrote {record_count} hourly aggregates.")
    hourly.show(5, truncate=False)

# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="backfill")
    args = parser.parse_args()

    spark = create_spark_session()
    
    try:
        # 1. Ingest PM2.5 (Topic: raw.berlin.pm25 -> Subject: raw.berlin.pm25-value)
        ingest_stream(spark, "raw.berlin.pm25", "raw.berlin.pm25-value", "pm25", "batch")
        
        # 2. Ingest Weather
        ingest_stream(spark, "raw.berlin.weather", "raw.berlin.weather-value", "weather", "batch")
        
        # 3. Aggregate
        compute_aggregates(spark)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
