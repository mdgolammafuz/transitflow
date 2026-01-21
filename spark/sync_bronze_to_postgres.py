"""
Sync Bronze: Lakehouse (MinIO) -> Database (Postgres)
Final Alignment: Physical Recovery + Column Derivation.
Principal: Forces creation of event_timestamp to align with dbt staging models.
"""

import argparse
import logging
import sys
from pyspark.sql.functions import lit, col, from_unixtime
from spark.config import create_spark_session, load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("BronzeSync")

def sync_layer(spark, config, table_name, source_path, target_table, date):
    logger.info(f"Syncing {table_name} for {date}...")
    try:
        # Inject S3A credentials explicitly
        h_conf = spark.sparkContext._jsc.hadoopConfiguration()
        h_conf.set("fs.s3a.access.key", config.minio_access_key)
        h_conf.set("fs.s3a.secret.key", config.minio_secret_key)
        h_conf.set("fs.s3a.endpoint", config.minio_endpoint)
        h_conf.set("fs.s3a.path.style.access", "true")
        h_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # 1. Physical Read from MinIO
        partition_path = f"{source_path}/date={date}"
        df = spark.read.format("parquet").load(partition_path)

        # 2. SURGICAL COLUMN ALIGNMENT
        # Standardize: Drop ghost 'timestamp', derive 'event_timestamp'
        if "timestamp" in df.columns:
            df = df.drop("timestamp")

        if table_name == "Telemetry":
            # This creates the exact column dbt is looking for
            df = df.withColumn("event_timestamp", (col("event_time_ms") / 1000).cast("timestamp"))
        elif table_name == "StopEvents":
            df = df.withColumn("arrival_timestamp", (col("arrival_time") / 1000).cast("timestamp"))

        # Add partition date back
        df = df.withColumn("date", lit(date).cast("date"))

        # 3. Write to Postgres (Recreation Mode)
        logger.info(f"Pouring {df.count()} records into {target_table}...")
        df.write \
            .format("jdbc") \
            .option("url", config.postgres_jdbc_url) \
            .option("dbtable", target_table) \
            .option("user", config.postgres_user) \
            .option("password", config.postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
            
        logger.info(f"SUCCESS: {target_table} is now physically aligned and typed.")

    except Exception as e:
        logger.error(f"CRITICAL FAILURE on {table_name}: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    config = load_config()
    spark = create_spark_session("TransitFlow-BronzeSync")

    sync_layer(spark, config, "Telemetry", f"{config.bronze_path}/enriched", "bronze.enriched", args.date)
    sync_layer(spark, config, "StopEvents", f"{config.bronze_path}/stop_events", "bronze.stop_events", args.date)

    spark.stop()

if __name__ == "__main__":
    main()