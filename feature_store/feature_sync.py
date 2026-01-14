"""
Feature Sync Job - Delta Lake to PostgreSQL.

Pattern:
Semantic Interface
ML Reproducibility

Syncs stop performance features from Delta Lake Gold layer 
to PostgreSQL marts.fct_stop_arrivals for unified serving.
"""

import argparse
import logging
import os
from datetime import datetime, date
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """Create Spark session with Delta Lake and PostgreSQL support."""
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    minio_password = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    return (
        SparkSession.builder.appName("TransitFlow-FeatureSync")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.6.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def read_stop_performance(spark: SparkSession, bucket: str) -> DataFrame:
    """
    Read from Delta Lake Gold layer and prepare for PostgreSQL.
    Aligns naming convention to the dbt/API contract.
    """
    gold_path = f"s3a://{bucket}/gold/stop_performance"
    
    try:
        df = spark.read.format("delta").load(gold_path)
    except Exception as e:
        logger.error(f"Could not load Delta table at {gold_path}. Ensure Spark Gold processing finished.")
        raise e

    # Mapping physical Spark columns to logical dbt/API names
    return df.select(
        F.col("stop_id").cast("string"),
        F.col("line_id").cast("string") if "line_id" in df.columns else F.lit("").alias("line_id"),
        F.col("hour_of_day").cast("int"),
        F.col("day_of_week").cast("int"),
        
        # Mapping Spark names to the names expected by our Postgres Mart
        F.col("avg_delay").cast("float").alias("historical_avg_delay"),
        
        (F.col("stddev_delay") if "stddev_delay" in df.columns else F.lit(0.0)).cast("float").alias("historical_stddev_delay"),
        
        # Keep MS for storage; API handles seconds conversion
        F.col("avg_dwell_time_ms").cast("float").alias("avg_dwell_time_ms"),
        
        # Mapping 'arrival_count' to 'historical_arrival_count'
        F.col("arrival_count").cast("long").alias("historical_arrival_count")
    ).na.fill(0.0)


def write_to_postgres(df: DataFrame) -> int:
    """Write features to PostgreSQL using staging and atomic upsert logic."""
    
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "transit")
    user = os.environ.get("POSTGRES_USER", "transit")
    password = os.environ.get("POSTGRES_PASSWORD", "transit_secure_local")
    
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }

    # Use staging table for atomic upsert
    temp_table = "marts.fct_stop_arrivals_staging"
    logger.info("Writing batch to staging table via JDBC: %s", temp_table)

    # 1. Overwrite staging table
    df.write.jdbc(url=jdbc_url, table=temp_table, mode="overwrite", properties=properties)

    # 2. Execute Upsert logic into the dbt Mart
    # Logic: Generates surrogate key to maintain dbt primary key consistency
    upsert_sql = """
        INSERT INTO marts.fct_stop_arrivals (
            feature_id, stop_id, line_id, hour_of_day, day_of_week,
            historical_arrival_count, historical_avg_delay, 
            historical_stddev_delay, historical_on_time_pct, avg_dwell_time_ms
        )
        SELECT 
            md5(stop_id || line_id || hour_of_day || day_of_week) as feature_id,
            stop_id, line_id, hour_of_day, day_of_week,
            historical_arrival_count, historical_avg_delay,
            historical_stddev_delay, 100.0 as historical_on_time_pct,
            avg_dwell_time_ms
        FROM marts.fct_stop_arrivals_staging
        ON CONFLICT (stop_id, line_id, hour_of_day, day_of_week)
        DO UPDATE SET
            historical_arrival_count = EXCLUDED.historical_arrival_count,
            historical_avg_delay = EXCLUDED.historical_avg_delay,
            historical_stddev_delay = EXCLUDED.historical_stddev_delay,
            avg_dwell_time_ms = EXCLUDED.avg_dwell_time_ms;
    """
    
    import psycopg2
    logger.info("Executing Upsert on production table: marts.fct_stop_arrivals")
    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
        conn.commit()
    finally:
        conn.close()

    row_count = df.count()
    logger.info("Successfully synced %d rows to marts.fct_stop_arrivals", row_count)
    return row_count


def run_feature_sync(bucket: str = "transitflow-lakehouse") -> dict:
    spark = get_spark_session()
    try:
        stop_features = read_stop_performance(spark, bucket)
        rows_written = write_to_postgres(stop_features)

        return {
            "status": "success",
            "rows_written": rows_written,
        }
    except Exception as e:
        logger.error("Feature sync failed: %s", e)
        return {"status": "failed", "error": str(e)}
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="transitflow-lakehouse")
    args = parser.parse_args()
    run_feature_sync(args.bucket)