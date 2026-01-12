"""
Feature Sync Job - dbt mart to PostgreSQL.

Pattern:
Semantic Interface
ML Reproducibility

Syncs stop performance features from Delta Lake Gold layer 
to PostgreSQL features.stop_historical table for serving.
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
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
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
    """Read stop performance from Delta Lake Gold layer."""
    gold_path = f"s3a://{bucket}/gold/stop_performance"
    logger.info("Reading stop performance from %s", gold_path)

    df = spark.read.format("delta").load(gold_path)

    return df.select(
        F.col("stop_id").cast("int"),
        F.col("hour_of_day").cast("int"),
        F.col("day_of_week").cast("int"),
        F.col("avg_delay_seconds").cast("float"),
        F.col("stddev_delay_seconds").cast("float"),
        # Convert ms to seconds for consistency with offline store schema
        (F.col("avg_dwell_time_ms").cast("float") / 1000).alias("avg_dwell_time_seconds"),
        F.col("p90_delay_seconds").cast("float"),
        F.col("sample_count").cast("int"),
    )


def write_to_postgres(df: DataFrame, computed_date: str) -> int:
    """Write features to PostgreSQL using staging and upsert logic."""
    df_with_date = df.withColumn("computed_date", F.lit(computed_date))
    
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "transit")
    user = os.environ.get("POSTGRES_USER", "transit")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }

    temp_table = "features.stop_historical_staging"
    logger.info("Writing batch to staging table: %s", temp_table)

    # 1. Overwrite staging table with current batch
    df_with_date.write.jdbc(url=jdbc_url, table=temp_table, mode="overwrite", properties=properties)

    # 2. Execute Upsert from staging to production
    upsert_sql = f"""
        INSERT INTO features.stop_historical (
            stop_id, hour_of_day, day_of_week,
            avg_delay_seconds, stddev_delay_seconds,
            avg_dwell_time_seconds, p90_delay_seconds,
            sample_count, computed_date
        )
        SELECT * FROM {temp_table}
        ON CONFLICT (stop_id, hour_of_day, day_of_week, computed_date)
        DO UPDATE SET
            avg_delay_seconds = EXCLUDED.avg_delay_seconds,
            stddev_delay_seconds = EXCLUDED.stddev_delay_seconds,
            avg_dwell_time_seconds = EXCLUDED.avg_dwell_time_seconds,
            p90_delay_seconds = EXCLUDED.p90_delay_seconds,
            sample_count = EXCLUDED.sample_count
    """
    
    # Execution using a temporary JDBC connection
    import psycopg2
    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
        conn.commit()
    finally:
        conn.close()

    row_count = df.count()
    logger.info("Synced %d rows to production features table", row_count)
    return row_count


def run_feature_sync(computed_date: Optional[str] = None, bucket: str = "transitflow-lakehouse") -> dict:
    if computed_date is None:
        computed_date = date.today().isoformat()

    spark = get_spark_session()
    try:
        stop_features = read_stop_performance(spark, bucket)
        rows_written = write_to_postgres(stop_features, computed_date)

        return {
            "status": "success",
            "computed_date": computed_date,
            "rows_written": rows_written,
        }
    except Exception as e:
        logger.error("Feature sync failed: %s", e)
        return {"status": "failed", "error": str(e)}
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="YYYY-MM-DD")
    parser.add_argument("--bucket", default="transitflow-lakehouse")
    args = parser.parse_args()
    run_feature_sync(args.date, args.bucket)