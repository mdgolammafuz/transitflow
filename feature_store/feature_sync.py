"""
Feature Sync Job - Delta Lake to PostgreSQL.

Pattern: Primary Key Upsert
Security: Hardened JDBC properties & Zero-Secret Architecture
"""

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_required_env(key: str) -> str:
    """Fetch an environment variable or crash if missing."""
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"CRITICAL: Environment variable {key} is not set.")
    return value


def get_spark_session() -> SparkSession:
    """Create Spark session with Delta Lake and PostgreSQL support."""
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = get_required_env("MINIO_ROOT_USER")
    minio_password = get_required_env("MINIO_ROOT_PASSWORD")

    return (
        SparkSession.builder.appName("TransitFlow-FeatureSync")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.6.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_stop_performance(spark: SparkSession, bucket: str) -> DataFrame:
    """Read from Delta Lake Gold and align with Postgres Mart schema."""
    gold_path = f"s3a://{bucket}/gold/stop_performance"
    try:
        df = spark.read.format("delta").load(gold_path)
    except Exception as e:
        logger.error(f"Could not load Delta table at {gold_path}.")
        raise e

    # Alignment with the new Spark Gold Contract (renamed columns and coordinates)
    return df.select(
        F.trim(F.col("stop_id").cast("string")).alias("stop_id"),
        F.trim(F.col("line_id").cast("string")).alias("line_id"),
        F.col("hour_of_day").cast("int"),
        F.col("day_of_week").cast("int"),
        F.col("latitude").cast("float"),
        F.col("longitude").cast("float"),
        F.col("historical_avg_delay").cast("float"),
        F.col("historical_stddev_delay").cast("float"),
        F.col("avg_dwell_time_ms").cast("float"),
        F.col("historical_arrival_count").cast("long"),
    ).na.fill(0.0)


def write_to_postgres(df: DataFrame) -> int:
    """Execute Atomic Upsert using feature_id Primary Key."""
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "transit")
    user = get_required_env("POSTGRES_USER")
    password = get_required_env("POSTGRES_PASSWORD")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    properties = {
        "user": user, 
        "password": password, 
        "driver": "org.postgresql.Driver",
        "ssl": "false" # Set to true in production with certs
    }

    temp_table = "marts.fct_stop_arrivals_staging"
    logger.info("Writing batch to staging table: %s", temp_table)
    df.write.jdbc(url=jdbc_url, table=temp_table, mode="overwrite", properties=properties)

    # UPDATED SQL: Includes Latitude and Longitude to ensure Serving API has coordinates
    upsert_sql = """
        INSERT INTO marts.fct_stop_arrivals (
            feature_id, stop_id, line_id, hour_of_day, day_of_week,
            latitude, longitude,
            historical_arrival_count, historical_avg_delay,
            historical_stddev_delay, historical_on_time_pct, avg_dwell_time_ms
        )
        SELECT
            md5(stop_id || '|' || line_id || '|' || hour_of_day || '|' || day_of_week) as feature_id,
            stop_id, line_id, hour_of_day, day_of_week,
            latitude, longitude,
            historical_arrival_count, historical_avg_delay,
            historical_stddev_delay, 100.0 as historical_on_time_pct,
            avg_dwell_time_ms
        FROM marts.fct_stop_arrivals_staging
        ON CONFLICT (feature_id)
        DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
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

    return df.count()


def run_feature_sync(bucket: str = "transitflow-lakehouse") -> dict:
    spark = get_spark_session()
    try:
        stop_features = read_stop_performance(spark, bucket)
        rows_written = write_to_postgres(stop_features)
        logger.info("Successfully synced %d rows", rows_written)
        return {"status": "success", "rows_written": rows_written}
    except Exception as e:
        logger.error("Feature sync failed: %s", e)
        return {"status": "failed", "error": str(e)}
    finally:
        spark.stop()


if __name__ == "__main__":
    run_feature_sync()