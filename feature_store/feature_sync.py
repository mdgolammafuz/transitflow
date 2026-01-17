"""
Feature Sync Job - Delta Lake to PostgreSQL.

Pattern: Atomic Staging-to-Mart Upsert
Methodical: Aligned with Phase 5 validated dbt Marts (marts.fct_stop_arrivals).
Robust: Implements hardened feature_id generation and NULL-safe casting.
"""

import logging
import os
import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def get_required_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"CRITICAL: Environment variable {key} is not set.")
    return value

def get_spark_session() -> SparkSession:
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = get_required_env("MINIO_ROOT_USER")
    minio_password = get_required_env("MINIO_ROOT_PASSWORD")

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
    """Read from Delta Lake Gold and align with the validated Mart schema."""
    gold_path = f"s3a://{bucket}/gold/stop_performance"
    logger.info(f"Loading Gold features from {gold_path}")
    
    df = spark.read.format("delta").load(gold_path)

    # Principal Fix: Use coalesce to prevent NULL IDs from breaking the MD5 hash
    # and trim strings to ensure exact matches with real-time telemetry.
    return df.select(
        F.coalesce(F.trim(F.col("stop_id").cast("string")), F.lit("UNKNOWN")).alias("stop_id"),
        F.coalesce(F.trim(F.col("line_id").cast("string")), F.lit("UNKNOWN")).alias("line_id"),
        F.col("hour_of_day").cast("int"),
        F.col("day_of_week").cast("int"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("historical_avg_delay").cast("double"),
        F.col("historical_arrival_count").cast("long"),
        F.col("avg_dwell_time_ms").cast("double"),
        # Methodical: Placeholder aligned with offline_store.StopFeatures dataclass
        F.lit(0.0).cast("double").alias("historical_stddev_delay")
    ).na.fill(0.0)

def write_to_postgres(df: DataFrame) -> int:
    """Execute Atomic Upsert into marts.fct_stop_arrivals."""
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "transit")
    user = get_required_env("POSTGRES_USER")
    password = get_required_env("POSTGRES_PASSWORD")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    properties = {
        "user": user, 
        "password": password, 
        "driver": "org.postgresql.Driver"
    }

    # 1. Write to temporary staging table
    temp_table = "marts.fct_stop_arrivals_staging"
    logger.info(f"Writing to staging table: {temp_table}")
    df.write.jdbc(url=jdbc_url, table=temp_table, mode="overwrite", properties=properties)

    # 2. Perform Atomic Upsert using hardened feature_id generation.
    # Principal Fix: Ensuring the separator and coalesce logic mirrors dbt/marts.
    upsert_sql = """
        INSERT INTO marts.fct_stop_arrivals (
            feature_id, stop_id, line_id, hour_of_day, day_of_week,
            latitude, longitude,
            historical_arrival_count, historical_avg_delay,
            historical_stddev_delay, avg_dwell_time_ms,
            valid_from, is_current
        )
        SELECT
            md5(COALESCE(stop_id, '') || '-' || COALESCE(line_id, '') || '-' || 
                CAST(hour_of_day AS TEXT) || '-' || CAST(day_of_week AS TEXT)) as feature_id,
            stop_id, line_id, hour_of_day, day_of_week,
            latitude, longitude,
            historical_arrival_count, historical_avg_delay,
            historical_stddev_delay, avg_dwell_time_ms,
            CURRENT_TIMESTAMP, TRUE
        FROM marts.fct_stop_arrivals_staging
        ON CONFLICT (feature_id)
        DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            historical_arrival_count = EXCLUDED.historical_arrival_count,
            historical_avg_delay = EXCLUDED.historical_avg_delay,
            historical_stddev_delay = EXCLUDED.historical_stddev_delay,
            avg_dwell_time_ms = EXCLUDED.avg_dwell_time_ms,
            valid_from = CURRENT_TIMESTAMP;
    """

    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
            cur.execute(f"DROP TABLE {temp_table}")
        conn.commit()
        logger.info("Atomic upsert completed successfully.")
    finally:
        conn.close()

    return df.count()

def run_feature_sync(bucket: str = "transitflow-lakehouse") -> dict:
    spark = get_spark_session()
    try:
        stop_features = read_stop_performance(spark, bucket)
        rows_written = write_to_postgres(stop_features)
        return {"status": "success", "rows_written": rows_written}
    except Exception as e:
        logger.error(f"Feature sync failed: {e}")
        return {"status": "failed", "error": str(e)}
    finally:
        spark.stop()

if __name__ == "__main__":
    run_feature_sync()