"""
Spark configuration loader.
Context: Medallion Architecture (Bronze -> Silver -> Gold)
All configuration from environment variables - no hardcoded values.
Methodical: Hardened with S3A retry limits to prevent silent hangs.
Aligned: Global UTC session lock to prevent timezone leaks.
"""

import os
from dataclasses import dataclass


@dataclass
class SparkConfig:
    """Configuration for Spark jobs."""

    # Kafka
    kafka_bootstrap_servers: str
    kafka_enriched_topic: str = "fleet.enriched"
    kafka_stops_topic: str = "fleet.stop_events"

    # MinIO / S3
    minio_endpoint: str = ""
    minio_access_key: str = ""
    minio_secret_key: str = ""
    lakehouse_bucket: str = "transitflow-lakehouse"

    # Retention (Days)
    bronze_retention_days: int = 7
    silver_retention_days: int = 30
    gold_retention_days: int = 365

    # Paths (Computed in __post_init__)
    bronze_path: str = ""
    silver_path: str = ""
    gold_path: str = ""

    # PostgreSQL
    postgres_jdbc_url: str = ""
    postgres_host: str = ""
    postgres_port: str = ""
    postgres_db: str = ""
    postgres_user: str = ""
    postgres_password: str = ""

    def __post_init__(self):
        """Construct logic-based paths using S3A protocol."""
        base = f"s3a://{self.lakehouse_bucket}"
        self.bronze_path = f"{base}/bronze"
        self.silver_path = f"{base}/silver"
        self.gold_path = f"{base}/gold"

    def get_checkpoint_path(self, job_name: str) -> str:
        """Centralized checkpoint management for streaming jobs."""
        return f"s3a://{self.lakehouse_bucket}/_checkpoints/{job_name}"


def load_config() -> SparkConfig:
    """Load configuration from environment variables."""

    def get_required(key: str) -> str:
        value = os.environ.get(key)
        if not value:
            raise ValueError(f"Required environment variable not set: {key}")
        return value

    def get_optional(key: str, default: str = "") -> str:
        return os.environ.get(key, default)

    # Database parameters
    pg_host = get_optional("POSTGRES_HOST", "postgres")
    pg_port = get_optional("POSTGRES_PORT", "5432")
    pg_db = get_optional("POSTGRES_DB", "transit")

    config = SparkConfig(
        kafka_bootstrap_servers=get_optional("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092"),
        minio_endpoint=get_optional("MINIO_ENDPOINT", "http://minio:9000"),
        minio_access_key=get_required("MINIO_ROOT_USER"),
        minio_secret_key=get_required("MINIO_ROOT_PASSWORD"),
        lakehouse_bucket=get_optional("LAKEHOUSE_BUCKET", "transitflow-lakehouse"),
        
        # Aligned: Pull retention days from env with fallback to defaults
        bronze_retention_days=int(get_optional("BRONZE_RETENTION_DAYS", "7")),
        silver_retention_days=int(get_optional("SILVER_RETENTION_DAYS", "30")),
        gold_retention_days=int(get_optional("GOLD_RETENTION_DAYS", "365")),
        
        postgres_host=pg_host,
        postgres_port=pg_port,
        postgres_db=pg_db,
        postgres_user=get_required("POSTGRES_USER"),
        postgres_password=get_required("POSTGRES_PASSWORD"),
    )

    # Construct JDBC URL based on provided host/port/db
    config.postgres_jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    return config

def create_spark_session(app_name: str):
    """Initializes Spark Session with optimized S3A and Delta settings."""
    from pyspark.sql import SparkSession
    config = load_config()

    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.postgresql:postgresql:42.6.0"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout", "5000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
        .config("spark.hadoop.fs.s3a.retry.limit", "1")
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
        )
        .getOrCreate()
    )

    hc = spark.sparkContext._jsc.hadoopConfiguration()
    hc.set("fs.s3a.connection.timeout", "5000")
    hc.set("fs.s3a.attempts.maximum", "1")

    return spark