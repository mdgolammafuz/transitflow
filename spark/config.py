"""
Spark configuration loader.
Context: Medallion Architecture (Bronze -> Silver -> Gold)
All configuration from environment variables - no hardcoded values.
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
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = ""
    minio_secret_key: str = ""
    lakehouse_bucket: str = "transitflow-lakehouse"

    # Paths (Computed in __post_init__)
    bronze_path: str = ""
    silver_path: str = ""
    gold_path: str = ""
    checkpoint_base: str = ""

    # Retention (days)
    bronze_retention_days: int = 7
    silver_retention_days: int = 30
    gold_retention_days: int = 365

    # PostgreSQL (for reconciliation results)
    postgres_jdbc_url: str = ""
    postgres_user: str = ""
    postgres_password: str = ""
    postgres_host: str = ""
    postgres_port: str = ""
    postgres_db: str = ""

    def __post_init__(self):
        """Construct logic-based paths after initialization."""
        # Define the base URI with the S3A protocol
        base = f"s3a://{self.lakehouse_bucket}"

        # Structure the medallion layers
        self.bronze_path = f"{base}/bronze"
        self.silver_path = f"{base}/silver"
        self.gold_path = f"{base}/gold"

        # Shallow checkpoint directory for easier cleanup
        self.checkpoint_base = f"{base}/_checkpoints"

    def get_checkpoint_path(self, stream_name: str) -> str:
        """Returns a unique checkpoint directory for a specific stream."""
        return f"{self.checkpoint_base}/{stream_name}"


def load_config() -> SparkConfig:
    """Load configuration from environment variables with strict validation."""

    def get_required(key: str) -> str:
        value = os.environ.get(key)
        if not value:
            raise ValueError(f"Required environment variable not set: {key}")
        return value

    def get_optional(key: str, default: str = "") -> str:
        return os.environ.get(key, default)

    def get_int(key: str, default: int) -> int:
        try:
            return int(os.environ.get(key, str(default)))
        except ValueError:
            return default

    # Required variables
    minio_user = get_required("MINIO_ROOT_USER")
    minio_pass = get_required("MINIO_ROOT_PASSWORD")
    pg_user = get_required("POSTGRES_USER")
    pg_pass = get_required("POSTGRES_PASSWORD")

    # Optional DB params
    pg_host = get_optional("POSTGRES_HOST", "postgres")
    pg_port = get_optional("POSTGRES_PORT", "5432")
    pg_db = get_optional("POSTGRES_DB", "transit")

    config = SparkConfig(
        kafka_bootstrap_servers=get_optional("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092"),
        minio_endpoint=get_optional("MINIO_ENDPOINT", "http://minio:9000"),
        minio_access_key=minio_user,
        minio_secret_key=minio_pass,
        lakehouse_bucket=get_optional("LAKEHOUSE_BUCKET", "transitflow-lakehouse"),
        bronze_retention_days=get_int("BRONZE_RETENTION_DAYS", 7),
        silver_retention_days=get_int("SILVER_RETENTION_DAYS", 30),
        gold_retention_days=get_int("GOLD_RETENTION_DAYS", 365),
        postgres_user=pg_user,
        postgres_password=pg_pass,
        postgres_host=pg_host,
        postgres_port=pg_port,
        postgres_db=pg_db,
    )

    # Construct JDBC URL
    config.postgres_jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    return config


def create_spark_session(app_name: str):
    """Initializes Spark Session with optimized S3A and Delta settings."""
    from pyspark.sql import SparkSession
    config = load_config()

    return (
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
        # --- S3A Connectivity & Auth ---
        .config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # --- Delta Lake Storage Reliability ---
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
        )
        .getOrCreate()
    )