"""
Metadata Initialization: Postgres (reference.seed_stops/seed_lines) → Gold (Delta Lake)
Context: Bootstraps Ingredient B (Stops) and C (Lines) for Feature Engineering.
Methodical: Strictly targets dbt-seeded tables.
Robustness: Leverages hardened Spark session to prevent S3A handshake hangs.
Aligned: Ensures Type Safety for IDs and enforces Primary Key uniqueness.
"""

import sys
import logging
from pyspark.sql.functions import col, trim
from spark.config import create_spark_session, load_config

# Configure logging to match the project standard
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def bootstrap_metadata():
    """Reads stop and line metadata from Postgres and writes as Delta tables in the Lakehouse."""
    
    # 1. Initialize Spark and Load Config
    spark = create_spark_session("TransitFlow-MetadataInit")
    config = load_config()

    # Inject S3A credentials explicitly - CRITICAL for MinIO connectivity
    h_conf = spark.sparkContext._jsc.hadoopConfiguration()
    h_conf.set("fs.s3a.access.key", config.minio_access_key)
    h_conf.set("fs.s3a.secret.key", config.minio_secret_key)
    h_conf.set("fs.s3a.endpoint", config.minio_endpoint)
    h_conf.set("fs.s3a.path.style.access", "true")
    h_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Strictly aligned with dbt_project.yml seeds
    target_stop_table = "reference.seed_stops"
    target_stop_path = f"{config.gold_path}/stops"
    
    logger.info(f"Starting metadata bootstrap from {target_stop_table} to Lakehouse at {target_stop_path}")
    
    try:
        # 2. Process STOPS (Ingredient B)
        logger.info(f"Connecting to Postgres to fetch {target_stop_table}...")
        stops_df = spark.read \
            .format("jdbc") \
            .option("url", config.postgres_jdbc_url) \
            .option("dbtable", target_stop_table) \
            .option("user", config.postgres_user) \
            .option("password", config.postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        if stops_df.count() == 0:
            logger.error(f"Source table {target_stop_table} is empty. Run 'make dbt-seed' first.")
            sys.exit(1)

        metadata_stop_df = stops_df.select(
            trim(col("stop_id").cast("string")).alias("stop_id"), 
            col("stop_name"), 
            col("stop_lat").alias("latitude"),
            col("stop_lon").alias("longitude")
        ).dropDuplicates(["stop_id"])

        logger.info(f"Writing {metadata_stop_df.count()} unique stops to {target_stop_path}...")
        metadata_stop_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(target_stop_path)

        logger.info("Metadata bootstrap complete. Ingredient B (Stop Coordinates) is ready.")

        # 3. Process LINES
        # Using seed_lines and line_id to match actual seed file structure
        target_line_table = "reference.seed_lines"
        target_line_path = f"{config.gold_path}/lines"
        
        logger.info(f"Lifting lines from {target_line_table}...")
        lines_df = spark.read.format("jdbc") \
            .option("url", config.postgres_jdbc_url) \
            .option("dbtable", target_line_table) \
            .option("user", config.postgres_user) \
            .option("password", config.postgres_password) \
            .option("driver", "org.postgresql.Driver").load()

        if lines_df.count() == 0:
            logger.error(f"Source table {target_line_table} is empty.")
            sys.exit(1)

        metadata_line_df = lines_df.select(
            trim(col("line_id").cast("string")).alias("line_id"),
            col("line_name"),
            col("line_type")
        ).dropDuplicates(["line_id"])

        logger.info(f"Writing {metadata_line_df.count()} unique lines to {target_line_path}...")
        metadata_line_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(target_line_path)
            
        logger.info("Lines bootstrap complete. Ingredient C is ready.")

    except Exception as e:
        logger.error(f"Metadata bootstrap failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    bootstrap_metadata()