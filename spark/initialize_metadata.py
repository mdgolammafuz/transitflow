"""
Metadata Initialization: Postgres (reference.stops) → Gold (Delta Lake)
Context: Bootstraps Ingredient B (Stop Coordinates) for Feature Engineering.
Methodical: Strictly targets the dbt-aliased 'reference.stops' table.
Robustness: Leverages hardened Spark session to prevent S3A handshake hangs.
"""

import sys
import logging
from spark.config import create_spark_session, load_config

# Configure logging to match the project standard
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def bootstrap_metadata():
    """Reads stop metadata from Postgres and writes it as a Delta table in the Lakehouse."""
    
    # 1. Initialize Spark and Load Config
    # create_spark_session now contains the permanent anti-hang circuit breakers
    spark = create_spark_session("TransitFlow-MetadataInit")
    config = load_config()
    
    # Strictly aligned with dbt_project.yml alias confirmed by logs
    target_table = "reference.stops"
    target_path = f"{config.gold_path}/stops"
    
    logger.info(f"Starting metadata bootstrap from {target_table} to Lakehouse at {target_path}")
    
    try:
        # 2. Read from the dbt-seeded table in Postgres via JDBC
        logger.info(f"Connecting to Postgres to fetch {target_table}...")
        stops_df = spark.read \
            .format("jdbc") \
            .option("url", config.postgres_jdbc_url) \
            .option("dbtable", target_table) \
            .option("user", config.postgres_user) \
            .option("password", config.postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # Methodical check: Fail fast if dbt-seed didn't actually populate the data
        if stops_df.count() == 0:
            logger.error(f"Source table {target_table} is empty. Run 'make dbt-seed' first.")
            sys.exit(1)

        # 3. Project only the columns needed for ML Feature Enrichment
        # Mapping standard GTFS names to our Gold Layer schema
        metadata_df = stops_df.select(
            "stop_id", 
            "stop_name", 
            "stop_lat", 
            "stop_lon"
        )

        # 4. Write to Gold Layer as Delta (The Lakehouse Bridge)
        # This triggers the S3A handshake with MinIO
        logger.info(f"Writing metadata to {target_path} as Delta Table...")
        metadata_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(target_path)

        logger.info("Metadata bootstrap complete. Ingredient B (Stop Coordinates) is ready.")

    except Exception as e:
        # Robust Error Handling: Captures S3A connection errors or JDBC failures
        logger.error(f"Metadata bootstrap failed: {e}")
        sys.exit(1)
    finally:
        # Clean up resources
        spark.stop()

if __name__ == "__main__":
    bootstrap_metadata()