"""
Sync Bronze: Delta Lake (MinIO) -> PostgreSQL
Idempotent: DELETE + INSERT per date partition.
"""
import argparse
import logging
import psycopg2
from pyspark.sql.functions import col
from spark.config import create_spark_session, load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeSync")


def sync_layer(spark, config, table_name, source_path, target_table, date):
    logger.info(f"Syncing {table_name} for {date}...")
    
    try:
        # 1. Read from Delta Lake (not raw Parquet)
        df = spark.read.format("delta").load(source_path).filter(col("date") == date)
        count = df.count()
        logger.info(f"Read {count} records from Delta Lake")
        
        if count == 0:
            logger.warning(f"No data for {date}, skipping")
            return

        # 2. Delete existing data for this date in Postgres
        conn = psycopg2.connect(
            host=config.postgres_host,
            port=config.postgres_port,
            database=config.postgres_db,
            user=config.postgres_user,
            password=config.postgres_password
        )
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {target_table} WHERE date = %s", (date,))
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Deleted {deleted} existing records for {date}")

        # 3. Insert fresh data from Delta Lake
        df.write \
            .format("jdbc") \
            .option("url", config.postgres_jdbc_url) \
            .option("dbtable", target_table) \
            .option("user", config.postgres_user) \
            .option("password", config.postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logger.info(f"SUCCESS: {target_table} synced {count} records for {date}")

    except Exception as e:
        logger.error(f"FAILURE on {table_name}: {e}")
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    config = load_config()
    spark = create_spark_session("TransitFlow-BronzeSync")

    sync_layer(spark, config, "Enriched", f"{config.bronze_path}/enriched", "bronze.enriched", args.date)
    sync_layer(spark, config, "StopEvents", f"{config.bronze_path}/stop_events", "bronze.stop_events", args.date)

    spark.stop()


if __name__ == "__main__":
    main()