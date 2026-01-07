import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config import load_config, create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_count(spark, path, date):
    try:
        return spark.read.format("delta").load(path).filter(col("date") == date).count()
    except Exception:
        return 0

def save_to_postgres(target_date, table_name, b_count, s_count, config):
    try:
        import psycopg2
        # Use getattr with fallbacks to match whatever names are in your config.py
        conn = psycopg2.connect(
            host=getattr(config, 'postgres_host', 'postgres'),
            port=getattr(config, 'postgres_port', '5432'),
            user=getattr(config, 'postgres_user', 'transit'),
            password=getattr(config, 'postgres_password', 'transit_secure_local'),
            database=getattr(config, 'postgres_db', 'transit')
        )
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS reconciliation_results (
                    date DATE, 
                    table_name TEXT, 
                    stream_count BIGINT, 
                    batch_count BIGINT,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute(
                "INSERT INTO reconciliation_results (date, table_name, stream_count, batch_count) VALUES (%s, %s, %s, %s)", 
                (target_date, table_name, b_count, s_count)
            )
        conn.commit()
        conn.close()
        logger.info(f"Successfully saved {table_name} audit to Postgres.")
    except Exception as e:
        logger.error(f"PostgreSQL persistence failed: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="YYYY-MM-DD")
    parser.add_argument("--save", action="store_true", default=True) # Default to true
    args = parser.parse_args()
    
    target_date = args.date if args.date else datetime.now().strftime("%Y-%m-%d")
    config = load_config()
    spark = create_spark_session("TransitFlow-Reconciliation")
    
    # Process enriched
    b_count = get_count(spark, f"{config.bronze_path}/enriched", target_date)
    s_count = get_count(spark, f"{config.silver_path}/enriched", target_date)
    
    print("\n" + "="*50)
    print(f"RECONCILIATION REPORT: {target_date}")
    print(f"Enriched | Bronze: {b_count:,} | Silver: {s_count:,} | Diff: {abs(b_count-s_count):,}")
    print("="*50)

    if args.save:
        save_to_postgres(target_date, 'enriched', b_count, s_count, config)
    
    spark.stop()

if __name__ == "__main__":
    main()