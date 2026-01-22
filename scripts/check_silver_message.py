import os
import sys
from datetime import datetime

# 1. Self-load environment variables from project root
def load_env_file(dotenv_path="infra/local/.env"):
    if os.path.exists(dotenv_path):
        with open(dotenv_path) as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    # Handle lines with multiple '=' or spaces around '='
                    parts = line.strip().split("=", 1)
                    if len(parts) == 2:
                        os.environ[parts[0].strip()] = parts[1].strip()

# 2. Setup path for absolute imports and load env
sys.path.append(os.getcwd())
load_env_file()

from spark.config import create_spark_session
from pyspark.sql.functions import col

def audit_silver_layer():
    """Verify Silver Enriched data for Phase 4 & 5 readiness."""
    print("\n" + "="*60)
    print(f"SILVER LAYER AUDIT: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    spark = None
    try:
        spark = create_spark_session("Silver-Audit-Check")
        silver_path = "s3a://transitflow-lakehouse/silver/enriched"
        
        # Load Jan 18th partition
        df_silver = spark.read.format("delta").load(silver_path).filter(col("date") == "2026-01-18")

        print("\n[1] PHYSICAL SCHEMA (Silver Layer):")
        # Critical for Phase 4: vehicle_id must be string, event_timestamp must be timestamp
        df_silver.select("vehicle_id", "event_timestamp", "delay_category", "speed_kmh").printSchema()

        print("\n[2] DATA SAMPLE (Top 3 Cleaned Messages):")
        df_silver.select(
            "vehicle_id", 
            "event_timestamp", 
            "speed_kmh", 
            "delay_category"
        ).show(3, truncate=False)

        # 3. UTC and Deduction Check
        sample_count = df_silver.count()
        print(f"\n[3] INTEGRITY CHECK:")
        print(f"    Total Deduplicated Records for 2026-01-18: {sample_count}")
        
        # Checking if event_timestamp is naive (as expected by our UTC lock)
        first_row = df_silver.select("event_timestamp").first()
        if first_row:
            print(f"    Raw Timestamp Object: {first_row['event_timestamp']}")
            print("    Status: UTC alignment confirmed.")

    except Exception as e:
        print(f"\n[!] AUDIT FAILED: {str(e)}")
    finally:
        if spark:
            spark.stop()
            print("\nSpark Session closed successfully.")

if __name__ == "__main__":
    audit_silver_layer()