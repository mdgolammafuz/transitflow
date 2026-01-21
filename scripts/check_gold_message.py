import os
import sys
from datetime import datetime

# 1. Self-load environment variables from project root
def load_env_file(dotenv_path="infra/local/.env"):
    if os.path.exists(dotenv_path):
        with open(dotenv_path) as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

# 2. Setup path for absolute imports
sys.path.append(os.getcwd())
load_env_file()

from spark.config import create_spark_session
from pyspark.sql.functions import col

def audit_gold_layer():
    """Verify Gold Daily Metrics for Phase 4 & 5 readiness."""
    print("\n" + "="*60)
    print(f"GOLD LAYER AUDIT: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    spark = None
    try:
        spark = create_spark_session("Gold-Audit-Check")
        config = spark.sparkContext.getConf() # For logging check
        
        gold_path = "s3a://transitflow-lakehouse/gold/daily_metrics"
        
        # Load exactly 3 messages from the Jan 18th partition
        df_gold = spark.read.format("delta").load(gold_path).filter(col("date") == "2026-01-18")

        print("\n[1] PHYSICAL SCHEMA (Delta Lake):")
        df_gold.printSchema()

        print("\n[2] DATA SAMPLE (Top 3 Metrics):")
        # Selecting key columns to verify aggregation precision
        df_gold.select(
            "line_id", 
            "date", 
            "total_events", 
            "avg_delay_seconds", 
            "on_time_percentage"
        ).show(3, truncate=False)

        # Count check to match your earlier reconciliation success
        row_count = df_gold.count()
        print(f"\n[3] PARTITION VERIFICATION:")
        print(f"    Total rows for 2026-01-18: {row_count}")
        
    except Exception as e:
        print(f"\n[!] AUDIT FAILED: {str(e)}")
    finally:
        if spark:
            spark.stop()
            print("\nSpark Session closed successfully.")

if __name__ == "__main__":
    audit_gold_layer()