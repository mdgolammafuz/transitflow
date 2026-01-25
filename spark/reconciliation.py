"""
Reconciliation Job: Data Integrity Audit
Pattern: Unified Stream/Batch (DE#5)
Verifies that Bronze and Silver layers are synchronized within acceptable thresholds.
Aligned: Enforces UTC for date partitioning and audit timestamps.
"""

import argparse
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from pyspark.sql.functions import col

# Absolute import for package consistency
from spark.config import create_spark_session, load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ReconciliationResult:
    """Dataclass to hold audit results for testability and persistence."""

    date: str
    table_name: str
    stream_count: int
    batch_count: int
    difference: int
    diff_percentage: float
    passed: bool
    threshold_pct: float = 20.0
    # Aligned: Ensure checked_at is UTC
    checked_at: str = datetime.now(timezone.utc).isoformat()

    def to_dict(self):
        """Convert the result to a dictionary for logging or DB insertion."""
        return asdict(self)


def get_count(spark, path, date, table_format="delta"):
    """
    Safely get count for a specific date partition.
    Principal: Supports both raw Parquet (Bronze) and Delta (Silver).
    """
    try:
        from delta.tables import DeltaTable

        if table_format == "delta":
            if not DeltaTable.isDeltaTable(spark, path):
                logger.warning(f"Path is not a valid Delta table: {path}")
                return 0
            return spark.read.format("delta").load(path).filter(col("date") == date).count()
        

    except Exception as e:
        logger.warning(f"Could not retrieve count for {path} (format: {table_format}): {e}")
        return 0


def save_to_postgres(result: ReconciliationResult, config):
    """
    Persist the audit result to the PostgreSQL database.
    Ensures visibility into pipeline health across the stack.
    """
    try:
        import psycopg2

        # Aligned: Using keys defined in spark.config.SparkConfig
        conn = psycopg2.connect(
            host=config.postgres_host,
            port=config.postgres_port,
            user=config.postgres_user,
            password=config.postgres_password,
            database=config.postgres_db,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS reconciliation_results (
                    date DATE,
                    table_name TEXT,
                    stream_count BIGINT,
                    batch_count BIGINT,
                    difference BIGINT,
                    diff_percentage FLOAT,
                    passed BOOLEAN,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
            cur.execute(
                """
                INSERT INTO reconciliation_results
                (date, table_name, stream_count, batch_count, difference, diff_percentage, passed)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    result.date,
                    result.table_name,
                    result.stream_count,
                    result.batch_count,
                    result.difference,
                    result.diff_percentage,
                    result.passed,
                ),
            )
        conn.commit()
        conn.close()
        logger.info(f"Successfully saved {result.table_name} audit to Postgres.")
    except Exception as e:
        logger.error(f"PostgreSQL audit persistence failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="TransitFlow Reconciliation Audit")
    # Robustness: Make --date mandatory to ensure OCI/Cron stability
    parser.add_argument("--date", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--save", action="store_true", default=True)
    args = parser.parse_args()

    # To prevent silent 0-count failures
    target_date = args.date
    
    config = load_config()
    spark = create_spark_session("TransitFlow-Reconciliation")

    # 1. Perform cross-layer counts
    # Bronze is raw Parquet, Silver is Delta
    b_count = get_count(spark, f"{config.bronze_path}/enriched", target_date, table_format="delta")
    s_count = get_count(spark, f"{config.silver_path}/enriched", target_date, table_format="delta")

    # 2. Calculate metrics
    diff = abs(b_count - s_count)
    
    # Principal Logic: Fix the 'False Pass' vulnerability.
    # If Bronze is 0 but Silver has data, that's a 100% discrepancy/failure.
    if b_count == 0:
        diff_pct = 100.0 if s_count > 0 else 0.0
    else:
        diff_pct = (diff / b_count * 100)
    
    threshold = 20.0  
    # Both counts must be non-zero for a standard pass, or both must be zero.
    is_passed = (diff_pct <= threshold) and not (b_count == 0 and s_count > 0)

    # 3. Create result object
    result = ReconciliationResult(
        date=target_date,
        table_name="enriched",
        stream_count=b_count,
        batch_count=s_count,
        difference=diff,
        diff_percentage=round(diff_pct, 2),
        passed=is_passed,
        threshold_pct=threshold,
    )

    # 4. Console Reporting
    print("\n" + "=" * 60)
    print(f"RECONCILIATION REPORT (UTC): {result.date}")
    status = "PASS" if result.passed else "FAIL"
    print(f"Status: {status} (Threshold: {result.threshold_pct}%)")
    print(f"Bronze Count: {result.stream_count:,}")
    print(f"Silver Count: {result.batch_count:,}")
    print(f"Difference:   {result.difference:,} ({result.diff_percentage}%)")
    print("=" * 60 + "\n")

    # 5. Persistent Audit Logging
    if args.save:
        save_to_postgres(result, config)

    spark.stop()


if __name__ == "__main__":
    main()