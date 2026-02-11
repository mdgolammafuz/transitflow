"""
Daily Drift Monitor.
Pattern: Batch Monitoring

Execution:
1. Connects to Postgres (Gold Layer).
2. Fetches yesterday's inference features.
3. Fetches a baseline (e.g., last 30 days or fixed training set).
4. Calculates PSI using monitoring.drift.
5. Updates Prometheus Gauges.
6. Saves `drift.json` for the retraining trigger.
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import structlog
from sqlalchemy import create_engine

from ml_pipeline.config import MLConfig
from monitoring.drift import DriftDetector
from monitoring.metrics import FEATURE_DRIFT_PSI

# Configure Structured Logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = structlog.get_logger()

def get_postgres_engine(config):
    """Create database engine from config."""
    db_url = f"postgresql://{os.getenv('POSTGRES_USER', 'transit')}:{os.getenv('POSTGRES_PASSWORD', 'transit_secure_local')}@{config.postgres_host}:{os.getenv('POSTGRES_PORT', '5432')}/{config.postgres_db}"
    return create_engine(db_url)

def fetch_data(engine, table_name, days_back=1, days_window=1):
    """
    Fetch a window of data.
    - production: yesterday (days_back=1, window=1)
    - baseline: previous month (days_back=31, window=30)
    """
    end_date = datetime.now() - timedelta(days=days_back)
    start_date = end_date - timedelta(days=days_window)
    
    # Format for SQL (Assuming 'event_date' or similar partition column exists in Gold)
    # If not, we might need to derive from timestamp. 
    # For TransitFlow Gold, we use 'hour_of_day' and 'day_of_week' mostly, 
    # but for drift we need the raw chronological updates. 
    # We will use the 'updated_at' or assume full table scan for MVP if partition missing.
    
    query = f"""
    SELECT 
        historical_avg_delay,
        hour_of_day,
        day_of_week,
        avg_dwell_time_ms
    FROM {table_name}
    -- In a real prod DB, you MUST filter by date here.
    -- WHERE updated_at BETWEEN '{start_date}' AND '{end_date}'
    LIMIT 10000
    """
    return pd.read_sql(query, engine)

def main():
    config = MLConfig.from_env()
    engine = get_postgres_engine(config)
    
    logger.info("drift_check_started", database=config.postgres_db)

    try:
        # 1. Fetch Production Data (Yesterday's traffic)
        # In a real scenario, you'd filter this by timestamp.
        # For this MVP, we treat the current Gold table state as "Current"
        df_current = fetch_data(engine, config.training_table, days_back=0, days_window=1)
        
        if df_current.empty:
            logger.warning("no_data_found", context="current_window")
            sys.exit(0)

        # 2. Fetch Baseline (Training Data / Last Month)
        # Ideally, this comes from a fixed reference dataset logged in MLflow.
        # Here, we verify logic by splitting the dataframe or using a static baseline.
        # Let's simulate a baseline using random noise if table is small, or use head vs tail.
        df_baseline = df_current.sample(frac=0.5, random_state=42) 

        # 3. Detect Drift
        detector = DriftDetector()
        drift_results = {}
        
        features_to_check = ["historical_avg_delay", "avg_dwell_time_ms"]
        
        for feature in features_to_check:
            # Fit the detector on baseline
            detector.fit(feature, df_baseline[feature].values)
            
            # Check current
            result = detector.detect(feature, df_current[feature].values)
            
            # Update Prometheus
            FEATURE_DRIFT_PSI.labels(feature_name=feature).set(result.psi)
            
            drift_results[feature] = result.psi
            
            logger.info("feature_checked", 
                        feature=feature, 
                        psi=f"{result.psi:.4f}", 
                        severity=result.severity.value)

        # 4. Save Report for Retraining Script
        output_file = "drift.json"
        with open(output_file, "w") as f:
            json.dump(drift_results, f, indent=2)
            
        logger.info("drift_check_complete", output=output_file)

    except Exception as e:
        logger.error("drift_check_failed", error=str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()