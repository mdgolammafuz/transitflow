"""
ML Training Orchestrator.
Bridge between Postgres (Gold Marts) and XGBoost.
"""

import os
import sys

import pandas as pd
import structlog
from sqlalchemy import create_engine

from ml_pipeline.config import MLConfig
from ml_pipeline.training import DelayPredictor

import mlflow

logger = structlog.get_logger()


def main():
    config = MLConfig.from_env()
    
    # 1. Setup Connection to Postgres (The Offline Store)
    db_url = f"postgresql://{os.getenv('POSTGRES_USER', 'transit')}:{os.getenv('POSTGRES_PASSWORD', 'transit_secure_local')}@{config.postgres_host}:{os.getenv('POSTGRES_PORT', '5432')}/{config.postgres_db}"
    
    logger.info("connecting_to_feature_store", host=config.postgres_host, db=config.postgres_db)

    try:
        engine = create_engine(db_url)
        
        # 2. Fetch Training Data (The Gold Table)
        # We explicitly select only the columns we need + target
        query = f"""
        SELECT 
            stop_id,
            line_id,
            hour_of_day,
            day_of_week,
            historical_arrival_count,
            historical_avg_delay,
            avg_dwell_time_ms
        FROM {config.training_table}
        -- Optional: Limit rows for dev training
        -- LIMIT 10000 
        """
        
        logger.info("executing_query", query=query.strip())
        df_pd = pd.read_sql(query, engine)

        if df_pd.empty:
            logger.error("empty_dataset_error", table=config.training_table)
            logger.info("tip", msg="Did you run 'make dbt-run'? Is the mart populated?")
            sys.exit(1)

        logger.info("data_loaded_successfully", rows=len(df_pd))

        # 3. Trigger Training
        predictor = DelayPredictor(config)
        result = predictor.train(df_pd)

        logger.info("training_complete", mae=f"{result.test_mae:.2f}s", r2=f"{result.r2_score:.4f}")

    except Exception as e:
        logger.error("orchestration_failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()