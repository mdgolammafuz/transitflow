"""
ML Training Orchestrator.
Bridge between Delta Lake (Spark) and XGBoost (Pandas).
"""

import sys
import structlog
from pyspark.sql import SparkSession
from ml_pipeline.config import MLConfig
from ml_pipeline.training import DelayPredictor

logger = structlog.get_logger()

def main():
    config = MLConfig.from_env()
    
    # 1. Initialize Spark Session with Delta support
    spark = SparkSession.builder \
        .appName("TransitFlow-ML-Training") \
        .getOrCreate()

    try:
        table_path = f"{config.delta_lake_path}/{config.training_table}"
        logger.info("loading_training_data", path=table_path)
        
        # 2. Read from Delta Lake Gold Layer
        df_spark = spark.read.format("delta").load(table_path)
        
        # 3. Convert to Pandas for local XGBoost training
        df_pd = df_spark.toPandas()
        
        if df_pd.empty:
            logger.error("empty_dataset_error", path=table_path)
            sys.exit(1)

        logger.info("data_loaded_successfully", rows=len(df_pd))

        # 4. Trigger Training from our ml_pipeline module
        predictor = DelayPredictor(config)
        result = predictor.train(df_pd)
        
        logger.info("training_complete", 
                    mae=f"{result.test_mae:.2f}s", 
                    r2=f"{result.r2_score:.4f}",
                    version=result.model_version)

    except Exception as e:
        logger.error("orchestration_failed", error=str(e))
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()