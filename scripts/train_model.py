"""
ML Training Orchestrator.
Bridge between Delta Lake (Spark) and XGBoost (Pandas).
"""

import os
import sys

import structlog
from pyspark.sql import SparkSession

from ml_pipeline.config import MLConfig
from ml_pipeline.training import DelayPredictor

logger = structlog.get_logger()


def main():
    config = MLConfig.from_env()

    # 1. Initialize Spark Session with S3A/MinIO credentials
    spark = (
        SparkSession.builder.appName("TransitFlow-ML-Training")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )

    try:
        table_path = f"{config.delta_lake_path}/{config.training_table}"
        logger.info("loading_training_data", path=table_path)

        # 2. Read from Delta Lake
        df_spark = spark.read.format("delta").load(table_path)

        # 3. Convert to Pandas
        df_pd = df_spark.toPandas()

        if df_pd.empty:
            logger.error("empty_dataset_error", path=table_path)
            sys.exit(1)

        logger.info("data_loaded_successfully", rows=len(df_pd))

        # 4. Trigger Training
        predictor = DelayPredictor(config)
        result = predictor.train(df_pd)

        logger.info("training_complete", mae=f"{result.test_mae:.2f}s", r2=f"{result.r2_score:.4f}")

    except Exception as e:
        logger.error("orchestration_failed", error=str(e))
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
