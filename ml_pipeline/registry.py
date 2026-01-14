import mlflow
import mlflow.xgboost
from ml_pipeline.config import MLConfig
import structlog

logger = structlog.get_logger()

class ModelRegistry:
    def __init__(self, config: MLConfig):
        self.config = config
        mlflow.set_tracking_uri(config.mlflow_tracking_uri)
        mlflow.set_experiment(config.mlflow_experiment_name)

    def log_model(self, model, metrics: dict, params: dict):
        """Log model, parameters, and metrics to MLflow."""
        with mlflow.start_run():
            # Log hyperparameters
            mlflow.log_params(params)
            
            # Log evaluation metrics (MAE, R2, etc.)
            mlflow.log_metrics(metrics)
            
            # Register the model in the registry
            mlflow.xgboost.log_model(
                xgb_model=model,
                artifact_path="model",
                registered_model_name=self.config.model_name
            )
            
            run_id = mlflow.active_run().info.run_id
            logger.info("model_registered", 
                        run_id=run_id, 
                        model_name=self.config.model_name)
            return run_id

    def load_latest_model(self):
        """Retrieve the latest version of the model for serving."""
        model_uri = f"models:/{self.config.model_name}/latest"
        logger.info("loading_model", uri=model_uri)
        return mlflow.xgboost.load_model(model_uri)