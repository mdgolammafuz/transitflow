import mlflow
import mlflow.pyfunc
import mlflow.sklearn
import structlog
from mlflow.models.signature import infer_signature

from ml_pipeline.config import MLConfig

logger = structlog.get_logger()


class ModelRegistry:
    def __init__(self, config: MLConfig):
        self.config = config
        mlflow.set_tracking_uri(config.mlflow_tracking_uri)
        mlflow.set_experiment(config.mlflow_experiment_name)

        # 1. Disable all autologging to prevent background interference
        mlflow.autolog(disable=True)

        # 2. Specifically target the XGBoost autologger if the library is present
        try:
            import xgboost  # noqa: F401

            # We use the full path to avoid variable shadowing
            mlflow.xgboost.autolog(disable=True)
        except (ImportError, AttributeError):
            pass

    def log_model(self, model, metrics: dict, params: dict = None, X_sample=None, y_sample=None):
        """
        Logs model using explicit signature to bypass MLflow internal bugs.
        """
        try:
            with mlflow.start_run() as run:
                # Log metrics and filtered params
                if params:
                    clean_params = {
                        k: v for k, v in params.items() if isinstance(v, (str, int, float, bool))
                    }
                    mlflow.log_params(clean_params)

                mlflow.log_metrics(metrics)

                # Define signature explicitly
                signature = None
                if X_sample is not None and y_sample is not None:
                    signature = infer_signature(X_sample, y_sample)

                # Use sklearn flavor for XGBRegressor compatibility
                mlflow.sklearn.log_model(
                    sk_model=model,
                    artifact_path="model",
                    registered_model_name=self.config.model_name,
                    signature=signature,
                )

                logger.info(
                    "model_registered", run_id=run.info.run_id, model_name=self.config.model_name
                )
                return run.info.run_id
        except Exception as e:
            logger.error("mlflow_log_failed", error=str(e))
            raise e

    def load_latest_model(self):
        """Standard loading logic for serving."""
        model_uri = f"models:/{self.config.model_name}/latest"
        return mlflow.pyfunc.load_model(model_uri)
