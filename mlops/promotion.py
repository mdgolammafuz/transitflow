"""
Model Promotion Automation.
Pattern: CD for ML

Promotes valid models from Experiment -> Staging -> Production.
Aligned with: ml_pipeline.config.py (Model Names)
"""

import argparse
import logging
import sys
import json
from datetime import datetime, timezone
import mlflow
from mlflow.tracking import MlflowClient

from ml_pipeline.config import MLConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

config = MLConfig.from_env()

def promote_to_stage(run_id: str, stage: str, archive_existing: bool = False):
    """Register and promote a model version."""
    client = MlflowClient()
    model_name = config.model_name  # Source of Truth

    try:
        # 1. Ensure Registered Model Exists
        try:
            client.get_registered_model(model_name)
        except Exception:
            logger.info(f"Creating registered model: {model_name}")
            client.create_registered_model(model_name)

        # 2. Create Version
        model_uri = f"runs:/{run_id}/model"
        version_info = client.create_model_version(
            name=model_name,
            source=model_uri,
            run_id=run_id
        )
        version = version_info.version
        logger.info(f"Created version {version} for {model_name}")

        # 3. Transition Stage
        client.transition_model_version_stage(
            name=model_name,
            version=version,
            stage=stage,
            archive_existing_versions=archive_existing
        )
        
        # 4. Tagging
        client.set_model_version_tag(model_name, version, "promoted_at", datetime.now(timezone.utc).isoformat())
        client.set_model_version_tag(model_name, version, "promoted_by", "ci_pipeline")
        
        return {"success": True, "version": version, "stage": stage}

    except Exception as e:
        logger.error(f"Promotion failed: {e}")
        return {"success": False, "error": str(e)}

def rollback_to_version(model_name: str, target_version: str):
    """Rollback Production to a specific previous version."""
    client = MlflowClient()
    try:
        # 1. Verify Target Exists
        target = client.get_model_version(model_name, target_version)
        
        # 2. Get Current Production
        prod_versions = client.get_latest_versions(model_name, stages=["Production"])
        if prod_versions:
            current = prod_versions[0]
            # Archive current production
            client.transition_model_version_stage(
                name=model_name,
                version=current.version,
                stage="Archived"
            )
            logger.info(f"Archived current production version {current.version}")

        # 3. Promote Target
        client.transition_model_version_stage(
            name=model_name,
            version=target_version,
            stage="Production"
        )
        
        # 4. Log
        timestamp = datetime.now(timezone.utc).isoformat()
        client.set_model_version_tag(model_name, target_version, "rollback_at", timestamp)
        
        return {"success": True, "rolled_back_to": target_version}

    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        return {"success": False, "error": str(e)}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--stage", choices=["Staging", "Production"], required=True)
    args = parser.parse_args()

    # Production promotions should archive previous versions
    archive = (args.stage == "Production")
    
    result = promote_to_stage(args.run_id, args.stage, archive_existing=archive)
    
    print(json.dumps(result, indent=2))
    sys.exit(0 if result["success"] else 1)

if __name__ == "__main__":
    main()