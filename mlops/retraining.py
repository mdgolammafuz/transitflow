"""
Auto-Retrain Trigger.
Pattern: Closed-loop ML feedback

Monitors drift metrics and triggers retraining when thresholds are exceeded.
Aligned with: monitoring.drift (PSI Logic)
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import requests
import mlflow

# Configuration
PSI_THRESHOLD = float(os.getenv("DRIFT_PSI_THRESHOLD", "0.2"))
GITHUB_REPO = os.getenv("GITHUB_REPO", "your-org/transitflow")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

@dataclass
class RetrainDecision:
    timestamp: str
    trigger_reason: str
    drift_metrics: dict
    decision: str
    action: str

def check_drift_and_decide(metrics: dict, threshold: float, dry_run: bool) -> RetrainDecision:
    """Decide whether to retrain based on PSI metrics."""
    drifted_features = [f for f, psi in metrics.items() if psi > threshold]
    
    if drifted_features:
        reason = f"Drift detected in: {', '.join(drifted_features)}"
        decision = "retrain"
    else:
        reason = "No significant drift"
        decision = "skip"

    action = "trigger_pipeline" if decision == "retrain" and not dry_run else "log_only"
    
    return RetrainDecision(
        timestamp=datetime.now(timezone.utc).isoformat(),
        trigger_reason=reason,
        drift_metrics=metrics,
        decision=decision,
        action=action
    )

def trigger_github_action(reason: str):
    """Trigger the retraining workflow via GitHub API."""
    if not GITHUB_TOKEN:
        logger.warning("No GITHUB_TOKEN set. Skipping trigger.")
        return False
        
    url = f"https://api.github.com/repos/{GITHUB_REPO}/dispatches"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    payload = {"event_type": "retrain-model", "client_payload": {"reason": reason}}
    
    try:
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        logger.info("GitHub Action triggered successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to trigger GitHub Action: {e}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--drift-file", help="Path to drift metrics JSON")
    parser.add_argument("--threshold", type=float, default=PSI_THRESHOLD)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Load metrics (In prod this comes from Prometheus, here we load from file/input)
    metrics = {}
    if args.drift_file and os.path.exists(args.drift_file):
        with open(args.drift_file) as f:
            metrics = json.load(f)
    else:
        # Fallback/Demo values
        metrics = {"speed": 0.05, "delay": 0.22} 

    decision = check_drift_and_decide(metrics, args.threshold, args.dry_run)
    
    # Log to MLflow
    try:
        mlflow.log_param("retrain_decision", decision.decision)
        mlflow.log_dict(asdict(decision), "retrain_decision.json")
    except Exception:
        pass

    print(json.dumps(asdict(decision), indent=2))

    if decision.decision == "retrain" and not args.dry_run:
        trigger_github_action(decision.trigger_reason)

if __name__ == "__main__":
    main()