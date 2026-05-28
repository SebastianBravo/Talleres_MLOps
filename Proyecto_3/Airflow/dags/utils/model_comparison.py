import logging
import os

import mlflow
import mlflow.sklearn
from mlflow.exceptions import MlflowException

logger = logging.getLogger(__name__)

# Promotion rule: MAE must improve by at least 3 %
# and RMSE must not worsen by more than 1 %
MAE_IMPROVEMENT_THRESHOLD = float(os.environ.get("PROMOTION_MAE_IMPROVEMENT", "0.03"))
RMSE_TOLERANCE = float(os.environ.get("PROMOTION_RMSE_TOLERANCE", "0.01"))


def _get_run_metrics(client, run_id):
    """Returns a dict of metrics logged to a given MLflow run."""
    run = client.get_run(run_id)
    return run.data.metrics


def compare_with_production(candidate_run_id, registered_model_name):
    """
    Compares the candidate model against the current production model using
    test metrics already logged in their respective MLflow runs.

    Returns a dict with:
      - should_promote (bool)
      - reason (str)
      - candidate_metrics (dict)
      - production_metrics (dict | None)
      - mae_improvement (float | None)
      - rmse_change (float | None)
    """
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = mlflow.MlflowClient()

    candidate_metrics = _get_run_metrics(client, candidate_run_id)
    candidate_mae = candidate_metrics.get("test_mae")
    candidate_rmse = candidate_metrics.get("test_rmse")

    if candidate_mae is None:
        raise ValueError(
            f"Candidate run {candidate_run_id} is missing 'test_mae' metric. "
            "Ensure train_candidate_model logs it correctly."
        )

    # Try to load the production model's originating run
    try:
        prod_version = client.get_model_version_by_alias(
            registered_model_name, "production"
        )
        prod_run_id = prod_version.run_id
        production_metrics = _get_run_metrics(client, prod_run_id)
        production_mae = production_metrics.get("test_mae")
        production_rmse = production_metrics.get("test_rmse")
    except MlflowException:
        # No production model exists yet
        return {
            "should_promote": True,
            "reason": "No production model exists — promoting candidate as baseline",
            "candidate_metrics": {k: v for k, v in candidate_metrics.items() if "test_" in k},
            "production_metrics": None,
            "mae_improvement": None,
            "rmse_change": None,
        }

    if production_mae is None:
        return {
            "should_promote": True,
            "reason": "Production model has no logged MAE — promoting candidate",
            "candidate_metrics": {k: v for k, v in candidate_metrics.items() if "test_" in k},
            "production_metrics": {k: v for k, v in production_metrics.items() if "test_" in k},
            "mae_improvement": None,
            "rmse_change": None,
        }

    mae_improvement = (production_mae - candidate_mae) / production_mae
    rmse_change = (
        (candidate_rmse - production_rmse) / production_rmse
        if production_rmse and candidate_rmse
        else 0.0
    )

    meets_mae = mae_improvement >= MAE_IMPROVEMENT_THRESHOLD
    meets_rmse = rmse_change <= RMSE_TOLERANCE
    should_promote = meets_mae and meets_rmse

    if should_promote:
        reason = (
            f"MAE improved by {mae_improvement:.2%} "
            f"(≥ {MAE_IMPROVEMENT_THRESHOLD:.0%}) and "
            f"RMSE change {rmse_change:.2%} "
            f"(≤ {RMSE_TOLERANCE:.0%} tolerance)"
        )
    else:
        parts = []
        if not meets_mae:
            parts.append(
                f"MAE improvement {mae_improvement:.2%} < required {MAE_IMPROVEMENT_THRESHOLD:.0%}"
            )
        if not meets_rmse:
            parts.append(
                f"RMSE worsened {rmse_change:.2%} > tolerance {RMSE_TOLERANCE:.0%}"
            )
        reason = "; ".join(parts)

    return {
        "should_promote": should_promote,
        "reason": reason,
        "candidate_metrics": {k: v for k, v in candidate_metrics.items() if "test_" in k},
        "production_metrics": {k: v for k, v in production_metrics.items() if "test_" in k},
        "mae_improvement": round(float(mae_improvement), 6),
        "rmse_change": round(float(rmse_change), 6),
    }


def promote_to_production(candidate_run_id, registered_model_name):
    """
    Registers a new model version from the candidate run and sets
    the 'production' alias on it.  Returns the new version number.
    """
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = mlflow.MlflowClient()

    model_uri = f"runs:/{candidate_run_id}/model"
    version = client.create_model_version(
        name=registered_model_name,
        source=model_uri,
        run_id=candidate_run_id,
    )
    client.set_registered_model_alias(
        registered_model_name, "production", version.version
    )
    logger.info(
        "Promoted %s v%s to production alias", registered_model_name, version.version
    )
    return version.version
