import logging
import os

import mlflow
from mlflow.exceptions import MlflowException

logger = logging.getLogger(__name__)

VOLUME_INCREASE_THRESHOLD = 0.10   # 10 % increase triggers retraining


def _production_model_exists(registered_model_name):
    """Returns True when a version with the 'production' alias exists in MLflow."""
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = mlflow.MlflowClient()
    try:
        version = client.get_model_version_by_alias(registered_model_name, "production")
        return version is not None
    except MlflowException:
        return False


def _volume_increase(batch_id, connection, raw_table):
    """
    Returns (triggered, reason).  Triggered when the current batch increases
    the total accumulated rows by at least VOLUME_INCREASE_THRESHOLD.
    """
    cursor = connection.cursor()
    cursor.execute(
        f"SELECT COUNT(*) FROM {raw_table} WHERE batch_id < %s", (batch_id,)
    )
    historical = cursor.fetchone()[0]

    cursor.execute(
        f"SELECT COUNT(*) FROM {raw_table} WHERE batch_id = %s", (batch_id,)
    )
    new = cursor.fetchone()[0]
    cursor.close()

    if historical == 0:
        return True, f"First data in system ({new} records — baseline training)"

    ratio = new / historical
    if ratio >= VOLUME_INCREASE_THRESHOLD:
        return (
            True,
            f"Batch increases volume by {ratio:.1%} ({new} new / {historical} historic)",
        )

    return (
        False,
        f"Volume increase {ratio:.1%} < required {VOLUME_INCREASE_THRESHOLD:.0%}",
    )


def should_train(
    batch_id,
    connection,
    drift_result,
    new_categories_result,
    registered_model_name,
    raw_table="property_raw",
):
    """
    Evaluates four independent criteria and returns (bool, list_of_reasons).

    Criteria (any one suffices to trigger training):
    1. No production model exists yet — train as baseline.
    2. New batch increases accumulated volume by >= 10 %.
    3. New significant categories detected (frequency >= 1 %).
    4. Distribution drift detected via KS test (p < 0.05).
    """
    reasons = []
    trigger = False

    # Criterion 1 — no production model
    if not _production_model_exists(registered_model_name):
        trigger = True
        reasons.append("No production model exists — training baseline")

    # Criterion 2 — volume increase
    vol_trigger, vol_reason = _volume_increase(batch_id, connection, raw_table)
    if vol_trigger:
        trigger = True
        reasons.append(f"Volume: {vol_reason}")

    # Criterion 3 — new categories
    if new_categories_result.get("significant_new", False):
        trigger = True
        for col, info in new_categories_result.get("new_categories", {}).items():
            reasons.append(
                f"New categories in '{col}': {info['new_values'][:5]} "
                f"({info['frequency']:.1%} of batch)"
            )

    # Criterion 4 — distribution drift
    if drift_result.get("drift_detected", False):
        trigger = True
        drifted = drift_result.get("drifted_columns", [])
        reasons.append(f"Distribution drift detected in: {drifted}")

    if not trigger:
        reasons.append(
            "No criterion met: volume increase < 10 %, no drift, "
            "no new categories, production model exists"
        )

    return trigger, reasons
