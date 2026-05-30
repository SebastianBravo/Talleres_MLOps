import logging
import os

import mlflow
from mlflow.exceptions import MlflowException

logger = logging.getLogger(__name__)

VOLUME_INCREASE_THRESHOLD = 0.10   # 10% volume increase triggers retraining
MIN_BATCH_RATIO = float(os.environ.get("MIN_BATCH_RATIO", "0.05"))  # 5% gate


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


def _batch_meets_minimum_size(batch_id, connection, raw_table):
    """
    Gate: returns (passes, reason).

    A batch that represents less than MIN_BATCH_RATIO of the accumulated
    historical data cannot meaningfully shift a model already trained on a
    much larger dataset. When this gate fails, all data-driven criteria
    (volume, drift, new categories) are bypassed — only the absence of a
    production model can still trigger training.

    Why: the KS test becomes excessively sensitive at large N. With 400k
    reference rows, even a 0.1% distributional shift yields p < 0.05,
    making drift detection fire on changes that have zero practical impact
    on model quality.
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
        return True, None  # First batch: gate always open

    ratio = new / historical
    if ratio < MIN_BATCH_RATIO:
        return False, (
            f"Batch too small relative to historical data: {new} records = {ratio:.1%} "
            f"of {historical} accumulated rows (minimum required: {MIN_BATCH_RATIO:.0%}). "
            "Volume, drift and category criteria skipped."
        )
    return True, None


def should_train(
    batch_id,
    connection,
    drift_result,
    new_categories_result,
    registered_model_name,
    raw_table="property_raw",
):
    """
    Evaluates criteria and returns (bool, list_of_reasons).

    Evaluation order:
    0. Gate — batch must be >= MIN_BATCH_RATIO of historical data.
       If the gate fails, criteria 2-4 are skipped entirely. Only the
       absence of a production model (criterion 1) can still force training.
    1. No production model → train as baseline (bypasses gate).
    2. Volume increase >= 10 % → train.
    3. New significant categories (frequency >= 1 %) → train.
    4. Distribution drift (KS p < 0.05) → train.
    """
    reasons = []
    trigger = False

    # Criterion 1 — no production model (evaluated before the gate)
    no_production = not _production_model_exists(registered_model_name)
    if no_production:
        trigger = True
        reasons.append("No production model exists — training baseline")

    # Gate — minimum batch size relative to historical data
    gate_passes, gate_reason = _batch_meets_minimum_size(batch_id, connection, raw_table)
    if not gate_passes:
        reasons.append(gate_reason)
        if not trigger:
            reasons.append(
                "Training skipped: batch did not pass the minimum size gate. "
                "Drift, volume and category criteria were not evaluated."
            )
        return trigger, reasons

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
