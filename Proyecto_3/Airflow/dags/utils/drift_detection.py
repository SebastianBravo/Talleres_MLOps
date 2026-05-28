import logging

import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

KEY_NUMERIC_COLS = ["price", "house_size", "bed", "bath", "acre_lot"]
DRIFT_P_VALUE_THRESHOLD = 0.05
MIN_REFERENCE_ROWS = 50


def detect_drift(records, connection, raw_table="property_raw"):
    """
    Applies a two-sample Kolmogorov-Smirnov test on each key numeric column,
    comparing the new batch distribution against all previously stored data.

    Returns a dict with 'drift_detected' (bool), 'drifted_columns' and
    per-column KS statistics under 'details'.
    """
    if not records:
        return {
            "drift_detected": False,
            "drifted_columns": [],
            "reason": "Empty batch",
            "details": {},
        }

    new_df = pd.DataFrame(records)

    # Load historical data (batches prior to the current one)
    try:
        cols_csv = ", ".join(KEY_NUMERIC_COLS)
        ref_df = pd.read_sql_query(
            f"SELECT {cols_csv} FROM {raw_table}", connection
        )
    except Exception as exc:
        logger.warning("Could not load reference data for drift detection: %s", exc)
        return {
            "drift_detected": False,
            "drifted_columns": [],
            "reason": "No reference data available",
            "details": {},
        }

    if len(ref_df) < MIN_REFERENCE_ROWS:
        return {
            "drift_detected": False,
            "drifted_columns": [],
            "reason": f"Insufficient reference data ({len(ref_df)} rows < {MIN_REFERENCE_ROWS})",
            "details": {},
        }

    details = {}
    drifted_cols = []

    for col in KEY_NUMERIC_COLS:
        if col not in new_df.columns or col not in ref_df.columns:
            continue

        new_vals = pd.to_numeric(new_df[col], errors="coerce").dropna()
        ref_vals = pd.to_numeric(ref_df[col], errors="coerce").dropna()

        if len(new_vals) < 10 or len(ref_vals) < 10:
            continue

        ks_stat, p_value = stats.ks_2samp(ref_vals.values, new_vals.values)
        drifted = bool(p_value < DRIFT_P_VALUE_THRESHOLD)

        details[col] = {
            "ks_statistic": round(float(ks_stat), 6),
            "p_value": round(float(p_value), 6),
            "drift_detected": drifted,
        }

        if drifted:
            drifted_cols.append(col)
            logger.info(
                "Drift detected in '%s': KS=%.4f, p=%.4f", col, ks_stat, p_value
            )

    return {
        "drift_detected": len(drifted_cols) > 0,
        "drifted_columns": drifted_cols,
        "reason": (
            f"KS test drift in: {drifted_cols}" if drifted_cols else "No significant drift"
        ),
        "details": details,
    }
