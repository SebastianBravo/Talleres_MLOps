import logging

import pandas as pd

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = {
    "brokered_by", "status", "price", "bed", "bath",
    "acre_lot", "street", "city", "state", "zip_code",
    "house_size", "prev_sold_date",
}

VALID_RANGES = {
    "price": (0, 1e9),
    "bed": (0, 100),
    "bath": (0, 100),
    "acre_lot": (0, 100_000),
    "house_size": (0, 1_000_000),
}


def validate_schema(records):
    """
    Checks that expected columns are present and no critical ones are missing.

    Returns a dict with 'valid' (bool) and 'issues' (list of strings).
    """
    if not records:
        return {"valid": False, "issues": ["Batch is empty — no records received"]}

    received = set(records[0].keys()) if isinstance(records[0], dict) else set()
    missing = EXPECTED_COLUMNS - received
    extra = received - EXPECTED_COLUMNS

    issues = []
    if missing:
        issues.append(f"Missing columns: {sorted(missing)}")
    if extra:
        logger.info("Extra columns (will be ignored): %s", sorted(extra))

    return {
        "valid": len(missing) == 0,
        "issues": issues,
        "missing_columns": sorted(missing),
        "extra_columns": sorted(extra),
    }


def validate_data_quality(records):
    """
    Evaluates null rates, duplicates, range violations and target consistency.

    Returns a dict with 'valid', 'quality_score' (0-1) and 'issues'.
    """
    if not records:
        return {"valid": False, "quality_score": 0.0, "issues": ["No records to evaluate"]}

    df = pd.DataFrame(records)
    issues = []

    # Null rates per column
    null_rates = {}
    for col in EXPECTED_COLUMNS:
        if col in df.columns:
            rate = float(df[col].isna().mean())
            null_rates[col] = rate
            if rate > 0.5:
                issues.append(f"'{col}' has {rate:.0%} null values (>50%)")

    # Duplicate rows
    dup_count = int(df.duplicated().sum())
    if dup_count > 0:
        issues.append(f"{dup_count} duplicate rows detected")

    # Range violations for numeric columns
    range_violations = {}
    for col, (lo, hi) in VALID_RANGES.items():
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            out_of_range = int(((numeric < lo) | (numeric > hi)).sum())
            if out_of_range > 0:
                issues.append(f"'{col}' has {out_of_range} out-of-range values")
            range_violations[col] = out_of_range

    # Price (target) sanity check
    if "price" in df.columns:
        price = pd.to_numeric(df["price"], errors="coerce")
        bad_price = int(price.isna().sum() + (price <= 0).sum())
        if bad_price > len(df) * 0.1:
            issues.append(
                f"High proportion of invalid prices: {bad_price}/{len(df)} rows"
            )

    # Minimum viable batch size
    if len(df) < 10:
        issues.append(f"Batch too small: {len(df)} records (minimum 10)")

    quality_score = max(0.0, 1.0 - len(issues) * 0.15)

    return {
        "valid": len(issues) == 0 or quality_score >= 0.5,
        "quality_score": quality_score,
        "issues": issues,
        "null_rates": null_rates,
        "range_violations": range_violations,
        "duplicate_count": dup_count,
        "record_count": len(df),
    }


def detect_new_categories(records, connection, raw_table="property_raw", batch_id=None):
    """
    Compares categorical values in the new batch against historical data
    from previous batches only (batch_id < current batch_id).

    A new category is considered significant when its frequency in the batch
    is >= 1 %.  Returns a dict with 'new_categories' and 'significant_new'.
    """
    if not records:
        return {"new_categories": {}, "significant_new": False}

    df = pd.DataFrame(records)
    tracked_cols = [c for c in ("status", "city", "state") if c in df.columns]

    new_categories = {}
    for col in tracked_cols:
        current_values = set(df[col].dropna().astype(str).unique())

        try:
            cursor = connection.cursor()
            if batch_id is not None:
                cursor.execute(
                    f"SELECT DISTINCT {col} FROM {raw_table} "
                    f"WHERE {col} IS NOT NULL AND batch_id < %s",
                    (batch_id,),
                )
            else:
                cursor.execute(
                    f"SELECT DISTINCT {col} FROM {raw_table} WHERE {col} IS NOT NULL"
                )
            historical_values = {str(row[0]) for row in cursor.fetchall()}
            cursor.close()
        except Exception as exc:
            logger.warning("Could not fetch historical values for '%s': %s", col, exc)
            historical_values = set()

        new_vals = current_values - historical_values
        if not new_vals:
            continue

        total = len(df[col].dropna())
        new_count = int(df[col].astype(str).isin(new_vals).sum())
        frequency = new_count / total if total > 0 else 0.0

        if frequency >= 0.01:
            new_categories[col] = {
                "new_values": sorted(new_vals)[:20],
                "count": new_count,
                "frequency": round(frequency, 4),
            }

    return {
        "new_categories": new_categories,
        "significant_new": len(new_categories) > 0,
    }
