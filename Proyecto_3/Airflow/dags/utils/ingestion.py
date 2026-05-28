import hashlib
import json
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

PROPERTY_COLUMNS = [
    "brokered_by", "status", "price", "bed", "bath",
    "acre_lot", "street", "city", "state", "zip_code",
    "house_size", "prev_sold_date",
]


def store_raw_batch(connection, table_name, records, batch_id):
    """
    Inserts raw API records into the property_raw table.

    Each row gets a row_hash for deduplication and a batch_record_id
    representing its index within the current batch.
    Returns the number of rows inserted.
    """
    if not records:
        logger.warning("No records to store for batch %d", batch_id)
        return 0

    df = pd.DataFrame(records)

    # Ensure all expected columns exist (fill missing with None)
    for col in PROPERTY_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df["batch_id"] = batch_id
    df["load_timestamp"] = datetime.utcnow()
    df["record_status"] = "active"
    df["batch_record_id"] = range(len(df))

    df["row_hash"] = (
        df[PROPERTY_COLUMNS]
        .astype(str)
        .agg("|".join, axis=1)
        .apply(lambda v: hashlib.md5(v.encode("utf-8")).hexdigest())
    )

    df = df.replace({float("nan"): None})

    insert_cols = (
        ["batch_id", "batch_record_id", "load_timestamp", "record_status", "row_hash"]
        + PROPERTY_COLUMNS
    )
    quoted_cols = ", ".join(f'"{c}"' for c in insert_cols)
    placeholders = ", ".join(["%s"] * len(insert_cols))

    values = [tuple(row) for row in df[insert_cols].values]
    cursor = connection.cursor()
    cursor.executemany(
        f"INSERT INTO {table_name} ({quoted_cols}) VALUES ({placeholders})", values
    )
    connection.commit()
    cursor.close()

    logger.info("Stored %d records in %s (batch %d)", len(values), table_name, batch_id)
    return len(values)


def create_batch_audit_entry(connection, table_name, batch_id, run_id, records_received):
    """Creates the initial audit row for a batch (status='running')."""
    cursor = connection.cursor()
    cursor.execute(
        f"""
        INSERT INTO {table_name} (batch_id, run_id, fetched_at, records_received, execution_status)
        VALUES (%s, %s, %s, %s, 'running')
        ON CONFLICT (batch_id) DO NOTHING
        """,
        (batch_id, run_id, datetime.utcnow(), records_received),
    )
    connection.commit()
    cursor.close()


def update_batch_audit(connection, table_name, batch_id, **fields):
    """
    Partial update of the batch_audit row for batch_id.

    Keyword arguments map directly to column names.  JSONB columns
    (dicts/lists) are automatically serialised.
    """
    if not fields:
        return

    jsonb_cols = {
        "quality_issues", "new_categories_detected", "drift_details", "training_reasons",
    }

    set_parts = []
    values = []
    for key, val in fields.items():
        set_parts.append(f"{key} = %s")
        if key in jsonb_cols and isinstance(val, (dict, list)):
            values.append(json.dumps(val))
        else:
            values.append(val)

    values.append(batch_id)
    cursor = connection.cursor()
    cursor.execute(
        f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE batch_id = %s",
        values,
    )
    connection.commit()
    cursor.close()
