import hashlib
import logging
import os
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler

from .db_schema import create_table_clean, delete_table_if_exists
from .storage_utils import connect_to_minio

logger = logging.getLogger(__name__)

NUMERIC_FEATURES = ["bed", "bath", "acre_lot", "house_size"]
HIGH_CARD_CATEGORICAL = ["brokered_by", "street", "city", "state", "zip_code"]
LOW_CARD_CATEGORICAL = ["status"]
DATE_FEATURE = "prev_sold_date"

MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "real-estate-project")


def _ensure_bucket(s3_client, bucket):
    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket"):
            s3_client.create_bucket(Bucket=bucket)
            logger.info("Bucket '%s' created in MinIO", bucket)
        else:
            raise


def _parse_date_features(df):
    """Extracts year, month and days-since-reference from prev_sold_date."""
    if DATE_FEATURE in df.columns:
        parsed = pd.to_datetime(df[DATE_FEATURE], errors="coerce")
        df["prev_sold_year"] = parsed.dt.year.astype("float64")
        df["prev_sold_month"] = parsed.dt.month.astype("float64")
        ref = pd.Timestamp("2025-01-01")
        df["prev_sold_days_ago"] = (ref - parsed).dt.days.astype("float64")
    else:
        df["prev_sold_year"] = np.nan
        df["prev_sold_month"] = np.nan
        df["prev_sold_days_ago"] = np.nan
    return df


def _deterministic_split(series_id):
    """80/20 train-test split using MD5 hash of row id (stable across reruns)."""
    return series_id.apply(
        lambda x: "test"
        if int(hashlib.md5(str(x).encode()).hexdigest(), 16) % 10 < 2
        else "train"
    )


def preprocess_and_store(
    connection,
    raw_table,
    clean_table,
    batch_id,
    bucket=MINIO_BUCKET,
    preprocessor_path="preprocessor",
):
    """
    Fits a preprocessing pipeline on training data, transforms all rows and
    writes the result to clean_table.  Saves the fitted preprocessor to MinIO
    and returns a version string for audit tracking.

    Unknown categories seen at transform time are mapped to -1 via OrdinalEncoder
    (handle_unknown='use_encoded_value') so new cities / states don't crash
    the pipeline.
    """
    df = pd.read_sql_query(f"SELECT * FROM {raw_table}", connection)
    if df.empty:
        raise ValueError("Raw table is empty — nothing to preprocess")

    logger.info("Loaded %d rows from %s", len(df), raw_table)

    # Parse dates
    df = _parse_date_features(df)

    # Coerce numeric columns
    for col in NUMERIC_FEATURES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with missing target
    before = len(df)
    df = df.dropna(subset=["price"])
    df["price"] = df["price"].astype(float)
    logger.info("Dropped %d rows with missing price", before - len(df))

    # Deterministic train/test split using row id
    df["dataset"] = _deterministic_split(df["id"])

    # Feature lists
    date_features = ["prev_sold_year", "prev_sold_month", "prev_sold_days_ago"]
    num_cols = [c for c in NUMERIC_FEATURES + date_features if c in df.columns]
    high_cat_cols = [c for c in HIGH_CARD_CATEGORICAL if c in df.columns]
    low_cat_cols = [c for c in LOW_CARD_CATEGORICAL if c in df.columns]
    feature_cols = num_cols + high_cat_cols + low_cat_cols

    X = df[feature_cols].copy()
    y = df["price"]

    train_mask = df["dataset"] == "train"
    test_mask = df["dataset"] == "test"
    X_train, X_test = X[train_mask], X[test_mask]
    y_train, y_test = y[train_mask], y[test_mask]

    logger.info("Train: %d rows, Test: %d rows", len(X_train), len(X_test))

    if X_train.empty:
        raise ValueError("Training split is empty after preprocessing")

    # Build pipelines
    numeric_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])
    # OrdinalEncoder with handle_unknown='use_encoded_value' maps unseen categories
    # to -1 instead of raising an error, keeping the pipeline robust to new data.
    high_cat_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value="unknown")),
        ("encoder", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
    ])
    low_cat_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
    ])

    transformers = []
    if num_cols:
        transformers.append(("num", numeric_pipe, num_cols))
    if high_cat_cols:
        transformers.append(("high_cat", high_cat_pipe, high_cat_cols))
    if low_cat_cols:
        transformers.append(("low_cat", low_cat_pipe, low_cat_cols))

    preprocessor = ColumnTransformer(transformers=transformers, remainder="drop")
    preprocessor.fit(X_train)

    X_train_p = preprocessor.transform(X_train)
    X_test_p = preprocessor.transform(X_test)

    feature_names_out = preprocessor.get_feature_names_out().tolist()
    sanitized = [
        n.replace("__", "_").replace(" ", "_").replace("-", "_")
        for n in feature_names_out
    ]

    # Persist preprocessor to MinIO
    minio = connect_to_minio()
    _ensure_bucket(minio, bucket)

    processed_at = datetime.utcnow()
    version = f"batch_{batch_id}_{processed_at.strftime('%Y%m%d%H%M%S')}"

    local_dir = os.path.join(preprocessor_path, version)
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, "preprocessor.joblib")
    joblib.dump(preprocessor, local_path)

    remote_key = f"preprocess/{version}/preprocessor.joblib"
    minio.upload_file(local_path, bucket, remote_key)
    logger.info("Preprocessor saved to MinIO: %s", remote_key)

    # Build clean DataFrames
    train_ids = df.loc[train_mask, "id"].values
    test_ids = df.loc[test_mask, "id"].values

    df_train = pd.DataFrame(X_train_p, columns=sanitized)
    df_train["batch_id"] = batch_id
    df_train["source_record_id"] = train_ids
    df_train["dataset"] = "train"
    df_train["price"] = y_train.values

    df_test = pd.DataFrame(X_test_p, columns=sanitized)
    df_test["batch_id"] = batch_id
    df_test["source_record_id"] = test_ids
    df_test["dataset"] = "test"
    df_test["price"] = y_test.values

    df_clean = pd.concat([df_train, df_test], ignore_index=True)

    # Recreate clean table (handles schema changes across batches)
    delete_table_if_exists(connection, clean_table)
    create_table_clean(connection, clean_table, sanitized, "price")

    all_cols = ["batch_id", "source_record_id", "dataset"] + sanitized + ["price"]
    quoted_cols = ", ".join(f'"{c}"' for c in all_cols)
    placeholders = ", ".join(["%s"] * len(all_cols))

    values = [tuple(row) for row in df_clean[all_cols].values]
    cursor = connection.cursor()
    cursor.executemany(
        f"INSERT INTO {clean_table} ({quoted_cols}) VALUES ({placeholders})", values
    )
    connection.commit()
    cursor.close()

    logger.info(
        "Inserted %d rows into %s (%d features)",
        len(values), clean_table, len(sanitized),
    )

    return {
        "preprocessor_version": version,
        "records_processed": len(df_clean),
        "train_count": len(df_train),
        "test_count": len(df_test),
        "feature_count": len(sanitized),
    }
