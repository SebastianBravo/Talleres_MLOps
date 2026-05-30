"""Utilities for the real estate MLOps DAG."""

from .db_connection import close_db_connection, connect_to_db, execute_query
from .db_schema import (
    create_batch_audit_table,
    create_inference_logs_table,
    create_table_clean,
    create_table_raw,
)
from .dataset_io import BatchExhaustedError, fetch_batch_from_api, load_batch_from_tmpfile, save_batch_to_tmpfile
from .drift_detection import detect_drift
from .inference_api import reload_inference_api
from .ingestion import create_batch_audit_entry, store_raw_batch, update_batch_audit
from .model_comparison import compare_with_production, promote_to_production
from .preprocess import preprocess_and_store
from .storage_utils import connect_to_minio
from .training import register_candidate, train_candidate
from .training_decision import should_train
from .validation import detect_new_categories, validate_data_quality, validate_schema

__all__ = [
    "connect_to_db",
    "close_db_connection",
    "execute_query",
    "connect_to_minio",
    "create_table_raw",
    "create_table_clean",
    "create_batch_audit_table",
    "create_inference_logs_table",
    "delete_table_if_exists",
    "BatchExhaustedError",
    "fetch_batch_from_api",
    "save_batch_to_tmpfile",
    "load_batch_from_tmpfile",
    "store_raw_batch",
    "create_batch_audit_entry",
    "update_batch_audit",
    "validate_schema",
    "validate_data_quality",
    "detect_new_categories",
    "detect_drift",
    "preprocess_and_store",
    "train_candidate",
    "register_candidate",
    "should_train",
    "compare_with_production",
    "promote_to_production",
    "reload_inference_api",
]
