from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "real_estate_mlops"
RAW_TABLE = "property_raw"
CLEAN_TABLE = "property_clean"
AUDIT_TABLE = "batch_audit"
REGISTERED_MODEL_NAME = "real-estate-price-model"


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def _create_tables():
    """Ensures all required DB tables exist before the pipeline runs."""
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.db_schema import (
        create_batch_audit_table,
        create_inference_logs_table,
        create_table_raw,
    )

    conn = connect_to_db()
    try:
        create_table_raw(conn, RAW_TABLE)
        create_batch_audit_table(conn, AUDIT_TABLE)
        create_inference_logs_table(conn)
    finally:
        close_db_connection(conn)


def _fetch_batch(**context):
    """Reads the next batch from the external data API and caches it locally."""
    from airflow.exceptions import AirflowSkipException

    from utils.dataset_io import BatchExhaustedError, fetch_batch_from_api, save_batch_to_tmpfile

    run_id = context["run_id"]

    try:
        records, batch_number = fetch_batch_from_api()
    except BatchExhaustedError as exc:
        print(f"All batches collected: {exc}")
        raise AirflowSkipException(str(exc))

    batch_file = save_batch_to_tmpfile(records, run_id)

    ti = context["ti"]
    ti.xcom_push(key="batch_id", value=batch_number)
    ti.xcom_push(key="batch_file", value=batch_file)
    ti.xcom_push(key="records_count", value=len(records))

    print(f"Fetched {len(records)} records for batch {batch_number}")


def _store_raw_batch(**context):
    """Inserts the cached batch into property_raw and creates the audit row."""
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.dataset_io import load_batch_from_tmpfile
    from utils.ingestion import create_batch_audit_entry, store_raw_batch

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")
    batch_file = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_file")
    records_count = ti.xcom_pull(task_ids="fetch_batch_from_api", key="records_count")
    run_id = context["run_id"]

    records = load_batch_from_tmpfile(batch_file)

    if not records:
        raise ValueError(
            f"Batch {batch_id} is empty — no data returned from API. "
            "Check the API endpoint and group configuration."
        )

    conn = connect_to_db()
    try:
        stored = store_raw_batch(conn, RAW_TABLE, records, batch_id)
        create_batch_audit_entry(conn, AUDIT_TABLE, batch_id, run_id, records_count)
    finally:
        close_db_connection(conn)

    print(f"Stored {stored} records for batch {batch_id}.")


def _validate_schema(**context):
    """Validates column presence and schema structure of the new batch."""
    from utils.dataset_io import load_batch_from_tmpfile
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.ingestion import update_batch_audit
    from utils.validation import validate_schema

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")
    batch_file = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_file")
    records = load_batch_from_tmpfile(batch_file)

    result = validate_schema(records)

    conn = connect_to_db()
    try:
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            schema_valid=result["valid"],
            schema_issues="; ".join(result["issues"]) if result["issues"] else None,
        )
    finally:
        close_db_connection(conn)

    if not result["valid"]:
        raise ValueError(
            f"Schema validation failed for batch {batch_id}: {result['issues']}"
        )

    ti.xcom_push(key="schema_result", value=result)
    print(f"Schema valid: {result['valid']}")


def _validate_data_quality(**context):
    """Evaluates null rates, duplicates and range violations."""
    from utils.dataset_io import load_batch_from_tmpfile
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.ingestion import update_batch_audit
    from utils.validation import validate_data_quality

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")
    batch_file = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_file")
    records = load_batch_from_tmpfile(batch_file)

    result = validate_data_quality(records)

    conn = connect_to_db()
    try:
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            quality_score=result["quality_score"],
            quality_issues=result["issues"],
        )
    finally:
        close_db_connection(conn)

    if not result["valid"]:
        raise ValueError(
            f"Data quality too poor for batch {batch_id} "
            f"(score={result['quality_score']:.2f}): {result['issues']}"
        )

    ti.xcom_push(key="quality_result", value=result)
    print(f"Quality score: {result['quality_score']:.2f}, valid: {result['valid']}")


def _detect_new_categories(**context):
    """Identifies categories in the new batch absent from historical data."""
    from utils.dataset_io import load_batch_from_tmpfile
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.ingestion import update_batch_audit
    from utils.validation import detect_new_categories

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")
    batch_file = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_file")
    records = load_batch_from_tmpfile(batch_file)

    conn = connect_to_db()
    try:
        result = detect_new_categories(records, conn, RAW_TABLE)
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            new_categories_detected=result["new_categories"],
        )
    finally:
        close_db_connection(conn)

    ti.xcom_push(key="new_categories_result", value=result)

    if result["significant_new"]:
        print(f"New significant categories detected: {list(result['new_categories'].keys())}")
    else:
        print("No significant new categories detected")


def _detect_data_drift(**context):
    """Runs KS test on numeric columns vs. historical distribution."""
    from utils.dataset_io import load_batch_from_tmpfile
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.drift_detection import detect_drift
    from utils.ingestion import update_batch_audit

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")
    batch_file = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_file")
    records = load_batch_from_tmpfile(batch_file)

    conn = connect_to_db()
    try:
        result = detect_drift(records, conn, RAW_TABLE)
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            drift_detected=result["drift_detected"],
            drift_details=result["details"],
        )
    finally:
        close_db_connection(conn)

    ti.xcom_push(key="drift_result", value=result)
    print(f"Drift detected: {result['drift_detected']} — {result.get('reason', '')}")


def _preprocess_data(**context):
    """Fits the preprocessing pipeline, transforms data and stores clean table."""
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.ingestion import update_batch_audit
    from utils.preprocess import preprocess_and_store

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")

    conn = connect_to_db()
    try:
        result = preprocess_and_store(conn, RAW_TABLE, CLEAN_TABLE, batch_id)
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            preprocessor_version=result["preprocessor_version"],
            records_stored=result["records_processed"],
        )
    finally:
        close_db_connection(conn)

    ti.xcom_push(key="preprocess_result", value=result)
    print(
        f"Preprocessed {result['records_processed']} rows "
        f"({result['train_count']} train / {result['test_count']} test), "
        f"version={result['preprocessor_version']}"
    )


def _decide_training(**context):
    """
    BranchPythonOperator callable.

    Evaluates technical criteria and returns the task_id for the next branch.
    """
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.ingestion import update_batch_audit
    from utils.training_decision import should_train

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")
    drift_result = ti.xcom_pull(task_ids="detect_data_drift", key="drift_result") or {}
    new_cat_result = (
        ti.xcom_pull(task_ids="detect_new_categories", key="new_categories_result") or {}
    )

    conn = connect_to_db()
    try:
        trigger, reasons = should_train(
            batch_id=batch_id,
            connection=conn,
            drift_result=drift_result,
            new_categories_result=new_cat_result,
            registered_model_name=REGISTERED_MODEL_NAME,
            raw_table=RAW_TABLE,
        )
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            should_train=trigger,
            training_reasons=reasons,
        )
    finally:
        close_db_connection(conn)

    decision = "train_candidate_model" if trigger else "skip_training"
    print(f"Training decision: {decision} — {reasons}")
    return decision


def _skip_training(**context):
    """Records the skip decision in the audit table."""
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.ingestion import update_batch_audit

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")

    conn = connect_to_db()
    try:
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            execution_status="skipped_training",
        )
    finally:
        close_db_connection(conn)

    print(f"Batch {batch_id}: training skipped — criteria not met")


def _train_candidate_model(**context):
    """Trains RandomForest configs and logs each run to MLflow."""
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.training import train_candidate

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")
    preprocess_result = (
        ti.xcom_pull(task_ids="preprocess_data", key="preprocess_result") or {}
    )
    drift_result = ti.xcom_pull(task_ids="detect_data_drift", key="drift_result") or {}
    new_cat_result = (
        ti.xcom_pull(task_ids="detect_new_categories", key="new_categories_result") or {}
    )

    # Collect training reasons for MLflow tags
    reasons = []
    if drift_result.get("drift_detected"):
        reasons.append(f"Drift in {drift_result.get('drifted_columns')}")
    if new_cat_result.get("significant_new"):
        reasons.append("New categories detected")

    conn = connect_to_db()
    try:
        result = train_candidate(
            connection=conn,
            clean_table=CLEAN_TABLE,
            batch_id=batch_id,
            preprocessor_version=preprocess_result.get("preprocessor_version"),
            training_reasons=reasons,
            registered_model_name=REGISTERED_MODEL_NAME,
        )
    finally:
        close_db_connection(conn)

    ti.xcom_push(key="train_result", value=result)
    print(
        f"Training complete — best_run_id={result['best_run_id']}, "
        f"test_mae={result['best_mae']:.4f}"
    )


def _evaluate_candidate_model(**context):
    """
    Loads the candidate run from MLflow and logs additional evaluation artifacts.
    Currently validates that the run exists and metrics were recorded correctly.
    """
    import mlflow
    import os

    ti = context["ti"]
    train_result = ti.xcom_pull(task_ids="train_candidate_model", key="train_result") or {}
    run_id = train_result.get("best_run_id")

    if not run_id:
        raise ValueError("No candidate run_id found — training may have failed")

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = mlflow.MlflowClient()
    run = client.get_run(run_id)
    metrics = run.data.metrics

    required = {"test_mae", "test_rmse", "test_r2"}
    missing = required - set(metrics.keys())
    if missing:
        raise ValueError(
            f"Candidate run {run_id} is missing required metrics: {missing}"
        )

    print(
        f"Candidate evaluation — "
        f"MAE={metrics['test_mae']:.4f}, "
        f"RMSE={metrics['test_rmse']:.4f}, "
        f"R²={metrics['test_r2']:.4f}"
    )
    ti.xcom_push(key="candidate_metrics", value={k: v for k, v in metrics.items() if "test_" in k})


def _register_candidate_in_mlflow(**context):
    """Creates a model version in the MLflow registry (no alias yet)."""
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.ingestion import update_batch_audit
    from utils.training import register_candidate

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")
    train_result = ti.xcom_pull(task_ids="train_candidate_model", key="train_result") or {}
    run_id = train_result.get("best_run_id")

    model_version = register_candidate(run_id, REGISTERED_MODEL_NAME)

    conn = connect_to_db()
    try:
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            model_run_id=run_id,
            model_version=str(model_version),
        )
    finally:
        close_db_connection(conn)

    ti.xcom_push(key="model_version", value=model_version)
    print(f"Registered model version: {model_version}")


def _compare_with_production(**context):
    """Compares candidate vs. production using stored MLflow test metrics."""
    from utils.model_comparison import compare_with_production
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.ingestion import update_batch_audit

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")
    train_result = ti.xcom_pull(task_ids="train_candidate_model", key="train_result") or {}
    run_id = train_result.get("best_run_id")

    result = compare_with_production(run_id, REGISTERED_MODEL_NAME)

    conn = connect_to_db()
    try:
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            candidate_mae=result["candidate_metrics"].get("test_mae"),
            candidate_rmse=result["candidate_metrics"].get("test_rmse"),
            production_mae=(
                result["production_metrics"].get("test_mae")
                if result["production_metrics"]
                else None
            ),
            production_rmse=(
                result["production_metrics"].get("test_rmse")
                if result["production_metrics"]
                else None
            ),
        )
    finally:
        close_db_connection(conn)

    ti.xcom_push(key="comparison_result", value=result)
    print(
        f"Comparison — should_promote={result['should_promote']}: {result['reason']}"
    )


def _decide_promotion(**context):
    """
    BranchPythonOperator callable.

    Returns 'promote_model' or 'reject_model' based on the comparison result.
    """
    ti = context["ti"]
    comparison = (
        ti.xcom_pull(task_ids="compare_with_production", key="comparison_result") or {}
    )
    if comparison.get("should_promote", False):
        return "promote_model"
    return "reject_model"


def _promote_model(**context):
    """Sets the production alias on the candidate version in MLflow."""
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.ingestion import update_batch_audit
    from utils.model_comparison import promote_to_production

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")
    train_result = ti.xcom_pull(task_ids="train_candidate_model", key="train_result") or {}
    run_id = train_result.get("best_run_id")
    comparison = (
        ti.xcom_pull(task_ids="compare_with_production", key="comparison_result") or {}
    )

    promoted_version = promote_to_production(run_id, REGISTERED_MODEL_NAME)

    conn = connect_to_db()
    try:
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            model_promoted=True,
            promotion_reason=comparison.get("reason", "Promoted to production"),
        )
    finally:
        close_db_connection(conn)

    print(f"Model v{promoted_version} promoted to production")


def _reject_model(**context):
    """Logs the rejection reason without touching the production alias."""
    from utils.db_connection import close_db_connection, connect_to_db
    from utils.ingestion import update_batch_audit

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")
    comparison = (
        ti.xcom_pull(task_ids="compare_with_production", key="comparison_result") or {}
    )

    conn = connect_to_db()
    try:
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            model_promoted=False,
            promotion_reason=comparison.get("reason", "Did not meet promotion criteria"),
        )
    finally:
        close_db_connection(conn)

    print(f"Model rejected — {comparison.get('reason', 'no reason recorded')}")


def _notify_or_log_result(**context):
    """
    Final step: marks the batch as completed in the audit table regardless
    of which branch was taken (train/skip, promote/reject).
    """
    from datetime import datetime

    from utils.db_connection import close_db_connection, connect_to_db
    from utils.ingestion import update_batch_audit

    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="fetch_batch_from_api", key="batch_id")

    conn = connect_to_db()
    try:
        update_batch_audit(
            conn, AUDIT_TABLE, batch_id,
            execution_status="success",
            completed_at=datetime.utcnow(),
        )
    finally:
        close_db_connection(conn)

    print(f"Batch {batch_id} processing complete")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": False,
}

with DAG(
    dag_id=DAG_ID,
    description=(
        "Incremental real estate MLOps pipeline: "
        "ingest → validate → drift → preprocess → decide → train → compare → promote"
    ),
    default_args=default_args,
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 5, 27),
    max_active_runs=1,
    catchup=False,
    tags=["real-estate", "mlops", "regression", "incremental"],
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_batch_from_api = PythonOperator(
        task_id="fetch_batch_from_api",
        python_callable=_fetch_batch,
        retries=3,
    )

    store_raw_batch = PythonOperator(
        task_id="store_raw_batch",
        python_callable=_store_raw_batch,
    )

    validate_schema = PythonOperator(
        task_id="validate_schema",
        python_callable=_validate_schema,
    )

    validate_data_quality = PythonOperator(
        task_id="validate_data_quality",
        python_callable=_validate_data_quality,
    )

    detect_new_categories = PythonOperator(
        task_id="detect_new_categories",
        python_callable=_detect_new_categories,
    )

    detect_data_drift = PythonOperator(
        task_id="detect_data_drift",
        python_callable=_detect_data_drift,
    )

    preprocess_data = PythonOperator(
        task_id="preprocess_data",
        python_callable=_preprocess_data,
    )

    decide_training = BranchPythonOperator(
        task_id="decide_training",
        python_callable=_decide_training,
    )

    skip_training = PythonOperator(
        task_id="skip_training",
        python_callable=_skip_training,
    )

    train_candidate_model = PythonOperator(
        task_id="train_candidate_model",
        python_callable=_train_candidate_model,
        execution_timeout=timedelta(hours=2),
    )

    evaluate_candidate_model = PythonOperator(
        task_id="evaluate_candidate_model",
        python_callable=_evaluate_candidate_model,
    )

    register_candidate_in_mlflow = PythonOperator(
        task_id="register_candidate_in_mlflow",
        python_callable=_register_candidate_in_mlflow,
    )

    compare_with_production = PythonOperator(
        task_id="compare_with_production",
        python_callable=_compare_with_production,
    )

    decide_promotion = BranchPythonOperator(
        task_id="decide_promotion",
        python_callable=_decide_promotion,
    )

    promote_model = PythonOperator(
        task_id="promote_model",
        python_callable=_promote_model,
    )

    reject_model = PythonOperator(
        task_id="reject_model",
        python_callable=_reject_model,
    )

    # Runs regardless of which branch was taken (skip/promote/reject)
    notify_or_log_result = PythonOperator(
        task_id="notify_or_log_result",
        python_callable=_notify_or_log_result,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # DAG init step (create tables before anything runs)
    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )

    # ---------------------------------------------------------------------------
    # Dependencies
    # ---------------------------------------------------------------------------

    (
        start
        >> create_tables
        >> fetch_batch_from_api
        >> store_raw_batch
        >> validate_schema
        >> validate_data_quality
        >> detect_new_categories
        >> detect_data_drift
        >> preprocess_data
        >> decide_training
    )

    # Training branch
    decide_training >> train_candidate_model
    (
        train_candidate_model
        >> evaluate_candidate_model
        >> register_candidate_in_mlflow
        >> compare_with_production
        >> decide_promotion
    )
    decide_promotion >> promote_model >> notify_or_log_result
    decide_promotion >> reject_model >> notify_or_log_result

    # Skip branch
    decide_training >> skip_training >> notify_or_log_result

    notify_or_log_result >> end
