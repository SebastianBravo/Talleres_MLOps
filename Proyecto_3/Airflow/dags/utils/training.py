import json
import logging
import os
import tempfile
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.exceptions import MlflowException
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import ParameterGrid
from sklearn.pipeline import Pipeline

from .storage_utils import connect_to_minio

logger = logging.getLogger(__name__)

REGISTERED_MODEL_NAME = os.environ.get(
    "MLFLOW_REGISTERED_MODEL", "real-estate-price-model"
)
EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT", "real-estate-price")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "real-estate-project")

# PARAM_GRID = {
#     "n_estimators": [100, 200],
#     "max_depth": [None, 15],
#     "random_state": [42],
# }

PARAM_GRID = {
    "n_estimators": [100],
    "max_depth": [15],
    "random_state": [42],
}

def _compute_metrics(y_true, y_pred):
    """Returns MAE, RMSE, R² and MAPE as a flat dict."""
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    ss_res = float(np.sum((np.array(y_true) - np.array(y_pred)) ** 2))
    ss_tot = float(np.sum((np.array(y_true) - np.mean(y_true)) ** 2))
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0
    nonzero = np.where(np.array(y_true) != 0, np.array(y_true), 1)
    mape = float(np.mean(np.abs((np.array(y_true) - np.array(y_pred)) / nonzero))) * 100
    return {"mae": mae, "rmse": rmse, "r2": r2, "mape": mape}


def _load_preprocessor(bucket, version, prefix="preprocess"):
    """Downloads the versioned preprocessor from MinIO and returns it."""
    import joblib
    minio = connect_to_minio()
    remote_key = f"{prefix}/{version}/preprocessor.joblib"
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, "preprocessor.joblib")
        minio.download_file(bucket, remote_key, local)
        return joblib.load(local)


def _setup_experiment(client, experiment_full_name):
    """Creates or restores a named MLflow experiment."""
    experiment = client.get_experiment_by_name(experiment_full_name)
    if experiment is not None and experiment.lifecycle_stage == "deleted":
        client.restore_experiment(experiment.experiment_id)
    mlflow.set_experiment(experiment_full_name)


def train_candidate(
    connection,
    clean_table,
    batch_id,
    preprocessor_version=None,
    training_reasons=None,
    registered_model_name=REGISTERED_MODEL_NAME,
    experiment_name=EXPERIMENT_NAME,
    preprocessor_bucket=MINIO_BUCKET,
):
    """
    Trains RandomForestRegressor configs on the clean table data, logs every
    run to MLflow and returns the run_id of the best config (lowest test MAE).

    The model is NOT promoted here — promotion is handled by promote_model.
    """
    df = pd.read_sql_query(f"SELECT * FROM {clean_table}", connection)
    if df.empty:
        raise ValueError("Clean table is empty — cannot train")

    target_col = "price"
    if target_col not in df.columns:
        raise ValueError(f"Target column '{target_col}' not found in {clean_table}")

    feature_cols = [
        c for c in df.columns
        if c not in ("id", "batch_id", "source_record_id", "dataset", target_col)
    ]
    if not feature_cols:
        raise ValueError("No feature columns found in clean table")

    X = df[feature_cols]
    y = df[target_col].astype(float)
    train_mask = df["dataset"] == "train"
    test_mask = df["dataset"] == "test"

    X_train, y_train = X[train_mask], y[train_mask]
    X_test, y_test = X[test_mask], y[test_mask]

    if X_train.empty or X_test.empty:
        raise ValueError("Train or test split is empty after reading clean table")

    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = mlflow.MlflowClient()

    _setup_experiment(client, experiment_name)

    # Ensure model registry entry exists
    try:
        client.get_registered_model(registered_model_name)
    except MlflowException:
        client.create_registered_model(registered_model_name)

    # Load preprocessor for pipeline creation
    preprocessor = None
    if preprocessor_version and preprocessor_bucket:
        try:
            preprocessor = _load_preprocessor(preprocessor_bucket, preprocessor_version)
            logger.info("Preprocessor loaded: %s", preprocessor_version)
        except Exception as exc:
            logger.warning("Could not load preprocessor: %s", exc)

    best_mae = None
    best_run_id = None
    batch_tag = f"batch_{batch_id}"

    with mlflow.start_run(run_name=f"rf_{batch_tag}"):
        mlflow.set_tag("batch_id", str(batch_id))
        mlflow.set_tag("model_type", "RandomForestRegressor")
        mlflow.set_tag("primary_metric", "test_mae")
        mlflow.set_tag("problem_type", "regression")
        if training_reasons:
            mlflow.set_tag("training_reasons", json.dumps(training_reasons)[:500])
        if preprocessor_version:
            mlflow.set_tag("preprocessor_version", preprocessor_version)

        for idx, params in enumerate(list(ParameterGrid(PARAM_GRID)), start=1):
            config_name = f"rf_{batch_tag}_cfg{idx:02d}"
            with mlflow.start_run(run_name=config_name, nested=True):
                mlflow.set_tag("batch_id", str(batch_id))
                mlflow.log_params(params)

                model = RandomForestRegressor(**params)
                model.fit(X_train, y_train)

                train_metrics = _compute_metrics(y_train, model.predict(X_train))
                test_metrics = _compute_metrics(y_test, model.predict(X_test))

                mlflow.log_metrics({f"train_{k}": v for k, v in train_metrics.items()})
                mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.items()})

                # Feature importances artifact
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False
                ) as fp:
                    importance = dict(
                        zip(feature_cols, model.feature_importances_.tolist())
                    )
                    top = dict(
                        sorted(importance.items(), key=lambda x: x[1], reverse=True)[:20]
                    )
                    json.dump(top, fp, indent=2)
                    fp.flush()
                    mlflow.log_artifact(fp.name, artifact_path="feature_importance")

                # Register model as pipeline when preprocessor available
                if preprocessor is not None:
                    pipeline = Pipeline([
                        ("preprocess", preprocessor),
                        ("model", model),
                    ])
                    mlflow.sklearn.log_model(pipeline, name="model")
                else:
                    mlflow.sklearn.log_model(model, name="model")

                test_mae = test_metrics["mae"]
                if best_mae is None or test_mae < best_mae:
                    best_mae = test_mae
                    best_run_id = mlflow.active_run().info.run_id

    logger.info(
        "Training complete — best run_id=%s, test_mae=%.4f", best_run_id, best_mae
    )
    return {
        "best_run_id": best_run_id,
        "best_mae": best_mae,
        "registered_model_name": registered_model_name,
    }


def register_candidate(candidate_run_id, registered_model_name=REGISTERED_MODEL_NAME):
    """
    Creates a model version from the candidate run WITHOUT setting any alias.
    Returns the new version number.
    """
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = mlflow.MlflowClient()

    model_uri = f"runs:/{candidate_run_id}/model"
    version = client.create_model_version(
        name=registered_model_name,
        source=model_uri,
        run_id=candidate_run_id,
    )
    logger.info(
        "Registered %s v%s (run %s)", registered_model_name, version.version, candidate_run_id
    )
    return version.version
