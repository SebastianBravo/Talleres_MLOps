import os
import json
import tempfile
from datetime import datetime

import mlflow
import mlflow.sklearn
import joblib
from mlflow.exceptions import MlflowException
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import ParameterGrid
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    classification_report,
    confusion_matrix,
    roc_auc_score,
)
from sklearn.preprocessing import label_binarize
from sklearn.pipeline import Pipeline

from .storage_utils import connect_to_minio


def _compute_metrics(
    y_true, y_pred, y_prob=None, class_label="<30", class_metric_name="lt30"
):
    metrics = {
        "accuracy": accuracy_score(y_true, y_pred),
        "precision_macro": precision_score(
            y_true, y_pred, average="macro", zero_division=0
        ),
        "recall_macro": recall_score(y_true, y_pred, average="macro", zero_division=0),
        "f1_macro": f1_score(y_true, y_pred, average="macro", zero_division=0),
        "f1_weighted": f1_score(
            y_true, y_pred, average="weighted", zero_division=0
        ),
    }

    class_report = classification_report(
        y_true, y_pred, output_dict=True, zero_division=0
    )
    if class_label in class_report:
        metrics[f"recall_{class_metric_name}"] = class_report[class_label]["recall"]
        metrics[f"precision_{class_metric_name}"] = class_report[class_label]["precision"]
        metrics[f"f1_{class_metric_name}"] = class_report[class_label]["f1-score"]

    if y_prob is not None:
        try:
            classes = sorted(pd.Series(y_true).unique().tolist())
            y_true_bin = label_binarize(y_true, classes=classes)
            if y_prob.shape[1] == y_true_bin.shape[1]:
                metrics["roc_auc_ovr"] = roc_auc_score(
                    y_true_bin, y_prob, average="macro", multi_class="ovr"
                )
        except Exception:
            pass

    return metrics, class_report


def _get_preprocessor_version(connection, processed_table, batch_id=None):
    cursor = connection.cursor()
    if batch_id is None:
        cursor.execute(
            f"""
            SELECT preprocessor_version
            FROM {processed_table}
            ORDER BY processed_at DESC
            LIMIT 1
            """
        )
    else:
        cursor.execute(
            f"""
            SELECT preprocessor_version
            FROM {processed_table}
            WHERE batch_id = %s
            ORDER BY processed_at DESC
            LIMIT 1
            """,
            (batch_id,),
        )

    row = cursor.fetchone()
    cursor.close()
    return row[0] if row else None


def _load_preprocessor_from_minio(bucket, preprocessor_version, prefix="preprocess"):
    minio_client = connect_to_minio()
    remote_key = f"{prefix}/{preprocessor_version}/preprocessor.joblib"

    with tempfile.TemporaryDirectory() as temp_dir:
        local_path = os.path.join(temp_dir, "preprocessor.joblib")
        minio_client.download_file(bucket, remote_key, local_path)
        return joblib.load(local_path)


def train_and_register_models(
    connection,
    cleaned_table,
    batch_id=None,
    preprocessor_bucket=None,
    preprocessor_prefix="preprocess",
    processed_table="diabetic_data_processed_batches",
    experiment_name="diabetic-readmission",
    registered_model_name="diabetic-readmission-model",
    primary_metric_name="recall_lt30",
):
    """Entrena modelos, registra en MLflow y promueve el mejor a production."""
    # Cargar datos procesados desde PostgreSQL
    df = pd.read_sql_query(f"SELECT * FROM {cleaned_table}", connection)
    if df.empty:
        print("No hay datos procesados para entrenar.")
        return

    # Separar features y target
    target_col = "readmitted"
    if target_col not in df.columns:
        raise ValueError("La columna target 'readmitted' no existe en la tabla limpia.")

    if "dataset" not in df.columns:
        raise ValueError("La columna 'dataset' no existe en la tabla limpia.")

    feature_cols = [
        col for col in df.columns if col not in ["id", "dataset", target_col]
    ]
    if not feature_cols:
        raise ValueError("No hay columnas de features disponibles para entrenar.")

    X = df[feature_cols]
    y = df[target_col].astype(str)

    # Separar train/test segun el split persistente
    train_mask = df["dataset"] == "train"
    test_mask = df["dataset"] == "test"

    X_train = X[train_mask]
    y_train = y[train_mask]
    X_test = X[test_mask]
    y_test = y[test_mask]

    if X_train.empty or X_test.empty:
        raise ValueError("Train o test estan vacios. Verifica el split.")

    # Configurar MLflow
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))

    experiment_suffix = datetime.utcnow().strftime("%Y%m%d")
    experiment_full_name = f"{experiment_name}_{experiment_suffix}"

    client = mlflow.MlflowClient()

    experiment = client.get_experiment_by_name(experiment_full_name)

    if experiment is not None and experiment.lifecycle_stage == "deleted":
        client.restore_experiment(experiment.experiment_id)

    mlflow.set_experiment(experiment_full_name)

    # Definir modelos candidatos y grids pequenos
    candidates = [
        # (
        #     "logistic",
        #     LogisticRegression,
        #     {
        #         "C": [0.1, 1.0],
        #         "max_iter": [1000],
        #         "solver": ["lbfgs"],
        #         "class_weight": ["balanced"],
        #     },
        # ),
        (
            "random_forest",
            RandomForestClassifier,
            {
                "n_estimators": [100, 200],
                "max_depth": [None, 10],
                "random_state": [42],
                "class_weight": ["balanced"],
            },
        ),
    ]

    best_metric = None
    best_run_id = None

    # Justificacion de metrica principal
    primary_metric_justification = (
        "En un problema clinico, es critico minimizar falsos negativos para "
        "readmision <30 dias, por eso se prioriza recall de la clase <30."
    )

    # Asegurar que el modelo registrado exista
    try:
        client.get_registered_model(registered_model_name)
    except MlflowException:
        client.create_registered_model(registered_model_name)

    preprocessor = None
    if preprocessor_bucket:
        preprocessor_version = _get_preprocessor_version(
            connection, processed_table, batch_id
        )
        if preprocessor_version:
            preprocessor = _load_preprocessor_from_minio(
                preprocessor_bucket, preprocessor_version, preprocessor_prefix
            )
            print(
                "Preprocessor cargado para registrar el pipeline: "
                f"{preprocessor_version}"
            )
        else:
            print(
                "No se encontro preprocessor versionado para el batch. "
                "Se registrara el modelo sin preprocesador."
            )

    # Entrenar y registrar cada modelo
    for model_name, model_cls, param_grid in candidates:
        batch_suffix = f"_batch_{batch_id}" if batch_id is not None else ""
        with mlflow.start_run(run_name=f"{model_name}{batch_suffix}"):
            mlflow.set_tag("group", "model")
            mlflow.set_tag("model_name", model_name)
            mlflow.set_tag("primary_metric", primary_metric_name)
            mlflow.set_tag("primary_metric_justification", primary_metric_justification)

            grid = list(ParameterGrid(param_grid))
            for config_idx, params in enumerate(grid, start=1):
                config_name = f"{model_name}{batch_suffix}_config_{config_idx:02d}"
                with mlflow.start_run(run_name=config_name, nested=True):
                    mlflow.set_tag("group", "config")
                    mlflow.set_tag("model_name", model_name)
                    mlflow.log_params(params)

                    # Entrenar modelo final con todo el train
                    final_model = model_cls(**params)
                    final_model.fit(X_train, y_train)

                    y_test_pred = final_model.predict(X_test)
                    y_test_prob = None
                    if hasattr(final_model, "predict_proba"):
                        try:
                            y_test_prob = final_model.predict_proba(X_test)
                        except Exception:
                            y_test_prob = None

                    test_metrics, test_report = _compute_metrics(
                        y_test, y_test_pred, y_test_prob
                    )
                    mlflow.log_metrics({f"test_{k}": v for k, v in test_metrics.items()})

                    report_path = "classification_report.json"
                    with open(report_path, "w", encoding="utf-8") as report_file:
                        json.dump(test_report, report_file, indent=2, ensure_ascii=False)
                    mlflow.log_artifact(report_path)

                    cm = confusion_matrix(y_test, y_test_pred)
                    cm_path = "confusion_matrix.json"
                    with open(cm_path, "w", encoding="utf-8") as cm_file:
                        json.dump(cm.tolist(), cm_file)
                    mlflow.log_artifact(cm_path)

                    if preprocessor is not None:
                        pipeline_model = Pipeline(
                            steps=[("preprocess", preprocessor), ("model", final_model)]
                        )
                        mlflow.sklearn.log_model(pipeline_model, name="model")
                    else:
                        mlflow.sklearn.log_model(final_model, name="model")

                    current_metric = test_metrics.get(primary_metric_name)
                    if current_metric is not None:
                        if best_metric is None or current_metric > best_metric:
                            best_metric = current_metric
                            best_run_id = mlflow.active_run().info.run_id

    # Promover el mejor modelo a production
    if best_run_id is not None:
        model_uri = f"runs:/{best_run_id}/model"
        model_version = client.create_model_version(
            name=registered_model_name, source=model_uri, run_id=best_run_id
        )
        client.set_registered_model_alias(
            registered_model_name, "production", model_version.version
        )
        print(
            f"Modelo promovido a production: {registered_model_name} v{model_version.version}"
        )
    else:
        print(
            "No se promueve a production: no se encontro un modelo con metrica principal."
        )
