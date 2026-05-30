import os
from datetime import datetime, timezone

import mlflow
import mlflow.sklearn

from .config import MLFLOW_TRACKING_URI, MODEL_ALIAS, REGISTERED_MODEL_NAME

os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "10")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

model = None
model_info = {
    "name": None,
    "alias": None,
    "version": None,
    "uri": None,
    "loaded_at": None,
}
model_status = {
    "ready": False,
    "message": "Modelo no cargado",
    "last_error": None,
}


def load_production_model():
    """Loads the model tagged with MODEL_ALIAS from the MLflow registry.

    The model is expected to be a sklearn Pipeline(preprocessor, RandomForestRegressor)
    so callers pass raw feature DataFrames directly — no manual preprocessing needed.
    Raises on failure so the caller can decide whether to abort or continue.
    """
    global model, model_info, model_status

    client = mlflow.MlflowClient()
    try:
        alias_info = client.get_model_version_by_alias(REGISTERED_MODEL_NAME, MODEL_ALIAS)
        model_uri = f"models:/{REGISTERED_MODEL_NAME}@{MODEL_ALIAS}"
        model = mlflow.sklearn.load_model(model_uri)
        model_info = {
            "name": REGISTERED_MODEL_NAME,
            "alias": MODEL_ALIAS,
            "version": alias_info.version,
            "uri": model_uri,
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        }
        model_status = {
            "ready": True,
            "message": "Modelo cargado correctamente",
            "last_error": None,
        }
        print(f"Modelo cargado: {REGISTERED_MODEL_NAME} v{alias_info.version} alias={MODEL_ALIAS}")
        return model_info
    except Exception as exc:
        model = None
        model_info = {
            "name": REGISTERED_MODEL_NAME,
            "alias": MODEL_ALIAS,
            "version": None,
            "uri": None,
            "loaded_at": None,
        }
        model_status = {
            "ready": False,
            "message": "No hay modelo productivo disponible todavia.",
            "last_error": str(exc),
        }
        print(f"No se pudo cargar el modelo: {exc}")
        raise
