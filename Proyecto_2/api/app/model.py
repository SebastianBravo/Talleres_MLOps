import os
import mlflow
import mlflow.sklearn
import pandas as pd
from datetime import datetime, timezone

from .config import MLFLOW_TRACKING_URI, REGISTERED_MODEL_NAME, MODEL_ALIAS

os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "10")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

model = None
model_info = {"name": None, "alias": None, "version": None, "uri": None, "loaded_at": None}
model_status = {"ready": False, "message": "Modelo no cargado", "last_error": None}


def load_production_model():
    global model, model_info, model_status

    client = mlflow.MlflowClient()
    try:
        alias_info = client.get_model_version_by_alias(REGISTERED_MODEL_NAME, MODEL_ALIAS)
        model_uri  = f"models:/{REGISTERED_MODEL_NAME}@{MODEL_ALIAS}"

        model = mlflow.sklearn.load_model(model_uri)
        model_info = {
            "name":      REGISTERED_MODEL_NAME,
            "alias":     MODEL_ALIAS,
            "version":   alias_info.version,
            "uri":       model_uri,
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        }
        model_status = {"ready": True, "message": "Modelo cargado correctamente", "last_error": None}
        print(f"Modelo cargado: {REGISTERED_MODEL_NAME} v{alias_info.version} alias={MODEL_ALIAS}")
        return model_info

    except Exception as exc:
        model = None
        model_info = {"name": REGISTERED_MODEL_NAME, "alias": MODEL_ALIAS,
                      "version": None, "uri": None, "loaded_at": None}
        model_status = {
            "ready":      False,
            "message":    "No hay modelo productivo disponible todavia.",
            "last_error": str(exc),
        }
        print(f"No se pudo cargar el modelo: {exc}")
        raise


def predict_probabilities(df: pd.DataFrame):
    if model is None or not hasattr(model, "predict_proba"):
        return None
    try:
        probs = model.predict_proba(df)[0]
        classes = _get_classes()
        if classes is None:
            return None
        return {str(cls): float(p) for cls, p in zip(classes, probs)}
    except Exception:
        return None


def _get_classes():
    if model is None:
        return None
    if hasattr(model, "classes_"):
        return list(model.classes_)
    if hasattr(model, "named_steps"):
        last = list(model.named_steps.values())[-1]
        if hasattr(last, "classes_"):
            return list(last.classes_)
    return None
