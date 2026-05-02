import os
from typing import Optional, Any

import mlflow
import mlflow.sklearn
import pandas as pd
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, field_validator


# =========================
# Configuración MLflow
# =========================

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
REGISTERED_MODEL_NAME = os.environ.get(
    "REGISTERED_MODEL_NAME",
    "diabetic-readmission-model",
)
MODEL_ALIAS = os.environ.get("MODEL_ALIAS", "production")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


# =========================
# Columnas esperadas por el preprocesador
# =========================

NUMERIC_COLUMNS = [
    "time_in_hospital",
    "num_lab_procedures",
    "num_procedures",
    "num_medications",
    "number_outpatient",
    "number_emergency",
    "number_inpatient",
    "number_diagnoses",
]

CATEGORICAL_COLUMNS = [
    "race",
    "gender",
    "age",
    "weight",
    "admission_type_id",
    "discharge_disposition_id",
    "admission_source_id",
    "payer_code",
    "medical_specialty",
    "diag_1",
    "diag_2",
    "diag_3",
    "max_glu_serum",
    "a1cresult",
    "metformin",
    "repaglinide",
    "nateglinide",
    "chlorpropamide",
    "glimepiride",
    "acetohexamide",
    "glipizide",
    "glyburide",
    "tolbutamide",
    "pioglitazone",
    "rosiglitazone",
    "acarbose",
    "miglitol",
    "troglitazone",
    "tolazamide",
    "examide",
    "citoglipton",
    "insulin",
    "glyburide-metformin",
    "glipizide-metformin",
    "glimepiride-pioglitazone",
    "metformin-rosiglitazone",
    "metformin-pioglitazone",
    "change",
    "diabetesmed",
]

FEATURE_COLUMNS = NUMERIC_COLUMNS + CATEGORICAL_COLUMNS


# =========================
# FastAPI
# =========================

app = FastAPI(title="Diabetic Readmission Inference API")

model = None

model_info = {
    "name": None,
    "alias": None,
    "version": None,
    "uri": None,
}

model_status = {
    "ready": False,
    "message": "Modelo no cargado",
    "last_error": None,
}


# =========================
# Request schema
# =========================

class DiabeticReadmissionFeatures(BaseModel):
    """
    Entrada cruda del paciente.

    Este schema debe coincidir con las columnas usadas en el preprocesamiento.
    No se debe enviar la variable objetivo 'readmitted'.
    """

    model_config = ConfigDict(extra="forbid")

    # Variables numéricas
    time_in_hospital: Optional[float] = None
    num_lab_procedures: Optional[float] = None
    num_procedures: Optional[float] = None
    num_medications: Optional[float] = None
    number_outpatient: Optional[float] = None
    number_emergency: Optional[float] = None
    number_inpatient: Optional[float] = None
    number_diagnoses: Optional[float] = None

    # Variables categóricas
    race: Optional[str] = None
    gender: Optional[str] = None
    age: Optional[str] = None
    weight: Optional[str] = None

    admission_type_id: Optional[int] = None
    discharge_disposition_id: Optional[int] = None
    admission_source_id: Optional[int] = None

    payer_code: Optional[str] = None
    medical_specialty: Optional[str] = None

    diag_1: Optional[str] = None
    diag_2: Optional[str] = None
    diag_3: Optional[str] = None

    max_glu_serum: Optional[str] = None
    a1cresult: Optional[str] = None

    metformin: Optional[str] = None
    repaglinide: Optional[str] = None
    nateglinide: Optional[str] = None
    chlorpropamide: Optional[str] = None
    glimepiride: Optional[str] = None
    acetohexamide: Optional[str] = None
    glipizide: Optional[str] = None
    glyburide: Optional[str] = None
    tolbutamide: Optional[str] = None
    pioglitazone: Optional[str] = None
    rosiglitazone: Optional[str] = None
    acarbose: Optional[str] = None
    miglitol: Optional[str] = None
    troglitazone: Optional[str] = None
    tolazamide: Optional[str] = None
    examide: Optional[str] = None
    citoglipton: Optional[str] = None
    insulin: Optional[str] = None
    glyburide_metformin: Optional[str] = None
    glipizide_metformin: Optional[str] = None
    glimepiride_pioglitazone: Optional[str] = None
    metformin_rosiglitazone: Optional[str] = None
    metformin_pioglitazone: Optional[str] = None

    change: Optional[str] = None
    diabetesmed: Optional[str] = None

    @field_validator("*", mode="before")
    @classmethod
    def empty_string_to_none(cls, value: Any):
        if value == "":
            return None
        return value

    def to_model_dataframe(self) -> pd.DataFrame:
        """
        Convierte el request al DataFrame esperado por el pipeline.

        Nota:
        En Python no se pueden usar guiones en nombres de atributos,
        por eso algunos campos llegan con underscore en el API y aquí
        se renombran al nombre original usado en entrenamiento.
        """

        data = self.model_dump()

        rename_map = {
            "glyburide_metformin": "glyburide-metformin",
            "glipizide_metformin": "glipizide-metformin",
            "glimepiride_pioglitazone": "glimepiride-pioglitazone",
            "metformin_rosiglitazone": "metformin-rosiglitazone",
            "metformin_pioglitazone": "metformin-pioglitazone",
        }

        data = {
            rename_map.get(key, key): value
            for key, value in data.items()
        }

        # Asegurar que el DataFrame tenga exactamente las columnas esperadas
        # y en el mismo orden del preprocesamiento.
        row = {column: data.get(column) for column in FEATURE_COLUMNS}

        return pd.DataFrame([row], columns=FEATURE_COLUMNS)


# =========================
# Carga del modelo
# =========================

def load_production_model():
    """
    Carga en memoria el modelo que actualmente tenga el alias production en MLflow.

    Si no existe un modelo productivo, no debe tumbar el contenedor cuando se usa
    desde startup. El error será manejado por quien invoque esta función.
    """

    global model, model_info, model_status

    client = mlflow.MlflowClient()

    try:
        alias_info = client.get_model_version_by_alias(
            REGISTERED_MODEL_NAME,
            MODEL_ALIAS,
        )

        model_uri = f"models:/{REGISTERED_MODEL_NAME}@{MODEL_ALIAS}"
        loaded_model = mlflow.sklearn.load_model(model_uri)

        model = loaded_model
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

        print(
            f"Modelo cargado desde MLflow: "
            f"{REGISTERED_MODEL_NAME} v{alias_info.version} alias={MODEL_ALIAS}"
        )

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
            "message": (
                "No hay modelo productivo disponible todavía. "
                "La API queda activa, pero /predict no estará disponible "
                "hasta que se entrene un modelo y se ejecute /reload."
            ),
            "last_error": str(exc),
        }

        print(f"No se pudo cargar el modelo productivo: {exc}")

        raise

@app.on_event("startup")
def startup():
    try:
        load_production_model()
    except Exception as exc:
        print(
            "La API inició sin modelo productivo. "
            "Esto es esperado en un despliegue limpio antes del primer entrenamiento. "
            f"Detalle: {exc}"
        )


# =========================
# Helpers
# =========================

def get_classes_from_model():
    """
    Obtiene las clases del modelo.

    Soporta tanto:
    - Pipeline(preprocess, model)
    - Modelo sklearn directo
    """

    if model is None:
        return None

    if hasattr(model, "classes_"):
        return list(model.classes_)

    if hasattr(model, "named_steps"):
        last_step = list(model.named_steps.values())[-1]
        if hasattr(last_step, "classes_"):
            return list(last_step.classes_)

    return None


def predict_probabilities(df: pd.DataFrame):
    """
    Retorna probabilidades si el modelo soporta predict_proba.
    """

    if model is None or not hasattr(model, "predict_proba"):
        return None

    try:
        probabilities = model.predict_proba(df)[0]
        classes = get_classes_from_model()

        if classes is None:
            return None

        return {
            str(class_name): float(prob)
            for class_name, prob in zip(classes, probabilities)
        }
    except Exception:
        return None


# =========================
# Endpoints
# =========================

@app.get("/")
def root():
    return {
        "message": "Diabetic Readmission Inference API",
        "model_loaded": model is not None,
        "model": model_info,
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "api_running": True,
        "model_ready": model is not None,
        "model_status": model_status,
        "model": model_info,
    }


@app.post("/predict")
def predict(features: DiabeticReadmissionFeatures):
    if model is None:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Modelo no cargado. La API está activa, pero aún no hay modelo productivo disponible.",
                "model_status": model_status,
            },
        )

    try:
        df = features.to_model_dataframe()

        prediction = model.predict(df)[0]
        probabilities = predict_probabilities(df)

        response = {
            "prediction": str(prediction),
            "model_version": model_info.get("version"),
            "model_alias": model_info.get("alias"),
            "model_name": model_info.get("name"),
        }

        if probabilities is not None:
            response["probabilities"] = probabilities

        return response

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error realizando predicción: {str(exc)}",
        )


@app.post("/reload")
def reload_model():
    try:
        old_version = model_info.get("version")

        load_production_model()

        new_version = model_info.get("version")

        return {
            "status": "Modelo recargado correctamente",
            "previous_version": old_version,
            "current_version": new_version,
            "model": model_info,
            "model_status": model_status,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=404,
            detail={
                "message": (
                    "No se pudo recargar el modelo porque todavía no existe "
                    "un modelo productivo en MLflow."
                ),
                "error": str(exc),
                "model_status": model_status,
            },
        )
    
    
@app.get("/model-info")
def get_model_info():
    return {
        "model_ready": model is not None,
        "model_status": model_status,
        "model": model_info,
    }