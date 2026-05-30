import time

from fastapi import APIRouter, HTTPException

from . import model as ml
from .database import log_inference
from .schemas import PropertyFeatures

router = APIRouter()


def _require_model():
    if ml.model is None:
        raise HTTPException(
            status_code=503,
            detail={"message": "Modelo no disponible.", "model_status": ml.model_status},
        )


@router.get("/")
def root():
    return {
        "message": "Real Estate Price Inference API",
        "model_loaded": ml.model is not None,
        "model": ml.model_info,
    }


@router.get("/health")
def health():
    return {
        "status": "ok",
        "api_running": True,
        "model_ready": ml.model is not None,
        "model_status": ml.model_status,
        "model": ml.model_info,
    }


@router.get("/model-info")
def model_info_endpoint():
    return {
        "model_ready": ml.model is not None,
        "model_status": ml.model_status,
        "model": ml.model_info,
    }


@router.post("/predict")
def predict(features: PropertyFeatures):
    """Predicts the price for a single property.

    Accepts raw feature values (including prev_sold_date as a string).
    Date parsing is handled internally using the same logic as the training pipeline.
    Returns the predicted price in the same currency unit as the training data.
    """
    _require_model()
    try:
        start = time.time()
        df = features.to_dataframe()
        predicted_price = float(ml.model.predict(df)[0])
        response_time = (time.time() - start) * 1000

        log_inference(
            input_data=features.model_dump(),
            predicted_price=predicted_price,
            response_time_ms=response_time,
            model_info=ml.model_info,
        )

        return {
            "predicted_price": predicted_price,
            "model_name": ml.model_info.get("name"),
            "model_version": ml.model_info.get("version"),
            "model_alias": ml.model_info.get("alias"),
            "response_time_ms": response_time,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/reload")
def reload():
    """Hot-reloads the production model from MLflow without restarting the container.

    Use this after the Airflow DAG promotes a new model version to the 'production' alias.
    """
    try:
        old_version = ml.model_info.get("version")
        ml.load_production_model()
        return {
            "status": "Modelo recargado",
            "previous_version": old_version,
            "current_version": ml.model_info.get("version"),
            "model": ml.model_info,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "No se pudo recargar el modelo.",
                "error": str(exc),
                "model_status": ml.model_status,
            },
        )
