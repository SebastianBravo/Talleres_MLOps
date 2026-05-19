import time

from fastapi import APIRouter, HTTPException

from . import model as ml
from .database import log_inference
from .schemas import PredictRequest

router = APIRouter()


@router.get("/")
def root():
    return {"message": "Diabetic Readmission Inference API",
            "model_loaded": ml.model is not None,
            "model": ml.model_info}


@router.get("/health")
def health():
    return {"status": "ok", "api_running": True,
            "model_ready": ml.model is not None,
            "model_status": ml.model_status,
            "model": ml.model_info}


@router.get("/model-info")
def model_info():
    return {"model_ready": ml.model is not None,
            "model_status": ml.model_status,
            "model": ml.model_info}


@router.post("/predict")
def predict(features: PredictRequest):
    if ml.model is None:
        raise HTTPException(status_code=503, detail={
            "message": "Modelo no disponible.",
            "model_status": ml.model_status,
        })

    try:
        start = time.time()
        df = features.to_dataframe()
        prediction    = ml.model.predict(df)[0]
        probabilities = ml.predict_probabilities(df)
        response_time = (time.time() - start) * 1000

        log_inference(
            input_data=features.model_dump(),
            prediction=str(prediction),
            probabilities=probabilities,
            response_time_ms=response_time,
            model_info=ml.model_info,
        )

        return {
            "prediction":      str(prediction),
            "probabilities":   probabilities,
            "model_name":      ml.model_info.get("name"),
            "model_version":   ml.model_info.get("version"),
            "model_alias":     ml.model_info.get("alias"),
            "response_time_ms": response_time,
        }

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/reload")
def reload():
    try:
        old_version = ml.model_info.get("version")
        ml.load_production_model()
        return {"status": "Modelo recargado",
                "previous_version": old_version,
                "current_version": ml.model_info.get("version"),
                "model": ml.model_info}
    except Exception as exc:
        raise HTTPException(status_code=404, detail={
            "message": "No se pudo recargar el modelo.",
            "error": str(exc),
            "model_status": ml.model_status,
        })
