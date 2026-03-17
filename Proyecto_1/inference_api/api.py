import os
import io
import json
import time
import logging
import joblib
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from functools import lru_cache
from fastapi import FastAPI, Request, Response, HTTPException
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("api_logger")


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        body = await request.body()
        logger.info("===== REQUEST =====")
        logger.info(f"Method: {request.method} URL: {request.url}")
        try:
            logger.info(f"Body: {json.loads(body)}")
        except Exception:
            logger.info(f"Body (raw): {body.decode('utf-8')}")
        async def receive():
            return {"type": "http.request", "body": body}
        request._receive = receive
        response = await call_next(request)
        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk
        logger.info(f"===== RESPONSE Status: {response.status_code} Time: {time.time() - start_time}s =====")
        return Response(content=response_body, status_code=response.status_code, headers=dict(response.headers), media_type=response.media_type)


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )


BUCKET = os.getenv("MINIO_BUCKET", "covertype-project")
V1_PREPROCESS_KEY = "v1/preprocess/preprocessor.joblib"
V1_MODELS_PREFIX = "v1/models/"
V2_PREPROCESS_PREFIX = "v2/preprocess/"
V2_MODELS_PREFIX = "v2/models/"

WILDERNESS_AREAS = ["Rawah", "Neota", "Comanche Peak", "Cache la Poudre"]
SOIL_TYPES = [
    "C2702", "C2703", "C2704", "C2705", "C2706", "C2717",
    "C3501", "C3502", "C4201", "C4703", "C4704", "C4744",
    "C4758", "C5101", "C5151", "C6101", "C6102", "C6731",
    "C7101", "C7102", "C7103", "C7201", "C7202", "C7700",
    "C7701", "C7702", "C7709", "C7710", "C7745", "C7746",
    "C7755", "C7756", "C7757", "C7790", "C8703", "C8707",
    "C8708", "C8771", "C8772", "C8776",
]


def bucket_exists(client=None) -> bool:
    if client is None:
        client = get_minio_client()
    try:
        buckets = client.list_buckets().get("Buckets", [])
        return any(b["Name"] == BUCKET for b in buckets)
    except Exception:
        return False


@lru_cache(maxsize=1)
def load_v1_preprocessor():
    client = get_minio_client()
    buf = io.BytesIO()
    client.download_fileobj(BUCKET, V1_PREPROCESS_KEY, buf)
    buf.seek(0)
    return joblib.load(buf)


@lru_cache(maxsize=10)
def load_v1_model(model_name: str):
    client = get_minio_client()
    key = f"{V1_MODELS_PREFIX}{model_name}.joblib"
    buf = io.BytesIO()
    try:
        client.download_fileobj(BUCKET, key, buf)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise FileNotFoundError
        raise
    buf.seek(0)
    return joblib.load(buf)


@lru_cache(maxsize=10)
def load_v2_preprocessor_for_model(model_name: str):
    client = get_minio_client()
    key = f"{V2_PREPROCESS_PREFIX}{model_name}.joblib"
    buf = io.BytesIO()
    try:
        client.download_fileobj(BUCKET, key, buf)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise FileNotFoundError(f"Preprocessor for model '{model_name}' not found in MinIO.")
        raise
    buf.seek(0)
    return joblib.load(buf)


@lru_cache(maxsize=10)
def load_v2_model(model_name: str):
    client = get_minio_client()
    key = f"{V2_MODELS_PREFIX}{model_name}.joblib"
    buf = io.BytesIO()
    try:
        client.download_fileobj(BUCKET, key, buf)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise FileNotFoundError
        raise
    buf.seek(0)
    return joblib.load(buf)


app = FastAPI(
    title="API de inferencia – Covertype",
    description="Predicción de tipo de cubierta forestal (cover_type 1–7) usando modelos almacenados en MinIO.",
)
app.add_middleware(LoggingMiddleware)


class CoverTypeFeatures(BaseModel):
    """Variables del dataset Covertype (Forest Cover Type)."""

    elevation: int = Field(..., ge=0, description="Elevación en metros")
    aspect: int = Field(..., ge=0, description="Aspecto en grados (0-360)")
    slope: int = Field(..., ge=0, description="Pendiente en grados")
    horizontal_distance_to_hydrology: int = Field(..., ge=0, description="Distancia horizontal a agua")
    vertical_distance_to_hydrology: int = Field(..., ge=0, description="Distancia vertical a agua")
    horizontal_distance_to_roadways: int = Field(..., ge=0, description="Distancia horizontal a carreteras")
    hillshade_9am: int = Field(..., ge=0, le=255, description="Sombra 9h")
    hillshade_noon: int = Field(..., ge=0, le=255, description="Sombra mediodía")
    hillshade_3pm: int = Field(..., ge=0, le=255, description="Sombra 15h")
    horizontal_distance_to_fire_points: int = Field(..., ge=0, description="Distancia horizontal a puntos de fuego")
    wilderness_area: str = Field(..., description="Área wilderness: Rawah, Neota, Comanche Peak, Cache la Poudre")
    soil_type: str = Field(..., description="Tipo de suelo (ej. C2702, C3501, ...)")


class PredictionRequest(BaseModel):
    model: str = Field(..., description="Nombre del modelo (sin .joblib). Ver GET /v1/models o GET /v2/models")
    data: CoverTypeFeatures


def preprocess_input(features: CoverTypeFeatures) -> pd.DataFrame:
    return pd.DataFrame([{
        "elevation": features.elevation,
        "aspect": features.aspect,
        "slope": features.slope,
        "horizontal_distance_to_hydrology": features.horizontal_distance_to_hydrology,
        "vertical_distance_to_hydrology": features.vertical_distance_to_hydrology,
        "horizontal_distance_to_roadways": features.horizontal_distance_to_roadways,
        "hillshade_9am": features.hillshade_9am,
        "hillshade_noon": features.hillshade_noon,
        "hillshade_3pm": features.hillshade_3pm,
        "horizontal_distance_to_fire_points": features.horizontal_distance_to_fire_points,
        "wilderness_area": features.wilderness_area,
        "soil_type": features.soil_type,
    }])


def _list_models(prefix: str):
    client = get_minio_client()
    if not bucket_exists(client):
        return {"modelos_disponibles": []}
    try:
        objs = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    except Exception:
        return {"modelos_disponibles": []}
    modelos = []
    for obj in objs.get("Contents") or []:
        key = obj["Key"]
        if key.endswith(".joblib"):
            name = key.replace(prefix, "").replace(".joblib", "")
            modelos.append(name)
    return {"modelos_disponibles": modelos}


@app.get("/v1/models", summary="Modelos v1", tags=["Preprocesamiento Con Dag"])
def list_v1_models():
    return _list_models(V1_MODELS_PREFIX)


@app.get("/v2/models", summary="Modelos v2", tags=["Preprocesamiento Dinamico en notebook"])
def list_v2_models():
    return _list_models(V2_MODELS_PREFIX)


@app.post("/v1/predict", summary="Predecir v1", tags=["Preprocesamiento Con Dag"])
def predict_v1(request: PredictionRequest):
    client = get_minio_client()
    if not bucket_exists(client):
        raise HTTPException(
            status_code=503,
            detail="Bucket de MinIO no disponible; no hay modelos para consumir.",
        )
    X = preprocess_input(request.data)
    try:
        preprocessor = load_v1_preprocessor()
        X_t = preprocessor.transform(X)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            raise HTTPException(status_code=503, detail="Bucket de MinIO no disponible.")
        raise HTTPException(status_code=503, detail=f"Error al cargar preprocesador: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Error en preprocesador: {str(e)}")
    try:
        model = load_v1_model(request.model)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Modelo '{request.model}' no existe. Use GET /v1/models para listar modelos.",
        )
    pred = model.predict(X_t)[0]
    return {
        "modelo_utilizado": request.model,
        "prediccion_cover_type": int(pred),
    }


@app.post("/v2/predict", summary="Predecir v2", tags=["Preprocesamiento Dinamico en notebook"])
def predict_v2(request: PredictionRequest):
    client = get_minio_client()
    if not bucket_exists(client):
        raise HTTPException(
            status_code=503,
            detail="Bucket de MinIO no disponible; no hay modelos para consumir.",
        )
    X = preprocess_input(request.data)
    try:
        preprocessor = load_v2_preprocessor_for_model(request.model)
        X_t = preprocessor.transform(X)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Preprocesador para el modelo '{request.model}' no encontrado. Guarde modelo y preprocesador con el mismo nombre en MinIO (v2/preprocess/).",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            raise HTTPException(status_code=503, detail="Bucket de MinIO no disponible.")
        raise HTTPException(status_code=503, detail=f"Error al cargar preprocesador: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Error en preprocesador: {str(e)}")
    try:
        model = load_v2_model(request.model)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Modelo '{request.model}' no existe. Use GET /v2/models para listar modelos.",
        )
    pred = model.predict(X_t)[0]
    return {
        "modelo_utilizado": request.model,
        "prediccion_cover_type": int(pred),
    }
