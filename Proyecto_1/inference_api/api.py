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
PREPROCESSOR_KEY = "preprocessor/preprocessor.joblib"
MODELS_PREFIX = "models/"


@lru_cache(maxsize=1)
def load_preprocessor():
    client = get_minio_client()
    buf = io.BytesIO()
    client.download_fileobj(BUCKET, PREPROCESSOR_KEY, buf)
    buf.seek(0)
    return joblib.load(buf)


@lru_cache(maxsize=10)
def load_model(model_name: str):
    client = get_minio_client()
    key = f"{MODELS_PREFIX}{model_name}.joblib"
    buf = io.BytesIO()
    try:
        client.download_fileobj(BUCKET, key, buf)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise FileNotFoundError
        raise
    buf.seek(0)
    return joblib.load(buf)


app = FastAPI()
app.add_middleware(LoggingMiddleware)


class CoverTypeFeatures(BaseModel):
    elevation: int = Field(..., ge=0)
    aspect: int = Field(..., ge=0)
    slope: int = Field(..., ge=0)
    horizontal_distance_to_hydrology: int = Field(..., ge=0)
    vertical_distance_to_hydrology: int = Field(..., ge=0)
    horizontal_distance_to_roadways: int = Field(..., ge=0)
    hillshade_9am: int = Field(..., ge=0, le=255)
    hillshade_noon: int = Field(..., ge=0, le=255)
    hillshade_3pm: int = Field(..., ge=0, le=255)
    horizontal_distance_to_fire_points: int = Field(..., ge=0)
    wilderness_area: str
    soil_type: str


class PredictionRequest(BaseModel):
    model: str
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


@app.get("/models")
def list_models():
    client = get_minio_client()
    try:
        objs = client.list_objects_v2(Bucket=BUCKET, Prefix=MODELS_PREFIX)
    except Exception:
        return {"models": []}
    models = []
    for obj in objs.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".joblib"):
            name = key.replace(MODELS_PREFIX, "").replace(".joblib", "")
            models.append(name)
    return {"models": models}


@app.post("/predict")
def predict(request: PredictionRequest):
    X = preprocess_input(request.data)
    try:
        preprocessor = load_preprocessor()
        X_t = preprocessor.transform(X)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Preprocessor error: {str(e)}")
    try:
        model = load_model(request.model)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Model '{request.model}' does not exist")
    pred = model.predict(X_t)[0]
    return {"model_used": request.model, "predicted_cover_type": int(pred)}
