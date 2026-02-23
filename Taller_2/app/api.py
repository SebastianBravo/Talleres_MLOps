import os
import joblib
import pandas as pd
from fastapi import FastAPI, Request, Response, HTTPException
from pydantic import BaseModel, Field
from typing import Literal
from starlette.middleware.base import BaseHTTPMiddleware
from functools import lru_cache
import time
import json
import logging

# =========================
# LOGGING CONFIGURATION
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/app.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("api_logger")

# =========================
# LOGGING MIDDLEWARE
# =========================

class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        body = await request.body()

        logger.info("===== REQUEST =====")
        logger.info(f"Method: {request.method}")
        logger.info(f"URL: {request.url}")
        logger.info(f"Headers: {dict(request.headers)}")

        try:
            logger.info(f"Body: {json.loads(body)}")
        except:
            logger.info(f"Body (raw): {body.decode('utf-8')}")

        async def receive():
            return {"type": "http.request", "body": body}

        request._receive = receive

        response = await call_next(request)

        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk

        process_time = time.time() - start_time

        logger.info("===== RESPONSE =====")
        logger.info(f"Status code: {response.status_code}")

        try:
            logger.info(f"Body: {json.loads(response_body)}")
        except:
            logger.info(f"Body (raw): {response_body.decode('utf-8')}")

        logger.info(f"Process time: {process_time}")
        logger.info("====================")

        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )

# =========================
# MODEL LOADING
# =========================

models_folder = "/models"

@lru_cache(maxsize=10)
def load_model(model_name: str):
    model_path = os.path.join(models_folder, f"{model_name}.pkl")
    if not os.path.exists(model_path):
        raise FileNotFoundError
    return joblib.load(model_path)

# =========================
# FASTAPI APP
# =========================

app = FastAPI()
app.add_middleware(LoggingMiddleware)

# =========================
# SCHEMAS
# =========================

class Penguin(BaseModel):
    island: Literal["Biscoe", "Dream", "Torgersen"]
    bill_length_mm: float = Field(..., gt=0)
    bill_depth_mm: float = Field(..., gt=0)
    flipper_length_mm: int = Field(..., gt=0)
    body_mass_g: float = Field(..., gt=0)
    sex: Literal["male", "female"]
    year: int = Field(..., ge=2000, le=2024)


class PredictionRequest(BaseModel):
    model: str
    data: Penguin


# =========================
# PREPROCESSING
# =========================

def preprocess_input(penguin: Penguin):
    df = pd.DataFrame(
        [
            {
                "bill_length_mm": penguin.bill_length_mm,
                "bill_depth_mm": penguin.bill_depth_mm,
                "flipper_length_mm": penguin.flipper_length_mm,
                "body_mass_g": penguin.body_mass_g,
                "year": penguin.year,
                "island_Biscoe": penguin.island == "Biscoe",
                "island_Dream": penguin.island == "Dream",
                "island_Torgersen": penguin.island == "Torgersen",
                "sex_female": penguin.sex == "female",
                "sex_male": penguin.sex == "male",
            }
        ]
    )
    return df


# =========================
# ENDPOINTS
# =========================

@app.get("/models")
def list_models():
    if not os.path.exists(models_folder):
        return {"models": []}

    files = os.listdir(models_folder)

    models = [
        os.path.splitext(f)[0]
        for f in files
        if f.endswith(".pkl")
    ]

    return {"models": models}


@app.post("/predict")
def predict_species(request: PredictionRequest):

    x = preprocess_input(request.data)

    try:
        selected_model = load_model(request.model)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Model '{request.model}' does not exist"
        )

    predicted_species = selected_model.predict(x)[0]

    species_mapping = {
        0: "Adelie",
        1: "Chinstrap",
        2: "Gentoo"
    }

    predicted_species_name = species_mapping.get(
        predicted_species,
        "Unknown"
    )

    return {
        "model_used": request.model,
        "predicted_species": predicted_species_name,
    }