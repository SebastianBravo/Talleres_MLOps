import os
import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Literal
from functools import lru_cache

MODELS_PATH = "/app/models"
SPECIES_MAP = {0: "Adelie", 1: "Chinstrap", 2: "Gentoo"}

app = FastAPI(title="Penguin Species Predictor")


@lru_cache(maxsize=10)
def load_model(model_name: str):
    model_path = os.path.join(MODELS_PATH, f"{model_name}.pkl")
    if not os.path.exists(model_path):
        raise FileNotFoundError
    return joblib.load(model_path)


class Penguin(BaseModel):
    island: Literal["Biscoe", "Dream", "Torgersen"]
    bill_length_mm: float = Field(..., gt=0)
    bill_depth_mm: float = Field(..., gt=0)
    flipper_length_mm: float = Field(..., gt=0)
    body_mass_g: float = Field(..., gt=0)
    sex: Literal["male", "female"]
    year: int = Field(..., ge=2000)


class PredictionRequest(BaseModel):
    model: Literal["svm", "logistic_regression", "random_forest"] = "random_forest"
    data: Penguin


def preprocess_input(penguin: Penguin) -> pd.DataFrame:
    return pd.DataFrame([{
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
    }])


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/models")
def list_models():
    if not os.path.exists(MODELS_PATH):
        return {"models": []}
    models = [
        os.path.splitext(f)[0]
        for f in os.listdir(MODELS_PATH)
        if f.endswith(".pkl")
    ]
    return {"models": models}


@app.post("/predict")
def predict(request: PredictionRequest):
    try:
        model = load_model(request.model)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Model '{request.model}' not found. Run the training DAG first.",
        )

    X = preprocess_input(request.data)
    prediction = model.predict(X)[0]
    species_name = SPECIES_MAP.get(prediction, "Unknown")

    return {
        "model_used": request.model,
        "predicted_species": species_name,
        "input": request.data.model_dump(),
    }
