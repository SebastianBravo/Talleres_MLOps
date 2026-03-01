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

# Global preprocessor variable
_preprocessor = None


def get_preprocessor():
    """Load the preprocessor from the models directory"""
    global _preprocessor
    if _preprocessor is None:
        preprocessor_path = os.path.join(MODELS_PATH, "preprocessor.joblib")
        if not os.path.exists(preprocessor_path):
            raise FileNotFoundError(f"Preprocessor not found at {preprocessor_path}")
        _preprocessor = joblib.load(preprocessor_path)
    return _preprocessor


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
    """Transform raw penguin data using the saved preprocessor"""
    # Create DataFrame with raw features in the same format as training data
    raw_data = pd.DataFrame([{
        "bill_length_mm": penguin.bill_length_mm,
        "bill_depth_mm": penguin.bill_depth_mm,
        "flipper_length_mm": penguin.flipper_length_mm,
        "body_mass_g": penguin.body_mass_g,
        "year": penguin.year,
        "island": penguin.island,
        "sex": penguin.sex,
    }])
    
    # Load and use the preprocessor
    preprocessor = get_preprocessor()
    X_processed = preprocessor.transform(raw_data)
    
    return pd.DataFrame(X_processed)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/models")
def list_models():
    if not os.path.exists(MODELS_PATH):
        return {"models": [], "preprocessor": False}
    models = [
        os.path.splitext(f)[0]
        for f in os.listdir(MODELS_PATH)
        if f.endswith(".pkl")
    ]
    preprocessor_exists = os.path.exists(os.path.join(MODELS_PATH, "preprocessor.joblib"))
    return {"models": models, "preprocessor": preprocessor_exists}


@app.post("/predict")
def predict(request: PredictionRequest):
    # Load the model
    try:
        model = load_model(request.model)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Model '{request.model}' not found. Run the training DAG first.",
        )

    # Load the preprocessor and transform input
    try:
        X = preprocess_input(request.data)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="Preprocessor not found. Run the training DAG first.",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error preprocessing input: {str(e)}",
        )

    # Make prediction
    prediction = model.predict(X)[0]
    species_name = SPECIES_MAP.get(prediction, "Unknown")

    return {
        "model_used": request.model,
        "predicted_species": species_name,
        "input": request.data.model_dump(),
    }
