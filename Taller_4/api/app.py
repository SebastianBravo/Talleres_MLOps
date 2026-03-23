import os
import mlflow
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

SPECIES_LABELS = {0: "Adelie", 1: "Chinstrap", 2: "Gentoo"}

app = FastAPI(title="Penguins Inference API")
model = None


class PenguinFeatures(BaseModel):
    bill_length_mm: float
    bill_depth_mm: float
    flipper_length_mm: float
    body_mass_g: float
    year: int = 2009
    island_Biscoe: int = 0
    island_Dream: int = 0
    island_Torgersen: int = 0
    sex_female: int = 0
    sex_male: int = 0


def load_production_model():
    global model
    client = mlflow.MlflowClient()

    for rm in client.search_registered_models():
        try:
            alias_info = client.get_model_version_by_alias(rm.name, "production")
            model_uri = f"models:/{rm.name}@production"
            model = mlflow.sklearn.load_model(model_uri)
            print(f"Modelo cargado: {rm.name} v{alias_info.version} (production)")
            return
        except Exception:
            continue

    raise RuntimeError("No se encontró un modelo con alias 'production'")


@app.on_event("startup")
def startup():
    load_production_model()


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": model is not None}


@app.post("/predict")
def predict(features: PenguinFeatures):
    if model is None:
        raise HTTPException(status_code=503, detail="Modelo no cargado")

    df = pd.DataFrame([features.model_dump()])
    prediction = int(model.predict(df)[0])
    return {
        "prediction": prediction,
        "species": SPECIES_LABELS.get(prediction, "Unknown"),
    }


@app.post("/reload")
def reload_model():
    try:
        load_production_model()
        return {"status": "Modelo recargado"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
