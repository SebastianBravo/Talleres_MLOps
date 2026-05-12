from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.database import ensure_inference_logs_table
from app.model import load_production_model
from app.router import router

app = FastAPI(title="Diabetic Readmission Inference API")

Instrumentator().instrument(app).expose(app)
app.include_router(router)


@app.on_event("startup")
def startup():
    ensure_inference_logs_table()
    try:
        load_production_model()
    except Exception as exc:
        print(f"API iniciada sin modelo productivo: {exc}")
