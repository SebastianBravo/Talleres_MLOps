from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.database import ensure_inference_logs_table
from app.model import load_production_model
from app.router import router

app = FastAPI(
    title="Real Estate Price Inference API",
    description=(
        "Predicción de precios de bienes raíces usando el modelo productivo registrado en MLflow. "
        "Acepta features crudas (incluyendo prev_sold_date como string) y retorna el precio estimado."
    ),
    version="1.0.0",
)

Instrumentator().instrument(app).expose(app)
app.include_router(router)


@app.on_event("startup")
def startup():
    ensure_inference_logs_table()
    try:
        load_production_model()
    except Exception as exc:
        print(
            "API iniciada sin modelo productivo. "
            "Esto es esperado en un despliegue limpio antes del primer entrenamiento. "
            f"Detalle: {exc}"
        )
