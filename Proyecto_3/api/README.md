# API de Inferencia — Real Estate Price

Este documento describe en detalle la API de inferencia del proyecto, sus endpoints, el esquema de datos, la integración con MLflow y la configuración necesaria para su despliegue.

---

## Tabla de contenido

1. [Visión general](#1-visión-general)
2. [Arquitectura](#2-arquitectura)
3. [Estructura del proyecto](#3-estructura-del-proyecto)
4. [Endpoints](#4-endpoints)
5. [Schema de entrada — PropertyFeatures](#5-schema-de-entrada--propertyfeatures)
6. [Carga del modelo](#6-carga-del-modelo)
7. [Persistencia de inferencias](#7-persistencia-de-inferencias)
8. [Integración con el DAG de Airflow](#8-integración-con-el-dag-de-airflow)
9. [Variables de entorno](#9-variables-de-entorno)
10. [Despliegue local con Docker Compose](#10-despliegue-local-con-docker-compose)

---

## 1. Visión general

La API expone el modelo de predicción de precios de bienes raíces entrenado por el DAG de Airflow. Está construida con **FastAPI** y expone métricas de Prometheus para observabilidad.

**Características principales:**

- Carga automática del modelo productivo desde MLflow al iniciar.
- Recarga en caliente (`/reload`) sin reiniciar el contenedor — llamada por el DAG tras cada promoción.
- Persistencia de cada inferencia en PostgreSQL para análisis posterior.
- El modelo es un **sklearn Pipeline** que incluye el preprocesador: la API recibe datos crudos y el pipeline hace toda la transformación internamente.
- La API arranca incluso si no hay modelo disponible todavía (`503` hasta que se llame `/reload` o se reinicie el contenedor después del primer entrenamiento).

---

## 2. Arquitectura

```
Cliente HTTP
     │
     ▼
┌─────────────────────────────────────────┐
│           FastAPI  (puerto 8000)        │
│                                         │
│  GET  /              → info general     │
│  GET  /health        → estado de la API │
│  GET  /model-info    → versión cargada  │
│  POST /predict       → predicción       │
│  POST /reload        → recarga modelo   │
│  GET  /metrics       → Prometheus       │
└──────────┬──────────────────┬───────────┘
           │                  │
           ▼                  ▼
    ┌─────────────┐    ┌──────────────────┐
    │   MLflow    │    │   PostgreSQL      │
    │  Registry   │    │  inference_logs   │
    └─────────────┘    └──────────────────┘
           │
           ▼
    ┌─────────────┐
    │    MinIO    │
    │  (modelo    │
    │  serializ.) │
    └─────────────┘
```

El modelo se carga desde el **Model Registry de MLflow** usando el alias `production`. MLflow descarga el artefacto desde **MinIO** y lo deserializa en memoria.

---

## 3. Estructura del proyecto

```
api/
├── Dockerfile              ← imagen Python 3.12-slim
├── requirements.txt        ← dependencias del contenedor
├── request_example.json    ← ejemplo de payload para /predict
├── main.py                 ← entrypoint: FastAPI app + startup
└── app/
    ├── __init__.py
    ├── config.py           ← variables de entorno centralizadas
    ├── schemas.py          ← PropertyFeatures + lógica de parseo de fechas
    ├── database.py         ← conexión PostgreSQL + tabla inference_logs
    ├── model.py            ← carga y estado del modelo MLflow
    └── router.py           ← definición de todos los endpoints
```

---

## 4. Endpoints

### `GET /`

Información básica sobre el estado de la API y el modelo cargado.

**Respuesta:**
```json
{
  "message": "Real Estate Price Inference API",
  "model_loaded": true,
  "model": {
    "name": "real-estate-price-model",
    "alias": "production",
    "version": "4",
    "uri": "models:/real-estate-price-model@production",
    "loaded_at": "2026-05-30T18:45:00+00:00"
  }
}
```

---

### `GET /health`

Health check completo. Devuelve estado de la API y del modelo.

**Respuesta:**
```json
{
  "status": "ok",
  "api_running": true,
  "model_ready": true,
  "model_status": {
    "ready": true,
    "message": "Modelo cargado correctamente",
    "last_error": null
  },
  "model": { ... }
}
```

**Uso típico:** health probe de Kubernetes o Docker Compose.

---

### `GET /model-info`

Información detallada del modelo actualmente cargado en memoria.

**Respuesta:**
```json
{
  "model_ready": true,
  "model_status": {
    "ready": true,
    "message": "Modelo cargado correctamente",
    "last_error": null
  },
  "model": {
    "name": "real-estate-price-model",
    "alias": "production",
    "version": "4",
    "uri": "models:/real-estate-price-model@production",
    "loaded_at": "2026-05-30T18:45:00+00:00"
  }
}
```

---

### `POST /predict`

Predice el precio de una propiedad a partir de sus características crudas.

**Request body:** ver [Sección 5 — PropertyFeatures](#5-schema-de-entrada--propertyfeatures).

**Respuesta exitosa (200):**
```json
{
  "predicted_price": 385000.50,
  "model_name": "real-estate-price-model",
  "model_version": "4",
  "model_alias": "production",
  "response_time_ms": 12.4
}
```

**Errores:**

| Código | Causa |
|---|---|
| `422` | Payload inválido (campo extra, tipo incorrecto) |
| `503` | Modelo no cargado — API activa pero sin modelo productivo disponible |
| `500` | Error interno al ejecutar el pipeline de predicción |

**Flujo interno:**

```
Request JSON
     │
     ▼
PropertyFeatures.to_dataframe()
     │  Parsea prev_sold_date → year / month / days_ago
     │  Construye DataFrame con las 13 columnas del ColumnTransformer
     ▼
model.predict(df)
     │  Pipeline: ColumnTransformer → RandomForestRegressor
     │  El preprocesador aplica imputación, escalado y OrdinalEncoding
     ▼
float (precio predicho)
     │
     ├── Persiste en inference_logs (PostgreSQL)
     └── Retorna en respuesta JSON
```

---

### `POST /reload`

Recarga el modelo productivo desde MLflow sin reiniciar el contenedor.

Este endpoint es llamado automáticamente por el DAG de Airflow tras cada promoción exitosa de un nuevo modelo. También puede invocarse manualmente si se necesita actualizar el modelo sin esperar al DAG.

**Request body:** ninguno.

**Respuesta exitosa (200):**
```json
{
  "status": "Modelo recargado",
  "previous_version": "3",
  "current_version": "4",
  "model": {
    "name": "real-estate-price-model",
    "alias": "production",
    "version": "4",
    "uri": "models:/real-estate-price-model@production",
    "loaded_at": "2026-05-30T19:05:30+00:00"
  }
}
```

**Error (404):** se devuelve si no existe ningún modelo con el alias `production` en MLflow.

```json
{
  "detail": {
    "message": "No se pudo recargar el modelo.",
    "error": "...",
    "model_status": { ... }
  }
}
```

---

### `GET /metrics`

Endpoint de Prometheus expuesto por `prometheus-fastapi-instrumentator`. Devuelve métricas HTTP estándar:

- `http_requests_total` — contador de peticiones por endpoint, método y código de respuesta.
- `http_request_duration_seconds` — histograma de latencia por endpoint.
- `http_requests_in_progress` — gauge de peticiones en curso.

**Uso típico:** scraping desde un servidor Prometheus para dashboards en Grafana.

```yaml
# prometheus.yml
scrape_configs:
  - job_name: "inference-api"
    static_configs:
      - targets: ["api:8000"]
```

---

## 5. Schema de entrada — PropertyFeatures

El schema de entrada (`app/schemas.py`) acepta las features **crudas** de la propiedad. El pipeline sklearn que incluye el preprocesador se encarga de todas las transformaciones internamente.

### Campos

| Campo | Tipo | Descripción |
|---|---|---|
| `bed` | `float \| null` | Número de habitaciones |
| `bath` | `float \| null` | Número de baños |
| `acre_lot` | `float \| null` | Tamaño del terreno en acres |
| `house_size` | `float \| null` | Superficie de la vivienda en pies cuadrados |
| `prev_sold_date` | `string \| null` | Fecha de última venta (ej. `"2021-08-15"`) |
| `brokered_by` | `string \| null` | Nombre de la inmobiliaria o agente |
| `street` | `string \| null` | Dirección de la propiedad |
| `city` | `string \| null` | Ciudad |
| `state` | `string \| null` | Estado (ej. `"Texas"`) |
| `zip_code` | `string \| null` | Código postal (tratado como categórico) |
| `status` | `string \| null` | Estado de la propiedad (`for_sale`, `sold`, etc.) |

Todos los campos son opcionales (`null` permitido). Los valores nulos son manejados por los imputadores del preprocesador:
- Numéricos: imputados con la **mediana** del conjunto de entrenamiento.
- Alta cardinalidad (`brokered_by`, `street`, `city`, `state`, `zip_code`): imputados con `"unknown"`.
- Baja cardinalidad (`status`): imputados con la **moda** del conjunto de entrenamiento.

El schema usa `extra="forbid"` — cualquier campo no declarado en el schema devuelve error `422`.

### Parseo de `prev_sold_date`

El campo `prev_sold_date` no se pasa directamente al modelo. Internamente, `to_dataframe()` lo convierte en tres variables numéricas que el `ColumnTransformer` espera:

```python
# Referencia: 2025-01-01 (misma que en training/preprocess.py)
prev_sold_year      = float(parsed.year)                         # ej. 2021.0
prev_sold_month     = float(parsed.month)                        # ej. 8.0
prev_sold_days_ago  = float((Timestamp("2025-01-01") - parsed).days)  # ej. 1235.0
```

Este parseo replica exactamente la función `_parse_date_features()` del pipeline de preprocesamiento. Si `prev_sold_date` es `null` o no parseable, las tres variables se setean a `NaN` y el imputador numérico las completa con la mediana.

### Ejemplo de request

```json
{
  "bed": 3,
  "bath": 2.0,
  "acre_lot": 0.12,
  "house_size": 1850.0,
  "prev_sold_date": "2021-08-15",
  "brokered_by": "Realty Group Inc",
  "street": "123 Maple St",
  "city": "Austin",
  "state": "Texas",
  "zip_code": "78701",
  "status": "for_sale"
}
```

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d @request_example.json
```

### Categorías desconocidas

El `OrdinalEncoder` fue entrenado con `handle_unknown="use_encoded_value", unknown_value=-1`. Cualquier valor en `city`, `state`, `status`, `brokered_by`, `street` o `zip_code` que no haya aparecido en el entrenamiento recibe el código -1 y el modelo produce una predicción igualmente — no lanza error.

---

## 6. Carga del modelo

### Al iniciar el contenedor

`main.py` ejecuta `load_production_model()` en el evento `startup` de FastAPI. Si no hay modelo disponible (despliegue limpio antes del primer entrenamiento), la API arranca de todas formas con `model = None`:

```
startup:
  ensure_inference_logs_table()   ← crea la tabla si no existe
  load_production_model()
    ├── OK  → model = Pipeline(preprocessor, RF), model_status.ready = True
    └── ERR → model = None, model_status.ready = False, mensaje descriptivo
```

Los endpoints `/`, `/health` y `/model-info` funcionan siempre. El endpoint `/predict` devuelve `503` hasta que el modelo esté disponible.

### Consulta al Model Registry

```python
client = mlflow.MlflowClient()
alias_info = client.get_model_version_by_alias("real-estate-price-model", "production")
model_uri  = "models:/real-estate-price-model@production"
model      = mlflow.sklearn.load_model(model_uri)
```

MLflow descarga el artefacto desde MinIO y lo deserializa. El objeto resultante es un sklearn `Pipeline` con dos pasos: `preprocess` (ColumnTransformer) y `model` (RandomForestRegressor).

### Recarga en caliente

`POST /reload` ejecuta `load_production_model()` de nuevo, sustituyendo la variable global `model`. No hay downtime — peticiones en curso completan con el modelo anterior antes de que se sustituya.

La secuencia en un ciclo normal del DAG es:

```
Airflow: promote_model     → MLflow: alias "production" → versión N
Airflow: reload_inference_api → POST /reload
API:     load_production_model() → carga versión N desde MinIO
```

El tiempo entre la promoción y la disponibilidad del nuevo modelo en la API es el tiempo de la llamada HTTP + el tiempo de descarga del artefacto desde MinIO (típicamente < 10 segundos para modelos de tamaño normal).

---

## 7. Persistencia de inferencias

Cada predicción exitosa se registra en la tabla `inference_logs` de PostgreSQL:

```sql
CREATE TABLE inference_logs (
    request_id       UUID      PRIMARY KEY DEFAULT gen_random_uuid(),
    requested_at     TIMESTAMP NOT NULL,
    input_data       JSONB     NOT NULL,
    predicted_price  FLOAT     NOT NULL,
    model_name       VARCHAR(128),
    model_version    VARCHAR(32),
    model_alias      VARCHAR(64),
    response_time_ms FLOAT     NOT NULL
);
```

**Campos:**

| Campo | Descripción |
|---|---|
| `request_id` | UUID único por petición |
| `requested_at` | Timestamp UTC de la petición |
| `input_data` | Payload original en JSONB (features crudas tal como llegaron) |
| `predicted_price` | Precio predicho por el modelo |
| `model_name` | Nombre del modelo en el registry |
| `model_version` | Versión del modelo que respondió |
| `model_alias` | Alias bajo el cual se cargó (siempre `production`) |
| `response_time_ms` | Latencia de la predicción en milisegundos |

**Características:**
- La escritura falla silenciosamente — un error de base de datos no bloquea la respuesta al cliente.
- La tabla se crea en el evento `startup` si no existe (`CREATE TABLE IF NOT EXISTS`).
- La misma tabla es creada también por el DAG de Airflow en la tarea `create_tables` para garantizar que existe antes de que llegue la primera inferencia.

**Consultas útiles:**

```sql
-- Predicciones del último día
SELECT request_id, requested_at, predicted_price, model_version, response_time_ms
FROM inference_logs
WHERE requested_at > NOW() - INTERVAL '1 day'
ORDER BY requested_at DESC;

-- Precio promedio predicho por versión de modelo
SELECT model_version, AVG(predicted_price), COUNT(*)
FROM inference_logs
GROUP BY model_version
ORDER BY model_version;

-- Latencia percentil 95
SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms)
FROM inference_logs;
```

---

## 8. Integración con el DAG de Airflow

La API y el DAG se coordinan mediante el endpoint `/reload`.

### Flujo completo tras un entrenamiento

```
DAG: train_candidate_model
        ↓
DAG: evaluate_candidate_model
        ↓
DAG: register_candidate_in_mlflow  →  MLflow: versión N (sin alias)
        ↓
DAG: compare_with_production
        ↓
DAG: decide_promotion
        ↓ (si mejora ≥ 3% MAE)
DAG: promote_model  →  MLflow: alias "production" → versión N
        ↓
DAG: reload_inference_api  →  POST http://api:8000/reload
                                      ↓
                            API: load_production_model()
                                      ↓
                            API: model = Pipeline(preprocessor_v3, RF_v4)
                                      ↓
                            API: /predict usa el nuevo modelo
```

### Configuración en Docker Compose

El servicio API debe poder resolver `http://api:8000` desde el contenedor de Airflow. En Docker Compose esto se logra definiendo ambos servicios en la misma red:

```yaml
services:
  api:
    build: ./api
    ports:
      - "8000:8000"
    environment:
      MLFLOW_TRACKING_URI: http://mlflow:5000
      MLFLOW_S3_ENDPOINT_URL: http://minio:9000
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
      POSTGRES_DATASET_HOST: postgres-dataset
      POSTGRES_DATASET_DATABASE: real_estate_data
    networks:
      - mlops-network

  airflow-worker:
    # ...
    environment:
      INFERENCE_API_URL: http://api:8000
    networks:
      - mlops-network
```

La variable `INFERENCE_API_URL` en los workers de Airflow indica a `utils/inference_api.py` a qué URL enviar el POST `/reload`.

---

## 9. Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | URL del servidor MLflow |
| `REGISTERED_MODEL_NAME` | `real-estate-price-model` | Nombre del modelo en el MLflow Registry |
| `MODEL_ALIAS` | `production` | Alias del modelo a cargar |
| `MLFLOW_S3_ENDPOINT_URL` | — | URL de MinIO (necesaria para que MLflow descargue artefactos) |
| `AWS_ACCESS_KEY_ID` | — | Credencial MinIO |
| `AWS_SECRET_ACCESS_KEY` | — | Credencial MinIO |
| `POSTGRES_DATASET_HOST` | `postgres-dataset` | Host de PostgreSQL |
| `POSTGRES_DATASET_PORT` | `5432` | Puerto PostgreSQL |
| `POSTGRES_DATASET_USER` | `airflow` | Usuario PostgreSQL |
| `POSTGRES_DATASET_PASSWORD` | `airflow` | Contraseña PostgreSQL |
| `POSTGRES_DATASET_DATABASE` | `real_estate_data` | Base de datos donde vive `inference_logs` |
| `MLFLOW_HTTP_REQUEST_TIMEOUT` | `10` | Timeout en segundos para llamadas al servidor MLflow |

---

## 10. Despliegue local con Docker Compose

### Dockerfile

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY app/ app/
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Dependencias (`requirements.txt`)

```
fastapi==0.128.8
uvicorn==0.39.0
mlflow==3.1.4
boto3==1.42.73
numpy>=1.24.0
pandas==2.3.3
scikit-learn==1.6.1
psycopg2-binary==2.9.10
prometheus-fastapi-instrumentator==7.0.2
```

### Verificación del despliegue

```bash
# Health check
curl http://localhost:8000/health

# Info del modelo cargado
curl http://localhost:8000/model-info

# Predicción con el ejemplo de request
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d @request_example.json

# Recarga manual del modelo (normalmente lo hace el DAG)
curl -X POST http://localhost:8000/reload

# Ver métricas de Prometheus
curl http://localhost:8000/metrics
```

### Documentación interactiva

FastAPI genera automáticamente la documentación OpenAPI disponible en:

- **Swagger UI:** `http://localhost:8000/docs`
- **ReDoc:** `http://localhost:8000/redoc`

Desde Swagger UI es posible ejecutar todas las peticiones directamente en el navegador sin necesidad de curl.
