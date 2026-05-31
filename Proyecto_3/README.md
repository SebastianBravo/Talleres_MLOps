# Proyecto 3 — Real Estate Price Prediction MLOps

Pipeline incremental de MLOps para predicción de precios de bienes raíces. El sistema ingiere datos por lotes desde una API externa, valida su calidad, detecta drift, entrena modelos con RandomForest, los evalúa frente al modelo en producción y los promueve automáticamente cuando mejoran las métricas. La API de inferencia se actualiza en caliente sin reiniciar el contenedor.

---

## Tabla de contenido

1. [Arquitectura](#1-arquitectura)
2. [Componentes](#2-componentes)
3. [Estructura del proyecto](#3-estructura-del-proyecto)
4. [Prerrequisitos](#4-prerrequisitos)
5. [Despliegue local — Docker Compose](#5-despliegue-local--docker-compose)
6. [Despliegue en Kubernetes](#6-despliegue-en-kubernetes)
7. [CI/CD — GitHub Actions](#7-cicd--github-actions)
8. [Flujo del pipeline MLOps](#8-flujo-del-pipeline-mlops)

---

## 1. Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Usuario / Cliente                               │
└───────────┬─────────────────────────────────┬───────────────────────────┘
            │                                 │
     ┌──────▼──────┐                   ┌──────▼──────┐
     │  Streamlit  │                   │  Locust     │
     │  :8501      │                   │  :8089      │
     └──────┬──────┘                   └──────┬──────┘
            │                                 │
     ┌──────▼─────────────────────────────────▼──────┐
     │              FastAPI Inference API             │
     │              :8000                             │
     │   /predict  /history  /reload  /metrics        │
     └──────┬───────────────────┬────────────┬────────┘
            │                   │            │
     ┌──────▼──────┐   ┌────────▼───────┐   │
     │   MLflow    │   │  PostgreSQL    │   │ métricas
     │  Registry   │   │  (dataset DB)  │   │
     └──────┬──────┘   └────────────────┘   │
            │                        ┌──────▼──────┐
     ┌──────▼──────┐                 │ Prometheus  │
     │    MinIO    │                 │  :9090      │
     │  Artifacts  │                 └──────┬──────┘
     └──────▲──────┘                        │
            │                        ┌──────▼──────┐
     ┌──────┴──────────────────┐     │   Grafana   │
     │      Airflow DAG        │     │  :3000      │
     │  (pipeline incremental) │     └─────────────┘
     └──────┬──────────────────┘
            │
     ┌──────▼──────┐
     │  API Source │  ← datos por batch (externa)
     │  :8001      │
     └─────────────┘
```

---

## 2. Componentes

### API de datos (`api-source`)
Servicio externo que provee los datos de propiedades inmobiliarias en lotes secuenciales. El DAG consulta `GET /data?group_number=7` para obtener cada batch.

---

### DAG de Airflow — Pipeline incremental
Orquesta el ciclo completo de MLOps: ingesta → validación → drift → preprocesamiento → decisión de entrenamiento → entrenamiento → evaluación → comparación → promoción → recarga de API.

→ Documentación detallada: [Airflow/README.md](Airflow/README.md)

---

### API de inferencia (FastAPI)
Sirve predicciones de precio usando el modelo productivo registrado en MLflow. Soporta recarga en caliente del modelo sin reiniciar el contenedor y expone métricas para Prometheus.

→ Documentación detallada: [api/README.md](api/README.md)

---

### Interfaz Streamlit
Dos secciones: formulario de predicción de precio por propiedad y vista del historial de entrenamiento y despliegue por lote (decisión de entrenamiento, métricas, identificadores MLflow).

→ Documentación detallada: [streamlit/README.md](streamlit/README.md)

---

### MLflow
Servidor de tracking de experimentos y registro de modelos. Almacena métricas, parámetros y artefactos. El alias `production` determina qué versión sirve la API.

| Componente | Imagen |
|---|---|
| Servidor MLflow | `bravosjs/mlops-mlflow:dev` |
| Base de datos | `postgres:13` |
| Artefactos | MinIO — bucket `mlflows3` |

---

### MinIO
Almacenamiento de objetos compatible con S3. Guarda los artefactos de MLflow (modelos serializados) y los datos del proyecto.

| Bucket | Uso |
|---|---|
| `mlflows3` | Artefactos de MLflow (modelos, métricas, artefactos) |
| `real-estate-project` | Datos del proyecto |

---

### Observabilidad

| Servicio | Puerto | Descripción |
|---|---|---|
| Prometheus | 9090 | Scraping de métricas de la API cada 15s |
| Grafana | 3000 | Dashboard con RPS, latencia (p50/p95/p99), tasa de error |
| Locust | 8089 | Pruebas de carga sobre `POST /predict` |

---

## 3. Estructura del proyecto

```
Proyecto_3/
├── Airflow/                    ← DAG y configuración de Airflow
│   ├── dags/
│   │   ├── data_dag.py         ← DAG principal del pipeline MLOps
│   │   └── utils/              ← módulos del DAG (ingesta, training, drift, etc.)
│   ├── Dockerfile              ← imagen de producción (DAGs baked in)
│   ├── Dockerfile.Compose      ← imagen local (DAGs montados como volumen)
│   └── README.md
│
├── api/                        ← FastAPI inference API
│   ├── app/
│   │   ├── config.py
│   │   ├── schemas.py          ← PropertyFeatures + parseo de fechas
│   │   ├── database.py         ← inference_logs + batch_history
│   │   ├── model.py            ← carga desde MLflow
│   │   └── router.py           ← endpoints
│   ├── Dockerfile
│   └── README.md
│
├── streamlit/                  ← interfaz web
│   ├── app.py
│   ├── Dockerfile
│   └── README.md
│
├── grafana/                    ← imagen custom con dashboards baked in
│   ├── Dockerfile
│   ├── dashboards/
│   └── provisioning/
│
├── prometheus/                 ← imagen custom con prometheus.yml baked in
│   └── Dockerfile
│
├── locust/                     ← imagen custom con locustfile.py baked in
│   └── Dockerfile
│
├── mlflow/                     ← imagen custom de MLflow
│   └── Dockerfile
│
├── compose/                    ← compose files por servicio
│   ├── airflow-slim.yml
│   ├── api.yml
│   ├── api_source.yml
│   ├── grafana.yml
│   ├── locust.yml
│   ├── minio.yml
│   ├── mlflow.yml
│   ├── postgres-dataset.yml
│   ├── prometheus.yml
│   └── streamlit.yml
│
├── manifests/                  ← manifiestos de Kubernetes
│   ├── namespaces/             ← mlops-infra, mlops-app, mlops-obs, mlops-airflow
│   ├── infra/                  ← postgres-dataset, minio, mlflow, api-source, adminer
│   ├── app/                    ← api, streamlit
│   ├── obs/                    ← prometheus, grafana, locust
│   └── airflow-helm-values/
│       └── values-local.yaml
│
├── docker-compose.yaml         ← compose unificado (incluye todos los servicios)
├── Makefile                    ← despliegue por etapas en Kubernetes
└── README.md
```

---

## 4. Prerrequisitos

**Local (Docker Compose):**
- Docker Desktop ≥ 4.x con al menos 6 GB de RAM asignados
- `docker compose` v2

**Kubernetes:**
- Docker Desktop con Kubernetes habilitado (o kind/minikube)
- `kubectl` configurado
- `helm` ≥ 3.x (`brew install helm`)

**CI/CD:**
- Cuenta en DockerHub
- Secretos `DOCKERHUB_USERNAME` y `DOCKERHUB_TOKEN` configurados en GitHub → Settings → Secrets

---

## 5. Despliegue local — Docker Compose

### Levantar el stack completo

```bash
docker compose up -d --build
```

### Levantar servicios individuales

```bash
# Solo infraestructura
docker compose -f compose/postgres-dataset.yml -f compose/minio.yml -f compose/mlflow.yml up -d

# API de inferencia
docker compose -f compose/api.yml up -d --build

# Observabilidad
docker compose -f compose/prometheus.yml -f compose/grafana.yml up -d --build
```

### Variables de entorno requeridas (`.env` en `Proyecto_3/`)

```env
AIRFLOW_UID=50000
GF_ADMIN_USER=admin
GF_ADMIN_PASSWORD=admin
```

### Servicios disponibles

| Servicio | URL | Credenciales |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| MLflow | http://localhost:5000 | — |
| API docs | http://localhost:8000/docs | — |
| Streamlit | http://localhost:8501 | — |
| Locust | http://localhost:8089 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| MinIO UI | http://localhost:9001 | minioadmin / minioadmin |

---

## 6. Despliegue en Kubernetes

Los manifiestos están organizados en **4 namespaces** para aislar responsabilidades:

| Namespace | Componentes |
|---|---|
| `mlops-infra` | postgres-dataset, minio, mlflow, api-source, adminer |
| `mlops-app` | api (FastAPI), streamlit |
| `mlops-obs` | prometheus, grafana, locust |
| `mlops-airflow` | airflow (Helm chart) |

### Despliegue completo

```bash
cd Proyecto_3
make deploy
```

Esto ejecuta en orden: `namespaces → deploy-infra → wait-infra → minio-init → deploy-app → deploy-obs → deploy-airflow`.

### Despliegue por etapas

```bash
make namespaces          # Crea los 4 namespaces
make deploy-infra        # PostgreSQL, MinIO, MLflow, api-source
make wait-infra          # Espera a que infra esté lista
make minio-init          # Crea los buckets en MinIO
make deploy-app          # API de inferencia + Streamlit
make deploy-obs          # Prometheus + Grafana + Locust
make deploy-airflow      # Helm chart de Airflow + migraciones de DB
```

### Acceso local (port-forward)

```bash
make forward             # Abre todos los servicios en localhost
make forward-adminer     # Adminer en localhost:8082 (opcional)
make stop-forward        # Cierra todos los port-forwards
```

### Otros targets útiles

```bash
make status              # Estado de pods y servicios en todos los namespaces
make restart-api         # Rollout restart de la API
make airflow-migrate     # Re-corre las migraciones de Airflow (útil tras reinstalar)
make delete-app          # Elimina mlops-app, mlops-obs y mlops-airflow
make delete              # Elimina todos los namespaces (destruye todo)
```

### Imágenes Docker (DockerHub)

| Imagen | Tag estable | Descripción |
|---|---|---|
| `bravosjs/mlops-api` | `dev` / `sha-*` | FastAPI inference API |
| `bravosjs/mlops-streamlit` | `dev` / `sha-*` | Interfaz Streamlit |
| `bravosjs/mlops-airflow` | `dev` / `sha-*` | Airflow con DAGs baked in |
| `bravosjs/mlops-airflow-compose` | `dev` / `sha-*` | Airflow para uso local con volúmenes |
| `bravosjs/mlops-mlflow` | `dev` / `sha-*` | MLflow tracking server |
| `bravosjs/mlops-prometheus` | `dev` / `sha-*` | Prometheus con config baked in |
| `bravosjs/mlops-grafana` | `dev` / `sha-*` | Grafana con dashboards baked in |
| `bravosjs/mlops-locust` | `dev` / `sha-*` | Locust con locustfile baked in |

---

## 7. CI/CD — GitHub Actions

Cinco workflows en `.github/workflows/` construyen y publican imágenes multi-arquitectura (`linux/amd64` + `linux/arm64`) en DockerHub.

| Workflow | Disparo | Imágenes |
|---|---|---|
| `build-api.yml` | Cambios en `Proyecto_3/api/**` | `mlops-api` |
| `build-streamlit.yml` | Cambios en `Proyecto_3/streamlit/**` | `mlops-streamlit` |
| `build-airflow.yml` | Cambios en `Proyecto_3/Airflow/**` | `mlops-airflow` + `mlops-airflow-compose` |
| `build-mlflow.yml` | Cambios en `Proyecto_3/mlflow/**` | `mlops-mlflow` |
| `build-observability.yml` | Cambios en `prometheus/`, `grafana/`, `locust/` | `mlops-prometheus`, `mlops-grafana`, `mlops-locust` |

**Estrategia de tags:**
- Cualquier push: `sha-<7chars>` (trazabilidad exacta al commit)
- Push a `main`: + `latest`
- Push a rama `feature/**`: + `dev`
- Pull Request: solo build local, sin push (validación de que compila en ambas arquitecturas)

**Secretos requeridos en GitHub:**

```
DOCKERHUB_USERNAME
DOCKERHUB_TOKEN    ← access token (no la contraseña)
```

---

## 8. Flujo del pipeline MLOps

```
Cada 2 minutos (schedule del DAG):

  1. fetch_batch_from_api     ← GET /data?group_number=7
  2. store_raw_batch          ← INSERT en property_raw + audit entry
  3. validate_schema          ← verifica columnas requeridas
  4. validate_data_quality    ← nulls, duplicados, rangos
  5. detect_new_categories    ← nuevas categorías en city, state, etc.
  6. detect_data_drift        ← KS test vs. distribución histórica
  7. preprocess_data          ← ColumnTransformer + split train/val/test
  8. decide_training ─┬─ NO → skip_training
     (BranchOperator)  └─ SÍ → train_candidate_model
                                   │
                           evaluate_candidate_model
                                   │
                           register_candidate_in_mlflow
                                   │
                           compare_with_production
                                   │
                    decide_promotion ─┬─ NO → reject_model
                                      └─ SÍ → promote_model
                                                  │
                                          reload_inference_api
                                          (POST /reload)
  9. notify_or_log_result     ← marca el batch como "success"
```

**Criterios de entrenamiento** (cualquiera dispara reentrenamiento):
- No existe modelo productivo (baseline inicial)
- Volumen del batch ≥ 10% del histórico acumulado
- Nuevas categorías significativas (frecuencia ≥ 1%)
- Drift en distribución (KS test, p < 0.05)

**Criterio de promoción:**
- MAE del candidato mejora ≥ 3% frente al modelo productivo
- Si no existe modelo previo, se promueve automáticamente como baseline
