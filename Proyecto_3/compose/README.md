# Docker Compose - Proyecto 3

Cada archivo en esta carpeta define una parte del stack local de Proyecto 3.
El `docker-compose.yaml` de la raiz de `Proyecto_3` los incluye para levantar el
entorno completo de desarrollo.

## Archivos

| Archivo | Servicios | Puertos host |
|---|---|---|
| `postgres.yml` | PostgreSQL de metadatos de Airflow | 5432 |
| `postgres-dataset.yml` | PostgreSQL de datos del proyecto | 5433 |
| `minio.yml` | MinIO + bootstrap de buckets | 19000 API, 19001 consola |
| `mlflow.yml` | MLflow + PostgreSQL de MLflow | 5001, 5434 |
| `api_source.yml` | API externa de datos por lotes | 8001 |
| `airflow-slim.yml` | Airflow local con DAGs montados como volumen | 8080 |
| `api.yml` | FastAPI de inferencia | 8000 |
| `streamlit.yml` | Interfaz web | 8501 |
| `prometheus.yml` | Prometheus | 9090 |
| `grafana.yml` | Grafana | 3000 |
| `locust.yml` | Locust | 8089 |
| `adminer.yml` | Adminer para inspeccionar bases de datos | 8085 |
| `redis.yml` | Redis opcional | interno |
| `airflow.yml` | Variante completa de Airflow con Celery/Flower | 8080, 5555 |

## Uso

Desde la carpeta `Proyecto_3`:

```bash
# Levantar todo con el compose unificado
docker compose up -d --build

# Ver logs
docker compose logs -f

# Detener el stack
docker compose down
```

## Servicios principales

| Servicio | URL | Credenciales |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| MLflow | http://localhost:5001 | - |
| API docs | http://localhost:8000/docs | - |
| Streamlit | http://localhost:8501 | - |
| Locust | http://localhost:8089 | - |
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3000 | admin / admin |
| MinIO UI | http://localhost:19001 | minioadmin / minioadmin |
| API Source | http://localhost:8001 | - |
| Adminer | http://localhost:8085 | - |

## Levantar partes del stack

```bash
# Infraestructura base
docker compose up -d --build postgres-dataset minio mlflow api-source

# API externa de datos por lotes
docker compose up -d api-source

# API de inferencia
docker compose up -d --build api

# Observabilidad
docker compose up -d --build prometheus grafana locust
```

Los servicios comparten la red del compose unificado y se resuelven por nombre
interno, por ejemplo `mlflow:5000`, `minio:9000`, `api:8000`,
`api-source:80` y `postgres-dataset:5432`.
