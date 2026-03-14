# Compose dividido - Proyecto 1

Cada archivo define un conjunto de servicios. El `docker-compose.yaml` en la raíz de `Proyecto_1` los incluye todos para levantar el sistema completo.

## Archivos

| Archivo | Servicios | Puertos |
|---------|-----------|---------|
| `minio.yml` | MinIO (almacenamiento S3) | 19000 (API), 19001 (consola) |
| `postgres.yml` | PostgreSQL (metadatos de Airflow) | 5432 |
| `mysql.yml` | MySQL (datos Covertype) | 3306 |
| `redis.yml` | Redis (broker Celery) | 6379 (interno) |
| `airflow.yml` | Airflow (webserver, scheduler, worker, triggerer, init), Flower (profile) | 8080, 5555 (flower) |
| `data_api.yml` | API de datos Covertype (pruebas locales) | 8082 |
| `jupyterlab.yml` | JupyterLab (scipy-notebook genérico) | 8889 |
| `model_training.yml` | JupyterLab para entrenamiento (lee MySQL, guarda en MinIO) | 8888 |
| `inference_api.yml` | API de inferencia (modelos y preprocesador desde MinIO) | 8001 |

## Uso

Desde la carpeta **Proyecto_1**:

```bash
# Levantar todo (compose unificado)
docker compose up -d --build

# Levantar solo un servicio de infra
docker compose -f compose/minio.yml up -d
docker compose -f compose/postgres.yml up -d
docker compose -f compose/mysql.yml up -d
docker compose -f compose/redis.yml up -d

# Levantar Airflow (requiere antes minio, postgres, mysql, redis en la misma red)
docker compose -f compose/airflow.yml up -d --build

# Levantar solo la API de datos
docker compose -f compose/data_api.yml up -d --build

# Levantar solo JupyterLab
docker compose -f compose/jupyterlab.yml up -d

# Levantar entrenamiento (requiere mysql_db y minio)
docker compose -f compose/model_training.yml up -d --build

# Levantar API de inferencia (requiere minio)
docker compose -f compose/inference_api.yml up -d --build
```

Con el compose unificado, todos los servicios comparten la misma red y pueden resolverse por nombre (`minio`, `postgres`, `mysql_db`, `redis`, `api-data`).
