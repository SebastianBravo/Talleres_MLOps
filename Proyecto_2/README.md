
# Proyecto 2 - MLOps

Este proyecto implementa un flujo MLOps end-to-end para el dataset de readmision diabetica. Incluye ingesta incremental, preprocesamiento, entrenamiento y registro de modelos en MLflow, y una API de inferencia que consume el modelo productivo. La orquestacion se realiza con Airflow, los artefactos se almacenan en MinIO (S3 compatible), y el despliegue se valida tanto con Docker Compose como con Kubernetes.

## Organizacion de carpetas

- [Airflow/](Airflow/): DAGs, utilidades, configuracion y values para Helm.
- [api/](api/): API de inferencia (FastAPI) que consulta MLflow.
- [mlflow/](mlflow/): Imagen personalizada de MLflow.
- [compose/](compose/): Archivos docker compose por servicio.
- [komposefiles/](komposefiles/): Manifiestos generados por Kompose para Kubernetes.
- [docker-compose.yaml](docker-compose.yaml): Compose unificado que incluye los servicios del proyecto.

## Servicios y responsabilidad

### Airflow

- Orquesta el pipeline completo, define dependencias y reintentos, y mantiene el estado del flujo.
- Ejecuta el DAG `data_dag` con LocalExecutor.
- Usa variables de Airflow para manejar progreso del batch y evitar reprocesos.
- Imagen personalizada con DAGs y dependencias locales.

### PostgreSQL (metadata Airflow)

- Base de datos de metadatos del orquestador.
- Guarda estado de DAGs, ejecuciones, logs y configuraciones internas de Airflow.

### PostgreSQL (dataset)

- Base de datos del negocio con tablas crudas, split train/test y tabla limpia.
- Recibe la carga incremental de datos y persiste el versionamiento por batch.

### MinIO

- Almacenamiento de artefactos y archivos versionados, compatible con S3.
- Guarda preprocesadores versionados y artefactos de entrenamiento.
- Se integra con MLflow para el artifact store.

### MLflow

- Tracking de experimentos, metricas, artefactos y modelos registrados.
- Guarda el mejor modelo y lo promueve al alias `production`.
- Usa una base Postgres dedicada para su backend store.
- Imagen personalizada con configuracion para MinIO y Postgres.

### API de inferencia (FastAPI)

- Expone el endpoint de prediccion y consume el modelo en `production` desde MLflow.
- Carga el modelo al iniciar y permite recargarlo cuando haya un nuevo modelo productivo.
- Imagen personalizada con dependencias del modelo.

Endpoints principales:

- `GET /`: estado general y metadatos del modelo cargado.
- `GET /health`: healthcheck con estado del modelo y mensajes de error si existen.
- `POST /predict`: recibe features del paciente y retorna prediccion (y probabilidades si el modelo las soporta).
- `POST /reload`: fuerza la recarga del modelo desde MLflow (alias `production`).
- `GET /model-info`: retorna informacion del modelo activo y su estado.

### Adminer (opcional)

- UI liviana para inspeccionar las bases de datos durante pruebas locales.

## DAG de Airflow: flujo y tareas

El DAG `data_dag` ejecuta un pipeline incremental por lotes y esta programado cada 2 minutos para pruebas locales.

Flujo de tareas:

1. `create_tables`
	- Verifica si existen tablas en el esquema publico.
	- Crea la tabla `diabetic_data_raw` cuando la base esta vacia.
2. `validate_source_file`
	- Verifica/descarga el dataset si no existe localmente.
	- Retorna la ruta del archivo fuente para las siguientes tareas.
3. `load_raw_batch`
	- Lee un batch del CSV usando offset y tamano fijo.
	- Inserta filas en `diabetic_data_raw` con metadatos de carga.
	- Actualiza variables de Airflow para offset, batch y estado de completitud.
4. `assign_dataset`
	- Asigna de forma determinista `train/test` a filas nuevas.
	- Persiste el split en `diabetic_data_split`.
5. `preprocess_batch`
	- Preprocesa datos con imputacion, escalado y one-hot encoding.
	- Versiona el preprocesador en MinIO y registra el batch procesado.
	- Reconstruye la tabla limpia para acomodar nuevas categorias.
6. `train_models`
	- Entrena modelos, registra metricas y artefactos en MLflow.
	- Promueve el mejor modelo al alias `production`.
7. `reload_api_model`
	- Llama a la API para recargar el modelo productivo desde MLflow.

## Uso con Docker Compose

Docker Compose permite levantar todo el stack rapidamente para pruebas locales.

```bash
cd Proyecto_2
docker compose up -d --build
```

Si deseas levantar servicios especificos, usa los archivos en [compose/](compose/).

## Despliegue en Kubernetes (paso a paso)

El despliegue en Kubernetes se hace con dos enfoques:

- **Airflow**: se instala con Helm usando el chart de Apache Airflow y los values en [Airflow/values/values-local.yaml](Airflow/values/values-local.yaml).
- **Resto de servicios**: se convierten con Kompose desde los compose individuales y se aplican con `kubectl`.

### 1) Construir imagenes personalizadas

```bash
docker build --pull --tag airflow-local-dags:0.0.1 /airflow
docker build --pull --tag mlflow-local:0.0.1 /mlflow
docker build --pull --tag api-local:0.0.1 /api
```

### 2) Generar manifiestos con Kompose (si necesitas regenerarlos)

```bash
kompose convert -f compose/minio.yml -o komposefiles/minio/
```

### 3) Crear namespace

```bash
kubectl create namespace airflow-local
```

### 4) Instalar Airflow con Helm

```bash
helm upgrade --install airflow apache-airflow/airflow --namespace airflow-local -f Airflow/values/values-local.yaml
```

### 5) Aplicar servicios por Kompose (orden recomendado)

```bash
kubectl apply -f komposefiles/postgres_dataset/ --namespace airflow-local
kubectl apply -f komposefiles/minio/ --namespace airflow-local
kubectl apply -f komposefiles/mlflow/ --namespace airflow-local
kubectl apply -f komposefiles/api/ --namespace airflow-local
```

### 6) Validar estado de los pods

```bash
kubectl get pods -n airflow-local
kubectl describe pod <pod> -n airflow-local
kubectl logs <nombre-del-pod> -n airflow-local
```

### 7) Acceder a los servicios con port-forward

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow-local
kubectl port-forward svc/mlflow 5000:5000 --namespace airflow-local
kubectl port-forward svc/api 8000:8000 --namespace airflow-local
```

### 8) Eliminar el despliegue completo

```bash
kubectl delete namespace airflow-local
```
