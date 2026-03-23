# Taller 4 - MLOps: Experiment Tracking con MLflow, MinIO y FastAPI

Este proyecto implementa un entorno completo de MLOps centrado en MLflow para tracking de experimentos, registro de modelos y servicio de inferencia. Se utilizan Docker Compose para orquestar todos los servicios: base de datos dedicada para metadata de MLflow (PostgreSQL), almacenamiento de artefactos (MinIO), base de datos para datos del proyecto (MySQL), JupyterLab para experimentacion y una API de inferencia (FastAPI).

## Diagrama del Sistema

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Docker Compose Network                         │
│                                                                        │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
│  │  PostgreSQL   │    │    MinIO      │    │       MLflow Server      │  │
│  │  (mlflow_db)  │◄───│  (artifacts) │◄───│   - Tracking URI         │  │
│  │  :5434        │    │  :9000/:9001 │    │   - Model Registry       │  │
│  │              │    │              │    │   :5001                  │  │
│  │  Metadata de  │    │  Bucket:     │    │                          │  │
│  │  MLflow       │    │  mlflows3    │    └──────────┬───────────────┘  │
│  └──────────────┘    └──────────────┘               │                  │
│                                                      │                  │
│         ┌────────────────────────────┬───────────────┘                  │
│         │                            │                                  │
│         ▼                            ▼                                  │
│  ┌──────────────┐           ┌──────────────────┐                       │
│  │  JupyterLab  │           │   FastAPI (API)   │                      │
│  │  :8888       │           │   :8000           │                      │
│  │              │           │                    │                      │
│  │  - Entrena   │           │  - /health         │                      │
│  │    modelos   │           │  - /predict        │                      │
│  │  - 24 exp.   │           │  - /reload         │                      │
│  │  - GridSearch│           │                    │                      │
│  │  - Promueve  │           │  Carga modelo con  │                      │
│  │    a prod.   │           │  alias "production"│                      │
│  └──────┬───────┘           └──────────────────┘                       │
│         │                                                               │
│         ▼                                                               │
│  ┌──────────────┐                                                       │
│  │    MySQL      │                                                       │
│  │  (data_db)   │                                                       │
│  │  :3306       │                                                       │
│  │              │                                                       │
│  │  penguins_raw│                                                       │
│  │  penguins_   │                                                       │
│  │  processed   │                                                       │
│  └──────────────┘                                                       │
└─────────────────────────────────────────────────────────────────────────┘
```

## Descripcion del Proyecto

### 1. Infraestructura con Docker Compose

Todos los servicios corren en el mismo `docker-compose.yaml`:

| Servicio | Imagen | Proposito | Puerto |
|---|---|---|---|
| **mlflow_db** | postgres:15 | Base de datos dedicada para metadata de MLflow | 5434 |
| **minio** | quay.io/minio/minio:latest | Almacenamiento S3 para artefactos de modelos | 9000 (API) / 9001 (Console) |
| **data_db** | mysql:8.0 | Base de datos exclusiva para datos del proyecto (penguins) | 3306 |
| **mlflow** | Custom (python:3.9-slim) | Servidor MLflow: tracking, model registry, artifact serving | 5001 |
| **jupyter** | jupyter/scipy-notebook:latest | JupyterLab para experimentacion y entrenamiento | 8888 |
| **api** | Custom (python:3.9-slim) | API FastAPI de inferencia con modelo de produccion | 8000 |

La separacion de bases de datos garantiza que PostgreSQL es **exclusiva para metadata de MLflow** y MySQL maneja unicamente los **datos del proyecto**.

### 2. Notebook de Entrenamiento (`penguins_training.ipynb`)

El notebook ejecuta el pipeline completo de experimentacion:

#### Paso 1: Carga y preprocesamiento de datos

- Carga el dataset Palmer Penguins (344 filas) desde la libreria `palmerpenguins`
- Guarda datos crudos en MySQL (`penguins_raw`)
- Limpieza: eliminacion de nulos y duplicados (344 → 333 filas)
- Transformacion: codificacion de species a numerico, one-hot encoding de categoricas
- Guarda datos procesados en MySQL (`penguins_processed`)
- Split: Train (233), Validation (50), Test (50)

#### Paso 2: Ejecucion de 24 experimentos

Se ejecutan 24 configuraciones con variaciones de hiperparametros, agrupados en 3 tipos de modelo:

| Modelo | Variaciones | Hiperparametros |
|---|---|---|
| **SVM** | 8 | kernel (rbf, linear, poly), C (0.1 a 100.0), degree |
| **Logistic Regression** | 8 | C (0.01 a 10.0), max_iter (500 a 2000), solver (lbfgs, saga), penalty |
| **Random Forest** | 8 | n_estimators (50 a 200), max_depth (3, 5, 10, None), min_samples_split |

Cada ejecucion registra en MLflow: parametros, metricas (accuracy, F1, precision, recall) y el modelo serializado.

![experimentos](img/experimentos.png)

#### Paso 3: Visualizacion en MLflow

Todos los runs aparecen en el experimento `penguins-classification` con sus metricas y modelos asociados.

![mlflow](img/mlflow.png)

#### Paso 4: GridSearchCV

Se ejecuta `GridSearchCV` con validacion cruzada (cv=5) para cada tipo de modelo, buscando la mejor combinacion de hiperparametros. Los resultados tambien se registran en MLflow.

![image](img/image.png)

#### Paso 5: Promocion del mejor modelo

El modelo con mejor `test_f1` se promueve con el alias **"production"** en el Model Registry de MLflow. En este caso, el mejor fue `penguins-svm` con `test_f1=0.9798`.

![model](img/model.png)

#### Paso 6: Resumen de resultados

El notebook muestra el top 5 de modelos por test_f1 y confirma la promocion del mejor modelo.

![notebook](img/notebook.png)

### 3. Model Registry de MLflow

Los 3 modelos quedan registrados en MLflow. El modelo `penguins-svm` tiene asignado el alias `@production`.

![models](img/models.png)

### 4. Almacenamiento de Artefactos en MinIO

MinIO actua como almacenamiento S3-compatible. Cada modelo registrado en MLflow guarda sus artefactos (model.pkl, conda.yaml, MLmodel, requirements.txt) en el bucket `mlflows3`.

![minio](img/minio.png)

Detalle de un artefacto individual:

![minioModel](img/minioModel.png)

### 5. API de Inferencia (FastAPI)

API REST que carga automaticamente el modelo con alias "production" desde MLflow al iniciar.

![api](img/api.png)

#### Endpoints

| Metodo | Endpoint | Descripcion |
|---|---|---|
| GET | `/health` | Healthcheck del servicio |
| POST | `/predict` | Predice la especie de un pinguino |
| POST | `/reload` | Recarga el modelo de produccion desde MLflow |

#### Modelo de Datos (PenguinFeatures)

| Campo | Tipo | Default | Descripcion |
|---|---|---|---|
| `bill_length_mm` | float | requerido | Longitud del pico (mm) |
| `bill_depth_mm` | float | requerido | Profundidad del pico (mm) |
| `flipper_length_mm` | float | requerido | Longitud de la aleta (mm) |
| `body_mass_g` | float | requerido | Masa corporal (g) |
| `year` | int | 2009 | Ano de observacion |
| `island_Biscoe` | int | 0 | One-hot: isla Biscoe |
| `island_Dream` | int | 0 | One-hot: isla Dream |
| `island_Torgersen` | int | 0 | One-hot: isla Torgersen |
| `sex_female` | int | 0 | One-hot: sexo femenino |
| `sex_male` | int | 0 | One-hot: sexo masculino |

#### Ejemplo de prediccion

![predict](img/predict.png)

#### Recarga de modelo

Si se promueve un nuevo modelo a "production" desde el notebook, se puede recargar sin reiniciar el contenedor:

![reload](img/reload.png)

## Estructura del Proyecto

```
Taller_4/
├── docker-compose.yaml          # Orquestacion de todos los servicios
├── README.md
├── img/                         # Imagenes de documentacion
├── mlflow/
│   └── Dockerfile               # Imagen custom de MLflow con psycopg2
├── notebooks/
│   ├── requirements.txt         # Dependencias del notebook
│   └── penguins_training.ipynb  # Notebook de entrenamiento y experimentacion
└── api/
    ├── Dockerfile               # Imagen de la API
    ├── requirements.txt         # Dependencias de la API
    └── app.py                   # FastAPI con endpoints de inferencia
```

## Pasos para Ejecutar

### 1. Iniciar Colima con 6GB de RAM

```bash
colima start --memory 6
```

### 2. Crear el bucket en MinIO

Antes de ejecutar el notebook, crear el bucket `mlflows3` en MinIO:

1. Acceder a http://localhost:9001
2. Login con `admin` / `supersecret`
3. Crear bucket con nombre `mlflows3`

### 3. Construir y levantar todos los servicios

```bash
cd Taller_4
docker-compose up -d --build
```

### 4. Verificar que todos los servicios estan corriendo

```bash
docker-compose ps
```

### 5. Ejecutar el notebook de entrenamiento

1. Acceder a JupyterLab: http://localhost:8888
2. Obtener el token de acceso:
   ```bash
   docker-compose logs jupyter | grep token
   ```
3. Abrir `work/penguins_training.ipynb`
4. Ejecutar todas las celdas: **Run → Run All Cells**

### 6. Verificar resultados

- **MLflow UI**: http://localhost:5001 — ver experimentos y modelos registrados
- **MinIO Console**: http://localhost:9001 — ver artefactos almacenados
- **API docs**: http://localhost:8000/docs — probar endpoints de inferencia

### 7. Probar la API

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "bill_length_mm": 39.1,
    "bill_depth_mm": 18.7,
    "flipper_length_mm": 181.0,
    "body_mass_g": 3750.0,
    "year": 2009,
    "island_Torgersen": 1,
    "sex_male": 1
  }'
```

Respuesta esperada:
```json
{"prediction": 0, "species": "Adelie"}
```

## Credenciales de Acceso

| Servicio | URL | Usuario | Contrasena |
|---|---|---|---|
| **MLflow UI** | http://localhost:5001 | — | — |
| **MinIO Console** | http://localhost:9001 | `admin` | `supersecret` |
| **JupyterLab** | http://localhost:8888 | — | Token (ver logs de jupyter) |
| **PostgreSQL (MLflow)** | localhost:5434 | `mlflow` | `mlflow` |
| **MySQL (Datos)** | localhost:3306 | `datauser` | `datapass` |
| **MySQL (Root)** | localhost:3306 | `root` | `rootpass` |
| **API Swagger** | http://localhost:8000/docs | — | — |

## Errores Encontrados y Soluciones

### 1. Modulo `psycopg2` no encontrado en MLflow
**Error**: La imagen base de MLflow no incluye el driver de PostgreSQL, por lo que no podia conectarse a `mlflow_db`.
**Solucion**: Se creo un `Dockerfile` custom en `mlflow/` que instala `psycopg2-binary` junto con `mlflow` y `boto3`.

### 2. Schema de base de datos desactualizado
**Error**: Despues de actualizar MLflow, la base de datos PostgreSQL tenia un schema incompatible.
**Solucion**: Se ejecuto la migracion de schema:
```bash
docker run --rm --network taller_4_default taller_4-mlflow \
  mlflow db upgrade postgresql+psycopg2://mlflow:mlflow@mlflow_db:5432/mlflow
```

## Comandos Utiles

```bash
# Ver logs de un servicio
docker-compose logs mlflow --tail 30

# Ver estado de los contenedores
docker-compose ps

# Reiniciar la API despues de promover un nuevo modelo
docker-compose restart api

# Verificar base de datos MySQL
docker-compose exec data_db mysql -u datauser -pdatapass penguins_data -e "SHOW TABLES;"

# Verificar metadata de MLflow en PostgreSQL
docker-compose exec mlflow_db psql -U mlflow -d mlflow -c "\dt"

# Detener todos los servicios
docker-compose down

# Detener y eliminar volumenes
docker-compose down -v
```

## Tecnologias Utilizadas

- **MLflow 3.1.4** — Experiment tracking, model registry y artifact serving
- **FastAPI** — API REST de inferencia
- **PostgreSQL 15** — Metadata de MLflow
- **MySQL 8.0** — Base de datos para datos del proyecto
- **MinIO** — Almacenamiento S3-compatible para artefactos
- **JupyterLab** — Entorno interactivo de experimentacion
- **Scikit-learn 1.6.1** — Entrenamiento de modelos (SVM, Logistic Regression, Random Forest)
- **Docker Compose** — Orquestacion de contenedores
- **Colima** — Runtime de Docker en macOS
- **Palmer Penguins Dataset** — Conjunto de datos
