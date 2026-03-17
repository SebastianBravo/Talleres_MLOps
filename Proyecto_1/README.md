# Proyecto 1 - MLOps: Pipeline de Datos, Entrenamiento e Inferencia

## Descripcion General

Este proyecto implementa un pipeline completo de MLOps sobre el dataset **Covertype**: desde la ingesta de datos hasta la inferencia en produccion. El sistema soporta **dos flujos de entrenamiento** con distintas filosofias, ambos compartiendo la misma infraestructura de servicios y la misma API de inferencia.

---

## Estructura del Proyecto

```
Proyecto_1/
├── .env                              # Variables de entorno (credenciales, configuraciones)
├── docker-compose.yaml               # Orquestacion unificada de todos los servicios
├── compose/                          # Compose dividido por servicio (para legibilidad)
│   ├── airflow.yml                   # Airflow: webserver, scheduler, worker, triggerer, init
│   ├── data_api.yml                  # API de datos Covertype (pruebas locales)
│   ├── inference_api.yml             # API de inferencia (v1 y v2)
│   ├── jupyterlab.yml                # JupyterLab generico
│   ├── minio.yml                     # MinIO: almacenamiento de objetos S3
│   ├── model_training.yml            # JupyterLab para entrenamiento (acceso a MySQL y MinIO)
│   ├── mysql.yml                     # MySQL: base de datos de datos Covertype
│   ├── postgres.yml                  # PostgreSQL: metadatos internos de Airflow
│   └── redis.yml                     # Redis: broker Celery para Airflow
├── Airflow/
│   ├── Dockerfile                    # Imagen personalizada de Airflow
│   ├── requirements.txt              # Dependencias de Python para Airflow
│   ├── dags/
│   │   ├── data_dag.py               # DAG principal: ingesta, preprocesamiento y almacenamiento
│   │   ├── db_utils.py               # Utilidades: MySQL, MinIO, API, preprocesamiento
│   │   └── train_utils.py            # Utilidades para entrenamiento
│   ├── logs/                         # Logs de ejecuciones del DAG
│   └── plugins/                      # Plugins personalizados de Airflow
├── data-api/
│   ├── main.py                       # Servidor FastAPI que sirve datos por batches
│   ├── generate_data.py              # Script generador del dataset
│   ├── Dockerfile
│   ├── requirements.txt
│   └── data/
│       ├── covertype.csv             # Dataset Covertype completo
│       └── timestamps.json           # Control de timestamps para los batches
├── model_training/
│   ├── train.ipynb                   # Flujo V2: preprocesamiento propio por modelo
│   └── train2.ipynb                  # Flujo V1: usa preprocesador del DAG desde MinIO
└── inference_api/
    ├── api.py                        # API FastAPI de inferencia (endpoints v1 y v2)
    └── requirements.txt
```

> El `docker-compose.yaml` en la raiz usa la directiva `include` para incorporar todos los archivos en `compose/`. Esto divide la configuracion por responsabilidad sin perder la posibilidad de levantar todo con un solo comando.

---

## Como Ejecutar el Proyecto

### Prerrequisitos

- [Docker](https://docs.docker.com/get-docker/) y [Docker Compose](https://docs.docker.com/compose/install/) instalados
- Puertos disponibles: `8080` (Airflow), `3306` (MySQL), `9000`/`9001` (MinIO), `8001` (Inference API), `8888` (Model Training), `8889` (JupyterLab), `8082` (Data API)

### 1. Configurar Variables de Entorno

```bash
cp .env.example .env
```

Editar `.env` con las credenciales correspondientes:

```env
# MySQL
MYSQL_HOST=mysql_db
MYSQL_USER=airflow
MYSQL_PASSWORD=airflow
MYSQL_DATABASE=covertype_data

# MinIO
MINIO_ENDPOINT=http://minio:9000
MINIO_BUCKET=covertype-project
AWS_ACCESS_KEY_ID=<tu_access_key>
AWS_SECRET_ACCESS_KEY=<tu_secret_key>
AWS_DEFAULT_REGION=us-east-1

# API de Datos
API_URL=http://api-data:8082        # Para pruebas locales
# API_URL=http://10.43.101.94:8080  # Para la API del profesor
API_GROUP_NUMBER=<tu_numero_de_grupo>
```

### 2. Levantar Todo el Proyecto

```bash
cd Proyecto_1
docker compose up --build
```

Este unico comando levanta todos los servicios: infraestructura (MinIO, MySQL, PostgreSQL, Redis), Airflow, la API de datos, JupyterLab, el entorno de entrenamiento y la API de inferencia.

### 3. Acceder a los Servicios

| Servicio               | URL                        | Credenciales           |
| ---------------------- | -------------------------- | ---------------------- |
| **Airflow**            | http://localhost:8080      | `airflow` / `airflow`  |
| **MinIO Console**      | http://localhost:9001      | Configuradas en `.env` |
| **Inference API**      | http://localhost:8001      | —                      |
| **Inference API Docs** | http://localhost:8001/docs | —                      |
| **Model Training**     | http://localhost:8888      | —                      |
| **JupyterLab**         | http://localhost:8889      | —                      |
| **Data API**           | http://localhost:8082      | —                      |

---

## Arquitectura de Servicios

| Servicio            | Descripcion                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------------- |
| **Data API**        | FastAPI que simula la fuente externa de datos. Sirve el dataset Covertype en 10 batches cada 5 minutos |
| **Apache Airflow**  | Orquestador. Ejecuta el DAG de ingesta y preprocesamiento                                               |
| **MySQL**           | Almacena datos crudos (`covertype_raw`) y datos preprocesados (`covertype_cleaned`)                     |
| **MinIO**           | Almacenamiento de objetos compatible con S3. Guarda preprocesadores y modelos entrenados                |
| **Model Training**  | JupyterLab con acceso a MySQL y MinIO para ejecutar los notebooks de entrenamiento                      |
| **Inference API**   | FastAPI que expone endpoints v1 y v2 para prediccion usando modelos almacenados en MinIO               |
| **PostgreSQL**      | Base de datos interna de Airflow (metadatos, estado del DAG)                                            |
| **Redis**           | Broker de mensajes para el Celery Executor de Airflow                                                   |

---

## Flujos de Entrenamiento

El proyecto implementa dos flujos con filosofias distintas pero complementarias, diferenciados por la version (`v1` / `v2`) tanto en MinIO como en la API de inferencia.

---

### Flujo V1 — Preprocesamiento Estandarizado (train2.ipynb)

```
Data API → DAG → MySQL (raw + cleaned) → MinIO (v1/preprocess/) → train2.ipynb → MinIO (v1/models/) → Inference API /v1/*
```

**Como funciona:**

1. El DAG recolecta todos los batches de la Data API y los guarda en `covertype_raw` (datos crudos).
2. Una vez completa la ingesta, el DAG ejecuta el preprocesamiento: limpieza, escalado, one-hot encoding, y divide los datos en train/test (80/20). El resultado se guarda en `covertype_cleaned` y el preprocesador entrenado se sube a MinIO en `v1/preprocess/preprocessor.joblib`.
3. El notebook `train2.ipynb` asume que estas tablas y el preprocesador ya existen. Lee los datos directamente desde `covertype_cleaned` (ya transformados) y descarga el preprocesador desde MinIO para asegurar consistencia con el pipeline de produccion.
4. El modelo entrenado se sube a MinIO en `v1/models/{nombre_modelo}.joblib`.
5. La Inference API expone `/v1/predict`: usa siempre el mismo preprocesador del DAG (`v1/preprocess/preprocessor.joblib`) y el modelo solicitado desde `v1/models/`.

**Ventajas:**
- **Consistencia garantizada**: el mismo preprocesador que transformo los datos de entrenamiento es el que se usa en produccion. No hay riesgo de divergencia entre entrenamiento e inferencia.
- **Eficiencia**: los datos ya estan preprocesados en MySQL; el notebook solo entrena, sin repetir el preprocesamiento.
- **Estandarizacion del pipeline**: todos los modelos v1 comparten un unico preprocesador, facilitando comparacion justa entre modelos.
- **Reproducibilidad**: cualquier cientifico de datos que ejecute `train2.ipynb` obtiene exactamente el mismo punto de partida.
- **Menor tiempo de experimentacion**: se puede entrenar y comparar multiples modelos sin volver a procesar los datos.

---

### Flujo V2 — Preprocesamiento Flexible por Modelo (train.ipynb)

```
Data API → DAG → MySQL (raw) → train.ipynb → MinIO (v2/preprocess/ + v2/models/) → Inference API /v2/*
```

**Como funciona:**

1. El DAG recolecta los batches y guarda los datos crudos en `covertype_raw`. No es necesario esperar al preprocesamiento del DAG.
2. El notebook `train.ipynb` lee directamente desde `covertype_raw`, construye su propio pipeline de preprocesamiento (puede variar segun el modelo o experimento) y lo ajusta con los datos de entrenamiento.
3. Tanto el modelo como su preprocesador se suben a MinIO bajo el mismo nombre: `v2/models/{nombre}.joblib` y `v2/preprocess/{nombre}.joblib`.
4. La Inference API expone `/v2/predict`: carga el preprocesador y el modelo por nombre, de modo que cada modelo tiene su propio preprocesamiento asociado.

**Ventajas:**
- **Flexibilidad total**: el cientifico de datos puede experimentar con diferentes estrategias de preprocesamiento (distintas features, distintos escaladores, distintos encoders) sin afectar otros modelos.
- **Autonomia del notebook**: no depende del estado del DAG ni de la tabla `covertype_cleaned`; basta con tener datos crudos.
- **Experimentacion rapida**: ideal para iterar sobre hipotesis de preprocesamiento y ver su impacto en el rendimiento del modelo.
- **Modelos autosuficientes**: cada modelo v2 lleva su propio preprocesador, lo que facilita el versionado y el despliegue independiente de cada experimento.
- **Adaptabilidad**: permite ajustar el preprocesamiento a las particularidades de cada algoritmo (por ejemplo, SVM vs Random Forest pueden beneficiarse de distintos escalados).

---

## Componentes en Detalle

### 1. Data API

FastAPI que simula la fuente externa de datos del dataset Covertype. Sirve los datos divididos en **10 batches**, rotando cada 5 minutos. El DAG consulta esta API en cada ejecucion para obtener el batch disponible en ese momento.

- En pruebas locales: `http://localhost:8082`
- En produccion: API externa del profesor en `http://10.43.101.94:8080`

---

### 2. DAG: `data_dag`

Orquestado por Airflow, se ejecuta cada 5 minutos y sigue este flujo:

```
create_tables → load_raw_data → check_should_preprocess → preprocess_data → pause_dag
```

| Tarea                    | Descripcion                                                                                                                                            |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `create_tables`          | Crea las tablas `covertype_raw` y `covertype_cleaned` en MySQL si no existen                                                                           |
| `load_raw_data`          | Consulta la Data API, obtiene el batch actual e inserta los datos en `covertype_raw`                                                                   |
| `check_should_preprocess`| **ShortCircuitOperator**: solo continua cuando se han recolectado los 10 batches y no se ha preprocesado antes                                         |
| `preprocess_data`        | Limpia datos, divide en train/test (80/20), aplica escalado y one-hot encoding. Guarda en `covertype_cleaned` y sube el preprocesador a MinIO          |
| `pause_dag`              | Pausa el DAG automaticamente al finalizar el proceso completo                                                                                          |

**Preprocesamiento del DAG:**
1. Elimina filas con valores nulos y duplicados
2. Divide en train (80%) y test (20%) con estratificacion por clase
3. Pipeline numerico: imputacion por mediana + `StandardScaler`
4. Pipeline categorico: imputacion por moda + `OneHotEncoder`
5. Guarda datos limpios en `covertype_cleaned` (con columna `dataset` = `train`/`test`)
6. Sube el preprocesador a `v1/preprocess/preprocessor.joblib` en MinIO

---

### 3. MySQL

Almacena dos tablas principales:

| Tabla                | Contenido                                                                   |
| -------------------- | --------------------------------------------------------------------------- |
| `covertype_raw`      | Datos originales tal como llegan de la API, sin transformaciones            |
| `covertype_cleaned`  | Datos preprocesados por el DAG, con columna `dataset` (`train` / `test`)    |

---

### 4. Notebooks de Entrenamiento

Accesibles desde el servicio **Model Training** en `http://localhost:8888`.

#### `train2.ipynb` — Flujo V1

- Lee datos desde `covertype_cleaned` (ya preprocesados)
- Descarga el preprocesador desde `v1/preprocess/preprocessor.joblib` en MinIO
- Extrae un conjunto de validacion desde el train para ajuste de hiperparametros
- Soporta SVM, Logistic Regression y Random Forest
- Sube el modelo entrenado a `v1/models/{nombre}.joblib` en MinIO

#### `train.ipynb` — Flujo V2

- Lee datos crudos desde `covertype_raw`
- Construye y ajusta su propio pipeline de preprocesamiento
- Divide en train/val/test
- Soporta SVM, Logistic Regression y Random Forest
- Sube el modelo a `v2/models/{nombre}.joblib` y el preprocesador a `v2/preprocess/{nombre}.joblib` en MinIO

---

### 5. MinIO

Almacenamiento de objetos compatible con S3. Organizado por version:

```
covertype-project/
├── v1/
│   ├── preprocess/
│   │   └── preprocessor.joblib       # Preprocesador unico del DAG (Flujo V1)
│   └── models/
│       ├── svm_v1.joblib
│       └── ...
└── v2/
    ├── preprocess/
    │   ├── logistic_regression_v2.joblib
│   │   └── ...                       # Un preprocesador por modelo (Flujo V2)
    └── models/
        ├── logistic_regression_v2.joblib
        └── ...
```

Consola web disponible en `http://localhost:9001`.

---

### 6. Inference API

FastAPI disponible en `http://localhost:8001`. Documentacion interactiva en `http://localhost:8001/docs`.

#### Endpoints V1 — Preprocesador compartido del DAG

| Metodo | Endpoint      | Descripcion                                                                                  |
| ------ | ------------- | -------------------------------------------------------------------------------------------- |
| `GET`  | `/v1/models`  | Lista todos los modelos disponibles en `v1/models/` de MinIO                                 |
| `POST` | `/v1/predict` | Realiza una prediccion usando el preprocesador del DAG y el modelo indicado en el body       |

#### Endpoints V2 — Preprocesador por modelo

| Metodo | Endpoint      | Descripcion                                                                                              |
| ------ | ------------- | -------------------------------------------------------------------------------------------------------- |
| `GET`  | `/v2/models`  | Lista todos los modelos disponibles en `v2/models/` de MinIO                                             |
| `POST` | `/v2/predict` | Realiza una prediccion cargando el preprocesador especifico del modelo desde `v2/preprocess/` en MinIO   |

#### Formato de Request para `/v1/predict` y `/v2/predict`

```json
{
  "model": "svm_v1",
  "data": {
    "elevation": 2596,
    "aspect": 51,
    "slope": 3,
    "horizontal_distance_to_hydrology": 258,
    "vertical_distance_to_hydrology": 0,
    "horizontal_distance_to_roadways": 510,
    "hillshade_9am": 221,
    "hillshade_noon": 232,
    "hillshade_3pm": 148,
    "horizontal_distance_to_fire_points": 6279,
    "wilderness_area": "Rawah",
    "soil_type": "C2702"
  }
}
```

#### Formato de Response

```json
{
  "modelo_utilizado": "svm_v1",
  "prediccion_cover_type": 5
}
```

---

## Dataset: Covertype

El dataset **Forest Covertype** contiene informacion cartografica para predecir el tipo de cobertura forestal (clases 1 a 7).

| Caracteristica                       | Tipo       | Descripcion                                          |
| ------------------------------------ | ---------- | ---------------------------------------------------- |
| `elevation`                          | Numerica   | Elevacion en metros                                  |
| `aspect`                             | Numerica   | Aspecto en grados azimut                             |
| `slope`                              | Numerica   | Pendiente en grados                                  |
| `horizontal_distance_to_hydrology`   | Numerica   | Distancia horizontal a la fuente de agua mas cercana |
| `vertical_distance_to_hydrology`     | Numerica   | Distancia vertical a la fuente de agua mas cercana   |
| `horizontal_distance_to_roadways`    | Numerica   | Distancia horizontal al camino mas cercano           |
| `hillshade_9am`                      | Numerica   | Indice de sombra a las 9am (0-255)                   |
| `hillshade_noon`                     | Numerica   | Indice de sombra al mediodia (0-255)                 |
| `hillshade_3pm`                      | Numerica   | Indice de sombra a las 3pm (0-255)                   |
| `horizontal_distance_to_fire_points` | Numerica   | Distancia horizontal al punto de fuego mas cercano   |
| `wilderness_area`                    | Categorica | Area silvestre: Rawah, Neota, Comanche Peak, Cache la Poudre |
| `soil_type`                          | Categorica | Tipo de suelo (ej. C2702, C3501, ...)                |
| `cover_type`                         | Objetivo   | Tipo de cobertura forestal (1-7)                     |

---

## Tecnologias Utilizadas

- **Apache Airflow** — Orquestacion de pipelines
- **MySQL** — Almacenamiento de datos estructurados
- **MinIO** — Almacenamiento de artefactos ML (modelos y preprocesadores)
- **FastAPI** — API de datos y API de inferencia
- **JupyterLab** — Entorno interactivo de entrenamiento
- **scikit-learn** — Preprocesamiento y modelos ML
- **Docker y Docker Compose** — Contenedorizacion y orquestacion de servicios
- **Python** — Lenguaje principal
