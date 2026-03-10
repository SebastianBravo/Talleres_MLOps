# Proyecto 1 - MLOps: Pipeline de Datos con Airflow

## Descripcion General

Este proyecto implementa un pipeline automatizado de recoleccion, preprocesamiento y almacenamiento de datos del dataset **Covertype** utilizando **Apache Airflow** como orquestador. Los datos se obtienen a traves de una API externa que entrega la informacion en **10 batches**, cambiando cada 5 minutos. El DAG se encarga de recolectar todos los batches, preprocesar los datos y almacenar tanto los datos limpios en **MySQL** como el preprocesador entrenado en **MinIO**.

---

## Estructura del Proyecto

```
Proyecto_1/
├── .env                          # Variables de entorno (credenciales, configuraciones)
├── .env.example                  # Ejemplo de variables de entorno
├── docker-compose.yaml           # Orquestacion de todos los servicios
├── Airflow/
│   ├── Dockerfile                # Imagen personalizada de Airflow
│   ├── requirements.txt          # Dependencias de Python para Airflow
│   ├── config/                   # Configuracion adicional de Airflow
│   ├── dags/
│   │   ├── data_dag.py           # DAG principal: recoleccion y preprocesamiento
│   │   ├── db_utils.py           # Utilidades: conexion a MySQL, MinIO, API, preprocesamiento
│   │   └── train_utils.py        # Utilidades para entrenamiento de modelos
│   ├── logs/                     # Logs generados por las ejecuciones del DAG
│   └── plugins/                  # Plugins personalizados de Airflow
└── data-api/                     # API de datos (solo para pruebas locales)
    ├── main.py                   # Servidor FastAPI que sirve los datos por batches
    ├── generate_data.py          # Script para generar los datos del dataset
    ├── diagram.py                # Diagrama del flujo de la API
    ├── docker-compose.yaml       # Docker Compose de la API de pruebas
    ├── Dockerfile                # Imagen Docker de la API
    ├── requirements.txt          # Dependencias de la API
    └── data/
        ├── covertype.csv         # Dataset Covertype completo
        └── timestamps.json       # Control de timestamps para los batches
```

---

## Arquitectura de Servicios

| Servicio           | Descripcion                                                                                             | Puerto          |
| ------------------ | ------------------------------------------------------------------------------------------------------- | --------------- |
| **Apache Airflow** | Orquestador de tareas. Ejecuta el DAG de recoleccion y preprocesamiento                                 | `8080`          |
| **MySQL**          | Base de datos relacional. Almacena datos crudos (`covertype_raw`) y preprocesados (`covertype_cleaned`) | `3306`          |
| **MinIO**          | Almacenamiento de objetos compatible con S3. Guarda el preprocesador (`preprocessor.joblib`)            | `9000` / `9001` |
| **API de Datos**   | API FastAPI que simula la fuente externa de datos. **Solo para pruebas locales**                        | `8000`          |

> **Nota:** El servicio `data-api` es unicamente para pruebas locales. En produccion, el DAG se conecta a la API externa del profesor en `http://10.43.101.94:8080`.

---

## DAG: `data_dag`

### Descripcion

El DAG `data_dag` se ejecuta automaticamente cada **5 minutos** (configurable a 20 segundos para pruebas) y realiza las siguientes tareas:

### Flujo de Tareas

```
create_tables -> load_raw_data -> check_should_preprocess -> preprocess_data -> pause_dag
```

| Tarea                               | Descripcion                                                                                                                                                                                              |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `t1: create_tables`                 | Verifica si las tablas existen en MySQL. Si no, crea la tabla `covertype_raw` con el esquema apropiado para el dataset Covertype                                                                         |
| `t2: load_raw_data`                 | Realiza una peticion GET a la API para obtener una porcion aleatoria del batch actual. Inserta los datos en la tabla `covertype_raw` y publica banderas en XCom                                          |
| `t2_check: check_should_preprocess` | **ShortCircuitOperator** que decide si se debe proceder al preprocesamiento. Solo permite continuar cuando se han recolectado **todos los batches** y el preprocesamiento no se ha realizado previamente |
| `t3: preprocess_data`               | Limpia los datos (elimina nulos y duplicados), aplica transformaciones (escalado, one-hot encoding), divide en train/test y almacena en `covertype_cleaned`. Sube el preprocesador a MinIO               |
| `t4: pause_dag`                     | **Pausa el DAG automaticamente** para detener futuras ejecuciones programadas una vez completado todo el proceso                                                                                         |

### Logica de Control (ShortCircuitOperator)

| Escenario                                      | `new_data_loaded` | `all_data_collected` | Ejecuta `t3`? |
| ---------------------------------------------- | :---------------: | :------------------: | :-----------: |
| Se obtuvieron datos, faltan batches            |      `True`       |       `False`        | No - Omitido  |
| API indica que se recolecto todo (primera vez) |      `False`      |        `True`        | Si - Ejecuta  |
| Todo recolectado y ya preprocesado             |      `False`      |        `True`        | No - Omitido  |

### Preprocesamiento

El paso de preprocesamiento realiza:

1. **Limpieza:** Eliminacion de filas con valores nulos y duplicados
2. **Division:** Separacion en conjuntos de entrenamiento (80%) y prueba (20%) con estratificacion
3. **Pipeline numerico:** Imputacion por mediana + Escalado estandar (`StandardScaler`)
4. **Pipeline categorico:** Imputacion por moda + Codificacion one-hot (`OneHotEncoder`)
5. **Almacenamiento:** Datos limpios en MySQL (`covertype_cleaned`) y preprocesador en MinIO

---

## Evidencias de Funcionamiento

### 1. Creacion de Tablas

> Captura que muestra la tarea `create_tables` creando la tabla `covertype_raw` en MySQL.

<!-- ![Creacion de tablas](images/01_create_tables.png) -->

### 2. Carga de Datos por Batches

> Captura que muestra la tarea `load_raw_data` obteniendo datos de la API e insertandolos en MySQL.

<!-- ![Carga de datos](images/02_load_raw_data.png) -->

### 3. Skip del Preprocesamiento Mientras se Recolectan Datos

> Captura que muestra como el `ShortCircuitOperator` omite las tareas `preprocess_data` y `pause_dag` mientras aun se estan recolectando batches.

<!-- ![Skip preprocessing](images/03_skip_preprocess.png) -->

### 4. Ejecucion del Preprocesamiento

> Captura que muestra la tarea `preprocess_data` ejecutandose una vez que todos los batches fueron recolectados.

<!-- ![Preprocesamiento](images/04_preprocess_data.png) -->

### 5. Carga del Preprocesador a MinIO

> Captura que muestra el preprocesador (`preprocessor.joblib`) almacenado en el bucket de MinIO.

<!-- ![MinIO upload](images/05_minio_preprocessor.png) -->

### 6. Datos en MySQL (Tabla Raw)

> Captura que muestra los datos crudos almacenados en la tabla `covertype_raw`.

<!-- ![Tabla raw](images/06_mysql_raw.png) -->

### 7. Datos en MySQL (Tabla Cleaned)

> Captura que muestra los datos preprocesados en la tabla `covertype_cleaned` con las columnas transformadas.

<!-- ![Tabla cleaned](images/07_mysql_cleaned.png) -->

### 8. DAG Pausado Automaticamente

> Captura que muestra como el DAG se pausa automaticamente tras completar el preprocesamiento.

<!-- ![DAG pausado](images/08_dag_paused.png) -->

### 9. Vista General del DAG en Airflow

> Captura de la vista de grafo del DAG mostrando el flujo completo de tareas.

<!-- ![Vista DAG](images/09_dag_graph_view.png) -->

### 10. Historial de Ejecuciones

> Captura del historial de ejecuciones mostrando las multiples corridas de recoleccion y la ejecucion final con preprocesamiento.

<!-- ![Historial](images/10_dag_runs_history.png) -->

---

## Como Ejecutar el Proyecto

### Prerrequisitos

- [Docker](https://docs.docker.com/get-docker/) y [Docker Compose](https://docs.docker.com/compose/install/) instalados
- Puerto `8080` (Airflow), `3306` (MySQL), `9000`/`9001` (MinIO) disponibles

### 1. Configurar Variables de Entorno

Copiar el archivo de ejemplo y completar con los valores correspondientes:

```bash
cp .env.example .env
```

Editar el archivo `.env` con las credenciales y configuraciones necesarias:

```env
# MySQL
MYSQL_HOST=mysql
MYSQL_USER=root
MYSQL_PASSWORD=<tu_contraseña>
MYSQL_DATABASE=covertype_db

# MinIO
MINIO_ENDPOINT=http://minio:9000
AWS_ACCESS_KEY_ID=<tu_access_key>
AWS_SECRET_ACCESS_KEY=<tu_secret_key>
AWS_DEFAULT_REGION=us-east-1

# API de Datos
API_URL=http://api-data:8000        # Para pruebas locales
# API_URL=http://10.43.101.94:8080  # Para la API del profesor
API_GROUP_NUMBER=<tu_numero_de_grupo>
```

### 2. Construir y Levantar los Servicios

```bash
cd Proyecto_1
docker compose up --build -d
```

### 3. (Opcional) Levantar la API de Datos para Pruebas Locales

Si se desea probar con la API de datos local en lugar de la API del profesor:

```bash
cd data-api
docker compose up --build -d
```

### 4. Acceder a los Servicios

| Servicio          | URL                                            | Credenciales           |
| ----------------- | ---------------------------------------------- | ---------------------- |
| **Airflow**       | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow`  |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | Configuradas en `.env` |

### 5. Activar el DAG

1. Abrir la interfaz web de Airflow en `http://localhost:8080`
2. Buscar el DAG `data_dag`
3. Activar el toggle para habilitar el DAG (despausar)
4. El DAG comenzara a ejecutarse automaticamente segun el intervalo configurado

### 6. Monitorear la Ejecucion

- En la vista de **Grid** o **Graph** del DAG se puede observar el progreso de cada ejecucion
- Las tareas `preprocess_data` y `pause_dag` apareceran en estado **skipped** (rosado) hasta que se recolecten todos los batches
- Una vez completado el proceso, el DAG se pausara automaticamente

### 7. Detener los Servicios

```bash
docker compose down
```

Para eliminar tambien los volumenes (datos persistentes):

```bash
docker compose down -v
```

---

## Dataset: Covertype

El dataset **Forest Covertype** contiene informacion cartografica utilizada para predecir el tipo de cobertura forestal. Incluye las siguientes caracteristicas:

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
| `wilderness_area`                    | Categorica | Area silvestre designada                             |
| `soil_type`                          | Categorica | Tipo de suelo                                        |
| `cover_type`                         | Objetivo   | Tipo de cobertura forestal (1-7)                     |

---

## Tecnologias Utilizadas

- **Apache Airflow** - Orquestacion de pipelines
- **MySQL** - Almacenamiento de datos estructurados
- **MinIO** - Almacenamiento de objetos (artefactos ML)
- **Docker y Docker Compose** - Contenedorizacion y orquestacion de servicios
- **scikit-learn** - Preprocesamiento de datos (StandardScaler, OneHotEncoder, SimpleImputer)
- **FastAPI** - API de datos (pruebas locales)
- **Python** - Lenguaje principal
