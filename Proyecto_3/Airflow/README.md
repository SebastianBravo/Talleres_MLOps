# Airflow — DAG `real_estate_mlops`

Este documento describe en detalle el DAG principal del proyecto, su flujo, las responsabilidades de cada tarea, la lógica de decisión en cada bifurcación, el esquema de persistencia de datos y la integración con MLflow.

---

## Tabla de contenido

1. [Visión general](#1-visión-general)
2. [Diagrama de flujo](#2-diagrama-de-flujo)
3. [Descripción de cada tarea](#3-descripción-de-cada-tarea)
4. [Lógica de decisión — Entrenamiento](#4-lógica-de-decisión--entrenamiento)
5. [Lógica de decisión — Promoción](#5-lógica-de-decisión--promoción)
6. [Persistencia de datos](#6-persistencia-de-datos)
7. [Integración con MLflow](#7-integración-con-mlflow)
8. [Manejo de errores y resiliencia](#8-manejo-de-errores-y-resiliencia)
9. [Variables de entorno y configuración](#9-variables-de-entorno-y-configuración)

---

## 1. Visión general

**DAG ID:** `real_estate_mlops`  
**Archivo:** `dags/data_dag.py`  
**Schedule:** cada 2 minutos (`timedelta(minutes=2)`)  
**Max active runs:** 1 (evita solapamiento entre ejecuciones)  
**Catchup:** deshabilitado (no reprocesa ejecuciones pasadas)

El DAG implementa un pipeline MLOps completo e incremental para predicción de precios de propiedades inmobiliarias. En cada ejecución:

1. Solicita un nuevo lote de datos a la API externa.
2. Almacena los datos crudos sin modificación.
3. Valida el esquema, la calidad y detecta cambios en distribuciones y categorías.
4. Preprocesa todos los datos acumulados y los almacena en la tabla limpia.
5. Decide técnicamente si es necesario entrenar un modelo nuevo.
6. Si entrena, compara el modelo candidato contra el productivo.
7. Promueve o rechaza el candidato según reglas explícitas.
8. Si el modelo es promovido, notifica a la API de inferencia para que recargue el modelo inmediatamente.
9. Registra el resultado completo en la tabla de auditoría.

El sistema **nunca borra datos crudos**. La tabla limpia es append-only versionada. Cada decisión queda registrada con su justificación técnica.

---

## 2. Diagrama de flujo

```
start
  │
  ▼
create_tables
  │
  ▼
fetch_batch_from_api
  │  (AirflowSkipException si no hay más lotes)
  ▼
store_raw_batch
  │
  ▼
validate_schema
  │  (ValueError si faltan columnas críticas)
  ▼
validate_data_quality
  │  (ValueError si quality_score < 0.5)
  ▼
detect_new_categories
  │
  ▼
detect_data_drift
  │
  ▼
preprocess_data
  │
  ▼
decide_training  ◄── BranchPythonOperator
  │                   Evalúa 4 criterios técnicos
  │
  ├── [train] ──► train_candidate_model
  │                    │
  │               evaluate_candidate_model
  │                    │
  │               register_candidate_in_mlflow
  │                    │
  │               compare_with_production
  │                    │
  │               decide_promotion  ◄── BranchPythonOperator
  │                    │
  │                    ├── [promote] ──► promote_model
  │                    │                     │
  │                    │               reload_inference_api ──►─┐
  │                    │                                        │
  │                    └── [reject]  ──► reject_model  ────────►─┤
  │                                                              │
  └── [skip] ──► skip_training ────────────────────────────────►─┤
                                                                  │
                                                          notify_or_log_result
                                                                  │
                                                                end
```

---

## 3. Descripción de cada tarea

### `start`
**Tipo:** `EmptyOperator`

Punto de entrada del DAG. No ejecuta lógica; su función es hacer el grafo visualmente claro y proporcionar un punto de referencia temporal de inicio de ejecución en la interfaz de Airflow.

---

### `create_tables`
**Tipo:** `PythonOperator`  
**Callable:** `_create_tables`

Garantiza que todas las tablas necesarias existen en la base de datos antes de que cualquier otra tarea intente escribir en ellas. Es idempotente gracias a `CREATE TABLE IF NOT EXISTS`.

Tablas que crea:

| Tabla | Propósito |
|---|---|
| `property_raw` | Almacén de datos crudos recibidos de la API |
| `batch_audit` | Registro de auditoría por ejecución |
| `inference_logs` | Log de cada petición de inferencia (escrita por FastAPI) |

La tabla `property_clean` **no** se crea aquí porque sus columnas son dinámicas (dependen del output del preprocesador). Se crea en `preprocess_data`.

---

### `fetch_batch_from_api`
**Tipo:** `PythonOperator` (retries=3)  
**Callable:** `_fetch_batch`  
**Archivo:** `utils/dataset_io.py`

Solicita el siguiente lote disponible a la API externa:

```
GET {DATA_API_URL}/data?group_number={GROUP_ID}
```

La API mantiene un cursor interno por grupo — cada petición avanza automáticamente al siguiente lote sin necesidad de pasar un identificador. El cliente HTTP utiliza reintentos automáticos para códigos 429, 500, 502, 503 y 504 con backoff.

**Casos de respuesta:**

- **Lote disponible:** La API devuelve `{"data": [...], "batch_number": N}`. Los registros se serializan a un archivo temporal en `/tmp/airflow_batches/{run_id}_batch.json` y el `batch_id` se publica en XCom.
- **Sin más lotes:** La API devuelve HTTP 400 con `{"detail": "..."}`. El callable lanza `AirflowSkipException`, que marca la ejecución completa como *skipped* (no como fallida). Esto es el comportamiento correcto: no es un error, simplemente no hay más datos disponibles.
- **Error de red o HTTP inesperado:** Se lanza `RuntimeError`, la tarea falla y se activan los reintentos.

**XCom publicado:**

| Clave | Descripción |
|---|---|
| `batch_id` | Número de lote devuelto por la API |
| `batch_file` | Ruta al archivo temporal con los registros |
| `records_count` | Número de registros en el lote |

---

### `store_raw_batch`
**Tipo:** `PythonOperator`  
**Callable:** `_store_raw_batch`  
**Archivo:** `utils/ingestion.py`

Persiste el lote en la tabla `property_raw` exactamente como llegó de la API, sin ninguna transformación. También crea la fila inicial en `batch_audit`.

**Diseño de la tabla `property_raw`:**

```sql
CREATE TABLE property_raw (
    id               SERIAL PRIMARY KEY,
    batch_id         INTEGER NOT NULL,
    batch_record_id  INTEGER,
    load_timestamp   TIMESTAMP NOT NULL DEFAULT NOW(),
    record_status    VARCHAR(16) DEFAULT 'active',
    row_hash         VARCHAR(64),    -- MD5 de los campos de negocio
    brokered_by      TEXT,
    status           TEXT,
    price            DOUBLE PRECISION,
    bed              DOUBLE PRECISION,
    bath             DOUBLE PRECISION,
    acre_lot         DOUBLE PRECISION,
    street           TEXT,
    city             TEXT,
    state            TEXT,
    zip_code         TEXT,
    house_size       DOUBLE PRECISION,
    prev_sold_date   TEXT
);
```

Cada fila recibe un `row_hash` calculado como MD5 de todos los campos de negocio concatenados. Este hash permite detectar duplicados exactos entre lotes en el futuro si se implementa esa validación.

El campo `batch_id` es la clave de trazabilidad: permite reconstruir qué datos estaban disponibles en cualquier ejecución histórica del DAG.

---

### `validate_schema`
**Tipo:** `PythonOperator`  
**Callable:** `_validate_schema`  
**Archivo:** `utils/validation.py`

Verifica que el lote recibido contiene exactamente las columnas esperadas:

```python
EXPECTED_COLUMNS = {
    "brokered_by", "status", "price", "bed", "bath",
    "acre_lot", "street", "city", "state", "zip_code",
    "house_size", "prev_sold_date",
}
```

- **Columnas faltantes:** Se considera un error crítico. La tarea falla con `ValueError` y el pipeline se detiene. El registro en `batch_audit.schema_valid = False` y `schema_issues` contiene la lista de columnas ausentes.
- **Columnas extra:** Se registran como advertencia en los logs pero **no** interrumpen el pipeline. El preprocesador ignorará columnas no esperadas (`remainder="drop"` en `ColumnTransformer`).

El resultado se guarda en `batch_audit` y se publica en XCom como `schema_result`.

---

### `validate_data_quality`
**Tipo:** `PythonOperator`  
**Callable:** `_validate_data_quality`  
**Archivo:** `utils/validation.py`

Evalúa la calidad interna del lote sobre cinco dimensiones:

| Dimensión | Criterio | Penalización |
|---|---|---|
| Nulos por columna | Tasa > 50% en cualquier columna | -0.15 por columna |
| Duplicados | Filas completamente duplicadas | -0.15 |
| Rangos inválidos | Valores fuera del rango definido | -0.15 por columna |
| Target inválido | > 10% de precios nulos o ≤ 0 | -0.15 |
| Tamaño mínimo | Lote con menos de 10 registros | -0.15 |

**Rangos válidos definidos:**

```python
VALID_RANGES = {
    "price":      (0, 1_000_000_000),
    "bed":        (0, 100),
    "bath":       (0, 100),
    "acre_lot":   (0, 100_000),
    "house_size": (0, 1_000_000),
}
```

El `quality_score` se calcula como `max(0, 1.0 - n_issues × 0.15)`. Si el score cae por debajo de 0.5 **y** existen issues, la tarea falla. Por encima de 0.5 el pipeline continúa aunque haya issues menores (se registran en el audit).

El resultado completo (score, issues, tasas de nulos, violaciones de rango, duplicados) se almacena en `batch_audit.quality_issues` como JSONB.

---

### `detect_new_categories`
**Tipo:** `PythonOperator`  
**Callable:** `_detect_new_categories`  
**Archivo:** `utils/validation.py`

Compara los valores categóricos del lote actual contra los valores históricos de lotes **anteriores** (filtra con `batch_id < actual` para evitar la auto-comparación). Las columnas monitoreadas son `status`, `city` y `state`.

Una categoría nueva se considera **significativa** cuando su frecuencia dentro del lote actual es ≥ 1%. Categorías que aparecen con frecuencia inferior son ruido estadístico y no justifican reentrenamiento.

**Resultado:**
```json
{
  "new_categories": {
    "city": {
      "new_values": ["Atlanta", "Denver"],
      "count": 45,
      "frequency": 0.035
    }
  },
  "significant_new": true
}
```

Este resultado alimenta directamente al criterio 3 de `decide_training`. También se almacena en `batch_audit.new_categories_detected`.

**Nota:** Las columnas de alta cardinalidad (`brokered_by`, `street`, `zip_code`) no se monitorizan aquí por razones prácticas — tienen miles de valores únicos y sus cambios son esperados en cada lote. El preprocesador las maneja con `OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)`, que asigna -1 a categorías desconocidas sin fallar.

---

### `detect_data_drift`
**Tipo:** `PythonOperator`  
**Callable:** `_detect_data_drift`  
**Archivo:** `utils/drift_detection.py`

Aplica el test de Kolmogorov-Smirnov de dos muestras para comparar la distribución del lote actual contra los datos de lotes anteriores (excluye el lote actual del histórico con `WHERE batch_id < actual`).

**Columnas monitoreadas:** `price`, `house_size`, `bed`, `bath`, `acre_lot`

**Parámetros del test:**
- **Umbral p-value:** 0.05. Si p < 0.05, se rechaza la hipótesis nula de que ambas muestras provienen de la misma distribución.
- **Mínimo de filas históricas:** 50. Con menos datos, el test carece de potencia estadística y se omite.
- **Mínimo por columna:** 10 observaciones no nulas tanto en el lote como en el histórico.

**Resultado:**
```json
{
  "drift_detected": true,
  "drifted_columns": ["price", "house_size"],
  "reason": "KS test drift in: ['price', 'house_size']",
  "details": {
    "price":      {"ks_statistic": 0.142, "p_value": 0.003, "drift_detected": true},
    "house_size": {"ks_statistic": 0.098, "p_value": 0.031, "drift_detected": true},
    "bed":        {"ks_statistic": 0.021, "p_value": 0.820, "drift_detected": false}
  }
}
```

El resultado se almacena en `batch_audit.drift_detected` y `batch_audit.drift_details`.

---

### `preprocess_data`
**Tipo:** `PythonOperator`  
**Callable:** `_preprocess_data`  
**Archivo:** `utils/preprocess.py`

Es la tarea más compleja del pipeline. Transforma **todos los datos acumulados** (todos los batches hasta el actual) y los almacena en la tabla limpia versionada.

**Pipeline de transformación:**

```
raw data (todos los batches)
    │
    ├── Parseo de fechas (prev_sold_date → year, month, days_ago)
    ├── Coerción de numéricos (errores → NaN)
    ├── Drop de filas sin precio (target obligatorio)
    │
    ├── Split determinístico 70/15/15 (MD5 hash del id de fila, módulo 20)
    │      → estable entre reruns: la misma fila siempre cae en el mismo split
    │      → buckets 0-13 → train | 14-16 → val | 17-19 → test
    │
    ├── Ajuste del preprocesador (solo sobre datos de train)
    │      ├── Numéricos: SimpleImputer(median) + StandardScaler
    │      ├── Alta cardinalidad: SimpleImputer(constant="unknown") + OrdinalEncoder
    │      └── Baja cardinalidad: SimpleImputer(most_frequent) + OrdinalEncoder
    │
    └── Transformación de train, val y test
```

**Columnas de entrada → salida:**

| Grupo | Columnas de entrada | Columnas de salida |
|---|---|---|
| Numéricas (4) | bed, bath, acre_lot, house_size | num__bed, num__bath, num__acre_lot, num__house_size |
| Fecha (3) | prev_sold_date | prev_sold_year, prev_sold_month, prev_sold_days_ago |
| Alta cardinalidad (5) | brokered_by, street, city, state, zip_code | high_cat__brokered_by, ... |
| Baja cardinalidad (1) | status | low_cat__status |

**Total: 13 columnas de features** (estable entre batches independientemente de las categorías nuevas).

El preprocesador ajustado se serializa y sube a MinIO:
```
s3://real-estate-project/preprocess/batch_{N}_{YYYYMMDDHHMMSS}/preprocessor.joblib
```

**Versioning de la tabla limpia:**

La tabla `property_clean` es **append-only**. Nunca se borra. Cada ejecución de `preprocess_data` inserta un conjunto completo de filas (todos los datos históricos re-procesados) etiquetadas con la nueva `preprocessor_version`:

```
preprocessor_version = "batch_{batch_id}_{YYYYMMDDHHMMSS}"
```

El entrenamiento posterior siempre filtra con `WHERE preprocessor_version = '<última_versión>'`, garantizando coherencia de features.

```
property_clean (estado tras 3 batches):
┌──────────┬────────────────────────┬──────────┬─────────┐
│ batch_id │  preprocessor_version  │ dataset  │  price  │
├──────────┼────────────────────────┼──────────┼─────────┤
│    1     │  batch_1_202605280900  │  train   │ 350000  │  ← generación v1 (histórica)
│    1     │  batch_2_202605281500  │  train   │ 350000  │  ← regenerada con v2
│    2     │  batch_2_202605281500  │  train   │ 410000  │
│    1     │  batch_3_202605290900  │  train   │ 350000  │  ← regenerada con v3
│    2     │  batch_3_202605290900  │  train   │ 410000  │  ← regenerada con v3
│    3     │  batch_3_202605290900  │  test    │ 295000  │  ← nueva
└──────────┴────────────────────────┴──────────┴─────────┘
                                    ↑ Estas filas usa el entrenamiento
```

**Resultado publicado en XCom (`preprocess_result`):**
```json
{
  "preprocessor_version": "batch_3_20260529090015",
  "records_processed": 1500,
  "train_count": 1050,
  "val_count": 225,
  "test_count": 225,
  "feature_count": 13
}
```

---

### `decide_training`
**Tipo:** `BranchPythonOperator`  
**Callable:** `_decide_training`  
**Archivo:** `utils/training_decision.py`

Evalúa cuatro criterios técnicos independientes y decide si se debe entrenar un modelo nuevo. Cualquiera de los criterios es suficiente para disparar el entrenamiento.

Ver [Sección 4](#4-lógica-de-decisión--entrenamiento) para el detalle completo.

**Retorna:** `"train_candidate_model"` o `"skip_training"`

---

### `skip_training`
**Tipo:** `PythonOperator`  
**Callable:** `_skip_training`

Se ejecuta cuando `decide_training` decide no entrenar. Actualiza `batch_audit.execution_status = "skipped_training"` y registra las razones técnicas del rechazo.

---

### `train_candidate_model`
**Tipo:** `PythonOperator` (execution_timeout=2h)  
**Callable:** `_train_candidate_model`  
**Archivo:** `utils/training.py`

Entrena el modelo usando los datos limpios de la última `preprocessor_version`. La tarea:

1. Lee desde `property_clean WHERE preprocessor_version = '<última>'`.
2. Extrae el rango de batches representados en los datos (para trazabilidad).
3. Crea o restaura el experimento MLflow `real-estate-price`.
4. Registra en el **run padre** los tags de lineage y los params de preprocesamiento.
5. Para cada combinación de hiperparámetros en `PARAM_GRID`:
   - Abre un **run anidado** en MLflow.
   - Entrena `RandomForestRegressor` sobre el split de train (70%).
   - Calcula métricas en train, validación y test (MAE, RMSE, R², MAPE).
   - Genera gráficos de desempeño y los registra como artefactos.
   - Registra feature importances como artefacto JSON.
   - Registra el modelo como sklearn Pipeline (preprocesador + modelo).
6. Selecciona el mejor config según `test_mae` mínimo.
7. Retorna el `run_id` del mejor run y su MAE en XCom.

**Tags registrados en el run padre Y en cada run hijo:**

| Tag | Valor | Descripción |
|---|---|---|
| `git_commit` | SHA del commit (12 chars) | Versión exacta del código que generó el modelo |
| `batch_id` | Número del batch actual | Batch que disparó el entrenamiento |
| `training_batches` | `[1, 2, 3]` | Lista completa de batches incluidos en el training set |
| `model_type` | `RandomForestRegressor` | Familia de algoritmo |
| `primary_metric` | `test_mae` | Métrica de selección del mejor modelo |
| `problem_type` | `regression` | Tipo de problema |
| `training_reasons` | JSON | Criterios técnicos que dispararon el entrenamiento |
| `preprocessor_version` | `batch_N_YYYYMMDDHHMMSS` | Versión del preprocesador usado |

**Params del run padre (configuración de preprocesamiento):**

| Param | Valor |
|---|---|
| `batch_count` | Número de batches incluidos en el training |
| `batch_range` | `"1-3"` (primer y último batch) |
| `preprocess_numeric_imputer` | `"median"` |
| `preprocess_scaler` | `"StandardScaler"` |
| `preprocess_cat_encoder` | `"OrdinalEncoder(unknown=-1)"` |
| `preprocess_split` | `"70/15/15"` |
| `feature_count` | Número de features del modelo |

**Métricas registradas por run hijo:**

| Prefijo | Split | Métricas |
|---|---|---|
| `train_` | train (70%) | mae, rmse, r2, mape |
| `val_` | validación (15%) | mae, rmse, r2, mape |
| `test_` | test (15%) | mae, rmse, r2, mape |

**Hiperparámetros actuales (`PARAM_GRID`):**
```python
{
    "n_estimators": [100],
    "max_depth": [15],
    "random_state": [42],
}
```
> Para ampliar la búsqueda, descomentar el grid completo en `training.py`.

---

### `evaluate_candidate_model`
**Tipo:** `PythonOperator`  
**Callable:** `_evaluate_candidate_model`

Valida que el run candidato en MLflow tiene las métricas mínimas requeridas antes de registrarlo. Las métricas requeridas son: `test_mae`, `test_rmse`, `test_r2`.

Si alguna falta (porque el training falló silenciosamente o hubo un problema de logging), la tarea falla con `ValueError` antes de que se intente registrar un modelo incompleto.

Publica las métricas de test filtradas en XCom como `candidate_metrics`.

---

### `register_candidate_in_mlflow`
**Tipo:** `PythonOperator`  
**Callable:** `_register_candidate_in_mlflow`  
**Archivo:** `utils/training.py → register_candidate()`

Crea una versión del modelo en el **Model Registry** de MLflow a partir del run candidato:

```python
client.create_model_version(
    name="real-estate-price-model",
    source=f"runs:/{run_id}/model",
    run_id=run_id,
)
```

En este punto el modelo existe en el registry pero **sin ningún alias**. No es productivo aún. La decisión de promoverlo viene en tareas posteriores.

Publica el número de versión en XCom como `model_version` y lo registra en `batch_audit.model_version`.

---

### `compare_with_production`
**Tipo:** `PythonOperator`  
**Callable:** `_compare_with_production`  
**Archivo:** `utils/model_comparison.py`

Compara las métricas del candidato contra el modelo que actualmente tiene el alias `production` en MLflow. Las métricas se obtienen directamente de los runs de MLflow (no se re-evalúan sobre datos nuevos).

Ver [Sección 5](#5-lógica-de-decisión--promoción) para los criterios exactos.

Registra en `batch_audit`:
- `candidate_mae`, `candidate_rmse`
- `production_mae`, `production_rmse`

Publica `comparison_result` en XCom.

---

### `decide_promotion`
**Tipo:** `BranchPythonOperator`  
**Callable:** `_decide_promotion`

Lee `comparison_result.should_promote` del XCom de `compare_with_production`.

**Retorna:** `"promote_model"` o `"reject_model"`

---

### `promote_model`
**Tipo:** `PythonOperator`  
**Callable:** `_promote_model`  
**Archivo:** `utils/model_comparison.py → promote_to_production()`

Asigna el alias `production` a la versión ya registrada del candidato:

```python
client.set_registered_model_alias("real-estate-price-model", "production", model_version)
```

Usa el `model_version` del XCom de `register_candidate_in_mlflow` — **no crea una versión nueva**. Esto es crítico: un modelo promovido existe exactamente una vez en el registry.

Registra `batch_audit.model_promoted = True` y `promotion_reason`.

---

### `reload_inference_api`
**Tipo:** `PythonOperator`  
**Callable:** `_reload_inference_api`  
**Archivo:** `utils/inference_api.py`

Notifica a la API de inferencia que debe recargar el modelo productivo desde MLflow. Esta tarea **solo se ejecuta si `promote_model` tuvo éxito** — su posición en el grafo es la guardia: si la tarea corre, es porque hubo una nueva promoción.

Internamente llama a `POST {INFERENCE_API_URL}/reload`. La API consulta MLflow por el alias `production` y carga el nuevo modelo en memoria sin necesidad de reiniciar el contenedor.

```python
response = reload_inference_api()
# → {"status": "Modelo recargado", "previous_version": "3", "current_version": "4", ...}
```

**Por qué esta tarea y no un reload automático en la API:**

La API podría consultar MLflow periódicamente por cambios de alias (polling). Sin embargo, eso introduce latencia variable entre la promoción y la disponibilidad del nuevo modelo. Con esta tarea, el modelo queda disponible en la API **en la misma ejecución del DAG que lo promovió**, sin delay adicional.

**Comportamiento ante fallo:**

Si la API está caída o responde con error, la tarea falla y activa `on_failure_callback`, registrando el batch como `failed` en auditoría. Esto es intencionado: un modelo promovido pero no recargado en la API es un estado inconsistente que merece atención.

---

### `reject_model`
**Tipo:** `PythonOperator`  
**Callable:** `_reject_model`

Registra la razón del rechazo en `batch_audit.model_promoted = False` y `promotion_reason`. El modelo candidato permanece en el registry como experimento (disponible para análisis en MLflow UI) pero el alias `production` no se modifica.

---

### `notify_or_log_result`
**Tipo:** `PythonOperator`  
**Trigger Rule:** `NONE_FAILED_MIN_ONE_SUCCESS`  
**Callable:** `_notify_or_log_result`

Punto de convergencia de las tres ramas posibles (skip, promote+reload, reject). Se ejecuta si al menos una rama tuvo éxito y ninguna falló.

Marca el batch como completado: `batch_audit.execution_status = "success"` y `completed_at = NOW()`.

El `trigger_rule = NONE_FAILED_MIN_ONE_SUCCESS` permite que esta tarea corra cuando las tareas no activas están en estado *skipped* (comportamiento de `BranchPythonOperator`), no solo cuando todas están en éxito.

---

### `end`
**Tipo:** `EmptyOperator`  
**Trigger Rule:** `NONE_FAILED_MIN_ONE_SUCCESS`

Punto de cierre visual del DAG. Mismo trigger rule que `notify_or_log_result` para tolerar las ramas skipped.

---

## 4. Lógica de decisión — Entrenamiento

La función `should_train()` en `utils/training_decision.py` sigue el siguiente orden de evaluación:

```
┌─────────────────────────────────────────────────────────┐
│  Criterio 1: ¿existe modelo productivo?                 │
│  NO → marcar trigger=True, continuar al gate            │
│  SÍ → continuar al gate                                 │
└────────────────────────┬────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────┐
│  Gate: ¿batch >= MIN_BATCH_RATIO del histórico?         │
│  NO → retornar (trigger actual, razón del bloqueo)      │
│       Los criterios 2-4 no se evalúan.                  │
│  SÍ → continuar a criterios 2-4                         │
└────────────────────────┬────────────────────────────────┘
                         ▼
              Criterios 2, 3 y 4 (cualquiera
              es suficiente para trigger=True)
```

### Criterio 1 — Sin modelo productivo

```python
if not _production_model_exists(registered_model_name):
    trigger = True
```

Se evalúa **antes del gate**. Si no existe ninguna versión con el alias `production` en MLflow, el sistema siempre entrena para establecer la línea base, sin importar el tamaño del batch. Esto ocurre obligatoriamente en el primer batch procesado.

---

### Gate — Tamaño mínimo del batch

```python
MIN_BATCH_RATIO = 0.05  # configurable via MIN_BATCH_RATIO env var

ratio = new_batch_count / historical_count
if ratio < MIN_BATCH_RATIO:
    # bloquear criterios 2-4, no entrenar
```

**Por qué existe este gate:**

El test de Kolmogorov-Smirnov (criterio 4) tiene una propiedad problemática: su potencia estadística crece con el tamaño de la muestra de referencia. Con 400.000 registros históricos, diferencias de distribución del orden de 0.1% en `house_size` producen p < 0.05, disparando el reentrenamiento por un cambio que no tiene ningún impacto práctico en el modelo.

Sin este gate, un batch de 4.000 registros sobre 400.000 acumulados (1% del histórico) puede activar drift, nuevas categorías o volumen, forzando un reentrenamiento costoso que con seguridad producirá un modelo prácticamente idéntico al actual.

**Comportamiento:**

| Histórico | Nuevo batch | Ratio | ¿Gate pasa? |
|---|---|---|---|
| 0 | 5.000 | — | ✅ Siempre (primer batch) |
| 10.000 | 600 | 6% | ✅ ≥ 5% |
| 10.000 | 400 | 4% | ❌ < 5% |
| 400.000 | 4.000 | 1% | ❌ < 5% |
| 400.000 | 22.000 | 5.5% | ✅ ≥ 5% |

Cuando el gate bloquea, la razón queda registrada en `batch_audit.training_reasons` con el ratio exacto y el umbral requerido. El criterio 1 (sin modelo productivo) **siempre** puede forzar entrenamiento aunque el gate falle.

---

### Criterio 2 — Incremento de volumen

```python
ratio = new_batch_count / historical_count
if ratio >= 0.10:  # umbral: 10%
    trigger = True
```

Si el lote representa al menos el 10% del histórico, hay suficiente información nueva para que el reentrenamiento sea significativo. Este criterio solo se evalúa si el gate pasó (ratio ≥ 5%), de modo que en la práctica el umbral efectivo de volumen es 10%.

**Ejemplo:**
- Histórico: 10.000 registros
- Lote actual: 1.200 → ratio 12% → gate pasa (≥5%) y volumen dispara (≥10%) ✅
- Lote actual: 600 → ratio 6% → gate pasa (≥5%) pero volumen no dispara (6% < 10%)
- Lote actual: 400 → ratio 4% → gate bloquea, volumen no se evalúa ❌

### Criterio 3 — Nuevas categorías significativas

```python
if new_categories_result.get("significant_new", False):
    trigger = True
```

Si `detect_new_categories` encontró categorías ausentes en el histórico con frecuencia ≥ 1% en el lote actual, el OrdinalEncoder debe re-aprender el espacio categórico para representarlas correctamente (en lugar de asignarles -1 indefinidamente).

Solo se evalúa si el gate pasó. Si el batch es pequeño, la presencia de nuevas categorías tampoco justifica reentrenamiento — el modelo las codificará como -1 con impacto mínimo hasta que llegue suficiente volumen.

### Criterio 4 — Drift de distribución

```python
if drift_result.get("drift_detected", False):
    trigger = True
```

Si el test KS detectó que alguna variable numérica clave (`price`, `house_size`, `bed`, `bath`, `acre_lot`) cambió significativamente de distribución (p < 0.05). Solo se evalúa si el gate pasó, evitando falsos positivos estadísticos cuando el dataset histórico es muy grande.

### Registro de decisión

Independientemente del resultado, `batch_audit.should_train` y `batch_audit.training_reasons` registran la decisión con su justificación técnica completa, incluyendo el bloqueo del gate cuando aplica.

---

## 5. Lógica de decisión — Promoción

La función `compare_with_production()` en `utils/model_comparison.py` aplica una regla explícita:

### Regla de promoción

> **El candidato se promueve si el MAE mejora al menos un 3% Y el RMSE no empeora más de un 1%.**

```python
MAE_IMPROVEMENT_THRESHOLD = 0.03  # configurable via PROMOTION_MAE_IMPROVEMENT
RMSE_TOLERANCE            = 0.01  # configurable via PROMOTION_RMSE_TOLERANCE

mae_improvement = (production_mae - candidate_mae) / production_mae
rmse_change     = (candidate_rmse - production_rmse) / production_rmse

should_promote = (mae_improvement >= 0.03) and (rmse_change <= 0.01)
```

**Razonamiento de la regla:**
- El **MAE** (Error Absoluto Medio) es la métrica principal porque en un problema de regresión de precios es directamente interpretable en dólares. Un modelo que reduce el MAE en menos del 3% podría estar mejorando por varianza estadística y no por capacidad real.
- El **RMSE** actúa como guardia secundaria. Permite que el candidato mejore el MAE incluso si comete algunos errores grandes ocasionales, pero pone un tope a cuánto puede empeorar para evitar modelos inestables.

### Caso sin modelo productivo previo

```python
except MlflowException:
    # No production model exists
    return {"should_promote": True, "reason": "No production model — promoting as baseline"}
```

El primer modelo siempre se promueve. No hay referencia contra la cual comparar.

### Caso productivo sin métricas

Si el modelo productivo existe en el registry pero su run de MLflow no tiene `test_mae` registrado (situación anómala), el candidato se promueve por precaución. Este caso no debería ocurrir en condiciones normales.

### Razón registrada

En todos los casos, `comparison_result.reason` documenta el resultado con los valores exactos:

- Promoción: `"MAE improved by 5.20% (≥ 3%) and RMSE change 0.30% (≤ 1% tolerance)"`
- Rechazo por MAE: `"MAE improvement 1.80% < required 3%"`
- Rechazo por RMSE: `"RMSE worsened 2.40% > tolerance 1%"`

Esta razón se almacena en `batch_audit.promotion_reason`.

---

## 6. Persistencia de datos

### Tabla `property_raw` — Datos crudos

Almacena cada registro exactamente como llegó de la API. **Nunca se borra ni modifica**. Permite:

- Reconstruir el estado exacto del sistema en cualquier ejecución histórica.
- Auditar qué datos estaban disponibles cuando se tomó cada decisión de entrenamiento.
- Re-ejecutar el preprocesamiento con cualquier versión histórica del preprocesador.

### Tabla `property_clean` — Datos procesados (versionados)

Implementa el patrón **append-only con versionado por `preprocessor_version`**.

**Qué hay en la tabla:**

Cada ejecución de `preprocess_data` inserta un snapshot completo — todos los datos históricos + el nuevo batch — transformados con el preprocesador actual. Las versiones anteriores permanecen en la tabla con su `preprocessor_version` original.

**Cómo se usa para entrenar:**

```sql
SELECT * FROM property_clean
WHERE preprocessor_version = 'batch_3_20260529090015'
```

Solo los datos de la versión más reciente se usan para entrenamiento, garantizando coherencia: todos los features fueron generados por el mismo `OrdinalEncoder` con el mismo vocabulario.

**Por qué este diseño:**

El `OrdinalEncoder` asigna códigos enteros a las categorías según el orden en que las ve durante el `fit()`. Si en batch 1 `"Phoenix"` recibe código 0 y en batch 2 aparece `"Atlanta"`, el re-fit podría asignar `"Atlanta"=0` y `"Phoenix"=1`. Las filas del batch 1 procesadas con la versión anterior ya no son compatibles con las nuevas. Mantener la `preprocessor_version` permite saber exactamente qué codificación tiene cada fila.

**Crecimiento de la tabla:**

Con N batches, la tabla tendrá aproximadamente `N*(N+1)/2 * avg_batch_size` filas. Para 10 batches de 1000 registros: ~55.000 filas. Perfectamente manejable para el alcance del proyecto.

### Tabla `batch_audit` — Registro de auditoría

Una fila por ejecución del DAG. Registra el ciclo de vida completo de cada batch:

```sql
CREATE TABLE batch_audit (
    id                      SERIAL PRIMARY KEY,
    batch_id                INTEGER UNIQUE NOT NULL,
    run_id                  VARCHAR(128),
    fetched_at              TIMESTAMP,
    records_received        INTEGER,
    records_stored          INTEGER,
    schema_valid            BOOLEAN,
    schema_issues           TEXT,
    quality_score           DOUBLE PRECISION,
    quality_issues          JSONB,
    new_categories_detected JSONB,
    drift_detected          BOOLEAN,
    drift_details           JSONB,
    should_train            BOOLEAN,
    training_reasons        JSONB,
    preprocessor_version    VARCHAR(256),
    model_run_id            VARCHAR(128),
    model_version           VARCHAR(64),
    model_promoted          BOOLEAN,
    promotion_reason        TEXT,
    candidate_mae           DOUBLE PRECISION,
    candidate_rmse          DOUBLE PRECISION,
    production_mae          DOUBLE PRECISION,
    production_rmse         DOUBLE PRECISION,
    execution_status        VARCHAR(32),   -- running | success | failed | skipped_training
    completed_at            TIMESTAMP
);
```

### Tabla `inference_logs` — Log de inferencias

Escrita por FastAPI (no por el DAG). Registra cada petición de predicción para monitoreo y análisis de uso:

```sql
CREATE TABLE inference_logs (
    request_id       UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    requested_at     TIMESTAMP NOT NULL,
    input_data       JSONB   NOT NULL,
    predicted_price  FLOAT   NOT NULL,
    model_name       VARCHAR(128),
    model_version    VARCHAR(32),
    model_alias      VARCHAR(64),
    response_time_ms FLOAT   NOT NULL
);
```

---

## 7. Integración con MLflow

### Experimento

Todas las ejecuciones de entrenamiento se registran bajo el experimento `real-estate-price` (configurable via `MLFLOW_EXPERIMENT`). Un único experimento estable permite comparar runs de diferentes días en la UI de MLflow.

### Estructura de runs

Cada invocación de `train_candidate_model` crea una jerarquía de runs:

```
run padre: rf_batch_{N}           ← tags globales del batch
  └── run hijo: rf_batch_{N}_cfg01  ← params + métricas + modelo
  └── run hijo: rf_batch_{N}_cfg02  ← (si hay más configs en PARAM_GRID)
```

El run padre agrupa los experimentos del batch. Los runs hijos contienen los detalles de cada configuración de hiperparámetros. El mejor run hijo es el que se registra en el Model Registry.

Todos los tags de lineage se asignan tanto al run padre como a cada run hijo, de modo que cualquier run es buscable de forma independiente en la UI.

### Model Registry

El modelo se gestiona bajo el nombre `real-estate-price-model` (configurable via `REGISTERED_MODEL_NAME`).

**Ciclo de vida de un modelo:**

```
1. register_candidate_in_mlflow → crea versión N (sin alias)
2. compare_with_production      → compara métricas vs alias "production"
3a. promote_model               → set alias "production" → versión N
    reload_inference_api        → POST /reload → API carga versión N en memoria
3b. reject_model                → sin cambios (versión N existe pero sin alias productivo)
```

**Alias usados:**

| Alias | Significado |
|---|---|
| `production` | Modelo actualmente servido por FastAPI |

### Artefactos almacenados por run

Los artefactos se guardan en los **runs hijos** (uno por config de hiperparámetros):

| Artefacto | Ruta en MinIO | Descripción |
|---|---|---|
| Modelo | `mlflow/.../artifacts/model/` | Sklearn Pipeline serializado (preprocesador + RF) |
| Feature importances | `mlflow/.../artifacts/feature_importance/*.json` | Top 20 features por importancia relativa |
| Pred vs Actual (test) | `mlflow/.../artifacts/performance_plots/pred_vs_actual_test.png` | Scatter predicho vs real en test set |
| Residuos (test) | `mlflow/.../artifacts/performance_plots/residuals_test.png` | Distribución de errores en test set |
| Pred vs Actual (val) | `mlflow/.../artifacts/performance_plots/pred_vs_actual_val.png` | Scatter predicho vs real en val set |
| Residuos (val) | `mlflow/.../artifacts/performance_plots/residuals_val.png` | Distribución de errores en val set |

El preprocesador se guarda fuera del árbol de MLflow, directamente en el bucket:

| Artefacto | Ruta en MinIO | Descripción |
|---|---|---|
| Preprocesador | `preprocess/batch_{N}_{ts}/preprocessor.joblib` | ColumnTransformer ajustado |

---

## 8. Manejo de errores y resiliencia

### Reintentos automáticos

- **Todas las tareas:** 2 reintentos con espera de 1 minuto (configurado en `default_args`).
- **`fetch_batch_from_api`:** 3 reintentos a nivel de tarea, dado que los errores de red son más frecuentes en el punto de entrada.
- **`train_candidate_model`:** Timeout de 2 horas para datasets grandes.

### `on_failure_callback`

Cuando una tarea agota todos sus reintentos y falla definitivamente, el callback `_on_task_failure` actualiza `batch_audit.execution_status = "failed"` y registra `completed_at`. Esto asegura que la tabla de auditoría siempre refleja el estado real del proceso, incluso en caso de error.

El callback falla silenciosamente (no propaga excepciones) si ocurre durante una tarea previa al establecimiento del `batch_id` (por ejemplo, `create_tables`).

### Fin de datos disponibles

Cuando la API ya no tiene más lotes, devuelve HTTP 400 con `{"detail": "..."}`. El callable detecta esta señal antes de llamar `raise_for_status()`, lanza `AirflowSkipException` y Airflow marca la ejecución completa como *skipped* (no como *failed*). El scheduler puede continuar intentando en el siguiente intervalo.

### Idempotencia del audit

`create_batch_audit_entry` usa `ON CONFLICT (batch_id) DO NOTHING`. Si el mismo batch se reintenta, no se duplica la fila de auditoría.

### Protección contra lotes duplicados

El campo `row_hash` en `property_raw` permite detectar registros exactamente idénticos entre lotes. La deduplicación activa no está implementada en esta versión pero la infraestructura está lista.

---

## 9. Variables de entorno y configuración

| Variable | Default | Descripción |
|---|---|---|
| `DATA_API_URL` | `http://api-source:80` | URL base de la API de datos |
| `DATA_API_GROUP` | `7` | Número de grupo (1-N según asignación del docente) |
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | Servidor MLflow |
| `MLFLOW_REGISTERED_MODEL` | `real-estate-price-model` | Nombre del modelo en el registry |
| `MLFLOW_EXPERIMENT` | `real-estate-price` | Nombre del experimento MLflow |
| `MINIO_BUCKET` | `real-estate-project` | Bucket de artefactos en MinIO |
| `MINIO_ENDPOINT_URL` | — | URL del servidor MinIO |
| `MINIO_ACCESS_KEY` | — | Credencial MinIO |
| `MINIO_SECRET_KEY` | — | Credencial MinIO |
| `DB_HOST` | — | Host de PostgreSQL (RAW/CLEAN data) |
| `DB_PORT` | `5432` | Puerto PostgreSQL |
| `DB_NAME` | — | Nombre de la base de datos |
| `DB_USER` | — | Usuario PostgreSQL |
| `DB_PASSWORD` | — | Contraseña PostgreSQL |
| `INFERENCE_API_URL` | `http://api:8000` | URL base de la API de inferencia para el reload |
| `PROMOTION_MAE_IMPROVEMENT` | `0.03` | Mejora mínima de MAE para promover (fracción) |
| `PROMOTION_RMSE_TOLERANCE` | `0.01` | Tolerancia máxima de empeoramiento de RMSE |
| `MIN_BATCH_RATIO` | `0.05` | Fracción mínima que debe representar el batch sobre el histórico para evaluar criterios de entrenamiento |
| `GIT_COMMIT` | — | SHA del commit que originó la imagen |
| `GIT_PYTHON_REFRESH` | `quiet` | Suprime el warning de GitPython cuando git no está en el PATH del contenedor |

---

### `GIT_COMMIT` — Trazabilidad del código con GitHub Actions

El tag `git_commit` en cada run de MLflow vincula el modelo entrenado con el commit exacto del código que lo produjo. Esto permite reproducir cualquier entrenamiento histórico haciendo checkout de ese commit.

#### Cómo funciona la resolución del SHA

La función `_get_git_commit()` en `utils/training.py` sigue este orden de prioridad:

```
1. Variable de entorno GIT_COMMIT (si existe) → usa su valor directamente
2. subprocess git rev-parse HEAD               → requiere git en el contenedor
3. Fallback                                    → registra "unknown"
```

En el contenedor de Airflow **git no está instalado**, por lo que la variable de entorno es el mecanismo principal.

#### Configuración en GitHub Actions

**En el workflow de GitHub Actions** (`.github/workflows/build.yml`):
```yaml
- name: Build and push Airflow image
  uses: docker/build-push-action@v5
  with:
    context: ./Proyecto_3/Airflow
    push: true
    tags: ${{ secrets.DOCKERHUB_USERNAME }}/mlops-airflow:${{ github.sha }}
    build-args: |
      GIT_COMMIT=${{ github.sha }}
```

**En el manifiesto de Kubernetes** (Deployment de Airflow worker/scheduler):
```yaml
env:
  - name: GIT_COMMIT
    value: "${{ github.sha }}"
```

#### Configuración en Docker Compose (desarrollo local)

```bash
GIT_COMMIT=$(git rev-parse HEAD) docker compose -f docker-compose.yaml up -d
```

O en el `docker-compose.yaml`:

```yaml
environment:
  GIT_COMMIT: "${GIT_COMMIT:-local}"
```

---

### Umbrales de decisión

| Parámetro | Valor | Ubicación |
|---|---|---|
| Gate de tamaño mínimo de batch | 5% del histórico | `training_decision.py:MIN_BATCH_RATIO` |
| Umbral de incremento de volumen | 10% | `training_decision.py:VOLUME_INCREASE_THRESHOLD` |
| Umbral de nueva categoría significativa | ≥ 1% frecuencia | `validation.py:detect_new_categories` |
| Umbral de drift (p-value KS) | < 0.05 | `drift_detection.py:DRIFT_P_VALUE_THRESHOLD` |
| Mínimo filas históricas para drift | 50 | `drift_detection.py:MIN_REFERENCE_ROWS` |
| Score mínimo de calidad de datos | 0.5 | `validation.py:validate_data_quality` |
| Tasa máxima de nulos por columna | 50% | `validation.py` |
| Tamaño mínimo de batch | 10 registros | `validation.py:validate_data_quality` |
