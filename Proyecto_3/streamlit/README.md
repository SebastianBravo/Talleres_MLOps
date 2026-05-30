# Interfaz Streamlit — Real Estate Price Prediction

Interfaz web construida con **Streamlit** que provee dos secciones funcionales sobre el pipeline de MLOps de predicción de precios inmobiliarios.

---

## Tabla de contenido

1. [Visión general](#1-visión-general)
2. [Secciones de la interfaz](#2-secciones-de-la-interfaz)
3. [Arquitectura y flujo de datos](#3-arquitectura-y-flujo-de-datos)
4. [Estructura del proyecto](#4-estructura-del-proyecto)
5. [Variables de entorno](#5-variables-de-entorno)
6. [Despliegue local con Docker Compose](#6-despliegue-local-con-docker-compose)

---

## 1. Visión general

La interfaz consume exclusivamente la **API de inferencia FastAPI** (`api:8000`). No accede directamente a PostgreSQL, MLflow ni MinIO — toda la lógica de datos pasa por la API.

**Dependencias externas:**

| Endpoint consumido | Propósito |
|---|---|
| `GET /model-info` | Estado y versión del modelo activo (sidebar) |
| `POST /predict` | Predicción de precio a partir de features crudas |
| `GET /history` | Historial completo de lotes del DAG de Airflow |

---

## 2. Secciones de la interfaz

### Sidebar — Modelo activo

Siempre visible. Muestra el nombre, versión, alias y timestamp de carga del modelo actualmente en producción. Cambia automáticamente tras cada recarga del modelo por el DAG.

| Estado | Indicador |
|---|---|
| Modelo cargado correctamente | Badge verde + detalles |
| Modelo no disponible aún | Advertencia amarilla |
| API inaccesible | Error rojo |

---

### Pestaña 1 — Inferencia

Formulario completo para ingresar los datos de una propiedad y obtener una predicción de precio.

#### Campos del formulario

Organizados en tres secciones expandibles:

**Características físicas:**

| Campo | Widget | Descripción |
|---|---|---|
| Habitaciones | `number_input` (entero) | Número de dormitorios |
| Baños | `number_input` (0.5 step) | Número de baños |
| Lote (acres) | `number_input` (float) | Superficie del terreno |
| Tamaño (sqft) | `number_input` (float) | Superficie construida en pies cuadrados |

**Ubicación:**

| Campo | Widget | Descripción |
|---|---|---|
| Ciudad | `text_input` | Ciudad de la propiedad |
| Estado | `text_input` | Estado de EE. UU. (ej. `Texas`) |
| Código postal | `text_input` | Tratado como categórico por el modelo |
| Calle | `text_input` | Dirección de la propiedad |
| Agencia | `text_input` | Nombre del bróker o inmobiliaria |

**Información adicional:**

| Campo | Widget | Opciones |
|---|---|---|
| Estado de la propiedad | `selectbox` | `for_sale`, `ready_to_build`, `sold` |
| Última fecha de venta | `date_input` | Cualquier fecha — se convierte en año, mes y días transcurridos |

El botón **"Cargar ejemplo"** rellena todos los campos con valores de una propiedad de referencia del dataset.

#### Resultado de la predicción

Tras presionar **"Predecir precio"**, se muestra:

- Precio estimado en formato monetario destacado (`$385,000`)
- Cuatro métricas de la inferencia: nombre del modelo, versión, alias, tiempo de respuesta

#### Manejo de errores

| Código HTTP | Mensaje mostrado |
|---|---|
| `503` | El modelo aún no está disponible — esperar al primer entrenamiento del DAG |
| `422` | Error de validación con detalle JSON |
| `500` | Error interno — detalle de la API |
| Conexión rechazada | No se pudo conectar con la API |
| Timeout | La API tardó demasiado en responder |

---

### Pestaña 2 — Historial de entrenamiento y despliegue

Vista que muestra la historia completa de cada lote procesado por el DAG de Airflow, en orden del más reciente al más antiguo.

#### Métricas de resumen

Cuatro indicadores en la parte superior:

| Métrica | Descripción |
|---|---|
| Lotes procesados | Total de lotes registrados en `batch_audit` |
| Con entrenamiento | Lotes en los que `should_train = true` |
| Modelos promovidos | Lotes donde se promovió un nuevo modelo a producción |
| Tasa de promoción | `promovidos / entrenados` |

#### Tarjeta por lote

Cada lote se muestra en un `st.expander` con el encabezado:

```
Lote N  ·  YYYY-MM-DD  ·  X,XXX registros
```

El lote más reciente aparece expandido por defecto. Dentro de cada tarjeta:

**Badges de estado:**

| Badge | Color | Significado |
|---|---|---|
| `SUCCESS` / `FAILED` / `RUNNING` | Verde / Rojo / Naranja | Estado de ejecución del lote |
| `ENTRENÓ` | Azul | El lote disparó un entrenamiento |
| `NO ENTRENÓ` | Gris | No se cumplieron los criterios de reentrenamiento |
| `PROMOVIDO` | Verde | El modelo candidato reemplazó al productivo |
| `RECHAZADO` | Rojo | El candidato no superó al modelo en producción |

**Narrativa de decisión:**

Texto legible construido directamente desde los campos de `batch_audit`:

- **Entrenamiento:** `"Entrenó porque: <razones de training_reasons>."` o `"No entrenó. <razones>."`
- **Promoción:** `"Promovido a producción. <promotion_reason>"` o `"No promovido. <promotion_reason>"`

Ejemplo de narrativa generada:

> **Entrenamiento:** Entrenó porque: No production model exists — training baseline.
> **Promocion:** Promovido a producción. No production model — first version promoted automatically.

> **Entrenamiento:** No entrenó. Batch too small relative to historical data: 800 records = 2.5% of 32000 accumulated rows (minimum required: 5%). Volume, drift and category criteria skipped.

**Señales detectadas:**

Si el lote presentó drift o nuevas categorías se muestra una línea de caption:

```
Señales: Drift en: house_size, acre_lot  |  Nuevas categorias en: city
```

**Desempeño del candidato** *(solo si se entrenó)*:

| Métrica | Descripción |
|---|---|
| MAE candidato | Error absoluto medio del modelo candidato en test set |
| RMSE candidato | Raíz del error cuadrático medio del candidato |
| MAE producción + Δ% | MAE del modelo en producción + diferencia porcentual |
| RMSE producción + Δ% | RMSE del modelo en producción + diferencia porcentual |

El delta usa `delta_color="inverse"`: negativo (candidato mejor) → verde, positivo (candidato peor) → rojo.

Si es el primer lote (sin modelo previo), las columnas de producción muestran `"Línea base — sin modelo previo"`.

**Identificadores MLflow** *(solo si se entrenó)*:

```
Run ID:  a1b2c3d4e5f6789...
Version: 3
```

---

## 3. Arquitectura y flujo de datos

```
                        ┌─────────────────────┐
                        │      Streamlit       │
                        │      :8501           │
                        └──────────┬──────────┘
                                   │ HTTP (API_URL)
                         ┌─────────▼─────────┐
                         │   FastAPI API      │
                         │   :8000            │
                         └──┬────────────┬───┘
                            │            │
                    ┌───────▼───┐  ┌─────▼──────────┐
                    │  MLflow   │  │   PostgreSQL    │
                    │ Registry  │  │ inference_logs  │
                    └───────────┘  │ batch_audit     │
                                   └─────────────────┘
                                          ▲
                                          │ escribe
                                   ┌──────┴──────┐
                                   │   Airflow   │
                                   │     DAG     │
                                   └─────────────┘
```

**Responsabilidades:**

- **Streamlit** solo hace peticiones HTTP a la API. No tiene acceso directo a la base de datos.
- **FastAPI** expone los datos de `batch_audit` vía `GET /history` (lectura) y persiste inferencias en `inference_logs` (escritura).
- **Airflow** es el único escritor de `batch_audit`.

---

## 4. Estructura del proyecto

```
streamlit/
├── Dockerfile          ← imagen Python 3.12-slim
├── requirements.txt    ← streamlit + requests
└── app.py              ← aplicación completa (single-file)
```

---

## 5. Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `API_URL` | `http://api:8000` | URL base de la API de inferencia FastAPI |

---

## 6. Despliegue local con Docker Compose

### Dockerfile

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

### Compose (`compose/streamlit.yml`)

```yaml
services:
  streamlit:
    build:
      context: ../streamlit
      dockerfile: Dockerfile
    container_name: Streamlit
    ports:
      - "8501:8501"
    environment:
      API_URL: http://api:8000
    depends_on:
      - api
    deploy:
      resources:
        limits:
          memory: 256m
          cpus: "0.25"
    restart: unless-stopped
```

### Verificación

```bash
# Levantar el stack completo
docker compose up -d --build

# Acceder a la interfaz
open http://localhost:8501

# Ver logs del contenedor
docker logs -f Streamlit
```

### Estados esperados al iniciar

| Condición | Comportamiento en Streamlit |
|---|---|
| API corriendo, modelo cargado | Sidebar verde, formulario disponible |
| API corriendo, sin modelo aún | Sidebar con advertencia, `/predict` devuelve 503 |
| API no disponible | Sidebar con error rojo, historial no carga |
| DAG no ha corrido aún | Historial muestra "Aún no hay lotes registrados" |
| DAG corriendo (primer lote) | Historial puede mostrar `execution_status: running` |
