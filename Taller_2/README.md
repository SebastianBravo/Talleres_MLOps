
# Taller 1 – Nivel 1
## Arquitectura MLOps con Docker Compose (Jupyter + FastAPI)

---

## Índice

1. Descripción General  
2. Arquitectura del Proyecto  
3. Estructura de Carpetas  
4. Descripción de Componentes  
5. Endpoints Disponibles  
6. Ejemplo de Consumo del Endpoint  
7. Cómo Entrenar el Modelo en Jupyter  
8. Flujo de Trabajo  
9. Cómo Ejecutarlo  
10. Objetivo del Taller  

---

## 1. Descripción General

En este taller se construyó una arquitectura básica de MLOps utilizando Docker Compose para orquestar dos servicios:

- Un servicio de Jupyter Lab encargado del entrenamiento de modelos.
- Un servicio de FastAPI encargado de exponer los modelos entrenados mediante una API REST.

Ambos servicios comparten un volumen llamado `models`, el cual permite que:

- Jupyter entrene y guarde modelos (.pkl).
- FastAPI cargue esos modelos y los exponga para predicción.

Se implementa así una separación clara entre entrenamiento e inferencia, siguiendo buenas prácticas de MLOps.

---

## 2. Arquitectura del Proyecto

Estructura general:

```
.
├── docker-compose.yml
├── models/
├── app/
│   ├── api.py
│   └── Dockerfile
├── train/
│   ├── train.ipynb
│   └── Dockerfile
```

---

## 3. Estructura de Carpetas

### models/

Carpeta compartida entre ambos servicios.

Aquí se almacenan los modelos entrenados en formato `.pkl`.  
Jupyter guarda los modelos y FastAPI los carga dinámicamente para hacer predicciones.

---

### train/

Contiene todo lo relacionado con el entrenamiento.

#### train.ipynb

Notebook donde se realiza:

- Carga del dataset.
- Limpieza y preprocesamiento de datos.
- Ingeniería de características.
- Entrenamiento del modelo.
- Evaluación del modelo.
- Guardado del modelo entrenado en la carpeta `/models`.

#### Dockerfile

Define la imagen para el servicio de Jupyter Lab, incluyendo:

- Python.
- Librerías necesarias para entrenamiento.
- Configuración para exponer el servidor Jupyter.

---

### app/

Contiene el servicio de inferencia.

#### api.py

Aplicación construida con FastAPI que:

- Lista modelos disponibles mediante `GET /models`.
- Realiza predicciones mediante `POST /predict`.
- Recibe todos los datos en el body de la petición.
- Carga modelos desde la carpeta `/models`.
- Implementa validación con Pydantic.
- Incluye middleware de logging.
- Usa joblib para cargar modelos serializados.

#### Dockerfile

Define la imagen del servicio FastAPI:

- Instala dependencias.
- Copia el código.
- Expone el puerto del servidor.

---

### docker-compose.yml

Archivo que:

- Define los servicios `train` y `app`.
- Configura puertos.
- Declara el volumen compartido `models`.
- Orquesta toda la arquitectura.

---

## 4. Endpoints Disponibles

### GET /models

Lista los modelos disponibles en la carpeta compartida.

Respuesta esperada:

```
{
  "models": ["nombre_modelo"]
}
```

---

### POST /predict

Realiza una predicción utilizando un modelo específico.

El endpoint recibe un JSON completo en el body con la siguiente estructura:

```
{
  "model": "nombre_modelo",
  "data": {
    "island": "Biscoe",
    "bill_length_mm": 45.2,
    "bill_depth_mm": 14.3,
    "flipper_length_mm": 210,
    "body_mass_g": 4200,
    "sex": "male",
    "year": 2022
  }
}
```

Respuesta esperada:

```
{
  "model_used": "nombre_modelo",
  "predicted_species": "Adelie"
}
```

---

## 5. Ejemplo de Consumo del Endpoint

Ejemplo usando curl:

```
curl -X POST "http://localhost:8000/predict" -H "Content-Type: application/json" -d '{
  "model": "rf_model",
  "data": {
    "island": "Dream",
    "bill_length_mm": 40.1,
    "bill_depth_mm": 18.3,
    "flipper_length_mm": 195,
    "body_mass_g": 3800,
    "sex": "female",
    "year": 2021
  }
}'
```

---

## 6. Cómo Entrenar el Modelo en Jupyter

Dentro del servicio `train`, el proceso recomendado es:

1. Importar librerías necesarias (pandas, sklearn, joblib).
2. Cargar el dataset.
3. Separar variables independientes (X) y variable objetivo (y).
4. Aplicar preprocesamiento (codificación y transformación).
5. Dividir en entrenamiento y prueba.
6. Entrenar el modelo.
7. Evaluar métricas básicas.
8. Guardar el modelo en la carpeta compartida:

```
import joblib
joblib.dump(modelo_entrenado, "/models/nombre_modelo.pkl")
```

El modelo guardado será automáticamente accesible por FastAPI.

---

## 7. Flujo de Trabajo

1. Levantar infraestructura con Docker Compose.
2. Acceder a Jupyter Lab.
3. Entrenar y guardar el modelo en `/models`.
4. Consultar `/models` en FastAPI para verificar disponibilidad.
5. Consumir `/predict` para obtener predicciones.

---

## 8. Cómo Ejecutarlo

Desde la raíz del proyecto ejecutar:

```
docker-compose up --build
```

Esto construye las imágenes y levanta ambos servicios.
