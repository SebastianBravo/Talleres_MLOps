## Descripcion del proyecto

Este proyecto entrena y exporta modelos de clasificacion con el conjunto de datos de pinguinos (Palmer Penguins). Incluye un script de entrenamiento que prepara los datos, divide en conjuntos de entrenamiento/validacion/prueba y genera modelos con diferentes algoritmos. Ademas, el proyecto monta un servidor con Jupyter Lab para trabajar en un entorno interactivo.

## Como ejecutar el entrenamiento

Para ejecutar [Scripts/train.py](Scripts/train.py), abre una consola en el entorno de Jupyter Notebook y ejecuta el siguiente comando:

```bash
uv run python Scripts/train.py --models_folder <ruta/a/carpeta_modelos>
```

El argumento `--models_folder` indica la ruta donde se guardaran los modelos exportados.

## Acceso a Jupyter Lab

El contenedor expone Jupyter Lab en el puerto 8888. Para acceder, inicia el contenedor y abre la URL en el navegador:

```bash
docker build -t jupyter_lab_app .
docker run -p 8888:8888 jupyter_lab_app
```

Luego visita `http://localhost:8888` y usa el token que aparece en la salida de la consola.

## Estructura del proyecto

- `Dockerfile`: define la imagen y arranca Jupyter Lab con `uv`.
- `pyproject.toml` y `uv.lock`: declaracion y bloqueo de dependencias.
- `Scripts/train.py`: script de entrenamiento y exportacion de modelos.
- `.python-version`: version de Python utilizada.
- `.dockerignore`: archivos excluidos del build.

## Datos del proyecto

El dataset se carga desde el paquete `palmerpenguins` en tiempo de ejecucion, por lo que no hay archivos de datos locales en el repositorio. Los modelos entrenados se guardan en la carpeta indicada por `--models_folder`.
