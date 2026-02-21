## Descripcion del proyecto

Este proyecto entrena y exporta modelos de clasificacion con el conjunto de datos de pinguinos (Palmer Penguins). Incluye un script de entrenamiento que prepara los datos, divide en conjuntos de entrenamiento/validacion/prueba y genera modelos con diferentes algoritmos.

## Como ejecutar el entrenamiento

Para ejecutar [Scripts/train.py](Scripts/train.py), abre una consola en el entorno de Jupyter Notebook y ejecuta el siguiente comando:

```bash
uv run python Scripts/train.py --models_folder <ruta/a/carpeta_modelos>
```

El argumento `--models_folder` indica la ruta donde se guardaran los modelos exportados.
