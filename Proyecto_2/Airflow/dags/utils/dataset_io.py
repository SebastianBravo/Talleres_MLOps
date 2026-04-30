import os

import pandas as pd
import requests


def ensure_dataset_file():
    """Verifica el dataset local y lo descarga si no existe."""
    data_root = os.getenv("DATASET_ROOT", "./data/Diabetes")
    data_filename = os.getenv("DATASET_FILENAME", "Diabetes.csv")
    data_url = os.getenv(
        "DATASET_URL",
        "https://docs.google.com/uc?export=download&confirm={{VALUE}}&id=1k5-1caezQ3zWJbKaiMULTGq-3sz6uThC",
    )

    # Crear carpeta local si no existe
    os.makedirs(data_root, exist_ok=True)
    data_filepath = os.path.join(data_root, data_filename)

    # Reusar archivo existente para evitar descargas innecesarias.
    if os.path.isfile(data_filepath):
        print(f"Archivo fuente disponible: {data_filepath}")
        return data_filepath

    # Descargar el dataset si no existe localmente.
    try:
        print(f"Descargando dataset desde {data_url} ...")
        response = requests.get(data_url, allow_redirects=True, stream=True)
        response.raise_for_status()
        # Escribir en disco por chunks
        with open(data_filepath, "wb") as file_handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file_handle.write(chunk)
        print(f"Dataset descargado en: {data_filepath}")
        return data_filepath
    except requests.exceptions.RequestException as exc:
        print(f"Error al descargar el dataset: {exc}")
        return None


def read_diabetes_batch(data_filepath, batch_size, offset):
    """Lee un batch del CSV usando offset y tamano fijo."""
    # Validar archivo fuente
    if not data_filepath or not os.path.isfile(data_filepath):
        return pd.DataFrame(), offset

    # Configurar filas a omitir para simular ingesta incremental
    skiprows = range(1, offset + 1) if offset > 0 else None
    df = pd.read_csv(data_filepath, skiprows=skiprows, nrows=batch_size)
    if df.empty:
        return df, offset

    next_offset = offset + len(df)
    return df, next_offset
