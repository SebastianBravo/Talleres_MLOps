import os
import boto3
import requests
import mysql.connector
import pandas as pd
import numpy as np
import joblib
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import train_test_split


# Función para conectarse a MinIO
def connect_to_minio():
    minio_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )
    print("Conectado a MinIO")
    return minio_client


# Función para conectarse a la base de datos MySQL
def connect_to_mysql():
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )
    print("Conectado a la base de datos MySQL")
    return connection


# Función para cerrar la conexión a MySQL
def close_mysql_connection(connection):
    if connection.is_connected():
        connection.close()
        print("Conexión a MySQL cerrada")


# Función para ejecutar una consulta SQL
def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    print("Consulta ejecutada exitosamente")
    return cursor.fetchall()


# Función para obtener datos desde una fuente FastAPI (API_URL)
def get_data_from_api():
    url = f"{os.getenv('API_URL')}/data"
    params = {"group_number": os.getenv("API_GROUP_NUMBER")}

    # Realizar la petición GET a la API
    try:
        print(f"Obteniendo datos de la API en {url} ...")
        response = requests.get(url, params=params)
        response.raise_for_status()  # Verificar errores HTTP
        data = response.json()

        print(f"Número de batch: {data['batch_number']}")
        print(f"Número de grupo: {data['group_number']}")
        print(f"Datos obtenidos de la API: {len(data['data'])} registros")
        return pd.DataFrame(data["data"])
    except requests.exceptions.RequestException as e:
        data = response.json()
        if data["detail"] == "Ya se recolectó toda la información minima necesaria":
            print(
                "Ya se recolectó toda la información mínima necesaria. No se cargarán nuevos datos."
            )
        else:
            print(f"Error al cargar datos desde la API: {e}")
        return None  # Retornar None en caso de error


# Función para eliminar una tabla si existe
def delete_table_if_exists(connection, table_name):
    query = f"DROP TABLE IF EXISTS {table_name}"
    execute_query(connection, query)
    print(f"Tabla {table_name} eliminada (si existía)")


# Función para crear la tabla cruda del dataset covertype
def create_table_raw(connection, table_name):
    # Crear tabla con esquema apropiado para el dataset covertype, permitiendo valores nulos
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        elevation INT NULL,
        aspect INT NULL,
        slope INT NULL,
        horizontal_distance_to_hydrology INT NULL,
        vertical_distance_to_hydrology INT NULL,
        horizontal_distance_to_roadways INT NULL,
        hillshade_9am INT NULL,
        hillshade_noon INT NULL,
        hillshade_3pm INT NULL,
        horizontal_distance_to_fire_points INT NULL,
        wilderness_area VARCHAR(50) NULL,
        soil_type VARCHAR(50) NULL,
        cover_type INT NULL
    );
    """

    execute_query(connection, query)
    print(f"Tabla {table_name} creada exitosamente")


# Función para crear la tabla limpia con el esquema apropiado para datos preprocesados
def create_table_cleaned(connection, table_name, feature_names):
    # Sanitizar nombres de columnas para compatibilidad con MySQL (reemplazar caracteres especiales)
    sanitized_cols = [
        name.replace("__", "_").replace(" ", "_") for name in feature_names
    ]
    feature_columns = ", ".join([f"`{col}` FLOAT" for col in sanitized_cols])
    query = f"""
    CREATE TABLE {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {feature_columns},
        dataset VARCHAR(10),
        cover_type INT
    )
    """
    execute_query(connection, query)
    print(
        f"Tabla {table_name} creada exitosamente con {len(feature_names)} características"
    )


# Función para insertar datos crudos del dataset covertype en la tabla
def insert_raw_covertype_data(connection, table_name, df=None):
    if df is None or df.empty:
        print(
            "No se cargaron datos desde la API o el DataFrame está vacío. No se insertarán datos en MySQL."
        )
        return
    else:
        print(f"Cargando datos: {len(df)} filas, {len(df.columns)} columnas...")

        # Reemplazar valores NaN con None para manejar NULLs correctamente en MySQL
        df = df.replace({float("nan"): None})

        cursor = connection.cursor()

        # Preparar la consulta SQL para insertar datos
        query = f"""INSERT INTO {table_name}
            (elevation, aspect, slope, horizontal_distance_to_hydrology,
            vertical_distance_to_hydrology, horizontal_distance_to_roadways, hillshade_9am, hillshade_noon, hillshade_3pm, horizontal_distance_to_fire_points,
            wilderness_area, soil_type, cover_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        # Convertir DataFrame a lista de tuplas para la inserción
        val = [tuple(row) for row in df.values]

        # Ejecutar la consulta para múltiples filas
        cursor.executemany(query, val)

        # Confirmar la transacción
        connection.commit()
        print(f"Se cargaron {len(val)} registros en la tabla {table_name}")


# Función para cargar datos desde la base de datos MySQL
def load_data_from_mysql(connection, table_name):
    query = f"SELECT * FROM {table_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    # Cargar el resultado en un DataFrame con la columna id como índice
    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
    df.set_index("id", inplace=True)

    return df


# Función para preprocesar datos e insertar en la tabla limpia
def preprocess_and_insert(
    connection,
    raw_table,
    cleaned_table,
    bucket,
    preprocessor_path,
    test_size=0.2,
    random_state=42,
):
    # 1. Cargar datos crudos desde MySQL
    df = load_data_from_mysql(connection, raw_table)
    print(f"Datos crudos cargados: {len(df)} filas")

    # 2. Eliminar filas con valores nulos y duplicados
    rows_before = len(df)
    df = df.dropna()
    df = df.drop_duplicates()
    print(f"Filas eliminadas (nulos y duplicados): {rows_before - len(df)}")

    # 3. Separar características (X) y variable objetivo (y)
    X = df.drop(columns=["cover_type"])
    y = df["cover_type"]

    # 4. Dividir en conjuntos de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    print(f"Entrenamiento: {len(X_train)} filas, Prueba: {len(X_test)} filas")

    # 5. Definir columnas numéricas y categóricas
    num_cols = [
        "elevation",
        "aspect",
        "slope",
        "horizontal_distance_to_hydrology",
        "vertical_distance_to_hydrology",
        "horizontal_distance_to_roadways",
        "hillshade_9am",
        "hillshade_noon",
        "hillshade_3pm",
        "horizontal_distance_to_fire_points",
    ]
    cat_cols = ["wilderness_area", "soil_type"]

    # 6. Crear pipeline de preprocesamiento
    # Pipeline numérico: imputación por mediana + escalado estándar
    numeric_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    # Pipeline categórico: imputación por moda + codificación one-hot
    categorical_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )

    # Transformador de columnas que combina ambos pipelines
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_pipe, num_cols),
            ("cat", categorical_pipe, cat_cols),
        ],
        remainder="drop",
    )

    # 7. Ajustar el preprocesador solo con datos de entrenamiento (evitar fuga de datos)
    preprocessor.fit(X_train)

    # 8. Transformar ambos conjuntos de datos
    X_train_p = preprocessor.transform(X_train)
    X_test_p = preprocessor.transform(X_test)

    print("Dimensiones tras el preprocesamiento:")
    print(f"  X_train_p: {X_train_p.shape}")
    print(f"  X_test_p:  {X_test_p.shape}")

    # 9. Guardar el preprocesador en MinIO
    # Verificar si el bucket existe, si no, crearlo
    minio_client = connect_to_minio()
    if not minio_client.list_buckets(Prefix=bucket)["Buckets"]:
        print(f"El bucket '{bucket}' no existe. Creando bucket...")
        minio_client.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' creado en MinIO")
    else:
        print(f"El bucket '{bucket}' ya existe en MinIO")

    # Guardar preprocesador localmente y luego subir a MinIO
    os.makedirs(preprocessor_path, exist_ok=True)
    preprocessor_path = os.path.join(preprocessor_path, "preprocessor.joblib")
    joblib.dump(preprocessor, preprocessor_path)
    print(f"Preprocesador guardado localmente en: {preprocessor_path}")

    # Subir preprocesador a MinIO
    print(f"Subiendo preprocesador al bucket '{bucket}' en MinIO...")
    minio_client.upload_file(
        preprocessor_path, bucket, "v1/preprocess/preprocessor.joblib"
    )
    print("Preprocesador subido a MinIO exitosamente")

    # 10. Crear DataFrames con los datos procesados, preservando los nombres de las características
    n_features = X_train_p.shape[1]
    feature_cols = preprocessor.get_feature_names_out().tolist()
    print(f"Nombres de características: {feature_cols}")

    # Sanitizar nombres de columnas para compatibilidad con MySQL
    sanitized_cols = [
        name.replace("__", "_").replace(" ", "_") for name in feature_cols
    ]

    # Crear DataFrame de entrenamiento con etiqueta de conjunto
    df_train = pd.DataFrame(X_train_p, columns=sanitized_cols)
    df_train["dataset"] = "train"
    df_train["cover_type"] = y_train.values

    # Crear DataFrame de prueba con etiqueta de conjunto
    df_test = pd.DataFrame(X_test_p, columns=sanitized_cols)
    df_test["dataset"] = "test"
    df_test["cover_type"] = y_test.values

    # Concatenar ambos conjuntos
    df_processed = pd.concat([df_train, df_test], ignore_index=True)

    print(f"Datos procesados: {len(df_processed)} filas, {n_features} características")
    print(df_processed.head())

    # 11. Crear la tabla limpia e insertar los datos
    create_table_cleaned(connection, cleaned_table, sanitized_cols)

    # 12. Insertar datos procesados en la tabla limpia
    columns = ", ".join(
        [f"`{col}`" for col in sanitized_cols] + ["dataset", "cover_type"]
    )
    placeholders = ", ".join(["%s"] * (n_features + 2))
    query = f"INSERT INTO {cleaned_table} ({columns}) VALUES ({placeholders})"

    values = [tuple(row) for row in df_processed.values]

    cursor = connection.cursor()
    cursor.executemany(query, values)
    connection.commit()
    cursor.close()
    print(f"Se insertaron {len(values)} filas en la tabla '{cleaned_table}'")