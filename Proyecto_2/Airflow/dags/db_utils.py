import os
import boto3
import requests
import psycopg2
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


# Función para conectarse a la base de datos PostgreSQL
def connect_to_db():
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_DATASET_HOST"),
        user=os.getenv("POSTGRES_DATASET_USER"),
        password=os.getenv("POSTGRES_DATASET_PASSWORD"),
        dbname=os.getenv("POSTGRES_DATASET_DATABASE"),
        port=os.getenv("POSTGRES_DATASET_PORT"),
    )
    print("Conectado a la base de datos PostgreSQL")
    return connection


# Función para cerrar la conexión a PostgreSQL
def close_db_connection(connection):
    if connection and not connection.closed:
        connection.close()
        print("Conexión a PostgreSQL cerrada")


# Función para ejecutar una consulta SQL
def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    print("Consulta ejecutada exitosamente")
    if cursor.description:
        return cursor.fetchall()
    return []


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
        response_obj = getattr(e, "response", None)
        if response_obj is not None:
            try:
                data = response_obj.json()
                if (
                    data.get("detail")
                    == "Ya se recolectó toda la información minima necesaria"
                ):
                    print(
                        "Ya se recolectó toda la información mínima necesaria. No se cargarán nuevos datos."
                    )
                    return pd.DataFrame()
            except ValueError:
                pass

        print(f"Error al cargar datos desde la API: {e}")
        return None  # Retornar None en caso de error


# Función para eliminar una tabla si existe
def delete_table_if_exists(connection, table_name):
    query = f"DROP TABLE IF EXISTS {table_name}"
    execute_query(connection, query)
    print(f"Tabla {table_name} eliminada (si existía)")


# Función para crear la tabla cruda del dataset diabetic_data
def create_table_raw(connection, table_name):
    # Crear tabla con esquema apropiado para el dataset diabetic_data, permitiendo valores nulos
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        load_batch VARCHAR(64) NULL,
        load_timestamp TIMESTAMP NOT NULL,
        data_source VARCHAR(128) NULL,
        record_status VARCHAR(32) NULL,
        encounter_id BIGINT NULL,
        patient_nbr BIGINT NULL,
        race VARCHAR(32) NULL,
        gender VARCHAR(16) NULL,
        age VARCHAR(16) NULL,
        weight VARCHAR(16) NULL,
        admission_type_id INT NULL,
        discharge_disposition_id INT NULL,
        admission_source_id INT NULL,
        time_in_hospital INT NULL,
        payer_code VARCHAR(32) NULL,
        medical_specialty VARCHAR(64) NULL,
        num_lab_procedures INT NULL,
        num_procedures INT NULL,
        num_medications INT NULL,
        number_outpatient INT NULL,
        number_emergency INT NULL,
        number_inpatient INT NULL,
        diag_1 VARCHAR(16) NULL,
        diag_2 VARCHAR(16) NULL,
        diag_3 VARCHAR(16) NULL,
        number_diagnoses INT NULL,
        max_glu_serum VARCHAR(16) NULL,
        A1Cresult VARCHAR(16) NULL,
        metformin VARCHAR(16) NULL,
        repaglinide VARCHAR(16) NULL,
        nateglinide VARCHAR(16) NULL,
        chlorpropamide VARCHAR(16) NULL,
        glimepiride VARCHAR(16) NULL,
        acetohexamide VARCHAR(16) NULL,
        glipizide VARCHAR(16) NULL,
        glyburide VARCHAR(16) NULL,
        tolbutamide VARCHAR(16) NULL,
        pioglitazone VARCHAR(16) NULL,
        rosiglitazone VARCHAR(16) NULL,
        acarbose VARCHAR(16) NULL,
        miglitol VARCHAR(16) NULL,
        troglitazone VARCHAR(16) NULL,
        tolazamide VARCHAR(16) NULL,
        examide VARCHAR(16) NULL,
        citoglipton VARCHAR(16) NULL,
        insulin VARCHAR(16) NULL,
        "glyburide-metformin" VARCHAR(16) NULL,
        "glipizide-metformin" VARCHAR(16) NULL,
        "glimepiride-pioglitazone" VARCHAR(16) NULL,
        "metformin-rosiglitazone" VARCHAR(16) NULL,
        "metformin-pioglitazone" VARCHAR(16) NULL,
        "change" VARCHAR(16) NULL,
        diabetesMed VARCHAR(8) NULL,
        readmitted VARCHAR(8) NOT NULL 
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
    feature_columns = ", ".join([f'"{col}" DOUBLE PRECISION' for col in sanitized_cols])
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
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
            "No se cargaron datos desde la API o el DataFrame está vacío. No se insertarán datos en PostgreSQL."
        )
        return
    else:
        print(f"Cargando datos: {len(df)} filas, {len(df.columns)} columnas...")

        # Reemplazar valores NaN con None para manejar NULLs correctamente en PostgreSQL
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


# Función para cargar datos desde la base de datos PostgreSQL
def load_data_from_db(connection, table_name):
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
    # 1. Cargar datos crudos desde PostgreSQL
    df = load_data_from_db(connection, raw_table)
    print(f"Datos crudos cargados: {len(df)} filas")

    # 2. Eliminar filas con valores nulos y duplicados
    rows_before = len(df)
    df = df.dropna()
    df = df.drop_duplicates()
    print(f"Filas eliminadas (nulos y duplicados): {rows_before - len(df)}")

    # 3. Separar características (X) y variable objetivo (y)
    X = df.drop(columns=["cover_type"]).copy()
    y = df["cover_type"]

    # 4. Definir columnas numéricas y categóricas
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

    # Definir categorías usando todo el conjunto disponible en esta iteración
    # (incluye filas que luego quedarán en train y test)
    X[cat_cols] = X[cat_cols].astype(str)
    all_categories = [sorted(X[col].dropna().unique().tolist()) for col in cat_cols]

    # 5. Dividir en conjuntos de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    print(f"Entrenamiento: {len(X_train)} filas, Prueba: {len(X_test)} filas")

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
            (
                "onehot",
                OneHotEncoder(
                    categories=all_categories,
                    handle_unknown="ignore",
                    sparse_output=False,
                ),
            ),
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

    # Sanitizar nombres de columnas para compatibilidad con PostgreSQL
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

    # 11. Re-crear la tabla limpia en cada iteración para soportar
    # cambios en columnas por nuevas categorías del one-hot encoding
    delete_table_if_exists(connection, cleaned_table)
    create_table_cleaned(connection, cleaned_table, sanitized_cols)

    # 12. Insertar datos procesados en la tabla limpia
    columns = ", ".join(
        [f'"{col}"' for col in sanitized_cols] + ["dataset", "cover_type"]
    )
    placeholders = ", ".join(["%s"] * (n_features + 2))
    query = f"INSERT INTO {cleaned_table} ({columns}) VALUES ({placeholders})"

    values = [tuple(row) for row in df_processed.values]

    cursor = connection.cursor()
    cursor.executemany(query, values)
    connection.commit()
    cursor.close()
    print(f"Se insertaron {len(values)} filas en la tabla '{cleaned_table}'")