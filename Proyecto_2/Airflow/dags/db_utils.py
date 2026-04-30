import os
import hashlib
from datetime import datetime

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
    """Crea y devuelve un cliente de MinIO usando variables de entorno."""
    # Leer credenciales y endpoint desde variables de entorno
    minio_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )
    # Confirmar conexion
    print("Conectado a MinIO")
    return minio_client


# Función para conectarse a la base de datos PostgreSQL
def connect_to_db():
    """Abre una conexion a PostgreSQL usando variables de entorno."""
    # Leer credenciales de la base desde variables de entorno
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_DATASET_HOST"),
        user=os.getenv("POSTGRES_DATASET_USER"),
        password=os.getenv("POSTGRES_DATASET_PASSWORD"),
        dbname=os.getenv("POSTGRES_DATASET_DATABASE"),
        port=os.getenv("POSTGRES_DATASET_PORT"),
    )
    # Confirmar conexion
    print("Conectado a la base de datos PostgreSQL")
    return connection


# Función para cerrar la conexión a PostgreSQL
def close_db_connection(connection):
    """Cierra la conexion abierta si existe."""
    # Evitar cierre de conexiones nulas o ya cerradas
    if connection and not connection.closed:
        connection.close()
        print("Conexión a PostgreSQL cerrada")


# Función para ejecutar una consulta SQL
def execute_query(connection, query):
    """Ejecuta una consulta SQL simple y retorna resultados si aplica."""
    # Ejecutar consulta
    cursor = connection.cursor()
    cursor.execute(query)
    # Confirmar transaccion
    connection.commit()
    print("Consulta ejecutada exitosamente")
    # Retornar filas si es una consulta con resultados
    if cursor.description:
        return cursor.fetchall()
    return []


# Función para validar disponibilidad del archivo fuente y descargarlo si no existe
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


# Función para eliminar una tabla si existe
def delete_table_if_exists(connection, table_name):
    """Elimina una tabla si existe en la base de datos."""
    # Ejecutar drop de forma segura
    query = f"DROP TABLE IF EXISTS {table_name}"
    execute_query(connection, query)
    print(f"Tabla {table_name} eliminada (si existía)")


# Función para crear la tabla cruda del dataset diabetic_data
def create_table_raw(connection, table_name):
    """Crea la tabla raw con el esquema del dataset de diabetes."""
    # Crear tabla con esquema apropiado para el dataset diabetic_data, permitiendo valores nulos
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        batch_id INT NULL,
        load_timestamp TIMESTAMP NOT NULL,
        data_source VARCHAR(128) NULL,
        record_status VARCHAR(32) NULL,
        source_record_id VARCHAR(64) NULL,
        row_hash VARCHAR(64) NULL,
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
def create_table_cleaned(
    connection,
    table_name,
    feature_names,
    target_name="readmitted",
    target_type="VARCHAR(16)",
):
    """Crea la tabla limpia con columnas de features dinamicas."""
    # Sanitizar nombres de columnas para compatibilidad con PostgreSQL
    sanitized_cols = [
        name.replace("__", "_").replace(" ", "_").replace("-", "_")
        for name in feature_names
    ]
    feature_columns = ", ".join([f'"{col}" DOUBLE PRECISION' for col in sanitized_cols])
    # Construir DDL dinamico
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        {feature_columns},
        dataset VARCHAR(10),
        {target_name} {target_type}
    )
    """
    execute_query(connection, query)
    print(
        f"Tabla {table_name} creada exitosamente con {len(feature_names)} características"
    )


def create_split_table(connection, table_name):
    """Crea la tabla para registrar el split train/test por registro."""
    # Crear estructura si no existe
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        source_record_id VARCHAR(64) PRIMARY KEY,
        dataset VARCHAR(10) NOT NULL,
        assigned_at TIMESTAMP NOT NULL
    )
    """
    execute_query(connection, query)


def create_processed_batches_table(connection, table_name):
    """Crea la tabla de auditoria de batches procesados."""
    # Crear estructura si no existe
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        batch_id INT NOT NULL,
        processed_at TIMESTAMP NOT NULL,
        preprocessor_version VARCHAR(128) NOT NULL,
        rows_total INT NOT NULL,
        rows_train INT NOT NULL,
        rows_test INT NOT NULL,
        PRIMARY KEY (batch_id, processed_at)
    )
    """
    execute_query(connection, query)


def assign_dataset_split(
    connection,
    raw_table,
    split_table,
    test_size=0.2,
    random_state=42,
):
    """Asigna train/test de forma determinista y persiste el split."""
    # Garantizar tabla de split
    create_split_table(connection, split_table)
    cursor = connection.cursor()
    # Recuperar registros sin asignacion previa
    cursor.execute(
        f"""
        SELECT r.source_record_id
        FROM {raw_table} r
        LEFT JOIN {split_table} s
            ON r.source_record_id = s.source_record_id
        WHERE r.source_record_id IS NOT NULL
            AND s.source_record_id IS NULL
        """
    )
    new_ids = [row[0] for row in cursor.fetchall()]
    if not new_ids:
        print("No hay nuevos registros para asignar a train/test.")
        return 0

    # Determinar asignacion usando hashing estable
    assigned_at = datetime.utcnow()
    test_threshold = int(test_size * 10000)
    rows = []
    for source_id in new_ids:
        hash_input = f"{source_id}-{random_state}"
        hash_value = int(hashlib.md5(hash_input.encode("utf-8")).hexdigest(), 16)
        bucket = hash_value % 10000
        dataset = "test" if bucket < test_threshold else "train"
        rows.append((source_id, dataset, assigned_at))

    # Insertar asignaciones nuevas
    cursor.executemany(
        f"""
        INSERT INTO {split_table} (source_record_id, dataset, assigned_at)
        VALUES (%s, %s, %s)
        """,
        rows,
    )
    connection.commit()
    print(f"Asignadas {len(rows)} filas a train/test.")
    return len(rows)


# Función para insertar datos crudos del dataset covertype en la tabla
def insert_raw_diabetic_data(
    connection, table_name, df=None, batch_id=None, data_source=None
):
    """Inserta datos crudos de diabetes en la tabla raw con metadatos."""
    if df is None or df.empty:
        print(
            "No se cargaron datos desde el archivo fuente o el DataFrame está vacío. No se insertarán datos en PostgreSQL."
        )
        return
    else:
        print(f"Cargando datos: {len(df)} filas, {len(df.columns)} columnas...")

        # Columnas esperadas del dataset
        data_columns = [
            "encounter_id",
            "patient_nbr",
            "race",
            "gender",
            "age",
            "weight",
            "admission_type_id",
            "discharge_disposition_id",
            "admission_source_id",
            "time_in_hospital",
            "payer_code",
            "medical_specialty",
            "num_lab_procedures",
            "num_procedures",
            "num_medications",
            "number_outpatient",
            "number_emergency",
            "number_inpatient",
            "diag_1",
            "diag_2",
            "diag_3",
            "number_diagnoses",
            "max_glu_serum",
            "a1cresult",
            "metformin",
            "repaglinide",
            "nateglinide",
            "chlorpropamide",
            "glimepiride",
            "acetohexamide",
            "glipizide",
            "glyburide",
            "tolbutamide",
            "pioglitazone",
            "rosiglitazone",
            "acarbose",
            "miglitol",
            "troglitazone",
            "tolazamide",
            "examide",
            "citoglipton",
            "insulin",
            "glyburide-metformin",
            "glipizide-metformin",
            "glimepiride-pioglitazone",
            "metformin-rosiglitazone",
            "metformin-pioglitazone",
            "change",
            "diabetesmed",
            "readmitted",
        ]

        # Normalizar nombres de columnas del CSV
        df = df.copy()
        if "A1Cresult" in df.columns and "a1cresult" not in df.columns:
            df = df.rename(columns={"A1Cresult": "a1cresult"})
        if "diabetesMed" in df.columns and "diabetesmed" not in df.columns:
            df = df.rename(columns={"diabetesMed": "diabetesmed"})
        # Completar columnas faltantes con None
        for col in data_columns:
            if col not in df.columns:
                df[col] = None

        # Agregar metadatos de carga
        load_timestamp = datetime.utcnow()
        df["batch_id"] = batch_id
        df["load_timestamp"] = load_timestamp
        df["data_source"] = data_source
        df["record_status"] = "new"
        if "encounter_id" in df.columns:
            df["source_record_id"] = df["encounter_id"].astype(str)
        else:
            df["source_record_id"] = None

        # Calcular hash por fila para trazabilidad
        df["row_hash"] = (
            df[data_columns]
            .astype(str)
            .agg("|".join, axis=1)
            .apply(lambda value: hashlib.md5(value.encode("utf-8")).hexdigest())
        )

        # Reemplazar valores NaN con None para manejar NULLs correctamente en PostgreSQL
        df = df.replace({float("nan"): None})

        cursor = connection.cursor()

        # Preparar la consulta SQL para insertar datos
        insert_columns = [
            "batch_id",
            "load_timestamp",
            "data_source",
            "record_status",
            "source_record_id",
            "row_hash",
        ] + data_columns

        quoted_columns = [f'"{col}"' for col in insert_columns]
        placeholders = ", ".join(["%s"] * len(insert_columns))
        query = (
            f"INSERT INTO {table_name} (" + ", ".join(quoted_columns) + ")"
            f" VALUES ({placeholders})"
        )

        # Convertir DataFrame a lista de tuplas para la inserción
        val = [tuple(row) for row in df[insert_columns].values]

        # Ejecutar la consulta para múltiples filas
        cursor.executemany(query, val)

        # Confirmar la transacción
        connection.commit()
        print(f"Se cargaron {len(val)} registros en la tabla {table_name}")


# Función para cargar datos desde la base de datos PostgreSQL
def load_data_from_db(connection, table_name):
    """Carga una tabla completa en un DataFrame y usa id como indice."""
    # Ejecutar consulta
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
    split_table,
    batch_id,
    bucket,
    preprocessor_path,
    processed_table="diabetic_data_processed_batches",
    test_size=0.2,
    random_state=42,
):
    """Preprocesa datos, versiona el preprocessor y carga la tabla limpia."""
    # 1. Cargar datos crudos desde PostgreSQL con asignacion train/test
    query = f"""
        SELECT r.*, s.dataset
        FROM {raw_table} r
        LEFT JOIN {split_table} s
            ON r.source_record_id = s.source_record_id
    """
    df = pd.read_sql_query(query, connection)
    print(f"Datos crudos cargados: {len(df)} filas")

    # 1.1 Definir reglas para eliminar columnas con demasiados nulos o cardinalidad alta
    max_cardinality = 100
    max_null_ratio = 0.4
    # Columnas de control que no deben evaluarse como features
    excluded_cols = {
        "id",
        "batch_id",
        "load_timestamp",
        "data_source",
        "record_status",
        "source_record_id",
        "row_hash",
        "dataset",
    }
    # Candidatas para analisis de calidad (excluye target)
    candidate_cols = [
        col for col in df.columns if col not in excluded_cols and col != "readmitted"
    ]

    # Columnas con demasiados nulos
    high_null_cols = [
        col for col in candidate_cols if df[col].isna().mean() > max_null_ratio
    ]
    # Columnas con alta cardinalidad
    high_card_cols = [
        col for col in candidate_cols if df[col].nunique(dropna=True) > max_cardinality
    ]
    # Conjunto final de columnas a eliminar
    removed_cols = set(high_null_cols + high_card_cols)
    if removed_cols:
        print(
            "Columnas eliminadas por cardinalidad o nulos: "
            + ", ".join(sorted(removed_cols))
        )
        df = df.drop(columns=removed_cols)

    # 2. Filtrar registros sin asignacion de dataset
    missing_dataset = df["dataset"].isna().sum()
    if missing_dataset > 0:
        print(f"Filas sin dataset asignado: {missing_dataset}. Se omitiran.")
        df = df.dropna(subset=["dataset"])

    # 3. Definir columnas de caracteristicas y target
    target_col = "readmitted"
    # Variables numericas
    num_cols = [
        "time_in_hospital",
        "num_lab_procedures",
        "num_procedures",
        "num_medications",
        "number_outpatient",
        "number_emergency",
        "number_inpatient",
        "number_diagnoses",
    ]
    # Variables categoricas
    cat_cols = [
        "race",
        "gender",
        "age",
        "weight",
        "admission_type_id",
        "discharge_disposition_id",
        "admission_source_id",
        "payer_code",
        "medical_specialty",
        "diag_1",
        "diag_2",
        "diag_3",
        "max_glu_serum",
        "a1cresult",
        "metformin",
        "repaglinide",
        "nateglinide",
        "chlorpropamide",
        "glimepiride",
        "acetohexamide",
        "glipizide",
        "glyburide",
        "tolbutamide",
        "pioglitazone",
        "rosiglitazone",
        "acarbose",
        "miglitol",
        "troglitazone",
        "tolazamide",
        "examide",
        "citoglipton",
        "insulin",
        "glyburide-metformin",
        "glipizide-metformin",
        "glimepiride-pioglitazone",
        "metformin-rosiglitazone",
        "metformin-pioglitazone",
        "change",
        "diabetesmed",
    ]

    # Eliminar columnas descartadas por calidad
    num_cols = [col for col in num_cols if col not in removed_cols]
    cat_cols = [col for col in cat_cols if col not in removed_cols]

    # Unir lista final de features
    feature_cols = num_cols + cat_cols
    # Asegurar columnas faltantes con None
    for col in feature_cols + [target_col]:
        if col not in df.columns and col not in removed_cols:
            df[col] = None

    # 4. Separar caracteristicas y variable objetivo
    X = df[feature_cols].copy()
    y = df[target_col].copy()

    # 5. Separar train/test usando asignacion persistente
    train_mask = df["dataset"] == "train"
    test_mask = df["dataset"] == "test"
    X_train = X[train_mask]
    y_train = y[train_mask]
    X_test = X[test_mask]
    y_test = y[test_mask]

    print(f"Entrenamiento: {len(X_train)} filas, Prueba: {len(X_test)} filas")

    # 6. Crear pipeline de preprocesamiento
    # Imputacion y escalado para numericas
    numeric_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    # Imputacion y one-hot para categoricas
    categorical_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )

    # Combinar transformaciones
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_pipe, num_cols),
            ("cat", categorical_pipe, cat_cols),
        ],
        remainder="drop",
    )

    # 7. Ajustar el preprocesador solo con datos de entrenamiento
    preprocessor.fit(X_train)

    # 8. Transformar ambos conjuntos de datos
    X_train_p = preprocessor.transform(X_train)
    X_test_p = preprocessor.transform(X_test)

    print("Dimensiones tras el preprocesamiento:")
    print(f"  X_train_p: {X_train_p.shape}")
    print(f"  X_test_p:  {X_test_p.shape}")

    # 9. Guardar el preprocesador en MinIO (version por batch)
    minio_client = connect_to_minio()
    if not minio_client.list_buckets(Prefix=bucket)["Buckets"]:
        print(f"El bucket '{bucket}' no existe. Creando bucket...")
        minio_client.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' creado en MinIO")
    else:
        print(f"El bucket '{bucket}' ya existe en MinIO")

    # Generar version del preprocesador
    processed_at = datetime.utcnow()
    preprocessor_version = f"batch_{batch_id}_{processed_at.strftime('%Y%m%d%H%M%S')}"

    # Guardar localmente antes de subir
    local_dir = os.path.join(preprocessor_path, preprocessor_version)
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, "preprocessor.joblib")
    joblib.dump(preprocessor, local_path)
    print(f"Preprocesador guardado localmente en: {local_path}")

    # Subir artefacto a MinIO
    remote_key = f"preprocess/{preprocessor_version}/preprocessor.joblib"
    print(f"Subiendo preprocesador al bucket '{bucket}' en MinIO...")
    minio_client.upload_file(local_path, bucket, remote_key)
    print("Preprocesador subido a MinIO exitosamente")

    # 10. Crear DataFrames con los datos procesados
    n_features = X_train_p.shape[1]
    feature_cols_out = preprocessor.get_feature_names_out().tolist()
    print(f"Nombres de caracteristicas: {feature_cols_out}")

    # Sanitizar nombres para PostgreSQL
    sanitized_cols = [
        name.replace("__", "_").replace(" ", "_").replace("-", "_")
        for name in feature_cols_out
    ]

    # Construir dataset de entrenamiento
    df_train = pd.DataFrame(X_train_p, columns=sanitized_cols)
    df_train["dataset"] = "train"
    df_train[target_col] = y_train.values

    # Construir dataset de prueba
    df_test = pd.DataFrame(X_test_p, columns=sanitized_cols)
    df_test["dataset"] = "test"
    df_test[target_col] = y_test.values

    # Unir ambos conjuntos en un solo DataFrame
    df_processed = pd.concat([df_train, df_test], ignore_index=True)

    print(f"Datos procesados: {len(df_processed)} filas, {n_features} caracteristicas")

    # 11. Re-crear la tabla limpia en cada iteracion para soportar
    # cambios en columnas por nuevas categorias del one-hot encoding
    delete_table_if_exists(connection, cleaned_table)
    create_table_cleaned(connection, cleaned_table, sanitized_cols, target_col)

    # 12. Insertar datos procesados en la tabla limpia
    columns = ", ".join(
        [f'"{col}"' for col in sanitized_cols] + ["dataset", target_col]
    )
    placeholders = ", ".join(["%s"] * (n_features + 2))
    insert_query = f"INSERT INTO {cleaned_table} ({columns}) VALUES ({placeholders})"

    # Convertir DataFrame a lista de tuplas para insercion
    values = [tuple(row) for row in df_processed.values]
    cursor = connection.cursor()
    cursor.executemany(insert_query, values)
    connection.commit()
    cursor.close()
    print(f"Se insertaron {len(values)} filas en la tabla '{cleaned_table}'")

    # 13. Registrar versionamiento del batch procesado
    create_processed_batches_table(connection, processed_table)
    cursor = connection.cursor()
    cursor.execute(
        f"""
        INSERT INTO {processed_table}
            (batch_id, processed_at, preprocessor_version, rows_total, rows_train, rows_test)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            batch_id,
            processed_at,
            preprocessor_version,
            len(df_processed),
            len(df_train),
            len(df_test),
        ),
    )
    connection.commit()
    cursor.close()
    print(f"Version del batch registrada: {preprocessor_version}")