import os
from datetime import datetime

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .db_schema import (
    delete_table_if_exists,
    create_table_cleaned,
    create_processed_batches_table,
)
from .storage_utils import connect_to_minio


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
