import hashlib
from datetime import datetime

import pandas as pd

from .db_schema import create_split_table


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


def insert_raw_diabetic_data(
    connection, table_name, df=None, batch_id=None, data_source=None
):
    """Inserta datos crudos de diabetes en la tabla raw con metadatos."""
    if df is None or df.empty:
        print(
            "No se cargaron datos desde el archivo fuente o el DataFrame está vacío. No se insertarán datos en PostgreSQL."
        )
        return

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
