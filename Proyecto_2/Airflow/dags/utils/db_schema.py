from .db_connection import execute_query


def delete_table_if_exists(connection, table_name):
    """Elimina una tabla si existe en la base de datos."""
    # Ejecutar drop de forma segura
    query = f"DROP TABLE IF EXISTS {table_name}"
    execute_query(connection, query)
    print(f"Tabla {table_name} eliminada (si existía)")


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


def create_inference_logs_table(connection, table_name="inference_logs"):
    """Crea la tabla de registros de inferencias realizadas por la API."""
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        request_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        requested_at TIMESTAMP NOT NULL,
        input_data JSONB NOT NULL,
        prediction VARCHAR(16) NOT NULL,
        probabilities JSONB,
        model_name VARCHAR(128),
        model_version VARCHAR(32),
        model_alias VARCHAR(64),
        response_time_ms FLOAT NOT NULL
    )
    """
    execute_query(connection, query)
    print(f"Tabla {table_name} verificada/creada exitosamente")


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
