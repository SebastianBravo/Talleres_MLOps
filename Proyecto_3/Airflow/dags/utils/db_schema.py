from .db_connection import execute_query


def create_table_raw(connection, table_name="property_raw"):
    """Creates the raw data table for real estate property listings."""
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        batch_id INTEGER NOT NULL,
        batch_record_id INTEGER,
        load_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
        record_status VARCHAR(16) DEFAULT 'active',
        row_hash VARCHAR(64),
        brokered_by TEXT,
        status TEXT,
        price DOUBLE PRECISION,
        bed DOUBLE PRECISION,
        bath DOUBLE PRECISION,
        acre_lot DOUBLE PRECISION,
        street TEXT,
        city TEXT,
        state TEXT,
        zip_code TEXT,
        house_size DOUBLE PRECISION,
        prev_sold_date TEXT
    )
    """
    execute_query(connection, query)
    print(f"Table {table_name} created/verified successfully")


def create_table_clean(connection, table_name, feature_names, target_name="price"):
    """
    Creates the clean data table with dynamic feature columns and a
    preprocessor_version column that tracks which preprocessing run produced each row.

    The table is append-only: rows from all preprocessing versions coexist,
    and training always filters to the latest preprocessor_version.
    """
    sanitized_cols = [
        name.replace("__", "_").replace(" ", "_").replace("-", "_")
        for name in feature_names
    ]
    feature_columns = ", ".join([f'"{col}" DOUBLE PRECISION' for col in sanitized_cols])
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        batch_id INTEGER NOT NULL,
        source_record_id INTEGER,
        dataset VARCHAR(8) NOT NULL,
        preprocessor_version VARCHAR(256) NOT NULL,
        {feature_columns},
        {target_name} DOUBLE PRECISION NOT NULL
    )
    """
    execute_query(connection, query)
    # Migration guard: add column to tables created before versioning was introduced.
    # IF NOT EXISTS makes this a no-op on newly created tables.
    execute_query(
        connection,
        f"ALTER TABLE {table_name} "
        f"ADD COLUMN IF NOT EXISTS preprocessor_version VARCHAR(256)",
    )
    print(f"Table {table_name} created/verified with {len(feature_names)} feature columns")



def create_batch_audit_table(connection, table_name="batch_audit"):
    """Creates the audit table that tracks each batch execution end-to-end."""
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        batch_id INTEGER UNIQUE NOT NULL,
        run_id VARCHAR(128),
        fetched_at TIMESTAMP DEFAULT NOW(),
        records_received INTEGER DEFAULT 0,
        records_stored INTEGER DEFAULT 0,
        schema_valid BOOLEAN,
        schema_issues TEXT,
        quality_score DOUBLE PRECISION,
        quality_issues JSONB,
        new_categories_detected JSONB,
        drift_detected BOOLEAN,
        drift_details JSONB,
        should_train BOOLEAN,
        training_reasons JSONB,
        preprocessor_version VARCHAR(256),
        model_run_id VARCHAR(128),
        model_version VARCHAR(64),
        model_promoted BOOLEAN,
        promotion_reason TEXT,
        candidate_mae DOUBLE PRECISION,
        candidate_rmse DOUBLE PRECISION,
        production_mae DOUBLE PRECISION,
        production_rmse DOUBLE PRECISION,
        execution_status VARCHAR(32) DEFAULT 'running',
        completed_at TIMESTAMP
    )
    """
    execute_query(connection, query)
    print(f"Table {table_name} created/verified successfully")


def create_inference_logs_table(connection, table_name="inference_logs"):
    """Creates the inference request audit log table."""
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        request_id UUID DEFAULT gen_random_uuid(),
        requested_at TIMESTAMP NOT NULL DEFAULT NOW(),
        input_data JSONB NOT NULL,
        prediction DOUBLE PRECISION,
        model_name VARCHAR(128),
        model_version VARCHAR(64),
        model_alias VARCHAR(64),
        response_time_ms DOUBLE PRECISION,
        status VARCHAR(16) DEFAULT 'success',
        error_message TEXT
    )
    """
    execute_query(connection, query)
    print(f"Table {table_name} created/verified successfully")
