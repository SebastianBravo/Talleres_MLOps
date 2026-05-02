from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


DATA_BATCH_SIZE = 15000
DAG_ID = "data_dag"


def get_total_rows(data_filepath):
    """Cuenta filas del CSV excluyendo el encabezado."""
    with open(data_filepath, "r", encoding="utf-8") as file_handle:
        total_lines = sum(1 for _ in file_handle)

    return max(total_lines - 1, 0)


def create_tables():
    """Crea las tablas necesarias en la base de datos si no existen."""

    from utils.db_connection import connect_to_db, close_db_connection
    from utils.db_schema import create_table_raw

    connection = None

    try:
        connection = connect_to_db()
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
            """
        )

        tables = cursor.fetchall()

    finally:
        if connection is not None:
            close_db_connection(connection)

    if not tables:
        print("Base de datos limpia: no hay tablas")

        connection = None

        try:
            connection = connect_to_db()
            print("Creando tabla diabetic_data_raw...")
            create_table_raw(connection, "diabetic_data_raw")

        finally:
            if connection is not None:
                close_db_connection(connection)

    else:
        print(f"Tablas existentes: {[table[0] for table in tables]}")
        print("No se crearán tablas nuevas para evitar conflictos")


def validate_source_file():
    """Valida la existencia del archivo fuente y devuelve su ruta."""

    import os
    from utils.dataset_io import ensure_dataset_file

    data_filepath = ensure_dataset_file()

    if not data_filepath or not os.path.isfile(data_filepath):
        raise FileNotFoundError("No se encontró el archivo fuente del dataset.")

    return data_filepath


def load_raw_batch(**context):
    """Lee el siguiente batch del CSV y lo inserta en la tabla raw."""

    from airflow.models import Variable
    from utils.db_connection import connect_to_db, close_db_connection
    from utils.dataset_io import read_diabetes_batch
    from utils.ingestion import insert_raw_diabetic_data

    ti = context["ti"]

    data_filepath = ti.xcom_pull(task_ids="validate_source_file")

    if not data_filepath:
        print("No hay archivo fuente disponible. Se omite la carga.")
        return

    offset = int(Variable.get("diabetic_data_offset", default_var=0))
    total_rows = int(Variable.get("diabetic_data_total_rows", default_var=0))

    if total_rows == 0:
        total_rows = get_total_rows(data_filepath)
        Variable.set("diabetic_data_total_rows", total_rows)
        print(f"Total de filas detectadas en el CSV: {total_rows}")

    df, next_offset = read_diabetes_batch(
        data_filepath,
        DATA_BATCH_SIZE,
        offset,
    )

    if df.empty:
        print("No hay nuevos registros por cargar.")
        Variable.set("diabetic_data_complete", "True")
        return

    batch_number = int(Variable.get("diabetic_data_batch_number", default_var=0)) + 1

    connection = None

    try:
        connection = connect_to_db()

        insert_raw_diabetic_data(
            connection,
            "diabetic_data_raw",
            df,
            batch_id=batch_number,
            data_source=data_filepath,
        )

    finally:
        if connection is not None:
            close_db_connection(connection)

    Variable.set("diabetic_data_offset", next_offset)
    Variable.set("diabetic_data_batch_number", batch_number)

    ti.xcom_push(key="batch_number", value=batch_number)

    if next_offset >= total_rows:
        Variable.set("diabetic_data_complete", "True")
        print("Todos los datos fueron ingestados.")
    else:
        Variable.set("diabetic_data_complete", "False")
        print(f"Progreso de ingesta: {next_offset}/{total_rows}")


def assign_dataset(**context):
    """Asigna deterministamente train/test para filas nuevas."""

    from utils.db_connection import connect_to_db, close_db_connection
    from utils.ingestion import assign_dataset_split

    connection = None

    try:
        connection = connect_to_db()

        assign_dataset_split(
            connection,
            raw_table="diabetic_data_raw",
            split_table="diabetic_data_split",
            test_size=0.2,
            random_state=42,
        )

    finally:
        if connection is not None:
            close_db_connection(connection)


def preprocess_batch(**context):
    """Preprocesa el batch actual y versiona el preprocesador."""

    from utils.db_connection import connect_to_db, close_db_connection
    from utils.preprocess import preprocess_and_insert

    ti = context["ti"]

    batch_number = ti.xcom_pull(
        task_ids="load_raw_batch",
        key="batch_number",
    )

    if not batch_number:
        print("No hay batch nuevo para preprocesar.")
        return

    connection = None

    try:
        connection = connect_to_db()

        preprocess_and_insert(
            connection,
            raw_table="diabetic_data_raw",
            cleaned_table="diabetic_data_cleaned",
            split_table="diabetic_data_split",
            batch_id=batch_number,
            bucket="diabetic-project",
            preprocessor_path="preprocessor",
        )

    finally:
        if connection is not None:
            close_db_connection(connection)


def train_models(**context):
    """Entrena modelos con datos procesados y registra en MLflow."""

    from airflow.models import Variable
    from utils.db_connection import connect_to_db, close_db_connection
    from utils.training import train_and_register_models

    ti = context["ti"]

    batch_number = ti.xcom_pull(
        task_ids="load_raw_batch",
        key="batch_number",
    )

    if not batch_number:
        print("No hay batch nuevo para entrenar.")
        return

    last_trained = int(Variable.get("diabetic_last_trained_batch", default_var=0))

    if int(batch_number) <= last_trained:
        print("El batch ya fue entrenado previamente. Se omite reentrenamiento.")
        return

    connection = None

    try:
        connection = connect_to_db()

        train_and_register_models(
            connection,
            cleaned_table="diabetic_data_cleaned",
            batch_id=batch_number,
            preprocessor_bucket="diabetic-project",
            experiment_name="diabetic-readmission",
            registered_model_name="diabetic-readmission-model",
            primary_metric_name="recall_lt30",
        )

    finally:
        if connection is not None:
            close_db_connection(connection)

    Variable.set("diabetic_last_trained_batch", batch_number)


def reload_api_model(**context):
    """Recarga el modelo productivo en la API de inferencia."""

    from airflow.models import Variable
    from utils.inference_api import reload_inference_api

    ti = context["ti"]

    batch_number = ti.xcom_pull(
        task_ids="load_raw_batch",
        key="batch_number",
    )

    if not batch_number:
        print("No hay batch nuevo. Se omite recarga de la API.")
        return

    last_trained = int(Variable.get("diabetic_last_trained_batch", default_var=0))

    if int(batch_number) != last_trained:
        print(
            "El batch actual no coincide con el último batch entrenado. "
            "Se omite recarga de la API."
        )
        return

    reload_inference_api()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id=DAG_ID,
    description=(
        "DAG para recolectar datos, cargarlos por lotes, "
        "preprocesarlos, entrenar modelos y recargar la API de inferencia."
    ),
    default_args=default_args,
    schedule=timedelta(minutes=2),  # Solo para pruebas
    start_date=datetime(2026, 2, 24),
    max_active_runs=1,
    catchup=False,
    tags=["diabetes", "ml", "training", "inference"],
) as dag:
    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    validate_source_file_task = PythonOperator(
        task_id="validate_source_file",
        python_callable=validate_source_file,
    )

    load_raw_batch_task = PythonOperator(
        task_id="load_raw_batch",
        python_callable=load_raw_batch,
    )

    assign_dataset_task = PythonOperator(
        task_id="assign_dataset",
        python_callable=assign_dataset,
    )

    preprocess_batch_task = PythonOperator(
        task_id="preprocess_batch",
        python_callable=preprocess_batch,
    )

    train_models_task = PythonOperator(
        task_id="train_models",
        python_callable=train_models,
    )

    reload_api_model_task = PythonOperator(
        task_id="reload_api_model",
        python_callable=reload_api_model,
    )

    (
        create_tables_task
        >> validate_source_file_task
        >> load_raw_batch_task
        >> assign_dataset_task
        >> preprocess_batch_task
        >> train_models_task
        >> reload_api_model_task
    )