import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.db_connection import connect_to_db, close_db_connection
from utils.db_schema import create_table_raw
from utils.dataset_io import ensure_dataset_file, read_diabetes_batch
from utils.ingestion import insert_raw_diabetic_data, assign_dataset_split
from utils.preprocess import preprocess_and_insert
from utils.training import train_and_register_models


DATA_BATCH_SIZE = 15000


def get_total_rows(data_filepath):
    """Cuenta filas del CSV excluyendo el encabezado."""
    # Leer el archivo y contar lineas
    with open(data_filepath, "r", encoding="utf-8") as file_handle:
        total_lines = sum(1 for _ in file_handle)
    # Restar el encabezado
    return max(total_lines - 1, 0)


def create_tables():
    """Crea las tablas necesarias en la base de datos si no existen."""
    # Consulta catalogo para evitar recrear tablas existentes.
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
    close_db_connection(connection)

    # Si no hay tablas, crear la tabla raw; de lo contrario, informar las existentes
    if not tables:
        print("Base de datos limpia: no hay tablas")
        connection = connect_to_db()
        print("Creando tabla diabetic_data_raw...")
        create_table_raw(connection, "diabetic_data_raw")
        close_db_connection(connection)
    else:
        print(f"Tablas existentes: {[t[0] for t in tables]}")
        print("No se crearán tablas nuevas para evitar conflictos")


def validate_source_file():
    """Valida la existencia del archivo fuente y devuelve su ruta."""
    # Asegura que el archivo fuente exista (o lo descarga).
    data_filepath = ensure_dataset_file()
    if not data_filepath or not os.path.isfile(data_filepath):
        raise FileNotFoundError("No se encontró el archivo fuente del dataset.")
    return data_filepath


def load_raw_batch(**context):
    """Lee el siguiente batch del CSV y lo inserta en la tabla raw."""
    # Lee el siguiente lote y lo inserta en la tabla raw.
    data_filepath = context["ti"].xcom_pull(task_ids="validate_source_file")
    if not data_filepath:
        print("No hay archivo fuente disponible. Se omite la carga.")
        return

    # Offset persistente para simulacion incremental.
    offset = int(Variable.get("diabetic_data_offset", default_var=0))
    total_rows = int(Variable.get("diabetic_data_total_rows", default_var=0))
    if total_rows == 0:
        total_rows = get_total_rows(data_filepath)
        Variable.set("diabetic_data_total_rows", total_rows)
        print(f"Total de filas detectadas en el CSV: {total_rows}")

    # Leer el siguiente lote
    df, next_offset = read_diabetes_batch(data_filepath, DATA_BATCH_SIZE, offset)
    if df.empty:
        print("No hay nuevos registros por cargar.")
        Variable.set("diabetic_data_complete", True)
        return

    # Identificador del lote para trazabilidad.
    batch_number = int(Variable.get("diabetic_data_batch_number", default_var=0)) + 1
    # Insertar batch en tabla raw
    connection = connect_to_db()
    insert_raw_diabetic_data(
        connection,
        "diabetic_data_raw",
        df,
        batch_id=batch_number,
        data_source=data_filepath,
    )
    close_db_connection(connection)
    # Actualizar variables de progreso
    Variable.set("diabetic_data_offset", next_offset)
    Variable.set("diabetic_data_batch_number", batch_number)
    context["ti"].xcom_push(key="batch_number", value=batch_number)

    # Actualizar estado de ingesta completa.
    if next_offset >= total_rows:
        Variable.set("diabetic_data_complete", True)
        print("Todos los datos fueron ingestados.")
    else:
        Variable.set("diabetic_data_complete", False)
        print(f"Progreso de ingesta: {next_offset}/{total_rows}")


def assign_dataset(**context):
    """Asigna deterministamente train/test para filas nuevas."""
    # Asignar split en base de datos
    connection = connect_to_db()
    assign_dataset_split(
        connection,
        raw_table="diabetic_data_raw",
        split_table="diabetic_data_split",
        test_size=0.2,
        random_state=42,
    )
    close_db_connection(connection)


def preprocess_batch(**context):
    """Preprocesa el batch actual y versiona el preprocesador."""
    # Recuperar batch actual desde XCom
    batch_number = context["ti"].xcom_pull(
        task_ids="load_raw_batch", key="batch_number"
    )
    if not batch_number:
        print("No hay batch nuevo para preprocesar.")
        return
    # Ejecutar preprocesamiento y versionado
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
    close_db_connection(connection)


def train_models(**context):
    """Entrena modelos con datos procesados y registra en MLflow."""
    # Entrenar solo si hay un batch nuevo
    batch_number = context["ti"].xcom_pull(
        task_ids="load_raw_batch", key="batch_number"
    )
    if not batch_number:
        print("No hay batch nuevo para entrenar.")
        return

    last_trained = int(Variable.get("diabetic_last_trained_batch", default_var=0))
    if batch_number <= last_trained:
        print("El batch ya fue entrenado previamente. Se omite reentrenamiento.")
        return

    # Entrenar y promover el mejor modelo
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
    close_db_connection(connection)
    Variable.set("diabetic_last_trained_batch", batch_number)


# Definición del DAG principal
with DAG(
    dag_id="data_dag",
    description="DAG para recolectar datos desde la API cada 5 minutos, preprocesarlos y pausarse automáticamente",
    # schedule_interval="*/1 * * * *",
    schedule_interval=timedelta(seconds=20),  # Cada 20 segundos para pruebas
    start_date=datetime(2026, 2, 24),
    max_active_runs=1,  # Solo una ejecución activa a la vez
    catchup=False,  # No ejecutar ejecuciones pasadas
) as dag:

    # Tarea 1: Crear tablas si no existen
    t1 = PythonOperator(task_id="create_tables", python_callable=create_tables)

    # Tarea 2: Validar disponibilidad del archivo fuente
    t2 = PythonOperator(
        task_id="validate_source_file", python_callable=validate_source_file
    )

    # Tarea 3: Carga incremental por lotes
    t3 = PythonOperator(task_id="load_raw_batch", python_callable=load_raw_batch)

    # Tarea 4: Asignar datasets train/test
    t4 = PythonOperator(task_id="assign_dataset", python_callable=assign_dataset)

    # Tarea 5: Preprocesar y versionar batch
    t5 = PythonOperator(task_id="preprocess_batch", python_callable=preprocess_batch)

    # Tarea 6: Entrenar y registrar modelos en MLflow
    t6 = PythonOperator(task_id="train_models", python_callable=train_models)

    # Flujo principal: crear tablas -> validar archivo -> cargar batch -> asignar split -> preprocesar -> entrenar
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
