import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import DagModel, Variable
from airflow.utils.session import create_session
from db_utils import (
    connect_to_db,
    close_db_connection,
    create_table_raw,
    ensure_dataset_file,
    read_diabetes_batch,
    insert_raw_diabetic_data,
    assign_dataset_split,
    preprocess_and_insert,
)


DATA_BATCH_SIZE = 15000


def get_total_rows(data_filepath):
    with open(data_filepath, "r", encoding="utf-8") as file_handle:
        total_lines = sum(1 for _ in file_handle)
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
    # Asegura que el archivo fuente exista (o lo descarga).
    data_filepath = ensure_dataset_file()
    if not data_filepath or not os.path.isfile(data_filepath):
        raise FileNotFoundError("No se encontró el archivo fuente del dataset.")
    return data_filepath


def load_raw_batch(**context):
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

    df, next_offset = read_diabetes_batch(data_filepath, DATA_BATCH_SIZE, offset)
    if df.empty:
        print("No hay nuevos registros por cargar.")
        Variable.set("diabetic_data_complete", True)
        return

    # Identificador del lote para trazabilidad.
    batch_number = int(Variable.get("diabetic_data_batch_number", default_var=0)) + 1
    connection = connect_to_db()
    insert_raw_diabetic_data(
        connection,
        "diabetic_data_raw",
        df,
        batch_id=batch_number,
        data_source=data_filepath,
    )
    close_db_connection(connection)
    Variable.set("diabetic_data_offset", next_offset)
    Variable.set("diabetic_data_batch_number", batch_number)
    context["ti"].xcom_push(key="batch_number", value=batch_number)

    if next_offset >= total_rows:
        Variable.set("diabetic_data_complete", True)
        print("Todos los datos fueron ingestados.")
    else:
        Variable.set("diabetic_data_complete", False)
        print(f"Progreso de ingesta: {next_offset}/{total_rows}")


def assign_dataset(**context):
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
    batch_number = context["ti"].xcom_pull(
        task_ids="load_raw_batch", key="batch_number"
    )
    if not batch_number:
        print("No hay batch nuevo para preprocesar.")
        return
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


# def load_raw_data(**context):
#     """Carga datos crudos desde la API y los inserta en db.

#     Publica banderas en XCom para indicar si se cargaron datos nuevos
#     y si ya se recolectaron todos los batches.
#     """
#     df = get_data_from_api()

#     if df is None:
#         print("No fue posible obtener datos de la API en esta ejecución.")
#         context["ti"].xcom_push(key="new_data_loaded", value=False)
#         context["ti"].xcom_push(key="all_data_collected", value=False)
#         return

#     if not df.empty:
#         # Se obtuvieron datos, insertarlos en db
#         connection = connect_to_db()
#         insert_raw_covertype_data(connection, "covertype_raw", df)
#         close_db_connection(connection)
#         # Se cargaron datos nuevos, pero aún faltan batches por recolectar
#         context["ti"].xcom_push(key="new_data_loaded", value=True)
#         context["ti"].xcom_push(key="all_data_collected", value=False)
#     else:
#         # No hay datos nuevos (ya se recolectaron todos o hubo un error)
#         context["ti"].xcom_push(key="new_data_loaded", value=False)
#         context["ti"].xcom_push(key="all_data_collected", value=True)


# def check_should_preprocess(**context):
#     """Verifica si se debe proceder al preprocesamiento.

#     Retorna True cuando en esta ejecución se cargaron datos nuevos.
#     """
#     new_data_loaded = context["ti"].xcom_pull(
#         task_ids="load_raw_data", key="new_data_loaded"
#     )
#     all_data_collected = context["ti"].xcom_pull(
#         task_ids="load_raw_data", key="all_data_collected"
#     )

#     if new_data_loaded:
#         print("Hay datos nuevos: se ejecutará preprocesamiento en esta iteración.")
#         return True

#     if all_data_collected:
#         print(
#             "La API reportó que ya se recolectó toda la información mínima. Se omite preprocesamiento."
#         )
#         return False

#     print("No hubo datos nuevos en esta ejecución. Se omite preprocesamiento.")
#     return False


# def check_should_pause(**context):
#     """Pausa el DAG únicamente cuando la API reporta fin de recolección."""
#     all_data_collected = context["ti"].xcom_pull(
#         task_ids="load_raw_data", key="all_data_collected"
#     )

#     if all_data_collected:
#         print("No hay más datos por recolectar. Se pausará el DAG.")
#         return True

#     print("Aún hay datos por recolectar. El DAG continuará ejecutándose.")
#     return False


# def preprocess_data():
#     """Ejecuta el preprocesamiento de los datos crudos y los inserta en la tabla limpia."""
#     print("Preprocesando datos...")
#     connection = connect_to_db()
#     preprocess_and_insert(
#         connection,
#         "covertype_raw",
#         "covertype_cleaned",
#         bucket="covertype-project",
#         preprocessor_path="preprocessor",
#     )
#     close_db_connection(connection)


# def pause_dag():
#     """Pausa este DAG para que no se programen más ejecuciones automáticas."""
#     with create_session() as session:
#         dag_model = (
#             session.query(DagModel).filter(DagModel.dag_id == "data_dag").first()
#         )
#         if dag_model:
#             dag_model.is_paused = True
#             session.commit()
#             print(
#                 "El DAG 'data_dag' ha sido pausado. No habrá más ejecuciones programadas."
#             )


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

    # # Tarea 2: Cargar datos crudos desde la API
    # t2 = PythonOperator(task_id="load_raw_data", python_callable=load_raw_data)

    # # Verificación: ¿Se debe preprocesar en esta iteración?
    # t2_check_preprocess = ShortCircuitOperator(
    #     task_id="check_should_preprocess",
    #     python_callable=check_should_preprocess,
    # )

    # # Tarea 3: Preprocesar datos crudos e insertar en tabla limpia
    # t3 = PythonOperator(task_id="preprocess_data", python_callable=preprocess_data)

    # # Verificación: ¿Se debe pausar el DAG?
    # t2_check_pause = ShortCircuitOperator(
    #     task_id="check_should_pause",
    #     python_callable=check_should_pause,
    # )

    # # Tarea 4: Pausar el DAG para detener ejecuciones futuras
    # t4 = PythonOperator(task_id="pause_dag", python_callable=pause_dag)

    # Flujo: crear tablas -> cargar datos -> (si hay nuevos, preprocesar) y (si se completó, pausar)
    t1 >> t2 >> t3 >> t4 >> t5
    # t1 >> t2
    # t2 >> t2_check_preprocess >> t3
    # t2 >> t2_check_pause >> t4
