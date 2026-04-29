from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import DagModel
from airflow.utils.session import create_session
from db_utils import (
    connect_to_db,
    close_db_connection,
    create_table_raw,
    get_data_from_api,
    insert_raw_covertype_data,
    preprocess_and_insert,
)


def create_tables():
    """Crea las tablas necesarias en la base de datos si no existen."""
    # Verificar qué tablas existen actualmente
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
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
    t1
    # t1 >> t2
    # t2 >> t2_check_preprocess >> t3
    # t2 >> t2_check_pause >> t4
