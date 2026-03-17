from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import DagModel
from airflow.utils.session import create_session
from db_utils import (
    connect_to_mysql,
    close_mysql_connection,
    create_table_raw,
    get_data_from_api,
    insert_raw_covertype_data,
    preprocess_and_insert,
)


def create_tables():
    """Crea las tablas necesarias en la base de datos si no existen."""
    # Verificar qué tablas existen actualmente
    connection = connect_to_mysql()
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    close_mysql_connection(connection)

    # Si no hay tablas, crear la tabla raw; de lo contrario, informar las existentes
    if not tables:
        print("Base de datos limpia: no hay tablas")
        connection = connect_to_mysql()
        print("Creando tabla covertype_raw...")
        create_table_raw(connection, "covertype_raw")
        close_mysql_connection(connection)
    else:
        print(f"Tablas existentes: {[t[0] for t in tables]}")
        print("No se crearán tablas nuevas para evitar conflictos")


def load_raw_data(**context):
    """Carga datos crudos desde la API y los inserta en MySQL.

    Publica banderas en XCom para indicar si se cargaron datos nuevos
    y si ya se recolectaron todos los batches.
    """
    df = get_data_from_api()

    if df is not None and not df.empty:
        # Se obtuvieron datos, insertarlos en MySQL
        connection = connect_to_mysql()
        insert_raw_covertype_data(connection, "covertype_raw", df)
        close_mysql_connection(connection)
        # Se cargaron datos nuevos, pero aún faltan batches por recolectar
        context["ti"].xcom_push(key="new_data_loaded", value=True)
        context["ti"].xcom_push(key="all_data_collected", value=False)
    else:
        # No hay datos nuevos (ya se recolectaron todos o hubo un error)
        context["ti"].xcom_push(key="new_data_loaded", value=False)
        context["ti"].xcom_push(key="all_data_collected", value=True)


def check_should_preprocess(**context):
    """Verifica si se debe proceder al preprocesamiento.

    Solo retorna True cuando se cumplan TODAS estas condiciones:
    - La API indica que ya se recolectaron todos los datos.
    - No se cargaron datos nuevos en esta ejecución (último batch ya fue insertado antes).
    - La tabla limpia aún no existe o está vacía (no se ha preprocesado antes).

    En ejecuciones posteriores, al ya existir la tabla con datos, se omite el preprocesamiento.
    """
    new_data_loaded = context["ti"].xcom_pull(
        task_ids="load_raw_data", key="new_data_loaded"
    )
    all_data_collected = context["ti"].xcom_pull(
        task_ids="load_raw_data", key="all_data_collected"
    )

    if new_data_loaded:
        # Aún se están recolectando batches, no preprocesar todavía
        print(
            "Se cargaron datos nuevos. Esperando a que se recolecten todos los batches antes de preprocesar."
        )
        return False
    elif all_data_collected and not new_data_loaded:
        # Verificar si el preprocesamiento ya se realizó revisando la tabla limpia
        connection = connect_to_mysql()
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES LIKE 'covertype_cleaned'")
        cleaned_exists = cursor.fetchone()

        if cleaned_exists:
            # La tabla existe, verificar si tiene datos
            cursor.execute("SELECT COUNT(*) FROM covertype_cleaned")
            row_count = cursor.fetchone()[0]
            close_mysql_connection(connection)

            if row_count > 0:
                print(
                    f"Todos los datos ya fueron recolectados y preprocesados ({row_count} filas en covertype_cleaned). Omitiendo."
                )
                return False
            else:
                print(
                    "La tabla limpia existe pero está vacía. Procediendo al preprocesamiento."
                )
                return True
        else:
            close_mysql_connection(connection)
            print(
                "Todos los datos fueron recolectados. Procediendo al preprocesamiento."
            )
            return True
    else:
        print("No hay datos disponibles. Omitiendo preprocesamiento.")
        return False


def preprocess_data():
    """Ejecuta el preprocesamiento de los datos crudos y los inserta en la tabla limpia."""
    print("Preprocesando datos...")
    connection = connect_to_mysql()
    preprocess_and_insert(
        connection,
        "covertype_raw",
        "covertype_cleaned",
        bucket="covertype-project",
        preprocessor_path="preprocessor",
    )
    close_mysql_connection(connection)


def pause_dag():
    """Pausa este DAG para que no se programen más ejecuciones automáticas."""
    with create_session() as session:
        dag_model = (
            session.query(DagModel).filter(DagModel.dag_id == "data_dag").first()
        )
        if dag_model:
            dag_model.is_paused = True
            session.commit()
            print(
                "El DAG 'data_dag' ha sido pausado. No habrá más ejecuciones programadas."
            )


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

    # Tarea 2: Cargar datos crudos desde la API
    t2 = PythonOperator(task_id="load_raw_data", python_callable=load_raw_data)

    # Verificación: ¿Se debe preprocesar? (Omite tareas posteriores si no)
    t2_check = ShortCircuitOperator(
        task_id="check_should_preprocess",
        python_callable=check_should_preprocess,
    )

    # Tarea 3: Preprocesar datos crudos e insertar en tabla limpia
    t3 = PythonOperator(task_id="preprocess_data", python_callable=preprocess_data)

    # Tarea 4: Pausar el DAG para detener ejecuciones futuras
    t4 = PythonOperator(task_id="pause_dag", python_callable=pause_dag)

    # Flujo: crear tablas → cargar datos → verificar → preprocesar → pausar DAG
    t1 >> t2 >> t2_check >> t3 >> t4