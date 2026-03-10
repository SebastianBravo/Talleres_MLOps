from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import DagModel
from airflow.utils.session import create_session
from db_utils import (
    connect_to_mysql,
    close_mysql_connection,
    delete_table_if_exists,
    create_table_raw,
    get_data_from_api,
    insert_raw_covertype_data,
    preprocess_and_insert,
)

def create_tables():
    # Validate tables
    connection = connect_to_mysql()
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    close_mysql_connection(connection)

    # Create tables if not exist, otherwise print existing tables
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
    """Load raw data from API. Pushes flags to XCom."""
    df = get_data_from_api()

    if df is not None and not df.empty:
        # Data was returned, insert into MySQL
        connection = connect_to_mysql()
        insert_raw_covertype_data(connection, "covertype_raw", df)
        close_mysql_connection(connection)
        # New data was loaded, but not all batches collected yet
        context['ti'].xcom_push(key='new_data_loaded', value=True)
        context['ti'].xcom_push(key='all_data_collected', value=False)
    else:
        # No new data (either all collected or error)
        context['ti'].xcom_push(key='new_data_loaded', value=False)
        context['ti'].xcom_push(key='all_data_collected', value=True)

def check_should_preprocess(**context):
    """Only proceed to preprocessing if all data is collected AND new data was just loaded in this run.
    
    Returns True only on the FIRST run where the API says all data is collected,
    meaning the last batch was just inserted. On subsequent runs, no new data
    is loaded so we skip.
    """
    new_data_loaded = context['ti'].xcom_pull(task_ids='load_raw_data', key='new_data_loaded')
    all_data_collected = context['ti'].xcom_pull(task_ids='load_raw_data', key='all_data_collected')

    if new_data_loaded:
        print("New data was loaded. Waiting for all batches to be collected before preprocessing.")
        return False
    elif all_data_collected and not new_data_loaded:
        # Check if preprocessing was already done by looking at the cleaned table
        connection = connect_to_mysql()
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES LIKE 'covertype_cleaned'")
        cleaned_exists = cursor.fetchone()
        close_mysql_connection(connection)

        if cleaned_exists:
            print("All data already collected and preprocessed. Skipping.")
            return False
        else:
            print("All data collected. Proceeding to preprocessing.")
            return True
    else:
        print("No data available. Skipping preprocessing.")
        return False

def preprocess_data():
    print("Preprocesando datos...")
    connection = connect_to_mysql()
    preprocess_and_insert(connection, "covertype_raw", "covertype_cleaned", "")
    close_mysql_connection(connection)

def pause_dag():
    """Pause this DAG so it stops scheduling new runs."""
    with create_session() as session:
        dag_model = session.query(DagModel).filter(DagModel.dag_id == "data_dag").first()
        if dag_model:
            dag_model.is_paused = True
            session.commit()
            print("DAG 'data_dag' has been paused. No more scheduled runs.")

with DAG(
    dag_id="data_dag",
    description="DAG para cargar datos desde API cada 5 minutos",
    # schedule_interval="*/1 * * * *",
    schedule_interval=timedelta(seconds=20),
    start_date=datetime(2026, 2, 24),
    max_active_runs=1,
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="create_tables", python_callable=create_tables)
    t2 = PythonOperator(task_id="load_raw_data", python_callable=load_raw_data)

    t2_check = ShortCircuitOperator(
        task_id="check_should_preprocess",
        python_callable=check_should_preprocess,
    )

    t3 = PythonOperator(task_id="preprocess_data", python_callable=preprocess_data)
    t4 = PythonOperator(task_id="pause_dag", python_callable=pause_dag)

    t1 >> t2 >> t2_check >> t3 >> t4