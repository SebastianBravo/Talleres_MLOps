from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from db_utils import (
    connect_to_mysql,
    close_mysql_connection,
    delete_table_if_exists,
    create_table_raw,
    insert_raw_penguin_data,
    load_data_from_mysql,
    preprocess_and_insert,
)
from train_utils import MODEL_CONFIGS, train_and_evaluate, save_model

MODELS_PATH = "/opt/airflow/models"


def clean_database():
    connection = connect_to_mysql()
    delete_table_if_exists(connection, "penguins_raw")
    delete_table_if_exists(connection, "penguins_cleaned")
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    cursor.close()
    if not tables:
        print("Base de datos limpia: no hay tablas restantes")
    else:
        print(f"Tablas restantes: {[t[0] for t in tables]}")
    close_mysql_connection(connection)


def load_raw_data():
    connection = connect_to_mysql()
    create_table_raw(connection, "penguins_raw")
    insert_raw_penguin_data(connection, "penguins_raw")
    df = load_data_from_mysql(connection, "penguins_raw")
    print(f"Datos cargados: {len(df)} filas, {len(df.columns)} columnas")
    print(df.head())
    close_mysql_connection(connection)


def preprocess_data():
    connection = connect_to_mysql()
    preprocess_and_insert(connection, "penguins_raw", "penguins_cleaned", MODELS_PATH)
    close_mysql_connection(connection)


def train_models():
    connection = connect_to_mysql()
    df_all = load_data_from_mysql(connection, "penguins_cleaned")
    close_mysql_connection(connection)

    # Filter by dataset split
    df_train = df_all[df_all["dataset"] == "train"]
    df_test = df_all[df_all["dataset"] == "test"]

    print(f"Datos para entrenamiento: {len(df_train)} filas")
    print(f"Datos para prueba: {len(df_test)} filas")

    # Separate features and target
    X_train = df_train.drop(columns=["species", "dataset"])
    y_train = df_train["species"]
    X_test = df_test.drop(columns=["species", "dataset"])
    y_test = df_test["species"]

    for name, model in MODEL_CONFIGS.items():
        print(f"\n{'='*50}")
        print(f"Entrenando: {name}")
        print(f"{'='*50}")
        trained_model = train_and_evaluate(model, X_train, X_test, y_train, y_test)
        save_model(trained_model, MODELS_PATH, name)


with DAG(
    dag_id="train_dag",
    description="DAG para entrenar modelo de penguins",
    start_date=datetime(2026, 2, 24),
    schedule_interval="@once",
    max_active_runs=1,
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="clean_database", python_callable=clean_database)
    t2 = PythonOperator(task_id="load_raw_data", python_callable=load_raw_data)
    t3 = PythonOperator(task_id="preprocess_data", python_callable=preprocess_data)
    t4 = PythonOperator(task_id="train_models", python_callable=train_models)

    t1 >> t2 >> t3 >> t4