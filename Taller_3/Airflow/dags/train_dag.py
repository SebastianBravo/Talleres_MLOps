from db_utils import (
    connect_to_mysql,
    close_mysql_connection,
    delete_table_if_exists,
    create_table_raw,
    create_table_cleaned,
    insert_raw_penguin_data,
    load_data_from_mysql,
)
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator


def clean_database():
    connection = connect_to_mysql()
    delete_table_if_exists(connection, "penguins_raw")
    delete_table_if_exists(connection, "penguins_cleaned")
    close_mysql_connection(connection)


def load_penguin_data():
    connection = connect_to_mysql()
    create_table_raw(connection, "penguins_raw")
    insert_raw_penguin_data(connection, "penguins_raw")
    close_mysql_connection(connection)


def clean_and_load_data():
    connection = connect_to_mysql()
    create_table_cleaned(connection, "penguins_cleaned")
    df = load_data_from_mysql(connection, "penguins_raw")

    print("Data loaded from MySQL:")
    print(df.head())
    print(df.dtypes)
    close_mysql_connection(connection)


default_args = {
    "start_date": datetime(2026, 2, 24),
    # 'depends_on_past': True,
}

with DAG(
    dag_id="train_dag",
    description="DAG para entrenar modelo de penguins",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@once",
) as dag:

    t1 = PythonOperator(task_id="clean_database", python_callable=clean_database)

    t2 = PythonOperator(task_id="load_penguin_data", python_callable=load_penguin_data)

    t3 = PythonOperator(task_id="clean_and_load_data", python_callable=clean_and_load_data)

    t1 >> t2 >> t3
