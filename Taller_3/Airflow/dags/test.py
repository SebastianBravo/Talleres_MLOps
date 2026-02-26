import os
import mysql.connector
from palmerpenguins import load_penguins
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def load_data():
    # Load the penguins dataset
    penguins = load_penguins()
    print(penguins.head())
    # Print column names and data types
    print(penguins.dtypes)

def connect_to_mysql():
    # Connect to MySQL database
    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )
    print("Connected to MySQL database")

with DAG(dag_id="test",
         description='primer dag',
         start_date=datetime(2026,2,24),
         schedule_interval="@once") as dag:
    
    t1 = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    t2 = PythonOperator(
        task_id="connect_to_mysql",
        python_callable=connect_to_mysql
    )

    t1 >> t2