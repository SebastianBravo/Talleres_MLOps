import mysql.connector
from palmerpenguins import load_penguins
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def load_data():
    # Load the penguins dataset
    penguins = load_penguins()
    print(penguins.head())

with DAG(dag_id="test",
         description='primer dag',
         start_date=datetime(2026,2,24),
         schedule_interval="@once") as dag:
    
    t1 = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )
    
    t1