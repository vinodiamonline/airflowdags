from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello, Vinod!")

dag = DAG(
    'hello_vin',
    description='A simple DAG that prints Hello Vinod',
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
)

task = PythonOperator(
    task_id='say_hello_task',
    python_callable=say_hello,
    dag=dag,
)

task