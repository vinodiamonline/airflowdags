from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def say_hello():
    print("Hello, Vinod Celary and Kubernetes!")

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
task1= BashOperator(
        task_id='Test_kubernetes_executor',
        bash_command='echo Kubernetes',
        queue = 'kubernetes'
    )
task2 = BashOperator(
        task_id='Test_Celery_Executor',
        bash_command='echo Celery',
    )

task
task1
task2
