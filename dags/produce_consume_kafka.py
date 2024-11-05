from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def print_hello():
    print('Hello world from first Airflow DAG!')

dag = DAG('produce_consume_kafka', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='hello_k8s', python_callable=print_hello, dag=dag)

hello_operator
