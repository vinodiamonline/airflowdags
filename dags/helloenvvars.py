from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def print_hello():
    print('Hello env vars' + Variable.get("MY_S3_BUCKET"))
    print('Hello env vars' + Variable.get("S3_ENDPOINT"))
    print('Hello env vars' + Variable.get("VIN_S3_BUCKET"))
    print('Hello env vars' + Variable.get("S3_DOCKER_ENDPOINT"))

dag = DAG('helloenvvars', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='helloenvvars', python_callable=print_hello, dag=dag)

hello_operator
