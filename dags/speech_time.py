from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s
from datetime import timedelta
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'speech_time',
    default_args=default_args,
    description='A DAG to calculate etl_speech_time',
    schedule_interval=None, # '@hourly',  # Runs every hour
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # print_envvars = PythonOperator(task_id='print_envvars', python_callable=print_envvars)
    spark_job = SparkKubernetesOperator(
        task_id="speech_time",
        namespace='airflow',
        application_file='speech_time.yaml',
        kubernetes_conn_id='spark-cluster-connection',
        params={
        "S3_ACCESS_KEY": os.getenv("AWS_S3_ACCESS_KEY"),
        "S3_SECRET_KEY": os.getenv("AWS_S3_SECRET_KEY"),
        "S3_END_POINT": os.getenv("AWS_S3_END_POINT")
    }
)

# Define the task sequence
# print_envvars >> spark_job
spark_job
