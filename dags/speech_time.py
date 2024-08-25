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

S3_ACCESS_KEY=os.getenv("AWS_S3_ACCESS_KEY")
S3_SECRET_KEY=os.getenv("AWS_S3_SECRET_KEY")
S3_ENDPOINT=os.getenv("AWS_S3_END_POINT")
BRONZE_TABLE_PATH='s3a://warehouse/micrawdata1'
SILVER_TABLE_PATH='s3a://warehouse/tbl_engagement_speech_silver'
TIME_WINDOW_IN_SECS='3456000'
CLASSNAME="speech"

def print_envvars():
    print("Hello etl speech time!")
    print("S3_ACCESS_KEY length = " + str(len(S3_ACCESS_KEY)))
    print("S3_SECRET_KEY length =  " + str(len(S3_SECRET_KEY)))
    print("S3_ENDPOINT = " + S3_ENDPOINT)
   
# Define the DAG
with DAG(
    'speech_time',
    default_args=default_args,
    description='A DAG to calculate etl_speech_time',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    print_envvars = PythonOperator(task_id='print_envvars', python_callable=print_envvars)
    spark_job = SparkKubernetesOperator(
        task_id="speech_time",
        namespace='airflow',
        application_file='speech_time.yaml',
        kubernetes_conn_id='kind-spark-cluster'
)

# Define the task sequence
print_envvars >> spark_job
