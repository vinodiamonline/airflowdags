from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
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
BRONZE_TABLE_PATH='s3a://warehouse/micrawdata3'
SILVER_TABLE_PATH='s3a://warehouse/tbl_engagement_speech_silver'
TIME_WINDOW_IN_SECS='3456000'
CLASS="--class"
CLASSNAME="speech"
MASTER="--master"
MODE="local[*]"
JARPATH="etlspeechtime.jar"


def print_hello():
    print('Hello ' + S3_ACCESS_KEY)
    print('Hello ' + S3_SECRET_KEY)
    print('Hello ' + S3_ENDPOINT)
    print('Hello world from first Airflow DAG!')
    


# Define the DAG
with DAG(
    'simpleetlspeechtime2',
    default_args=default_args,
    description='A DAG to submit a simple scala job to kubernates',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    hello = PythonOperator(task_id='hello_k8s', python_callable=print_hello)
    spark_job=KubernetesPodOperator(
    task_id="simpleetlspeechtime2",
    image="etlspeechtime:1.0.1",
    cmds=["spark-submit"],
    arguments=[CLASS, CLASSNAME, MASTER, MODE, JARPATH, S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT, BRONZE_TABLE_PATH, SILVER_TABLE_PATH, TIME_WINDOW_IN_SECS]
)

# Define the task sequence
hello >> spark_job
