from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import os
from airflow.operators.python_operator import PythonOperator

#
# Define below variables in Airflow UI
# SPEECHTIME_BRONZE_TABLE_PATH
# SPEECHTIME_SILVER_TABLE_PATH
# SPEECHTIME_WINDOW_IN_SECS
#

BRONZE_TABLE_PATH = Variable.get("SPEECHTIME_BRONZE_TABLE_PATH", 
                                 default_var="s3a://connect-analytics-platform/dl_engagement_bronze/")
SILVER_TABLE_PATH = Variable.get("SPEECHTIME_SILVER_TABLE_PATH", 
                                 default_var="s3a://connect-analytics-platform/dl_engagement_speech_silver/")
TIME_WINDOW_IN_SECS = Variable.get("SPEECHTIME_WINDOW_IN_SECS", default_var=86400)

run_schedule = Variable.get("SPEECHTIME_SCHEDULE_TIME", default_var=None) # Every 10 mins

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print('Hello world from first Airflow DAG!')
    S3_ACCESS_KEY = str(os.getenv("AWS_S3_ACCESS_KEY"))
    S3_SECRET_KEY = str(os.getenv("AWS_S3_SECRET_KEY"))
    S3_END_POINT = str(os.getenv("AWS_S3_END_POINT"))
    print(BRONZE_TABLE_PATH)
    print(SILVER_TABLE_PATH)
    print(TIME_WINDOW_IN_SECS)
    print(S3_ACCESS_KEY)
    print(S3_SECRET_KEY)
    print(S3_END_POINT)

print_hello

# Define the DAG
with DAG(
    'speech_time',
    default_args=default_args,
    description='A DAG to calculate etl_speech_time',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['SpeechTime', 'ETL'],
) as dag:
  # Define Operator
    spark_job = SparkKubernetesOperator(
        task_id="speech_time_1",
        namespace='airflow',
        application_file='speechtimetest.yaml',
        kubernetes_conn_id='spark-local'
    )
  
# spark_job = PythonOperator(task_id='speech_time', python_callable=print_hello)

# Define the task sequence
spark_job
