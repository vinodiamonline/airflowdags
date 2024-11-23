from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import os

# run_schedule = Variable.get("SPEECHTIME_SCHEDULE_TIME", default_var=None) # Every 10 mins

print(os.getenv("AWS_S3_ACCESS_KEY"))
print(os.getenv("AWS_S3_SECRET_KEY"))
print(os.getenv("AWS_S3_END_POINT"))

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

dag = DAG('speech_time', description='speech time',
          schedule_interval=None,
          start_date=days_ago(1), 
          catchup=False)

# spark_job = PythonOperator(task_id='speech_time', python_callable=print_hello, dag=dag)
spark_job = SparkKubernetesOperator(
        task_id="speech_time",
        namespace='airflow',
        application_file='speechtimetest.yaml',
        kubernetes_conn_id='spark-cluster-connection',
        dag=dag)

# Define the task sequence
spark_job
