from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'simpleetlspeechtime',
    default_args=default_args,
    description='A DAG to submit a simple scala job to kubernates',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    spark_job=KubernetesPodOperator(
    task_id="simpleetlspeechtime",
    image="etlspeechtime:1.0.0"
)
    
# Define the task sequence
spark_job
