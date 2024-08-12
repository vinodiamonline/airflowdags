from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
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
    'spark_kubernetes_example',
    default_args=default_args,
    description='A DAG to submit a Spark job to Kubernetes',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    spark_job = SparkKubernetesOperator(
        task_id="spark_task",
        image="etlspeechtime:1.0.0",
        image_pull_secrets=["regcred"],
        code_path="local://app/etlspeechtime.jar",
        application_file="application_config.yaml",
        dag=dag,
    )

# Define the task sequence
spark_job
