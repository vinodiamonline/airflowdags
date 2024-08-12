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

    SparkKubernetesOperator(
    task_id="spark_task",
    image="gcr.io/spark-operator/spark-py:v3.1.1",  # OR custom image using that
    code_path="local://path/to/spark/code.py",
    application_file="spark_job_template.json",  # OR spark_job_template.json
    dag=dag,
  )

# Define the task sequence
spark_job

