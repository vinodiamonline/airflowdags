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
    'retries': 1,
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

    # Define the SparkKubernetesOperator
    spark_job = SparkKubernetesOperator(
        task_id='spark_kubernetes_task',
        application_file='/path/to/spark/application.yaml',  # Path to the Kubernetes application YAML file
        namespace='default',  # Kubernetes namespace
        cluster_manager='kubernetes',
        # Specify additional Spark configurations as needed
        conf={
            'spark.executor.memory': '4g',
            'spark.executor.cores': '2',
            'spark.driver.memory': '4g',
        },
    )

    # Define the task sequence
    spark_job
