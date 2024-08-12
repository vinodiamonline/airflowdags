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

    spark_job = SparkKubernetesOperator(
        task_id='spark_kubernetes_task',
        application={
            'apiVersion': 'spark.apache.org/v1beta2',
            'kind': 'SparkApplication',
            'metadata': {
                'name': 'spark-kubernetes-inline-example',
                'namespace': 'default',
            },
            'spec': {
                'type': 'Scala',
                'mode': 'cluster',
                'image': 'spark:latest',
                'mainClass': 'org.example.Main',
                'sparkVersion': '3.2.0',
                'restartPolicy': {
                    'type': 'OnFailure',
                },
                'driver': {
                    'cores': 1,
                    'memory': '1g',
                },
                'executor': {
                    'cores': 1,
                    'instances': 2,
                    'memory': '1g',
                },
                'applications': [
                    {
                        'name': 'example',
                        'className': 'org.example.Main',
                        'arguments': ['arg1', 'arg2'],
                    },
                ],
            },
        },
        namespace='default',
        cluster_manager='kubernetes',
    )

# Define the task sequence
spark_job
