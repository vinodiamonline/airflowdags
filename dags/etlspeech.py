from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    'etlspeech',
    description='etl speech time transformation',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
)

task = SparkSubmitOperator(
 task_id='etlspeech',
 application='/opt/airflow/etlspeech-assembly-0.1.0-SNAPSHOT.jar',
 conn_id='kind-spark',
 queue = 'kubernetes',
 dag = dag
)

task
