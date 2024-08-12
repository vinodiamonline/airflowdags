from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    'etlspeechtime',
    description='etl speech time transformation',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
)

task = SparkSubmitOperator(
 task_id='etlspeechtime',
 application='./dags/repo/dags/etlspeechtimetransformer.py',
 application_args=['admin', 'password', 'http://host.docker.internal:9000', 's3a://warehouse/micrawdata1', 's3a://warehouse/tbl_engagement_speech_silver', '3456000'],
 conn_id='kind-spark',
 queue = 'kubernetes',
 dag = dag
)

task
