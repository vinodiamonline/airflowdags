from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

dag = DAG(
    'etlspeech',
    description='etl speech time transformation',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
)

S3_ACCESS_KEY=os.getenv("AWS_S3_ACCESS_KEY")
S3_SECRET_KEY=os.getenv("AWS_S3_SECRET_KEY")
S3_ENDPOINT=os.getenv("AWS_S3_ENDPOINT")
BRONZE_TABLE_PATH='s3a://warehouse/micrawdata1'
SILVER_TABLE_PATH='s3a://warehouse/tbl_engagement_speech_silver'
TIME_WINDOW_IN_SECS='3456000'

task = SparkSubmitOperator(
 task_id='etlspeech',
 application='/opt/airflow/etlspeechtime-assembly-0.1.0-SNAPSHOT.jar',
 java_class='speech',
 application_args=[S3_ACCESS_KEY, S3_SECRET_KEY, 'http://host.docker.internal:9000', BRONZE_TABLE_PATH, SILVER_TABLE_PATH, TIME_WINDOW_IN_SECS],
 conn_id='kind-spark',
 dag = dag
)

task
