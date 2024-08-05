from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.models import Variable

dag = DAG(
    'etlspeech',
    description='etl speech time transformation',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
)

S3_ACCESS_KEY=Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY=Variable.get("S3_SECRET_KEY")
S3_ENDPOINT=Variable.get("S3_ENDPOINT")
BRONZE_TABLE_PATH='s3a://warehouse/micrawdata1'
SILVER_TABLE_PATH='s3a://warehouse/tbl_engagement_speech_silver'
TIME_WINDOW_IN_SECS='3456000'

task = SparkSubmitOperator(
 task_id='etlspeech',
 application='/opt/airflow/etlspeechtime-assembly-0.1.0-SNAPSHOT.jar',
 java_class='speech',
 application_args=['admin', 'password', 'http://host.docker.internal:9000', BRONZE_TABLE_PATH, SILVER_TABLE_PATH, TIME_WINDOW_IN_SECS],
 conn_id='kind-spark',
 dag = dag
)

task
