from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    'etlspeechkubernatescluster',
    description='etl speech time transformation',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
)

task = SparkSubmitOperator(
 task_id='etlspeechkubernatescluster',
 application='/opt/airflow/etlspeechtime-assembly-0.1.0-SNAPSHOT.jar',
 java_class='speech',
 application_args=['admin', 'password', 'http://host.docker.internal:9000', 's3a://warehouse/micrawdata1', 's3a://warehouse/tbl_engagement_speech_silver', '3456000'],
 conn_id='kind-spark',
 num_executors=4,
 conf={
    'spark.kubernetes.container.image': 'airflow-spark-delta-hadoop-scala:1.0.0',  # Required image for driver
    'spark.kubernetes.driver.docker.image': 'airflow-spark-delta-hadoop-scala:1.0.0',  # Docker image for driver
    'spark.kubernetes.executor.docker.image': 'airflow-spark-delta-hadoop-scala:1.0.0',  # Docker image for executors (if different)
    'spark.kubernetes.namespace': 'airflow',  # Kubernetes namespace
    'spark.kubernetes.authenticate.driver.serviceAccountName': 'airflow-service',  # Service account (if required)
 },
 queue = 'kubernetes',
 dag = dag
)

task
