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
 verbose=True,
 packages='io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.1',
 conf={
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.1',
            'spark.spark.hadoop.fs.s3a.access.key': 'admin',
            'spark.hadoop.fs.s3a.secret.key': 'password',
            'spark.hadoop.fs.s3a.endpoint': 'http://host.docker.internal:9000',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.databricks.delta.retentionDurationCheck.enabled': 'false',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
        },
 queue = 'kubernetes',
 dag = dag
)

task
