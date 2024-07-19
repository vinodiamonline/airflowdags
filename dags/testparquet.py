from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    'spark_submit_job',
    description='A simple DAG that reads parquet file and print',
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
)

task = SparkSubmitOperator(
 task_id='submit_job',
 application='opt/airflow/testsparksubmit.py',
 conn_id='kind-spark',
 queue = 'kubernetes',
 dag = dag
)

task