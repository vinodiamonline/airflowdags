from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    'submit_job_cluster_mode',
    description='A simple DAG that reads parquet file and print',
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
)

task = SparkSubmitOperator(
 task_id='submit_job',
 application='testsparksubmit.py',
 conn_id='kind-spark-cluster-mode',
 queue = 'kubernetes',
 dag = dag
)

task
