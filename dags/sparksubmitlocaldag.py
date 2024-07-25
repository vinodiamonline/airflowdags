from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    'submit_job_local_mode',
    description='A simple DAG that test spark dataframe',
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
)

task = SparkSubmitOperator(
 task_id='submit_job',
 application='dags/testsparkdataframe.py',
 conn_id='kind-spark',
 queue = 'kubernetes',
 dag = dag
)

task
