from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG('spark_submit_job',
         start_date=datetime(2022, 1, 1),
         schedule_interval='@daily) as dag:

    submit_job = SparkSubmitOperator(
          task_id='submit_job',
          application='opt/airflow/testsparksubmit.py',
          conn_id='kind-spark'
    )

    submit_job
