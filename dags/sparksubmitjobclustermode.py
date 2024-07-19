from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    'spark_submit_job_in_cluster_mode',
    description='A simple DAG that reads parquet file and print',
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
)

_config ={'application':'testsparksubmit.py',
    'deploy-mode' : 'cluster',
    'num_executors':'2',
    'executor_cores': 1,
    'EXECUTORS_MEM': '2G'
}

task = SparkSubmitOperator(
 task_id='submit_job',
 conn_id='kind-spark',
 dag = dag,
 **_config
)

task
