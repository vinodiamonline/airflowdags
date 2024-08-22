from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

dag = DAG(
    'sparkclusterpi',
    description='sparkclusterpi',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
)

sparkclusterpi = SparkSubmitOperator(
 task_id='sparkclusterpi',
 application='/opt/bitnami/spark/examples/jars/spark-examples_2.12-3.5.2.jar',
 java_class='org.apache.spark.examples.SparkPi',
 conn_id='kind-spark',
 dag = dag
)

sparkclusterpi
