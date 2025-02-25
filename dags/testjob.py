#
# Airflow DAG to run test and debug
#

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession

def test_job():
  spark = SparkSession.builder \
      .appName("vacuum") \
      .master("local[*]") \
      .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
      .config("spark.hadoop.fs.s3a.path.style.access", "true") \
      .config("spark.hadoop.fs.s3a.aws.credentials.provider",
              "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
      .getOrCreate()


# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Define the DAG
with DAG(
    'testjob',
    default_args=default_args,
    description='test dag',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['TEST']
) as dag:
    # Define Operator
    testtask = PythonOperator(
        task_id='test_job',
        python_callable=test_job,
        dag=dag
    )

# Define the task sequence
testtask
