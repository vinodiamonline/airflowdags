#
# Airflow DAG to run vacumm over obsolete data
#

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
import logging
import os

# Define logging
logger = logging.getLogger(__name__)
delta_table_path = "s3a://warehouse/color_10/"
retention_hours = 168
retention_check = "true"

# Vacuum table Method
def vacuum_table():
    S3_ACCESS_KEY = str(os.getenv("AWS_S3_ACCESS_KEY")),
    S3_SECRET_KEY = str(os.getenv("AWS_S3_SECRET_KEY")),
    S3_END_POINT = str(os.getenv("AWS_S3_END_POINT"))

    # for testing
    logger.info("params ",
                S3_ACCESS_KEY
                + S3_SECRET_KEY
                + S3_END_POINT
                )

    if (len(S3_ACCESS_KEY) > 0) and (len(S3_SECRET_KEY) > 0) and (len(S3_END_POINT) > 0):
        logger.info("Start vacuuming!!!")

        if(retention_hours < 168) : # 7 days is default
            retention_check = "false"
        try:
            spark = SparkSession.builder \
                .appName("vacuum") \
                .master("local[*]") \
                .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.1") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
                .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
                .config("spark.hadoop.fs.s3a.endpoint", S3_END_POINT) \
                .config("spark.databricks.delta.retentionDurationCheck.enabled", retention_check) \
                .getOrCreate()

            spark.sql(f'VACUUM delta.`{delta_table_path}` RETAIN {retention_hours} HOURS')

            # for testing
            spark.read.format("delta").load(delta_table_path).printSchema()

        except Exception as e:
            logger.info(f"An error occurred: {e}")
        finally:
            # Stop the Spark session
            spark.stop()
            logger.info("Vacuum complete!!!")
    else:
        logger.info("Invalid params ",
            str(len(S3_ACCESS_KEY))
            + str(len(S3_SECRET_KEY))
            + str(len(S3_END_POINT))
        )

# Vacuum table Method end

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
    'vacuum_delta_table',
    default_args=default_args,
    description='A DAG to vacuum delta table',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['etl']
) as dag:
    # Define Operator
    vacuum_table = PythonOperator(
        task_id='vacuum_delta_table',
        python_callable=vacuum_table,
        dag=dag
    )

# Define the task sequence
vacuum_table
