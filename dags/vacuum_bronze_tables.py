
#
# Airflow DAG to run vacumm over obsolete data
#

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from airflow.models import Variable
import logging
import os

#
# Define below variables in Airflow UI
# VACUUM_BRONZE_TABLE_PATHS : This is a comma separated path of tables which needs to be vacuumed
# VACUUM_BRONZE_TABLE_RETENTION_HOURS : Retension hours
# VACUUM_BRONZE_TABLE_SCHEDULE_TIME : Time when this dag needs to run
#

# Define logging
logger = logging.getLogger(__name__)

TABLE_PATHS = "s3a://connect-analytics-platform/dl_engagement_bronze/"
RETENTION_HOURS = 168
SEVEN_DAYS_IN_HOURS = 168
SCHEDULE_TIME = '0 5 * * *'  # Every day at 5 AM UTC

# Vacuum table Method
def vacuum_tables():
    S3_ACCESS_KEY = str(os.getenv("AWS_S3_ACCESS_KEY"))
    S3_SECRET_KEY = str(os.getenv("AWS_S3_SECRET_KEY"))
    S3_END_POINT = str(os.getenv("AWS_S3_END_POINT"))

    table_paths = Variable.get("VACUUM_BRONZE_TABLE_PATHS", default_var=TABLE_PATHS)
    retention_hours = Variable.get("VACUUM_BRONZE_TABLE_RETENTION_HOURS", default_var=RETENTION_HOURS)

    logger.info(f"params {len(S3_ACCESS_KEY)} {len(S3_SECRET_KEY)} {len(S3_END_POINT)} {table_paths} {retention_hours}")

    if (len(S3_ACCESS_KEY) > 0) and (len(S3_SECRET_KEY) > 0) and (len(S3_END_POINT) > 0):
        logger.info("Start vacuuming!!!")

        retention_check = "false" if int(retention_hours) < SEVEN_DAYS_IN_HOURS else "true" # 7 days is default

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
            .config("spark.executor.cores", "2") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.cores", "2") \
            .config("spark.driver.memory", "2g") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", retention_check) \
            .getOrCreate()

        try:
            tables = table_paths.split(",")
            for table in tables:
              logger.info(f"Vacuuming table : {table}")
              spark.sql(f'VACUUM delta.`{table}` RETAIN {retention_hours} HOURS')

        except Exception as e:
            logger.info(f"An error occurred: {e}")
        finally:
            # Stop the Spark session
            spark.stop()
            logger.info("Vacuum complete!!!")
    else:
        logger.info(f"Invalid params {len(S3_ACCESS_KEY)} {len(S3_SECRET_KEY)} {len(S3_END_POINT)}")

# Vacuum table Method end

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

run_schedule = Variable.get("VACUUM_BRONZE_TABLE_SCHEDULE_TIME", default_var=SCHEDULE_TIME)

# Define the DAG
with DAG(
    'vacuum_bronze_tables',
    default_args=default_args,
    description='A DAG to vacuum all the bronze tables',
    schedule_interval=run_schedule,
    start_date=days_ago(1),
    catchup=False,
    tags=['VACUUM_BRONZE_TABLE', 'VACUUM', 'NIGHTLY']
) as dag:
    # Define Operator
    vacuum_table = PythonOperator(
        task_id='vacuum_bronze_tables',
        python_callable=vacuum_tables,
        dag=dag
    )

# Define the task sequence
vacuum_table
