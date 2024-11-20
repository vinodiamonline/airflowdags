from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import os
import logging

#
# Define below variables in Airflow UI
# SPEECHTIME_BRONZE_TABLE_PATH
# SPEECHTIME_SILVER_TABLE_PATH
# SPEECHTIME_WINDOW_IN_SECS
#

# Define logging
logger = logging.getLogger(__name__)

BRONZE_TABLE_PATH = Variable.get("SPEECHTIME_BRONZE_TABLE_PATH", 
                                 default_var="s3a://connect-analytics-platform/dl_engagement_bronze/")
SILVER_TABLE_PATH = Variable.get("SPEECHTIME_SILVER_TABLE_PATH", 
                                 default_var="s3a://connect-analytics-platform/dl_engagement_speech_silver/")
TIME_WINDOW_IN_SECS = Variable.get("SPEECHTIME_WINDOW_IN_SECS", default_var=86400)

run_schedule = Variable.get("SPEECHTIME_SCHEDULE_TIME", default_var=None) # Every 10 mins

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Vacuum table Method
def vacuum_table():
    VACUUM_DELTA_TABLE_PATH = SILVER_TABLE_PATH
    RETENTION_HOURS = 168
    SEVEN_DAYS_IN_HOURS = 168

    S3_ACCESS_KEY = str(os.getenv("AWS_S3_ACCESS_KEY"))
    S3_SECRET_KEY = str(os.getenv("AWS_S3_SECRET_KEY"))
    S3_END_POINT = str(os.getenv("AWS_S3_END_POINT"))

    delta_table_path = VACUUM_DELTA_TABLE_PATH
    retention_hours = RETENTION_HOURS

    logger.info(f"params {len(S3_ACCESS_KEY)} {len(S3_SECRET_KEY)} {len(S3_END_POINT)} {delta_table_path} {retention_hours}")

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
            .config("spark.databricks.delta.retentionDurationCheck.enabled", retention_check) \
            .getOrCreate()

        try:
            spark.sql(f'VACUUM delta.`{delta_table_path}` RETAIN {retention_hours} HOURS')
            # for testing
            # spark.read.format("delta").load(delta_table_path).printSchema()

        except Exception as e:
            logger.info(f"An error occurred: {e}")
        finally:
            # Stop the Spark session
            spark.stop()
            logger.info("Vacuum complete!!!")
    else:
        logger.info(f"Invalid params {len(S3_ACCESS_KEY)} {len(S3_SECRET_KEY)} {len(S3_END_POINT)}")



# Define the DAG
with DAG(
    'speech_time',
    default_args=default_args,
    description='A DAG to calculate etl_speech_time',
    schedule_interval=run_schedule,
    start_date=days_ago(1),
    catchup=False,
    tags=['SpeechTime', 'ETL'],
) as dag:
    # Speechtime Operator
    spark_job = SparkKubernetesOperator(
        task_id="speech_time",
        namespace='airflow',
        application_file='speech_time.yaml',
        kubernetes_conn_id='spark-cluster-connection',
        params={
        "S3_ACCESS_KEY": os.getenv("AWS_S3_ACCESS_KEY"),
        "S3_SECRET_KEY": os.getenv("AWS_S3_SECRET_KEY"),
        "S3_END_POINT": os.getenv("AWS_S3_END_POINT"),
        "BRONZE_TABLE_PATH": BRONZE_TABLE_PATH,
        "SILVER_TABLE_PATH": SILVER_TABLE_PATH,
        "TIME_WINDOW_IN_SECS": TIME_WINDOW_IN_SECS
    }

    # Vacuum Operator
    vacuum_table = PythonOperator(
        task_id='vacuum_delta_table',
        python_callable=vacuum_table,
        dag=dag
    )
)

# Define the task sequence
vacuum_table >> spark_job
