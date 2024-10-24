from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from delta.tables import *

def vacuum_table():
    print('Hello from spark!')

    spark = SparkSession.builder \
        .appName("k2d") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    spark.sql(f"VACUUM delta.`s3a://warehouse/color_10/` RETAIN 168 HOURS")

    spark.read.format("delta").load(delta_table_path).printSchema()

    # Stop the Spark session
    spark.stop()
    
    print('Done!!!')


dag = DAG('vacuum_delta_table', description='Vacuum delta table from DAG',
          schedule_interval=None, # '@hourly',  # Runs every hour
          start_date=datetime(2024, 1, 1), catchup=False)

vacuum_table = PythonOperator(
    task_id='vacuum_delta_table', 
    python_callable=vacuum_table,
    dag=dag
    )

vacuum_table
