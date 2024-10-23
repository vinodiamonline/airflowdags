from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from delta.tables import *

def vacuum_table():
    print('Hello from spark!')

    spark = SparkSession.builder \
        .appName("vacuum_delta_table") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
       
    data = [
        ("Alice", 34),
        ("Bob", 45),
        ("Cathy", 29),
        ("David", 40)
    ]
    columns = ["Name", "Age"]
    
    spark.createDataFrame(data, columns).show()

    pathToTable = "s3a://warehouse/color_10/"

    deltaTable = DeltaTable.forPath(spark, pathToTable)  # path-based tables, or

    deltaTable.vacuum()

    print('Done!!!')


dag = DAG('vacuum_delta_table', description='Vacuum delta table from DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2024, 1, 1), catchup=False)

vacuum_table = PythonOperator(
    task_id='vacuum_delta_table', 
    python_callable=vacuum_table,
    dag=dag
    )

vacuum_table
