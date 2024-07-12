from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
#from pyspark.sql import SparkSession
#from deltalake import DeltaTable

def k2d_execute_method():
    print("Hello, K2d!")
#    spark = SparkSession.builder \
#        .appName("k2d") \
#        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
#        .config("spark.hadoop.fs.s3a.access.key", "admin") \
#        .config("spark.hadoop.fs.s3a.secret.key", "password") \
#        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
#        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
#        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
#        .getOrCreate()
#    path = "s3a://warehouse/large_1/"
#    dt = DeltaTable.forPath(spark, path)
#    print(dt.version())
#    print(dt.files())

dag = DAG(
    'k2d',
    description='A simple DAG that read kafka and push to delta',
    schedule_interval='@once',
    start_date=datetime(2022, 1, 1),
)

task = PythonOperator(
    task_id='k2d_task',
    python_callable=k2d_execute_method,
    dag=dag,
)

task
