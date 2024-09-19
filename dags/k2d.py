from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
#from deltalake import DeltaTable

def k2d_execute_method():
    print("Hello, K2d!")
    sc =SparkContext()
    nums= sc.parallelize([1,2,3,4])
    squared = nums.map(lambda x: x*x).collect()
    for num in squared:
        print('%i ' % (num))
    print("Hello, K2d Done")
    spark = SparkSession.builder \
       .appName("k2d") \
       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
       .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
       .config("spark.hadoop.fs.s3a.access.key", "admin") \
       .config("spark.hadoop.fs.s3a.secret.key", "password") \
       .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
       .config("spark.hadoop.fs.s3a.path.style.access", "true") \
       .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
       .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
       .getOrCreate()
#    path = "s3a://warehouse/large_1/"
#    dt = DeltaTable.forPath(spark, path)
#    print(dt.version())
#    print(dt.files())

    path = "s3a://warehouse/pqtest/p1.parquet"
    
    df1 = spark.read.format('parquet').options(header=True,inferSchema=True).load(path)
    df1.show()

    print("Hello, spark Done")

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
