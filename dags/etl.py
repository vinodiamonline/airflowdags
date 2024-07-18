from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType, StringType
import json

def func1(y):
    if 'events' in y.keys():
        x = y['events']
        if(x):
            json_list = []
            json_list.append(json.loads(x))
            json.dumps(json_list)
            obj = json_list[0][0]
            print(obj['ts'] , obj['detail'])

def writedata(userId, data):
    print(userId, data)

def func2(arr):
    subscriber_stats_bitrates = []
    userId = 0
    if 'subscriberStats' in arr.keys():
        subscriberStats = arr['subscriberStats']
        if(subscriberStats):
            subscriberStats_list = []
            subscriberStats_list.append(json.loads(subscriberStats))
            json.dumps(subscriberStats_list)
            for item in subscriberStats_list[0]:
                userId = item['userID']
                stats = item['stats']
                if 'video' in stats.keys():
                    video = stats['video']
                    if 'bitrates' in video.keys():
                        bitrates = video['bitrates']
                        # print(userId, bitrates)
                        subscriber_stats_bitrates = subscriber_stats_bitrates + bitrates
    writedata(userId, subscriber_stats_bitrates)


print("Hello, K2d!")

sc = SparkContext('local[*]')

spark = SparkSession.builder \
    .appName("k2d") \
    .master("local[*]") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

path = "s3a://warehouse/analytics_1/"

df1 = spark.read.format('parquet').load(path)
df1.show()

df2 = df1.where(col("_c3") == ('80adfef49a7d61c4a2a56ae0b5b5c057ef7eb2fcad6a24a974865c26e01553da'))

df2.show(5)

df3 = df2.select(col("_c4"))
df3.show(5)

df4 = df3.withColumn("_c4",from_json(df3._c4,MapType(StringType(),StringType())))
df4.show(5)

df4.foreach(lambda x: func2(x["_c4"]))
