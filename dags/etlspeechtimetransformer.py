from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, last, unix_timestamp, current_timestamp, sum as spark_sum
from pyspark.sql.types import LongType, TimestampType
from pyspark.sql.window import Window
import json
from datetime import datetime, timedelta

def main(args):
    if len(args) != 6:
        raise ValueError(
            "Not enough arguments. Need "
            "\nS3_ACCESS_KEY "
            "\nS3_SECRET_KEY "
            "\nS3_ENDPOINT "
            "\nBRONZE_TABLE_PATH "
            "\nSILVER_TABLE_PATH "
            "\nTIME_WINDOW_IN_SECS "
            "\narguments in same order."
        )
    
    S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT, BRONZE_TABLE_PATH, SILVER_TABLE_PATH, TIME_WINDOW_IN_SECS = args

    starttime = datetime.now()

    print("Hello Transformation!")
    print(S3_ACCESS_KEY)
    print(S3_SECRET_KEY)
    print(S3_ENDPOINT)
    print(BRONZE_TABLE_PATH)
    print(SILVER_TABLE_PATH)
    print(TIME_WINDOW_IN_SECS)

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SpeechDataTransformer") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    # Load Delta tables
    in_delta_table = DeltaTable.forPath(spark, BRONZE_TABLE_PATH)
    out_delta_table = DeltaTable.forPath(spark, SILVER_TABLE_PATH)

    # Load data as DataFrame
    df = in_delta_table.toDF().filter(
        (col("event_subtype") == "speechTime") &
        (col("timestamp_client") < (unix_timestamp(current_timestamp()) - int(TIME_WINDOW_IN_SECS)).cast(TimestampType()))
    ).groupBy(
        "cluster_type", "zone_id", "account_id", "sco_id", "sco_session", "user_id", "user_session", "user_ticket"
    ).count().drop("count")

    print(df.count())
    # df.show(truncate=False)

    # Collect results
    map_seq = [row.asDict() for row in df.collect()]

    silver_df = None
    for row in map_seq:
        try:
            cluster_type = row["cluster_type"]
            zone_id = row["zone_id"]
            account_id = row["account_id"]
            sco_id = row["sco_id"]
            sco_session = row["sco_session"]
            user_id = row["user_id"]
            user_session = row["user_session"]
            user_ticket = row["user_ticket"]

            print(row)

            user_df = in_delta_table.toDF().filter(
                (col("cluster_type") == cluster_type) &
                (col("zone_id") == zone_id) &
                (col("account_id") == account_id) &
                (col("sco_id") == sco_id) &
                (col("sco_session") == sco_session) &
                (col("user_id") == user_id) &
                (col("user_session") == user_session) &
                (col("user_ticket") == user_ticket)
            )

            out_df = process_data(spark, user_df)

            if silver_df is None:
                silver_df = out_df
            else:
                silver_df = silver_df.union(out_df)

        except Exception as e:
            print(f"Error processing row: {row}, Exception: {str(e)}")
            raise Exception(f"Error processing row: {row}, Exception: {str(e)}")

    # Update Delta table
    out_delta_table.alias("bronze") \
        .merge(
            silver_df.alias("silver"),
            "bronze.user_ticket = silver.user_ticket"
        ) \
        .whenMatchedUpdateExpr(
            {
                "role": "silver.role",
                "room_id": "silver.room_id",
                "speech_time": "silver.speech_time"
            }
        ) \
        .whenNotMatchedInsertExpr(
            {
                "cluster_type": "silver.cluster_type",
                "zone_id": "silver.zone_id",
                "account_id": "silver.account_id",
                "sco_id": "silver.sco_id",
                "sco_session": "silver.sco_session",
                "user_id": "silver.user_id",
                "user_session": "silver.user_session",
                "user_ticket": "silver.user_ticket",
                "role": "silver.role",
                "room_id": "silver.room_id",
                "speech_time": "silver.speech_time"
            }
        ) \
        .execute()

    print("upsert record count", silver_df.count())
    print("total time taken", (datetime.now() - starttime).total_seconds(), "seconds")

def process_data(spark, user_df):
    from pyspark.sql.functions import lit, last
    from pyspark.sql.window import Window

    user_with_role_df = user_df \
        .orderBy("timestamp_client") \
        .withColumn("role", lit(col("data").getItem("role")))

    user_with_role_and_room_df = user_with_role_df \
        .orderBy("timestamp_client") \
        .withColumn("room_id", lit(col("data").getItem("roomId")))

    window_spec = Window.orderBy("timestamp_client").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    user_with_role_filled_df = user_with_role_and_room_df.withColumn("role", last("role", ignoreNulls=True).over(window_spec))
    user_with_role_and_room_filled_df = user_with_role_filled_df.withColumn("room_id", last("room_id", ignoreNulls=True).over(window_spec))

    processed_rdd = user_with_role_and_room_filled_df.rdd.map(lambda row: (
        row["cluster_type"],
        row["zone_id"],
        row["account_id"],
        row["sco_id"],
        row["sco_session"],
        row["user_id"],
        row["user_session"],
        row["user_ticket"],
        row["event_subtype"],
        row["role"],
        row["room_id"],
        calculate_speech_time(json.loads(row["data"]).get("intervals", ""))
    ))

    role_room_and_speech_df = processed_rdd.toDF(
        ["cluster_type", "zone_id", "account_id", "sco_id", "sco_session", "user_id", "user_session", "user_ticket", "event_subtype", "role", "room_id", "micdata"]
    )

    agg_df = role_room_and_speech_df.filter(col("event_subtype") == "speechTime").groupBy(
        "cluster_type", "zone_id", "account_id", "sco_id", "sco_session", "user_id", "user_session", "user_ticket", "role", "room_id"
    ).agg(
        spark_sum("micdata").alias("speech_time").cast(LongType())
    )

    result = agg_df.na.fill("0", ["room_id"]).na.fill("participant", ["role"])

    return result

def calculate_speech_time(intervals):
    try:
        speech_intervals = json.loads(intervals)
        total_time = sum(
            max(int(time["end"]) - int(time["start"]), 0)
            for time in speech_intervals
        )
        print("sum =", total_time)
        return total_time
    except Exception as e:
        print(f"Error parsing intervals: {str(e)}")
        return 0

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
