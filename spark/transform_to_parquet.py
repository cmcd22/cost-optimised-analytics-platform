from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year, month, hour, unix_timestamp
)

spark = (
    SparkSession.builder
        .appName("NYC Taxi Transform")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .getOrCreate()
)

input_path = "/opt/data/raw/yellow_tripdata_2022-01.parquet"
output_path = "/opt/data/gold/fact_trips"

df = spark.read.parquet(input_path)

df = df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp("tpep_dropoff_datetime") -
     unix_timestamp("tpep_pickup_datetime")) / 60
)

df = (
    df.withColumn("pickup_year", year("tpep_pickup_datetime"))
      .withColumn("pickup_month", month("tpep_pickup_datetime"))
      .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
)

output_path_local = "/opt/data/gold/fact_trips"
output_path_s3 = "s3a://cmcd-cost-optimised-ap/nyc-taxi/gold/fact_trips"

(
    df.write
      .mode("overwrite")
      .partitionBy("pickup_year", "pickup_month")
      .parquet(output_path_local)
)

(
    df.write
      .mode("overwrite")
      .partitionBy("pickup_year", "pickup_month")
      .parquet(output_path_s3)
)

spark.stop()
