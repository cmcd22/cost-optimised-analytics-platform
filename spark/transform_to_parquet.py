from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year, month, hour, unix_timestamp
)

spark = (
    SparkSession.builder
    .appName("NYC Taxi Transform")
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

(
    df.write
      .mode("overwrite")
      .partitionBy("pickup_year", "pickup_month")
      .parquet(output_path)
)

spark.stop()
