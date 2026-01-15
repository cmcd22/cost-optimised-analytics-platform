from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year, month, hour, unix_timestamp, col, when
)
from pyspark.sql.types import DoubleType, IntegerType

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

numeric_cols = [
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "total_amount",
    "trip_duration_minutes"
]

df_clean = df
for c in numeric_cols:
    df_clean = df_clean.withColumn(
        c,
        when(col(c).cast(DoubleType()).isNotNull(), col(c).cast(DoubleType()))
        .otherwise(None)
    )

int_cols = [
    "passenger_count",
    "VendorID",
    "payment_type",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "pickup_hour"
]

df_clean_int = df_clean
for c in int_cols:
    df_clean_int = df_clean_int.withColumn(
        c,
        when(col(c).cast(IntegerType()).isNotNull(), col(c).cast(IntegerType()))
        .otherwise(None)
    )

csv_cols = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "total_amount",
    "payment_type",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "pickup_hour",
    "trip_duration_minutes"
]

df_csv = df_clean_int.select(*csv_cols)

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

(
    df_csv
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv("s3a://cmcd-cost-optimised-ap/nyc-taxi/redshift_csv/")
)

spark.stop()
