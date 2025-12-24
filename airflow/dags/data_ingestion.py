from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests

DATA_DIR = "/opt/data/raw"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Function to download NYC taxi data
def download_taxi_data(year: int, month: int):
    os.makedirs(DATA_DIR, exist_ok=True)

    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{file_name}"
    local_path = os.path.join(DATA_DIR, file_name)

    if os.path.exists(local_path):
        print(f"{file_name} already exists, skipping.")
        return

    response = requests.get(url)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        f.write(response.content)

    print(f"Downloaded {file_name}")

default_args = {
    "start_date": datetime(2022, 1, 1),
    "catchup": False
}

# Define the DAG
with DAG(
    dag_id="nyc_taxi_ingestion",
    schedule_interval=None,  # manual trigger for now
    default_args=default_args,
    tags=["nyc", "taxi", "batch"]
) as dag:

    download_task = PythonOperator(
        task_id="download_nyc_taxi_data",
        python_callable=download_taxi_data,
        op_kwargs={"year": 2022, "month": 1}
    )
    spark_transform = BashOperator(
        task_id="spark_transform_to_parquet",
        bash_command=(
            "spark-submit "
            "/opt/spark_jobs/transform_to_parquet.py"
        )
    )

    download_task >> spark_transform
