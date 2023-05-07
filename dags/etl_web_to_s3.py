import pyarrow
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import timedelta, datetime
from airflow.models import Variable
import os
import requests
from gzip import GzipFile
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
import logging
import pyarrow as pa
from datetime import timedelta
import boto3
import logging
from airflow.utils.task_group import TaskGroup
from airflow.operators.email import EmailOperator

# Get variable in airflow.
months = Variable.get("months")
year = Variable.get("year")
color = Variable.get("color")
access_key = Variable.get("aws_access_key_id")
secret_key = Variable.get("aws_secret_access_key")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
base_path = os.path.abspath(__file__ + "/../../../")
default_args = {
    "owner": "tanlx",
    "retries": 1,
    "email_on_failure": True,
    "email": ["lexuantan2000@gmail.com"],
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="etl_web_to_s3",
    default_args=default_args,
    start_date=datetime(2023, 4, 8),
    schedule_interval="@daily",
)
def extract_data_to_s3():
    # Define create folder save file source or raw.
    def create_folder_if_not_exist(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    # Define task download
    @task
    def download_dataset_task(source_path, source_url):
        create_folder_if_not_exist(source_path)
        with open(source_path, mode="wb") as source_file:
            response = requests.get(source_url, verify=False)
            source_file.write(response.content)

    # Define task extract file source and save file in raw path
    @task
    def unzip_save_data(raw_csv_path, source_path):
        create_folder_if_not_exist(raw_csv_path)
        with GzipFile(source_path, mode="rb") as f:
            df = pd.read_csv(f)
            df.to_csv(raw_csv_path, index=False)

    # Define task convert file csv to parquet with schema.
    @task
    def format_to_parquet(raw_parquet_path, raw_csv_path):
        create_folder_if_not_exist(raw_parquet_path)
        if not raw_parquet_path.endswith(".csv"):
            logger.info("Can only accept source files in csv format, for the moment")
        schema_green = {
            "VendorID": pd.Int32Dtype(),
            "store_and_fwd_flag": str,
            "RatecodeID": pd.Int32Dtype(),
            "PULocationID": pd.Int32Dtype(),
            "DOLocationID": pd.Int32Dtype(),
            "passenger_count": pd.Int32Dtype(),
            "trip_distance": pd.Float32Dtype(),
            "fare_amount": pd.Float32Dtype(),
            "extra": pd.Float32Dtype(),
            "mta_tax": pd.Float32Dtype(),
            "tip_amount": pd.Float32Dtype(),
            "tolls_amount": pd.Float32Dtype(),
            "ehail_fee": pd.Float32Dtype(),
            "improvement_surcharge": pd.Float32Dtype(),
            "total_amount": pd.Float32Dtype(),
            "payment_type": pd.Int32Dtype(),
            "trip_type": pd.Int32Dtype(),
            "congestion_surcharge": pd.Float32Dtype(),
        }
        schema_yellow = {
            "VendorID": pd.Int32Dtype(),
            "passenger_count": pd.Int32Dtype(),
            "trip_distance": pd.Float32Dtype(),
            "RatecodeID": pd.Int32Dtype(),
            "store_and_fwd_flag": str,
            "PULocationID": pd.Int32Dtype(),
            "DOLocationID": pd.Int32Dtype(),
            "payment_type": pd.Int32Dtype(),
            "fare_amount": pd.Float32Dtype(),
            "extra": pd.Float32Dtype(),
            "mta_tax": pd.Float32Dtype(),
            "tip_amount": pd.Float32Dtype(),
            "tolls_amount": pd.Float32Dtype(),
            "improvement_surcharge": pd.Float32Dtype(),
            "total_amount": pd.Float32Dtype(),
            "congestion_surcharge": pd.Float32Dtype(),
        }
        if color == "yellow":
            df_yellow = pd.read_csv(raw_csv_path, dtype=schema_yellow)
            df_yellow.tpep_pickup_datetime = pd.to_datetime(
                df_yellow.tpep_pickup_datetime
            )
            df_yellow.tpep_dropoff_datetime = pd.to_datetime(
                df_yellow.tpep_dropoff_datetime
            )
            table = pa.Table.from_pandas(df_yellow)
            sum = len(df_yellow)
        else:
            df_green = pd.read_csv(raw_csv_path, dtype=schema_green)
            df_green.lpep_pickup_datetime = pd.to_datetime(
                df_green.lpep_pickup_datetime
            )
            df_green.lpep_dropoff_datetime = pd.to_datetime(
                df_green.lpep_dropoff_datetime
            )
            table = pa.Table.from_pandas(df_green)
            sum = len(df_green)
        pq.write_table(table, raw_parquet_path)
        return sum

    # Define task load file parquet to s3. - Define s3 client with s3, access_key, secret_key. bucketname, object_name, file_path, s3.upload_file()
    @task
    def load_file_to_s3(dataset_file_parquet, raw_parquet_path):
        s3 = boto3.client(
            "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
        )
        bucket_name = "data-lake-tanlx"
        object_name_green = "green/" + dataset_file_parquet
        object_name_yellow = "yellow/" + dataset_file_parquet
        if color == "yellow":
            s3.upload_file(raw_parquet_path, bucket_name, object_name_yellow)
        else:
            s3.upload_file(raw_parquet_path, bucket_name, object_name_green)

    @task
    def cal_number_row(**kwargs):
        sum = 0
        number_months = len(months.split(" "))
        if number_months == 1:
            sum = kwargs["ti"].xcom_pull(task_ids="group1.format_to_parquet")
        else:
            sum = kwargs["ti"].xcom_pull(task_ids="group1.format_to_parquet")
            for i in range(1, number_months):
                number_row = kwargs["ti"].xcom_pull(
                    task_ids=f"group1.format_to_parquet__{i}"
                )
                sum += number_row
        return sum

    @task
    def send_email(**kwargs):
        number_rows = kwargs["ti"].xcom_pull(task_ids="cal_number_row")
        email = EmailOperator(
            task_id="send_email",
            to="lexuantan2000@gmail.com",
            subject="Number of rows in the data",
            html_content=f"Number of rows in the data is {number_rows}",
        )
        email.execute(context=None)

    with TaskGroup(group_id="group1") as gr1:
        for month in months.split(" "):
            dataset_file_gz = f"{color}_tripdata_{year}-{month}.csv.gz"
            dataset_file_csv = f"{color}_tripdata_{year}-{month}.csv"
            dataset_file_parquet = f"{color}_tripdata_{year}-{month}.parquet"
            source_path = base_path + "/data/source/{dataset_file_gz}".format(
                dataset_file_gz=dataset_file_gz
            )
            raw_csv_path = base_path + "/data/raw/csv/{dataset_file_csv}".format(
                dataset_file_csv=dataset_file_csv
            )
            raw_parquet_path = (
                base_path
                + "/data/raw/parquet/{dataset_file_parquet}".format(
                    dataset_file_parquet=dataset_file_parquet
                )
            )
            source_url = (
                "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/".format(
                    color=color
                )
                + dataset_file_gz
            )
            (
                download_dataset_task(source_path, source_url)
                >> unzip_save_data(raw_csv_path, source_path)
                >> format_to_parquet(raw_parquet_path, raw_csv_path)
                >> load_file_to_s3(dataset_file_parquet, raw_parquet_path)
            )
    gr1 >> cal_number_row() >> send_email()


etl = extract_data_to_s3()
