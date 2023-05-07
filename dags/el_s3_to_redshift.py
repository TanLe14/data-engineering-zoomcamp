import pandas as pd
import os
import boto3
from airflow.decorators import task,dag
from airflow.models import Variable
import logging
from datetime import timedelta,datetime
import psycopg2
from sqlalchemy import create_engine
import time
from airflow.operators.email import EmailOperator
import smtplib
#git test
#Get variable in airflow.
months=Variable.get('months')
year=Variable.get('year')
color=Variable.get('color')
access_key=Variable.get('aws_access_key_id')
secret_key=Variable.get('aws_secret_access_key')
port=Variable.get('port')
user=Variable.get('user')
password=Variable.get('password')
dbName=Variable.get('dbName')
host=Variable.get('host')


logging.basicConfig(level=logging.INFO)
logger=logging.getLogger()

base_path= os.path.abspath(__file__+"/../../../")
print(base_path)
default_args={
    'owner':'tanlx',
    'retries':2,
    'email':['lexuantan2000@gmail.com'],
    'email_on_failure':True,
    'retry_delay':timedelta(minutes=1)
}
@dag(
    dag_id="el_s3_to_redshift",
    default_args=default_args,
    start_date=datetime(2023,4,8),
    schedule_interval='@daily'
)
def el_s3_to_redshift():
    #Define function create folder if not exist
    def create_folder_if_not_exist(path):
        os.makedirs(os.path.dirname(path),exist_ok=True)
    #Define task download file parquet from s3 bucket save it
    @task
    def extract_from_s3(month:int, year:int, color:str):
        s3_parquet_path=base_path + f'/data/s3/{color}_tripdata_{year}-{month}.parquet'
        create_folder_if_not_exist(s3_parquet_path)
        s3=boto3.client("s3",
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
        bucket_name='data-lake-tanlx'
        object_name_green='green/'+f"{color}_tripdata_{year}-{month}.parquet"
        object_name_yellow='yellow/'+f"{color}_tripdata_{year}-{month}.parquet"
        if(color=="yellow"):
            s3.download_file(bucket_name,object_name_yellow,s3_parquet_path)
        else:
            s3.download_file(bucket_name,object_name_green,s3_parquet_path)

    #Define function load dataframe to redshift database.
    def import_data_to_redshift(month:int, year:int,color:str):
        conn=psycopg2.connect(
            host=host,
            port=port,  
            dbname=dbName,
            user=user,
            password=password
        )
        bucket_name='data-lake-tanlx'
        if(color=="yellow"):
            table='yellow_taxi_data'
        else:
            table='green_taxi_data'
        query=f"""
            COPY {table}    
            FROM 's3://{bucket_name}/{color}/{color}_tripdata_{year}-{month}.parquet'
            IAM_ROLE 'arn:aws:iam::666243375423:role/DataCamp_Redshift_S3_role'
            FORMAT AS PARQUET;
            """
        cur=conn.cursor()
        cur.execute(query)
        #commit result 
        conn.commit()
        #Close all connect
        cur.close()
        conn.close()
    #Define task import multiple file from s3 to redshift.
    @task
    def write_redshift(months:list,year:int,color:str):
        sum=0
        for month in months.split(" "):
            import_data_to_redshift(month,year,color)
            file_parquet_path=base_path+f'/data/raw/parquet/{color}_tripdata_{year}-{month}.parquet'
            df=pd.read_parquet(file_parquet_path)
            sum+= len(df)
        print(sum)
        
    send_email=EmailOperator(
        task_id='send_email',
        to='lexuantan2000@gmail.com',
        subject='dag complete',
        html_content="dag complete"
    )
    write_redshift(months,year,color) >> send_email
etl=el_s3_to_redshift()
