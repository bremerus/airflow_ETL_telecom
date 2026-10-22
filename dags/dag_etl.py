from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
import pandas as pd
import io

default_args = {
    'owner' : 'primer',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id='dag_etl_v06',
    default_args=default_args,
    start_date= datetime(2020, 1, 1),
    end_date= datetime(2020, 3, 31),
    schedule_interval='@monthly'
) as dag:
    @task
    def extract_data(**context):
        print('Connection with minio to get the data')
        s3_hook = S3Hook(aws_conn_id = 'minio_conn')

        file_content = s3_hook.read_key(
            key = 'telecom_churn.csv',
            bucket_name = 'bronze-raw-data'
        )

        df = pd.read_csv(io.StringIO(file_content))

        print(f'There was {len(df)} lines read.')

        execution_date = context['logical_date']
        target_year = execution_date.year
        target_month = execution_date.month

        print(f'filtere date for {target_year} year and {target_month} month.')

        df['signup_date'] = pd.to_datetime(df['signup_date'])

        df_filtered = df[(df['signup_date'].dt.year == target_year) & (df['signup_date'].dt.month == target_month)].copy()

        print(f'After the filter there was {len(df_filtered)} left.')

        return df_filtered

    @task
    def clean_data(df):
        # print(df.head(10))

        df['customer_id'] = df['customer_id'].astype(str).str.strip().astype(int)           #transform customer_ids to ints
        
        return 'data cleaned'

    df = extract_data()
    task1 = clean_data(df)