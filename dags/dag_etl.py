from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
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
    dag_id='dag_etl_v09',
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
        df['customer_id'] = df['customer_id'].astype(str).str.strip().astype(int)           #transform customer_ids to ints

        df['gender'] = df['gender'].str.strip().str.lower().replace({'f' : 'female', 'm':'male'})       #Standartise the genders to only 'male' and 'female'

        df['senior_citizen'] = df['senior_citizen'].str.strip().str.lower().replace({'yes' : 1, 'no' : 0}).astype(int)      #standartize senior_citizen status to only 1 for yes and 0 for no

        df['partner'] = df['partner'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0})  #standartize partner status to only 1 for yes and 0 for no

        df['dependents'] = df['dependents'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0})  #standartize dependents status to only 1 for yes and 0 for no

        df['tenure'] = pd.to_numeric(df['tenure'], errors='coerce')  # convert tenure strs to ints and keeping <NA> for nulls

        df['tenure'] = df['tenure'].astype('Int64')

        df['phone_service'] = df['phone_service'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0}) # standartize phone_service status to only 1 for yes and 0 for no

        df['multiple_lines'] = df['multiple_lines'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0, 'no phone service' : 0}) # standartize multiple_lines status to only 1 for yes and 0 for no

        df['internet_service'] = df['internet_service'].str.strip().replace({'Fiber optic' : 'fiber', 'No' : 'no', 'Dsl' : 'DSL'}) # standartize phone_service status to only fiber, DSL and no

        df['online_security'] = df['online_security'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0, 'no internet service' : 0}) # standartize online_security status to only 1 for yes and 0 for no

        df['online_backup'] = df['online_backup'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0, 'no internet service' : 0}) # standartize online_security status to only 1 for yes and 0 for no

        df['device_protection'] = df['device_protection'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0, 'no internet service' : 0}) # standartize online_security status to only 1 for yes and 0 for no

        df['tech_support'] = df['tech_support'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0, 'no internet service' : 0}) # standartize online_security status to only 1 for yes and 0 for no

        df['streaming_tv'] = df['streaming_tv'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0, 'no internet service' : 0}) # standartize online_security status to only 1 for yes and 0 for no

        df['streaming_movies'] = df['streaming_movies'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0, 'no internet service' : 0}) # standartize online_security status to only 1 for yes and 0 for no

        df['contract'] = df['contract'].str.strip().str.lower().replace({'one year' : 12, 'month-to-month' : 1,'month to month' : 1, 'two year' : 24}) # contract duration in MONTHS

        df['paperless_billing'] = df['paperless_billing'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0}) # standartize online_security status to only 1 for yes and 0 for no

        df['payment_method'] = df['payment_method'].str.strip().replace({'e-check' : 'Electronic check', 'e-check' : 'Electronic check', 'E-check' : 'Electronic check'}) # standartize payment_method status 

        df['monthly_charges'] = pd.to_numeric(df['monthly_charges'].replace({'$' : ''}), errors='coerce')  # convert coerce strs to ints and keeping nan for nulls

        df['monthly_charges'] = df['monthly_charges'].astype(float) 

        df['total_charges'] = pd.to_numeric(df['total_charges'].replace({'$' : ''}), errors='coerce')  # total_charges tenure strs to ints and keeping nan for nulls

        df['total_charges'] = df['total_charges'].astype(float)

        df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce')  # convert signup_date to the standard sql format

        df['signup_date'] = df['signup_date'].dt.strftime('%Y-%m-%d')

        df['churn'] = df['churn'].str.strip().replace({'Yes' : 1, 'No' : 0}).astype(int)    # convert all yes to 1 and no to 0


        return df

    @task
    def load_data_minio(df, **context):
        execution_date = context['logical_date']
        target_year = execution_date.year
        target_month = f'{execution_date.month:02d}'

        file_name = f'telecome_cleaned_{target_month}_{target_year}.csv'

        df_csv = df.to_csv(index=False)

        print(f'Uploading data file {file_name}')

        s3_hook = S3Hook(aws_conn_id = 'minio_conn')

        s3_hook.load_string(
            string_data = df_csv,
            key = file_name,
            bucket_name = 'silver-processed-data',
            replace = True
        )
        print('Upload succesfull')
        return file_name
    
    @task
    def load_data_ssms(df):
        print('connection with sql server')

        mssql_hook = MsSqlHook(mssql_conn_id = 'my_mssql_connection')
        engine = mssql_hook.get_sqlalchemy_engine()

        print('Uploading the data')

        df.to_sql(
            name='telecom_churn',
            schema = 'silver',
            con=engine,
            if_exists='append',
            index=False
        )

        print(f'Uploaded succesfully {len(df)} lines of data.')


    df = extract_data()
    df = clean_data(df)
    task1 = load_data_minio(df)
    task2 = load_data_ssms(df)