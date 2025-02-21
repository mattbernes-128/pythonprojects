# Import required libraries
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import pandas as pd
# Import custom library tools
from utils.sql_tools import ps_engine_init

# Create SQLAlchemy engine to push data to Postgres
postgres_hook = PostgresHook(postgres_conn_id="research_db_v1")
CSV_FILE_PATH = "/opt/airflow/ingestion_files/"

# Set default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


# Extract data using API key from CoinMarketCap
def upload_csv():
    # Connect to Postgres using Airflow's connection
    postgres_hook = PostgresHook(postgres_conn_id="research_db_v1")

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(CSV_FILE_PATH+"chinese_fdi_data.csv")
    df.to_sql("chinese_fdi", postgres_hook.get_sqlalchemy_engine(), schema="land", if_exists="replace", index=False)

# Create DAG
with DAG(
        dag_id="upload_csv_to_postgres",
        default_args=default_args,
        description='upload_csv_to_postgres',
        start_date=datetime(2023, 1, 1),
        schedule=None,
        # schedule_interval='@daily',
        catchup=False
) as dag:
    upload_task = PythonOperator(
        task_id="upload_task",
        python_callable=upload_csv
    )

    upload_task