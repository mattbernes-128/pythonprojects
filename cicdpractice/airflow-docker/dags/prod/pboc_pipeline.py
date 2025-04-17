# ----------------------------------------
# DAG: Upload PBOC Formatted Data to Postgres
# ----------------------------------------

# Standard library imports
from datetime import datetime
import glob
import pandas as pd

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Custom utility import
from utils.sql_tools import ps_engine_init  # (currently unused, you might consider removing it)

# ----------------------------------------
# Configuration
# ----------------------------------------

data_collection_year = '2018'
INGESTION_PATH = "/opt/airflow/ingestion_files"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


# ----------------------------------------
# Upload a single CSV file to Postgres
# ----------------------------------------

def upload_csv(CSV_FILE_PATH, data_file_name):
    """
    Read a CSV file, transform the table name, and upload to PostgreSQL.
    """
    postgres_hook = PostgresHook(postgres_conn_id="research_db_v1")
    df = pd.read_csv(f"{CSV_FILE_PATH}/{data_file_name}")

    # Add metadata columns
    df["data_file_name"] = data_file_name
    df["ingestiond_dt"] = datetime.today().date()

    # Derive table name with cleaning rules
    table_name = data_file_name.split('.')[0]
    table_name = table_name.replace('Aggregate_Financing_to_the_Real_Economy', 'Aggregate_Financing')
    table_name = table_name.replace('__Corporate_Goods_Price_Indices', '')
    table_name = table_name.replace('Financial_Assets_and_Liabilities_Statement', 'fals')
    table_name = table_name.replace('Flow_of_Funds_StatementXFinancial_AccountsXX', 'fofsfa_')
    table_name = table_name.replace('__Financial_Market_Statistics__Statistics_of_', '')
    table_name = table_name.replace('__Money_and_Banking_Statistics', '')
    table_name = table_name.replace('Domestic_RMB_Financial_Assets_Held_by_Overseas_Entities__clean',
                                    'RMB_Assets_Held_Overseas__clean')

    # Upload DataFrame to Postgres
    df.to_sql(
        name=table_name,
        con=postgres_hook.get_sqlalchemy_engine(),
        schema="land",
        if_exists="replace",
        index=False
    )


# ----------------------------------------
# Upload all files for a specific year
# ----------------------------------------

def upload_files(**kwargs):
    """
    Iterate over all formatted CSV files for a given year and upload them to Postgres.
    """
    year = kwargs['data_collection_year']
    path = kwargs['INGESTION_PATH']

    # Path to formatted files
    FORMATTED_FILE_PATH = f"{path}/pboc/{year}/formatted"
    files = glob.glob(f"{FORMATTED_FILE_PATH}/*.csv")  # List all CSVs

    for file in files:
        file_name = file.split('/')[-1]
        print(f"Uploading: {file_name}")
        upload_csv(FORMATTED_FILE_PATH, file_name)
        print(f"✅ Upload complete: {file_name}")


# ----------------------------------------
# DAG Definition
# ----------------------------------------

with DAG(
        dag_id="pboc_pipeline",
        default_args=default_args,
        description='Upload formatted PBOC data to PostgreSQL (land schema)',
        start_date=datetime(2023, 1, 1),
        schedule_interval=None,  # Run manually
        catchup=False
) as dag:
    upload_task = PythonOperator(
        task_id="upload_files",
        python_callable=upload_files,
        op_kwargs={
            'data_collection_year': data_collection_year,
            'INGESTION_PATH': INGESTION_PATH
        }
    )

    # Define task dependencies
    upload_task
