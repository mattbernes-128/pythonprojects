# Import required libraries
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
from utils.sql_tools import ps_engine_init
import pandas as pd
import glob

# Import custom library tools
from utils.sql_tools import ps_engine_init

year='2020'

# Create SQLAlchemy engine to push data to Postgres
postgres_hook = PostgresHook(postgres_conn_id="research_db_v1")
CSV_FILE_PATH = f"/opt/airflow/ingestion_files/pboc/{year}/formatted"

# Set default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Extract data using API key from CoinMarketCap
def upload_csv(CSV_FILE_PATH,data_file_name):
    # Connect to Postgres using Airflow's connection
    postgres_hook = PostgresHook(postgres_conn_id="research_db_v1")
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(f"{CSV_FILE_PATH}/{data_file_name}")
    df["data_file_name"] = data_file_name
    df["ingestiond_dt"] = datetime.today().date()
    table_name=data_file_name.split('.')[0]
    table_name=table_name.replace('Aggregate_Financing_to_the_Real_Economy','Aggregate_Financing')
    table_name=table_name.replace('__Corporate_Goods_Price_Indices','')
    table_name=table_name.replace('Financial_Assets_and_Liabilities_Statement','fals')
    table_name=table_name.replace('Flow_of_Funds_StatementXFinancial_AccountsXX','fofsfa_')
    table_name=table_name.replace('__Financial_Market_Statistics__Statistics_of_','')
    table_name=table_name.replace('__Money_and_Banking_Statistics','')
    table_name=table_name.replace('Domestic_RMB_Financial_Assets_Held_by_Overseas_Entities__clean','RMB_Assets_Held_Overseas__clean')

    df.to_sql(f"{table_name}", postgres_hook.get_sqlalchemy_engine(), schema="land", if_exists="replace", index=False)

def upload_pboc_data(CSV_FILE_PATH) :
    files = glob.glob(f"{CSV_FILE_PATH}/*.csv")  # Get all CSV files
    for file in files:
        file_name=file.split('/')[-1]
        print(file_name)
        print(len(file_name))
        upload_csv(CSV_FILE_PATH,file_name)
        print(f'file upload complete {file}')

# Create DAG
with DAG(
        dag_id="upload_pboc_data",
        default_args=default_args,
        description='upload_pboc_data',
        start_date=datetime(2023, 1, 1),
        schedule=None,
        # schedule_interval='@daily',
        catchup=False
) as dag:
    upload_task = PythonOperator(
        task_id="upload_task",
        python_callable=upload_pboc_data,
        op_args=[CSV_FILE_PATH]  # Pass the argument CSV_FILE_PATH
    )

    upload_task