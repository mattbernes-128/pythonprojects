# Import required libraries
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import pandas as pd
import json
# Import custom library tools
from utils.sql_tools import ps_engine_init

# Set target schema for data upload
schema = "land"

# Create SQLAlchemy engine to push data to Postgres
db_name = "research_db_v1"
engine = ps_engine_init(db_name)

# Retrieve API key
conn = BaseHook.get_connection("crypto_api")
api_key = conn.password

# Set default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


# Extract data using API key from CoinMarketCap
def extract():
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    # Define API parameters
    parameters = {
        'start': '1',
        'limit': '5',
        'convert': 'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key,
    }
    session = Session()
    session.headers.update(headers)

    # Pull data
    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)

    pd.set_option('display.max_columns', None)
    # Convert JSON to DataFrame
    data = pd.json_normalize(data['data'])
    print(data)

    # Load data into Postgres
    data.to_sql("crypto_api", engine, schema=schema, if_exists="replace", index=False)


# Create DAG
with DAG(
        dag_id="crypto_api",
        default_args=default_args,
        description='crypto_api',
        start_date=datetime(2023, 1, 1),
        schedule=None,
        # schedule_interval='@daily',
        catchup=False
) as dag:
    load_data = PythonOperator(
        task_id='extract',
        python_callable=extract
    )
