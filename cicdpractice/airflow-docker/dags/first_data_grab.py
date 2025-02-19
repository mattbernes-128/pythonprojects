from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import pandas as pd
import json
from sqlalchemy import create_engine



DB_NAME = "research_db_v1"
USER = Variable.get("POSTGRES_USER", default_var="not found")
PASSWORD = Variable.get("POSTGRES_PASSWORD", default_var="not found")
HOST = "postgres"  # Change to container name or IP if needed
PORT = "5432"  # Use the exposed port
SCHEMA="land"

# Create SQLAlchemy engine
engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
}

#postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
#conn = postgres_hook.get_conn()
#sql_engine = conn.cursor()



def extract():
	url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
	parameters = {
	  'start':'1',
	  'limit':'5',
	  'convert':'USD'
	}
	headers = {
	  'Accepts': 'application/json',
	  'X-CMC_PRO_API_KEY': '1bab87fd-ff0f-4cb7-9ded-96361cfcda0e',
	}

	session = Session()
	session.headers.update(headers)

	try:
	  response = session.get(url, params=parameters)
	  data = json.loads(response.text)
	except (ConnectionError, Timeout, TooManyRedirects) as e:
	  print(e)
	pd.set_option('display.max_columns', None)
	data=pd.json_normalize(data['data'])
	print(data)

	data.to_sql("crypto_api", engine, schema=SCHEMA, if_exists="replace", index=False)

	print("Extracting data...")


def transform():
	print("Transforming data...")

def load():
	print("Loading data...")


with DAG(
	dag_id="crypto_api",
	default_args=default_args,
	description='crypto_api',
	start_date=datetime(2023, 1, 1),
	schedule=None,
	#schedule_interval='@daily',
	catchup=False
) as dag:

	task1 = PythonOperator(task_id='extract', python_callable=extract)
	task2 = PythonOperator(task_id='transform', python_callable=transform)
	task3 = PythonOperator(task_id='load', python_callable=load)

	task1 >> task2 >> task3