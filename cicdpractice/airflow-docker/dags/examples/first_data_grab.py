
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import pandas as pd
import json
from utils.sql_tools import ps_engine_init

db_name = "research_db_v1"
schema="land"
engine = ps_engine_init(db_name)
conn = BaseHook.get_connection("crypto_api")
api_key = conn.password


default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
}

def extract():
	url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
	parameters = {
	  'start':'1',
	  'limit':'5',
	  'convert':'USD'
	}
	headers = {
	  'Accepts': 'application/json',
	  'X-CMC_PRO_API_KEY': api_key,
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

	data.to_sql("crypto_api", engine, schema=schema, if_exists="replace", index=False)

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