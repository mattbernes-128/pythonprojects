from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def extract():
	print("Extracting data...")

def transform():
	print("Transforming data...")

def load():
	print("Loading data...")

default_args = {
	'owner': 'airflow',
	'depends_on_past': False,
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
}

with DAG(
	'simple_etl',
	default_args=default_args,
	description='A simple ETL process',
	schedule_interval='@daily',
	start_date=datetime(2023, 1, 1),
	catchup=False,
) as dag:

	task1 = PythonOperator(task_id='extract', python_callable=extract)
	task2 = PythonOperator(task_id='transform', python_callable=transform)
	task3 = PythonOperator(task_id='load', python_callable=load)

	task1 >> task2 >> task3