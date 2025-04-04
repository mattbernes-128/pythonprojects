# Import required libraries
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

# Create Postgres hook to perform SQL tasks
postgres_hook = PostgresHook(postgres_conn_id="research_db_v1")

# Set default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


# Create table and insert data into Postgres
def load_data_to_postgres(**kwargs):
    sql = """
        CREATE TABLE IF NOT EXISTS land.my_table (
            id INT PRIMARY KEY,
            name VARCHAR(255)
        );
        INSERT INTO land.my_table (id, name) VALUES (3, 'Airflow Data');
    """
    postgres_hook.run(sql)


# Create DAG
with DAG(
        dag_id="postgres_example",
        default_args=default_args,
        start_date=datetime(2023, 10, 27),
        schedule=None,  # Run manually or set a schedule
        catchup=False,
) as dag:
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data_to_postgres,
    )
