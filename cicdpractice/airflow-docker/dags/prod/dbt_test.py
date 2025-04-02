from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='run_dbt',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt_pboc && dbt deps',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_pboc && dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_pboc && dbt test',
    )

    dbt_run >> dbt_test