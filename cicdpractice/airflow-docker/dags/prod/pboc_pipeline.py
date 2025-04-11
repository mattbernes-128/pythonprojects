# Import required libraries
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
from utils.sql_tools import ps_engine_init
from utils.pboc_lib import ps_engine_init
import pandas as pd
import glob

# Import custom library tools
from utils.sql_tools import ps_engine_init
data_collection_year='2022'

# Create SQLAlchemy engine to push data to Postgres
postgres_hook = PostgresHook(postgres_conn_id="research_db_v1")


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

def collect_files() :
    target_path=RAW_FILE_PATH
    dir_path = Path(target_path)
    dir_path.mkdir(parents=True, exist_ok=True)
    web_driver = init_driver()
    data_index_page = "http://www.pbc.gov.cn/en/3688247/3688975/index.html"
    web_driver.get(data_index_page)
    year_links = find_elements_wait(driver=web_driver, selector='ul > li[class="class_A"] > a')
    year_df = collect_links('Year', year_links)
    year_df.to_csv(f'/opt/airflow/ingestion_files/year_link.csv', index=False)
    year_df_2 = collect_dimension_links(year_df, data_collection_year)
    year_df_3 = collect_file_links(year_df_2)
    year_df_3.to_csv(rf"/opt/airflow/ingestion_files/year_df_3_{data_collection_year}.csv", index=False)

for index, row in year_df_3.iterrows():
    print(row)
    year = row['Year']
    dimension = row['Dimension']
    dimension_clean = row['Dimension_Clean']
    data_name = row['Data']
    data_name_clean = row['Data_Name_Clean']
    data_link = row['Data_Link']
    data_type = data_link.split('/')[-1]
    new_filename = f'{year}__{dimension_clean.replace(' ', '_')}__{data_name_clean.replace(' ', '_')}__{data_type}'
    new_filename = replace_special_chars(new_filename)
    print(new_filename)
    target_path = fr"C:\Users\mattb\PycharmProjects\pythonprojects\cicdpractice\airflow-docker\china_econ_data\download\{target_year}"
    dir_path = Path("target_path")
    dir_path.mkdir(parents=True, exist_ok=True)
    failed_downloads = pd.DataFrame(columns=['new_filename', 'link'])
    try:
        print(f"Downloading: {data_link}")
        web_driver = init_driver(target_path)
        web_driver.get(data_link)
        time.sleep(5)
        wait_for_download(target_path, new_filename, data_type, timeout=60)

    except Exception as e:
        link_dict = {'new_filename': new_filename, 'link': data_link}
        failed_downloads = pd.concat([failed_downloads, pd.DataFrame([link_dict])], ignore_index=True)
        print(f"❌ Failed: {e}")

    finally:
        web_driver.quit()
        print("WebDriver Closed\n")
    failed_downloads.to_csv(script_outputs_directory + f'failed_downloads_{target_year}.csv')


def format_files() :
    files = glob.glob(f"{CSV_FILE_PATH}/*.csv")  # Get all CSV files
    for file in files:
        file_name=file.split('/')[-1]
        print(file_name)
        print(len(file_name))
        upload_csv(CSV_FILE_PATH,file_name)
        print(f'file upload complete {file}')
def upload_files(CSV_FILE_PATH) :
    files = glob.glob(f"{FINAL_FILE_PATH}/*.csv")  # Get all CSV files
    for file in files:
        file_name=file.split('/')[-1]
        print(file_name)
        print(len(file_name))
        upload_csv(FINAL_FILE_PATH,file_name)
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
    collect_files = PythonOperator(
        task_id="collect",
        python_callable=collect_files,
        op_args=[CSV_FILE_PATH]  # Pass the argument CSV_FILE_PATH
    format_files = PythonOperator(
        task_id="format_files",
        python_callable=format_files,
        op_args=[CSV_FILE_PATH]  # Pass the argument CSV_FILE_PATH
    upload_files = PythonOperator(
        task_id="upload_files",
        python_callable=upload_files,
        op_args=[CSV_FILE_PATH]  # Pass the argument CSV_FILE_PATH
    )

    collect_files > format_files > upload_files