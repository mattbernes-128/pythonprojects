import urllib3
import pandas as pd
import sys
from pathlib import Path
import time
import os
import argparse

# Add local module path to the system path
sys.path.append(r'C:\Users\mattb\PycharmProjects\pythonprojects\cicdpractice\prod_lib')

# Import custom libraries and utilities
from pboc_lib import (
    collect_links, collect_dimension_links, collect_file_links,
    replace_special_chars, wait_for_download
)
from web_tools import (
    logger_setup, init_driver, find_element_wait, find_elements_wait
)
import pboc_lib

# Set root ingestion path
INGESTION_PATH = r'C:\Users\mattb\PycharmProjects\pythonprojects\cicdpractice\airflow-docker\ingestion_files'


###################################################################################################
# Collect Data from PBOC Website
###################################################################################################
def collect_data(year):
    logger = logger_setup()
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Initialize Selenium web driver
    web_driver = init_driver()
    data_collection_year = f'{year}'
    RAW_FILE_PATH = rf'{INGESTION_PATH}\pboc\{data_collection_year}\raw'

    # Ensure output directory exists
    Path(RAW_FILE_PATH).mkdir(parents=True, exist_ok=True)

    # Load target page
    data_index_page = "http://www.pbc.gov.cn/en/3688247/3688975/index.html"
    web_driver.get(data_index_page)

    # Scrape links for the specified year
    year_links = find_elements_wait(driver=web_driver, selector='ul > li[class="class_A"] > a')
    year_df = collect_links('Year', year_links)
    year_df.to_csv(f'{INGESTION_PATH}/year_link.csv', index=False)

    # Gather dimension and file-level links
    year_df_2 = collect_dimension_links(year_df, data_collection_year)
    year_df_3 = collect_file_links(year_df_2)
    year_df_3.to_csv(rf"{INGESTION_PATH}/year_df_3_{data_collection_year}.csv", index=False)

    # Reload for iteration
    year_df_3 = pd.read_csv(rf"{INGESTION_PATH}/year_df_3_{data_collection_year}.csv")

    for _, row in year_df_3.iterrows():
        print(row)
        year = row['Year']
        dimension = row['Dimension']
        dimension_clean = row['Dimension_Clean']
        data_name = row['Data']
        data_name_clean = row['Data_Name_Clean']
        data_link = row['Data_Link']
        data_type = data_link.split('/')[-1]

        # Format filename
        new_filename = f'{year}__{dimension_clean.replace(" ", "_")}__{data_name_clean.replace(" ", "_")}__{data_type}'
        new_filename = replace_special_chars(new_filename)

        print(new_filename)
        failed_downloads = pd.DataFrame(columns=['new_filename', 'link'])

        try:
            print(f"Downloading: {data_link}")
            web_driver = init_driver(RAW_FILE_PATH)
            web_driver.get(data_link)
            time.sleep(5)
            wait_for_download(RAW_FILE_PATH, new_filename, data_type, timeout=60)

        except Exception as e:
            failed_downloads = pd.concat(
                [failed_downloads, pd.DataFrame([{'new_filename': new_filename, 'link': data_link}])],
                ignore_index=True
            )
            print(f"❌ Failed: {e}")

        finally:
            web_driver.quit()
            print("WebDriver Closed\n")

        failed_downloads.to_csv(rf'{INGESTION_PATH}/failed_downloads_{data_collection_year}.csv', index=False)


###################################################################################################
# Format downloaded files using predefined templates
###################################################################################################
def format_files(year):
    data_collection_year = f'{year}'
    RAW_FILE_PATH = rf'{INGESTION_PATH}\pboc\{data_collection_year}\raw'
    FORMATTED_FILE_PATH = rf'{INGESTION_PATH}\pboc\{data_collection_year}\formatted'
    Path(FORMATTED_FILE_PATH).mkdir(parents=True, exist_ok=True)

    files = os.listdir(RAW_FILE_PATH)

    for file in files:
        print(file)
        parts = file.split('__')
        year, dimension, data_name = parts[0], parts[1], parts[2]

        print(f'Year: {year}\nDimension: {dimension}\nData Name: {data_name}')

        # Dynamically select and apply the appropriate formatting template
        template_func = None
        try:
            if dimension == 'Aggregate_Financing_to_the_Real_Economy':
                if data_name == 'Aggregate_Financing_to_the_Real_Economy_XFlowX':
                    template_func = pboc_lib.Aggregate_Financing_to_the_Real_Economy__Aggregate_Financing_to_the_Real_Economy_XFlowX_template
                elif data_name == 'Aggregate_Financing_to_the_Real_Economy_XStockX':
                    template_func = pboc_lib.Aggregate_Financing_to_the_Real_Economy__Aggregate_Financing_to_the_Real_Economy_XStockX_template

            elif dimension == 'Corporate_Goods_Price_Indices' and data_name == 'Corporate_Goods_Price_Indices_XCGPIX':
                template_func = pboc_lib.Corporate_Goods_Price_Indices__Corporate_Goods_Price_Indices_XCGPIX_template

            elif dimension == 'Financial_Accounts':
                template_func = pboc_lib.Financial_Accounts__Financial_Assets_and_Liabilities_StatementXFinancial_AccountsX__template

            elif dimension == 'Financial_Market_Statistics':
                template_map = {
                    'Statistics_of_Shibor': pboc_lib.Financial_Market_Statistics__Statistics_of_Shibor_template,
                    'Statistics_of_Interbank_Pledged_Repo': pboc_lib.Financial_Market_Statistics__Statistics_of_Interbank_Pledged_Repo_template,
                    'Statistics_of_Interbank_Lending': pboc_lib.Financial_Market_Statistics__Statistics_of_Interbank_Lending_template,
                    'Statistics_of_Chinabond_Government_Securities_Yield': pboc_lib.Financial_Market_Statistics__Statistics_of_Chinabond_Government_Securities_Yield_template,
                    'Statistics_of_Domestic_Debt_Securities': pboc_lib.Financial_Market_Statistics__Statistics_of_Domestic_Debt_Securities_template,
                    'Statistics_of_Stock_Market': pboc_lib.Financial_Market_Statistics__Statistics_of_Stock_Market_template,
                }
                template_func = template_map.get(data_name)

            elif dimension == 'sucffi':
                if data_name in ('sucfssoecb_RMB', 'sucflsoecb_big4_RMB', 'sucflsoecb_RMB'):
                    template_func = pboc_lib.sucffi__sucfssoecb_RMB_template
                elif data_name in ('sucDffi_RMB', 'sucffi_RMB'):
                    template_func = pboc_lib.sucffi__sucffi_RMB_template
                elif data_name in ('sucDffi_Foreign_Currency', 'sucffi_Foreign_Currency'):
                    template_func = pboc_lib.sucffi__sucffi_Foreign_Currency_template

            elif dimension == 'Money_and_Banking_Statistics':
                template_map = {
                    'Domestic_RMB_Financial_Assets_Held_by_Overseas_Entities': pboc_lib.Money_and_Banking_Statistics__Domestic_RMB_Financial_Assets_Held_by_Overseas_Entities_template,
                    'Exchange_Rate': pboc_lib.Money_and_Banking_Statistics__Exchange_Rate__template,
                    'Money_Supply': pboc_lib.Money_and_Banking_Statistics__Money_Supply__template,
                    'Balance_Sheet_of_Other_Depository_Corporations': pboc_lib.Money_and_Banking_Statistics__Balance_Sheet_of_Other_Depository_Corporations__template,
                    'Depository_Corporations_Survey': pboc_lib.Money_and_Banking_Statistics__Depository_Corporations_Survey__template,
                    'Balance_Sheet_of_Monetary_Authority': pboc_lib.Money_and_Banking_Statistics__Balance_Sheet_of_Monetary_Authority__template,
                    'Official_reserve_assets': pboc_lib.Money_and_Banking_Statistics__Official_reserve_assets__template,
                }
                template_func = template_map.get(data_name)

            if template_func:
                data_clean = template_func(file, year, RAW_FILE_PATH)
                print(data_clean)
                output_path = f'{FORMATTED_FILE_PATH}/{year}__{dimension}__{data_name}__clean.csv'
                data_clean.to_csv(output_path, index=False, encoding="utf-8-sig" if 'Official_reserve_assets' in data_name else None)
            else:
                print('No Template Found!')

        except Exception as e:
            print(f"⚠️ Error formatting {file}: {e}")


###################################################################################################
# Entry Point: CLI Interface
###################################################################################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run specific steps in the pipeline")
    parser.add_argument("--step", required=True, help="Step(s) to run: collect, format")
    parser.add_argument("--year", required=True, help="Year of data to process")

    args = parser.parse_args()
    steps = args.step.split(',')

    for step in steps:
        if step == "collect":
            print("### Step: Collect raw files ###")
            collect_data(args.year)
        elif step == "format":
            print("### Step: Format files with templates ###")
            format_files(args.year)
