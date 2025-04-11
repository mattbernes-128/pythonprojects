import urllib3
import pandas as pd
import sys
from pathlib import Path
import time
sys.path.append(r'C:\Users\mattb\PycharmProjects\pythonprojects\cicdpractice\prod_lib')
from pboc_lib import collect_links, collect_dimension_links, collect_file_links,replace_special_chars,wait_for_download
from web_tools import logger_setup,init_driver,find_element_wait,find_elements_wait
import pboc_lib
import os
import argparse
#set collection year
#set paths
INGESTION_PATH=r'C:\Users\mattb\PycharmProjects\pythonprojects\cicdpractice\airflow-docker\ingestion_files'
#setup


def collect_data(year):
    logger = logger_setup()
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    web_driver = init_driver()
    data_collection_year = f'{year}'
    RAW_FILE_PATH = rf'{INGESTION_PATH}\pboc\{data_collection_year}\raw'
    dir_path = Path(RAW_FILE_PATH)
    dir_path.mkdir(parents=True, exist_ok=True)
    data_index_page = "http://www.pbc.gov.cn/en/3688247/3688975/index.html"
    web_driver.get(data_index_page)
    year_links = find_elements_wait(driver=web_driver, selector='ul > li[class="class_A"] > a')
    year_df = collect_links('Year', year_links)
    year_df.to_csv(f'{INGESTION_PATH}/year_link.csv', index=False)
    year_df_2 = collect_dimension_links(year_df, data_collection_year)
    year_df_3 = collect_file_links(year_df_2)
    year_df_3.to_csv(rf"{INGESTION_PATH}/year_df_3_{data_collection_year}.csv", index=False)
    year_df_3=pd.read_csv(rf"{INGESTION_PATH}/year_df_3_{data_collection_year}.csv")
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
        failed_downloads = pd.DataFrame(columns=['new_filename', 'link'])
        try:
            print(f"Downloading: {data_link}")
            web_driver = init_driver(RAW_FILE_PATH)
            web_driver.get(data_link)
            time.sleep(5)
            wait_for_download(RAW_FILE_PATH, new_filename, data_type, timeout=60)

        except Exception as e:
            link_dict = {'new_filename': new_filename, 'link': data_link}
            failed_downloads = pd.concat([failed_downloads, pd.DataFrame([link_dict])], ignore_index=True)
            print(f"❌ Failed: {e}")

        finally:
            web_driver.quit()
            print("WebDriver Closed\n")
        failed_downloads.to_csv(rf'{INGESTION_PATH}/failed_downloads_{data_collection_year}.csv', index=False)

def format_files(year):
    data_collection_year = f'{year}'
    RAW_FILE_PATH = rf'{INGESTION_PATH}\pboc\{data_collection_year}\raw'
    FORMATTED_FILE_PATH = rf'{INGESTION_PATH}\pboc\{data_collection_year}\formatted'
    dir_path = Path(FORMATTED_FILE_PATH)
    dir_path.mkdir(parents=True, exist_ok=True)
    files = os.listdir(RAW_FILE_PATH)  # List all files and folders
    for file in files:
        print(file)
        file_parts=file.split('__')
        year=file_parts[0]
        dimension=file_parts[1]
        data_name=file_parts[2]
        print(f'Year: {year}')
        print(f'Dimension: {dimension}')
        print(f'Data Name: {data_name}')
        if dimension=='Aggregate_Financing_to_the_Real_Economy' and data_name=='Aggregate_Financing_to_the_Real_Economy_XFlowX':
            data_clean=pboc_lib.Aggregate_Financing_to_the_Real_Economy__Aggregate_Financing_to_the_Real_Economy_XFlowX_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Aggregate_Financing_to_the_Real_Economy' and data_name=='Aggregate_Financing_to_the_Real_Economy_XStockX':
            data_clean=pboc_lib.Aggregate_Financing_to_the_Real_Economy__Aggregate_Financing_to_the_Real_Economy_XStockX_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Corporate_Goods_Price_Indices' and data_name=='Corporate_Goods_Price_Indices_XCGPIX':
              data_clean=pboc_lib.Corporate_Goods_Price_Indices__Corporate_Goods_Price_Indices_XCGPIX_template(file,year,RAW_FILE_PATH)
              print(data_clean)
              data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Financial_Accounts' and data_name=='Financial_Assets_and_Liabilities_StatementXFinancial_AccountsX':
            data_clean=pboc_lib.Financial_Accounts__Financial_Assets_and_Liabilities_StatementXFinancial_AccountsX__template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Financial_Accounts' and data_name=='Flow_of_Funds_StatementXFinancial_AccountsXXthe_First_Half_YearX':
            data_clean=pboc_lib.Financial_Accounts__Financial_Assets_and_Liabilities_StatementXFinancial_AccountsX__template(file,year,RAW_FILE_PATH)
            print(data_clean.iloc[25:])
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Financial_Market_Statistics' and data_name=='Statistics_of_Shibor':
            data_clean=pboc_lib.Financial_Market_Statistics__Statistics_of_Shibor_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Financial_Market_Statistics' and data_name=='Statistics_of_Interbank_Pledged_Repo':
            data_clean=pboc_lib.Financial_Market_Statistics__Statistics_of_Interbank_Pledged_Repo_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Financial_Market_Statistics' and data_name=='Statistics_of_Interbank_Lending':
            data_clean=pboc_lib.Financial_Market_Statistics__Statistics_of_Interbank_Lending_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Financial_Market_Statistics' and data_name=='Statistics_of_Chinabond_Government_Securities_Yield':
            data_clean=pboc_lib.Financial_Market_Statistics__Statistics_of_Chinabond_Government_Securities_Yield_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Financial_Market_Statistics' and data_name=='Statistics_of_Domestic_Debt_Securities':
            data_clean=pboc_lib.Financial_Market_Statistics__Statistics_of_Domestic_Debt_Securities_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Financial_Market_Statistics' and data_name=='Statistics_of_Stock_Market':
            data_clean=pboc_lib.Financial_Market_Statistics__Statistics_of_Stock_Market_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='sucffi' and data_name in ('sucfssoecb_RMB','sucflsoecb_big4_RMB','sucflsoecb_RMB') :
            data_clean=pboc_lib.sucffi__sucfssoecb_RMB_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='sucffi' and data_name in ('sucDffi_RMB','sucffi_RMB') :
            data_clean=pboc_lib.sucffi__sucffi_RMB_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='sucffi' and data_name in ('sucDffi_Foreign_Currency','sucffi_Foreign_Currency') :
            data_clean=pboc_lib.sucffi__sucffi_Foreign_Currency_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Money_and_Banking_Statistics' and data_name in ('Domestic_RMB_Financial_Assets_Held_by_Overseas_Entities') :
            data_clean=pboc_lib.Money_and_Banking_Statistics__Domestic_RMB_Financial_Assets_Held_by_Overseas_Entities_template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Money_and_Banking_Statistics' and data_name in ('Exchange_Rate') :
            data_clean=pboc_lib.Money_and_Banking_Statistics__Exchange_Rate__template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Money_and_Banking_Statistics' and data_name in ('Money_Supply') :
            data_clean=pboc_lib.Money_and_Banking_Statistics__Money_Supply__template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Money_and_Banking_Statistics' and data_name in ('Balance_Sheet_of_Other_Depository_Corporations') :
            data_clean=pboc_lib.Money_and_Banking_Statistics__Balance_Sheet_of_Other_Depository_Corporations__template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Money_and_Banking_Statistics' and data_name in ('Depository_Corporations_Survey') :
            data_clean=pboc_lib.Money_and_Banking_Statistics__Depository_Corporations_Survey__template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Money_and_Banking_Statistics' and data_name in ('Balance_Sheet_of_Monetary_Authority') :
            data_clean=pboc_lib.Money_and_Banking_Statistics__Balance_Sheet_of_Monetary_Authority__template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False)
        elif dimension=='Money_and_Banking_Statistics' and data_name in ('Official_reserve_assets') :
            data_clean=pboc_lib.Money_and_Banking_Statistics__Official_reserve_assets__template(file,year,RAW_FILE_PATH)
            print(data_clean)
            data_clean.to_csv(FORMATTED_FILE_PATH+f'/{year}__{dimension}__{data_name}__clean.csv', index=False, encoding="utf-8-sig")
        else:
            print('No Template Found!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run specific steps in the pipeline")
    parser.add_argument("--step", required=True, help="Which step to run")
    parser.add_argument("--year", required=True, help="Which step to run")
    args = parser.parse_args()
    print(args.step)
    print(args.year)
    steps=args.step.split(',')
    for step in steps:
        if step == "collect":
            print("#########################################################################################")
            print('download raw files')
            print("#########################################################################################")
            collect_data(args.year)
        elif step == "format":
            print("#########################################################################################")
            print('template raw files')
            print("#########################################################################################")
            format_files(args.year)
