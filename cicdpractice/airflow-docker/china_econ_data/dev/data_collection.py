import urllib3
import re
import sys
import pandas as pd
import os
import shutil
from selenium.webdriver.common.by import By
from pathlib import Path
import time

sys.path.append(r'/cicdpractice/prod_lib')
from web_tools import logger_setup, init_driver, find_element_wait, find_elements_wait

script_outputs_directory = r'C:\Users\mattb\PycharmProjects\pythonprojects\cicdpractice\airflow-docker\china_econ_data\script_outputs'
target_year = 2025
logger = logger_setup()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def replace_special_chars(text):
    return re.sub(r'[^A-Za-z0-9_.]', 'X', text)


def collect_links(Dimension, data_links):
    df = pd.DataFrame(columns=[Dimension, 'Link'])
    for data_link in data_links:
        link_dict = {Dimension: data_link.text, 'Link': data_link.get_attribute('href')}
        df = pd.concat([df, pd.DataFrame([link_dict])], ignore_index=True)
    print(f'collected {len(df)} links')
    return df


def collect_dimension_links(year_df, year=None):
    year_df_2_list = []
    if year is None:
        year_df = year_df
    else:
        year_df = year_df[year_df['Year'] == year]
        print(year_df)
    for index, row in year_df.iterrows():
        print(row)
        year = row['Year']
        year_link = row['Link']
        web_driver = init_driver()
        print(f"Navigating to {year_link}...")
        web_driver.get(year_link)
        dimension_links = find_elements_wait(driver=web_driver, selector='div[class="ListR"] > a')
        for data_link in dimension_links:
            dimension = data_link.text
            dimension_link = data_link.get_attribute("href")
            print(dimension)
            print(dimension_link)
            link_dict = {'Year': year, 'Year_Link': year_link, 'Dimension': dimension, 'Dimension_Link': dimension_link}
            year_df_2_list.append(link_dict)
        web_driver.quit()
    year_df_2 = pd.DataFrame(year_df_2_list)
    print(f'collected {len(year_df_2)} links')
    return year_df_2


def collect_file_links(year_df_2_sample):
    year_df_3_list = []
    for index, row in year_df_2_sample.iterrows():
        print(row)
        year = row['Year']
        year_link = row['Year_Link']
        dimension = row['Dimension']
        dimension_link = row['Dimension_Link']

        web_driver = init_driver()
        web_driver.get(dimension_link)

        table_cells = find_elements_wait(driver=web_driver, selector='table[class="data_table"] tbody > tr > td')

        row_name = None
        xls_link_text = None

        for cell in table_cells:
            if cell.get_attribute("class") != "data_xz":
                row_name = cell.text
                print(row_name)
            elif cell.get_attribute("class") == "data_xz":
                xls_link = cell.find_element(By.CSS_SELECTOR, 'a')
                if xls_link.text == "xls":
                    xls_link_text = xls_link.get_attribute('href')
                    print(xls_link_text)
                    # xls_link.click()

                    # Append to list
                    link_dict = {
                        'Year': year,
                        'Year_Link': year_link,
                        'Dimension': dimension,
                        'Dimension_Link': dimension_link,
                        'Data': row_name,
                        'Data_Link': xls_link_text
                    }
                    year_df_3_list.append(link_dict)

        web_driver.quit()

    # Create the DataFrame only once at the end
    year_df_3 = pd.DataFrame(year_df_3_list)
    return year_df_3


def wait_for_download(download_folder, new_filename, file_name, timeout=60):
    print(download_folder)
    end_time = time.time() + timeout
    print("Waiting for download to finish...")

    while time.time() < end_time:
        files = os.listdir(download_folder)

        # Exclude temporary download files
        downloading = [f for f in files if f.endswith(".crdownload") or f.endswith(".part")]

        print("✅ Download Completed")
        # Find the downloaded file (Assume the last modified file is the download)
        downloaded_file = os.path.join(download_folder, file_name)
        downloaded_file = max([os.path.join(download_folder, f) for f in files], key=os.path.getmtime)
        renamed_file = os.path.join(download_folder, new_filename)
        os.rename(downloaded_file, renamed_file)
        print(f"File renamed to: {new_filename}")
        return True
        time.sleep(2)  # Check every 2 seconds

    print("❌ Download Timed Out")
    return False

web_driver = init_driver()
data_index_page = "http://www.pbc.gov.cn/en/3688247/3688975/index.html"
web_driver.get(data_index_page)
year_links = find_elements_wait(driver=web_driver, selector='ul > li[class="class_A"] > a')
year_df = collect_links('Year', year_links)
year_df.to_csv(script_outputs_directory+'\year_link.csv', index=False)

year_df=pd.read_csv(script_outputs_directory+'\year_link.csv')
year_df_2=collect_dimension_links(year_df,target_year)

year_df_3=collect_file_links(year_df_2)
year_df_3.to_csv(script_outputs_directory+f'\year_df_3_{target_year}.csv', index=False)

year_df_3=pd.read_csv(script_outputs_directory+f'\year_df_3_{target_year}.csv')
#year_df_3=year_df_3[year_df_3['Dimension'] == 'Sources and Uses of Credit Funds of Financial Institutions']
year_df_3['Dimension_Clean']=year_df_3['Dimension']
year_df_3['Data_Name_Clean']=year_df_3['Data']
year_df_3['Dimension_Clean'] = year_df_3['Dimension_Clean'].replace('Sources and Uses of Credit Funds of Financial Institutions','sucffi')
year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace('Sources & Uses of Credit Funds of Financial Institutions (in RMB and Foreign Currency)','sucffi RMB and Foreign Currency')
year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace('Sources & Uses of Credit Funds of Financial Institutions (in Foreign Currency)','sucffi Foreign Currency')
year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace('Sources & Uses of Credit Funds of Depository Financial Institutions (RMB)','sucDffi RMB')
year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace('Sources & Uses of Credit Funds of Depository Financial Institutions (in RMB and Foreign Currency)','sucDffi RMB and Foreign Currency')
year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace('Sources & Uses of Credit Funds of Depository Financial Institutions (in Foreign Currency)','sucDffi Foreign Currency')
year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace('Sources & Uses of Credit Funds of Financial Institutions (RMB)','sucffi RMB')
year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace('Sources & Uses of Credit Funds of large-sized State-owned National-operating Commercial Banks (RMB)','sucflsoecb RMB')
year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace('Sources & Uses of Credit Funds of 4 largest State-owned National-operating Commercial Banks (RMB)','sucflsoecb big4 RMB')
year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace('Sources & Uses of Credit Funds of medium & small-sized State-owned National-operating Commercial Banks (RMB)','sucfssoecb RMB')

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

