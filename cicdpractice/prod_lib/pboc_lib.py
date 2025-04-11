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
    year_df_3['Dimension_Clean'] = year_df_3['Dimension']
    year_df_3['Data_Name_Clean'] = year_df_3['Data']
    year_df_3['Dimension_Clean'] = year_df_3['Dimension_Clean'].replace(
        'Sources and Uses of Credit Funds of Financial Institutions', 'sucffi')
    year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace(
        'Sources & Uses of Credit Funds of Financial Institutions (in RMB and Foreign Currency)',
        'sucffi RMB and Foreign Currency')
    year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace(
        'Sources & Uses of Credit Funds of Financial Institutions (in Foreign Currency)', 'sucffi Foreign Currency')
    year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace(
        'Sources & Uses of Credit Funds of Depository Financial Institutions (RMB)', 'sucDffi RMB')
    year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace(
        'Sources & Uses of Credit Funds of Depository Financial Institutions (in RMB and Foreign Currency)',
        'sucDffi RMB and Foreign Currency')
    year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace(
        'Sources & Uses of Credit Funds of Depository Financial Institutions (in Foreign Currency)',
        'sucDffi Foreign Currency')
    year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace(
        'Sources & Uses of Credit Funds of Financial Institutions (RMB)', 'sucffi RMB')
    year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace(
        'Sources & Uses of Credit Funds of large-sized State-owned National-operating Commercial Banks (RMB)',
        'sucflsoecb RMB')
    year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace(
        'Sources & Uses of Credit Funds of 4 largest State-owned National-operating Commercial Banks (RMB)',
        'sucflsoecb big4 RMB')
    year_df_3['Data_Name_Clean'] = year_df_3['Data_Name_Clean'].replace(
        'Sources & Uses of Credit Funds of medium & small-sized State-owned National-operating Commercial Banks (RMB)',
        'sucfssoecb RMB')

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


##################################################################################################################
##################################################################################################################
##templates
##################################################################################################################
##################################################################################################################
import pandas as pd
import os


def Aggregate_Financing_to_the_Real_Economy__Aggregate_Financing_to_the_Real_Economy_XFlowX_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    if year == '2022':
        df.columns = df.iloc[9, 0:12].str.strip()  # Row 10 as header (zero-based index = 9)
        print(df)
        df = df.iloc[11:23, 0:12]
        df = df.reset_index(drop=True)
        print(df)
        df = df.astype({"Month": "string"})
        return df
    elif year == '2020':
        df.columns = df.iloc[8, 0:12].str.strip()  # Row 10 as header (zero-based index = 9)
        df = df.iloc[10:22, 0:12]
        df = df.reset_index(drop=True)
        df.rename(columns={df.columns[0]: "Month"}, inplace=True)
        print(df.columns)
        df = df.astype({"Month": "string"})
    else:
        df.columns = df.iloc[8, 0:12].str.strip()  # Row 10 as header (zero-based index = 9)
        df = df.iloc[10:22, 0:12]
        df = df.reset_index(drop=True)
        print(df.columns)
        df = df.astype({"Month": "string"})
    return df


def Aggregate_Financing_to_the_Real_Economy__Aggregate_Financing_to_the_Real_Economy_XStockX_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    if year == '2022':
        df_clean = df.iloc[7:, 1:].reset_index(drop=True).dropna()
        df_years = df.iloc[4, 1:].reset_index(drop=True).dropna()
        print(df_years)
    elif year == '2024':
        df_clean = df.iloc[6:, 1:].reset_index(drop=True).dropna()
        df_years = df.iloc[3, 1:].reset_index(drop=True).dropna()
    elif year == '2025':
        df_clean = df.iloc[6:, 1:].reset_index(drop=True).dropna(axis=1, how='all')
        df_clean = df_clean.reset_index(drop=True).dropna()
        df_years = df.iloc[3, 1:].reset_index(drop=True).dropna()
    else:
        df_clean = df.iloc[7:, 1:].reset_index(drop=True).dropna()
        df_years = df.iloc[4, 1:].reset_index(drop=True).dropna()
    df_clean = df_clean.iloc[:, ::2]

    column_length = len(df_clean.columns)
    df_clean.columns = df_years[0:column_length]
    column_names = ['AFRE(stock)', 'RMB loans', 'Foreign currency-denominated loans', 'Entrusted loans', 'Trust loans',
                    'Undiscounted bankers acceptances',
                    'Net financing of corporate bonds', 'Government bonds',
                    ' Equity financing on the domestic stock market by non-financial entities',
                    'Asset-backed securities of depository financial institutions', 'Loans written off']
    df_clean.insert(0, 'column_names', column_names)
    df_clean = df_clean.reset_index(drop=True)
    df_clean = df_clean.set_index("column_names").T.reset_index()
    df_clean.rename(columns={df_clean.columns[0]: "Year"}, inplace=True)
    return (df_clean)


def Corporate_Goods_Price_Indices__Corporate_Goods_Price_Indices_XCGPIX_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    #    df.columns = df.iloc[5, 0:4].str.strip()  # Row 10 as header (zero-based index = 9)
    if year == '2024':
        df = df.iloc[5:21, 0:4]
        df = df.reset_index(drop=True)
        df.columns = df.iloc[0]  # Set first row as column names
    else:
        df = df.iloc[6:21, 0:4]
        df = df.reset_index(drop=True)
        df.columns = df.iloc[0]  # Set first row as column names
    df = df.drop(0).reset_index(drop=True)
    df.rename(columns={df.columns[0]: "Year"}, inplace=True)
    # df=df.astype({"Month": "string"})
    return df


def Financial_Accounts__Financial_Assets_and_Liabilities_StatementXFinancial_AccountsX__template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    #    df.columns = df.iloc[5, 0:4].str.strip()  # Row 10 as header (zero-based index = 9)
    df = df.iloc[8:31, 2:18]
    dimensions = ['Net financial investment', 'Financial uses', 'Financial sources', 'Currency and Deposits',
                  'Currency and Deposits and Demand Deposits', 'Currency and Deposits and Time Deposits',
                  'Currency and Deposits and Fiscal Deposits', 'Currency and Deposits and Foriegn Exchange Deposits',
                  'Loans', 'Loans Short Term and Bills of Financing', 'Loans Medium Term and Long Term',
                  'Loans Foriegn Exchange',
                  'Undiscounted Bankers Acceptance Bills', 'Insurance Technical Reserves', 'Bonds',
                  'Bonds Government Bonds', 'Bpnds Corporate Bonds',
                  'Securities Investment Fund Shares', 'Misc', 'FDI', 'FDI Equity', 'Other Assets and Debts',
                  'Internstional Reserve Assets']
    columns = ['Serial num', 'Households Uses', 'Households Sources', 'Non-Financial Corportations Uses',
               'Non-Financial Corportations Sources', 'General Government Uses'
        , 'General Government Sources', 'Financial Institutions Uses', 'Financial Institutions Sorces',
               'All Domestic Sectors Uses', 'All Domestic Sectors Sources', 'The Rest of the World Uses',
               'The Rest of the World Sources', 'Total Uses', 'Total Sources']
    df.columns = columns
    df['dimensions'] = dimensions
    df = df.reset_index(drop=True)
    return df


def Financial_Market_Statistics__Statistics_of_Shibor_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[5:17][0:]
    df_columns = df.iloc[4][0:].reset_index(drop=True)
    df_columns.iloc[0] = 'Year'
    df_data.columns = df_columns
    return df_data


def Financial_Market_Statistics__Statistics_of_Interbank_Pledged_Repo_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[8:20, 0:23]
    df_columns_1 = ['', '1-Day_', '1-Day_', '7-Day_', '7-Day_', '14-Day_', '14-Day_', '21-Day', ' 21-Day_', '1-Month_',
                    '1-Month_',
                    '2-Month_', '2-Month_', '3-Month_', '3-Month_', '4-Month_', '4-Month_', '6-Month_', '6-Month_',
                    '9-Month_',
                    '9-Month_', '1-Year_', '1-Year_']
    df_columns_2 = df.iloc[7, 0:23].reset_index(drop=True)
    df_columns_2.iloc[0] = 'Year'
    df_columns = [a + b for a, b in zip(df_columns_1, df_columns_2)]
    df_data.columns = df_columns
    return df_data


def Financial_Market_Statistics__Statistics_of_Interbank_Lending_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[5:20, 0:25]
    df_columns = df.iloc[7, 0:25].reset_index(drop=True)
    df_columns.iloc[0] = 'Year'
    df_data.columns = df_columns
    return df_data


def Financial_Market_Statistics__Statistics_of_Chinabond_Government_Securities_Yield_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[5:17, 0:12]
    df_columns = df.iloc[4, 0:12].reset_index(drop=True)
    df_columns.iloc[0] = 'Year'
    df_data.columns = df_columns
    return df_data


def Financial_Market_Statistics__Statistics_of_Domestic_Debt_Securities_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[7:19, 0:13]
    df_columns_1 = df.iloc[4, 0:13].reset_index(drop=True)
    df_columns_1 = ['', 'Government_Securities__', 'Government_Securities__', 'Central_Bank_Bills__',
                    'Central_Bank_Bills__', 'Financial_Bonds__',
                    'Financial_Bonds__', 'Corporate_Debenture_Bonds__', 'Corporate_Debenture_Bonds__',
                    'International_Institution_Bonds__',
                    'International_Institution_Bonds__', 'Total__', 'Total__']
    df_columns_2 = df.iloc[6, 0:13].reset_index(drop=True)
    df_columns_2.iloc[0] = 'Year'
    df_columns = [a + b for a, b in zip(df_columns_1, df_columns_2)]
    df_data.columns = df_columns
    return df_data


def Financial_Market_Statistics__Statistics_of_Stock_Market_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[7:19, 0:7]
    df_columns = df.iloc[4, 0:7].reset_index(drop=True)
    df_columns.iloc[0] = 'Year'
    df_data.columns = df_columns
    return df_data


def sucffi__sucfssoecb_RMB_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[6:42, 1:13]
    df_years = df.iloc[4, 1:13]
    df_data.columns = df_years
    funds_list = [
        "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources",
        "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources",
        "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources",
        "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources",
        "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources",
        "Funds Sources", "Funds Uses", "Funds Uses", "Funds Uses",
        "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses",
        "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses",
        "Funds Uses", "Funds Uses"
    ]
    funds_list_2 = [
        "Funds Sources", "Total Deposits", "Domestic Deposits", "Domestic Deposits",
        "Domestic Deposits", "Domestic Deposits", "Domestic Deposits", "Domestic Deposits",
        "Domestic Deposits", "Domestic Deposits", "Domestic Deposits", "Domestic Deposits",
        "Domestic Deposits", "Domestic Deposits", "Overseas Deposits", "Financial Bonds",
        "Repo", "Borrowings from the Central Bank", "Inter-bank Transactions", "Other Items",
        "Total Funds Sources", "Funds Uses", "Total Loans", "Domestic Loans", "Domestic Loans",
        "Domestic Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans", "Overseas Loans",
        "Portfoli Investments", "Shares and other Investments", "Reverse Repo",
        "Reserves with the Central Bank", "Inter-bank Transactions", "Total Funds Uses"
    ]
    funds_list_3 = [
        "Funds Sources", "Total Deposits", "Domestic Deposits", "Individual Deposits",
        "Individual Deposits", "Individual Deposits", "Individual Deposits", "Corporate Deposits",
        "Corporate Deposits", "Corporate Deposits", "Corporate Deposits", "Corporate Deposits",
        "Time Depsits of Treasury", "Deposits of Non-depository Financial Institutions", "Overseas Deposits",
        "Financial Bonds", "Repo", "Borrowings from the Central Bank", "Inter-bank Transactions",
        "Other Items", "Total Funds Sources", "Funds Uses", "Total Loans", "Domestic Loans",
        "Domestic Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans",
        "Overseas Loans", "Portfoli Investments", "Shares and other Investments", "Reverse Repo",
        "Reserves with the Central Bank", "Inter-bank Transactions", "Total Funds Uses"
    ]
    funds_list_4 = [
        "Funds Sources", "Total Deposits", "Domestic Deposits", "Individual Deposits",
        "Demand Deposits", "Time Deposits", "Structural Deposits", "Corporate Deposits",
        "Demand Deposits", "Time Deposits", "Margin Deposits", "Structural Deposits",
        "Time Depsits of Treasury", "Deposits of Non-depository Financial Institutions",
        "Overseas Deposits", "Financial Bonds", "Repo", "Borrowings from the Central Bank",
        "Inter-bank Transactions", "Other Items", "Total Funds Sources", "Funds Uses",
        "Total Loans", "Domestic Loans", "Short-term Loans", "Mid and Long-term Loans",
        "Paper Financing", "Financial Leases", "Total Advences", "Overseas Loans",
        "Portfoli Investments", "Shares and other Investments", "Reverse Repo",
        "Reserves with the Central Bank", "Inter-bank Transactions", "Total Funds Uses"
    ]
    df_data['funds_list'] = funds_list
    df_data['funds_list_2'] = funds_list_2
    df_data['funds_list_3'] = funds_list_3
    df_data['funds_list_4'] = funds_list_4
    df_data = df_data.iloc[:, -4:].join(df_data.iloc[:, :-4])
    return df_data


def sucffi__sucffi_RMB_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[6:48, 1:13]
    df_years = df.iloc[4, 1:13]
    df_data.columns = df_years
    funds_list = [
        "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources",
        "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources",
        "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources",
        "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses",
        "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses",
        "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses",
        "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses"]
    funds_list_2 = [
        "Funds Sources", "Total Deposits", "Domestic Deposits", "Domestic Deposits", "Domestic Deposits",
        "Domestic Deposits",
        "Domestic Deposits", "Domestic Deposits", "Domestic Deposits", "Domestic Deposits", "Domestic Deposits",
        "Domestic Deposits", "Overseas Deposits",
        "Financial Bonds", "Currency in Circulation", "Liabilities to International Financial Institutions",
        "Other Items", "Total Funds Sources",
        "Total Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans",
        "Domestic Loans", "Domestic Loans",
        "Domestic Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans",
        "Domestic Loans", "Domestic Loans",
        "Overseas Loans", "Portfolio Loans", "Portfolio Investments", "Shares and Other Investments",
        "Positions for Bullion Purchases",
        "Foreign Exchange", "Assets with International Financial Institutions", "Total Funds Uses"]
    funds_list_3 = [
        "Funds Sources", "Total Deposits", "Domestic Deposits", "Deposits of Households", "Deposits of Households",
        "Deposits of Households",
        "Deposits of Non-Financial Enterprises", "Deposits of Non-Financial Enterprises",
        "Deposits of Non-Financial Enterprises",
        "Deposits of Government Departments and Organizations", "Fiscal Deposits",
        "Deposits of Non-Depository Financial Institutions", "Overseas Deposits",
        "Financial Bonds", "Currency in Circulation", "Liabilities to International Financial Institutions",
        "Other Items", "Total Funds Sources",
        "Total Loans", "Domestic Loans", "Loans to Households", "Loans to Households", "Loans to Households",
        "Loans to Households",
        "Loans to Households", "Loans to Households", "Loans to Households",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-Depository Financial Institutions",
        "Overseas Loans", "Portfolio Loans", "Portfolio Investments", "Shares and Other Investments",
        "Positions for Bullion Purchases", "Foreign Exchange",
        "Assets with International Financial Institutions", "Total Funds Uses"]
    funds_list_4 = [
        "Funds Sources", "Total Deposits", "Domestic Deposits", "Deposits of Households", "Demand Deposits",
        "Time and Other Deposits",
        "Deposits of Non-Financial Enterprises", "Demand Deposits", "Time and Other Deposits",
        "Deposits of Government Departments and Organizations",
        "Fiscal Deposits", "Deposits of Non-Depository Financial Institutions", "Overseas Deposits", "Financial Bonds",
        "Currency in Circulation",
        "Liabilities to International Financial Institutions", "Other Items", "Total Funds Sources", "Total Loans",
        "Domestic Loans", "Loans to Households",
        "Short-term Loans", "Consumption Loans", "Operating Loans", "Mid and Long-term Loans", "Consumption Loans",
        "Operating Loans", "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Short-term Loans", "Mid and Long-term Loans", "Paper Financing", "Financial Leases", "Total Advances",
        "Loans to Non-Depository Financial Institutions",
        "Overseas Loans", "Portfolio Loans", "Portfolio Investments", "Shares and Other Investments",
        "Positions for Bullion Purchases",
        "Foreign Exchange", "Assets with International Financial Institutions", "Total Funds Uses"]
    df_data['funds_list'] = funds_list
    df_data['funds_list_2'] = funds_list_2
    df_data['funds_list_3'] = funds_list_3
    df_data['funds_list_4'] = funds_list_4
    df_data = df_data.iloc[:, -4:].join(df_data.iloc[:, :-4])
    return df_data


def sucffi__sucffi_Foreign_Currency_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[7:48, 1:13]
    print(df_data)
    df_years = df.iloc[6, 1:13]
    df_data.columns = df_years
    funds_list = ["Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources",
                  "Funds Sources",
                  "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources", "Funds Sources",
                  "Funds Sources", "Funds Sources", "Funds Sources",
                  "Funds Sources", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses",
                  "Funds Uses", "Funds Uses", "Funds Uses",
                  "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses",
                  "Funds Uses", "Funds Uses", "Funds Uses",
                  "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses", "Funds Uses"]
    funds_list_2 = [
        "Funds Sources", "Total Deposits", "Domestic Deposits", "Domestic Deposits", "Domestic Deposits",
        "Domestic Deposits",
        "Domestic Deposits", "Domestic Deposits", "Domestic Deposits", "Domestic Deposits", "Domestic Deposits",
        "Domestic Deposits",
        "Overseas Deposits", "Financial Bonds", "Repo",
        "Borrowings and Placements from Non-banking Financial Institutions", "Other Items",
        "Total Funds Sources", "Funds Uses", "Total Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans",
        "Domestic Loans", "Domestic Loans",
        "Domestic Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans", "Domestic Loans",
        "Domestic Loans",
        "Domestic Loans", "Domestic Loans", "Domestic Loans", "Overseas Loans", "Portfolio Investments",
        "Shares and Other Investments", "Reverse Repo",
        "Due From Non-banking Financial Institutions", "Total Funds Uses"]
    funds_list_3 = [
        "Funds Sources", "Total Deposits", "Domestic Deposits", "Deposits of Households", "Deposits of Households",
        "Deposits of Households", "Deposits of Non-Financial Enterprises",
        "Deposits of Non-Financial Enterprises", "Deposits of Non-Financial Enterprises",
        "Deposits of Government Departments and Organizations", "Fiscal Deposits",
        "Deposits of Non-Depository Financial Institutions", "Overseas Deposits", "Financial Bonds", "Repo",
        "Borrowings and Placements from Non-banking Financial Institutions",
        "Other Items", "Total Funds Sources", "Funds Uses", "Total Loans", "Domestic Loans", "Loans to Households",
        "Loans to Households",
        "Loans to Households", "Loans to Households", "Loans to Households", "Loans to Households",
        "Loans to Households",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations",
        "Loans to Non-banking Financial Institutions", "Overseas Loans", "Portfolio Investments",
        "Shares and Other Investments", "Reverse Repo",
        "Due From Non-banking Financial Institutions", "Total Funds Uses"]
    funds_list_4 = [
        "Funds Sources", "Total Deposits", "Domestic Deposits", "Deposits of Households", "Demand Deposits",
        "Time and Other Deposits",
        "Deposits of Non-Financial Enterprises", "Demand Deposits", "Time and Other Deposits",
        "Deposits of Government Departments and Organizations", "Fiscal Deposits",
        "Deposits of Non-Depository Financial Institutions",
        "Overseas Deposits", "Financial Bonds", "Repo",
        "Borrowings and Placements from Non-banking Financial Institutions",
        "Other Items", "Total Funds Sources", "Funds Uses", "Total Loans", "Domestic Loans", "Loans to Households",
        "Short-term Loans", "Consumption Loans", "Operating Loans", "Mid and Long-term Loans", "Consumption Loans",
        "Operating Loans",
        "Loans to Non-Financial Enterprises and Government Departments and Organizations", "Short-term Loans",
        "Mid and Long-term Loans", "Paper Financing",
        "Financial Leases", "Total Advances", "Loans to Non-banking Financial Institutions", "Overseas Loans",
        "Portfolio Investments",
        "Shares and Other Investments", "Reverse Repo", "Due From Non-banking Financial Institutions",
        "Total Funds Uses"]
    df_data['funds_list'] = funds_list
    df_data['funds_list_2'] = funds_list_2
    df_data['funds_list_3'] = funds_list_3
    df_data['funds_list_4'] = funds_list_4
    df_data = df_data.iloc[:, -4:].join(df_data.iloc[:, :-4])
    return df_data


def Money_and_Banking_Statistics__Domestic_RMB_Financial_Assets_Held_by_Overseas_Entities_template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[3:10, 1:14]
    df_data.drop(df_data.index[1], inplace=True)
    df_data.iloc[0, 0] = 'Year'
    df_data.columns = df_data.iloc[0]
    df_data['Year'] = df_data['Year'].str.split(' ').str[-1]
    df_data = df_data.set_index(df_data.columns[0]).T
    df_data = df_data.reset_index()
    df_data.rename(columns={df_data.columns[0]: "Year"}, inplace=True)
    return df_data


def Money_and_Banking_Statistics__Exchange_Rate__template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[2:10, 1:14].reset_index(drop=True).dropna()
    df_data.columns = df_data.iloc[0]
    df_columns = df.iloc[2:10, 0].reset_index(drop=True).dropna()
    df_columns = df_columns[df_columns.str.contains(r'[a-zA-Z]', na=False)]
    df_data.drop(df_data.index[0], inplace=True)
    df_data = df_data.reset_index(drop=True)
    df_columns.iloc[0] = 'Year'
    df_columns.columns = df_columns.iloc[0]
    df_columns.drop(df_columns.index[0], inplace=True)
    df_columns = df_columns.reset_index(drop=True)
    df_data.insert(0, 'Year', df_columns)
    df_data = df_data.set_index(df_data.columns[0]).T
    df_data = df_data.reset_index()
    df_data.rename(columns={df_data.columns[0]: "Year"}, inplace=True)
    return df_data


def Money_and_Banking_Statistics__Money_Supply__template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[4:12, 3:15].reset_index(drop=True).dropna()
    df_data.columns = df_data.iloc[0]
    df_data.drop(df_data.index[0], inplace=True)
    df_data = df_data.reset_index(drop=True)
    df_columns = ['M2 Money and Quasi-Money', 'M1 Money', 'M0 Currency in Circulation']
    df_data.insert(0, 'Year', df_columns)
    df_data = df_data.set_index(df_data.columns[0]).T
    df_data = df_data.reset_index()
    df_data.rename(columns={df_data.columns[0]: "Year"}, inplace=True)
    return df_data


def Money_and_Banking_Statistics__Balance_Sheet_of_Other_Depository_Corporations__template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[4:37, 1:15]
    df_data.drop(df_data.index[1], inplace=True)
    df_data.columns = df_data.iloc[0]
    df_data.drop(df_data.index[0], inplace=True)
    df_data = df_data.reset_index(drop=True)
    columns = ["foreign assets", "reserve assets", "deposits with central bank", "cash in vault",
               "claims on government",
               "of which central government", "claims on central bank", "claims on other depository corporations",
               "claims on other financial institutions",
               "claims on non-financial institutions", "claims on other resident sectors", "other assets",
               "total assets", "liabilities to non-financial institutions and households",
               "deposits included in broad money", "corporate demand deposits", "corporate time deposits",
               "personal deposits",
               "deposits excluded from broad money", "transferable deposits", "other deposits", "other liabilities",
               "liabilities to central bank", "liabilities to other depository corporations",
               "liabilities to other financial corporations",
               "of which deposits included in broad money", "foreign liabilities", "bond issue", "paid-in capital",
               "other liabilities", "total liabilities"]
    df_data['year'] = columns
    df_data = df_data.iloc[:, -1:].join(df_data.iloc[:, :-1])
    df_data = df_data.set_index(df_data.columns[0]).T
    df_data = df_data.reset_index()
    df_data.rename(columns={df_data.columns[0]: "year"}, inplace=True)
    return df_data


def Money_and_Banking_Statistics__Depository_Corporations_Survey__template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[4:23, 1:15]
    df_data.drop(df_data.index[1], inplace=True)
    df_data.columns = df_data.iloc[0]
    df_data.drop(df_data.index[0], inplace=True)
    df_data = df_data.reset_index(drop=True)
    columns = ["Net Foreign Assets", "Domestic Credits", "Claims on Government (net)",
               "Claims on Non-financial Sectors", "Claims on Other Financial Sectors",
               "Money and Quasi Money", "Money", "Currecny in Circulation", "Corporate Demand Deposits",
               "Quasi Money", "Corporate Time Deposits", "Personal Deposits", "Other Deposits",
               "Deposits Excluded from Broad Money",
               "Bonds", "Paid in Capital", "Other Items"]
    df_data['year'] = columns
    df_data = df_data.iloc[:, -1:].join(df_data.iloc[:, :-1])
    df_data = df_data.set_index(df_data.columns[0]).T
    df_data = df_data.reset_index()
    df_data.rename(columns={df_data.columns[0]: "year"}, inplace=True)
    return df_data


def Money_and_Banking_Statistics__Balance_Sheet_of_Monetary_Authority__template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    df_data = df.iloc[4:30, 1:15]
    df_data.drop(df_data.index[1], inplace=True)
    df_data.columns = df_data.iloc[0]
    df_data.drop(df_data.index[0], inplace=True)
    df_data = df_data.reset_index(drop=True)
    columns = ["foreign assets",
               "foreign exchange", "monetary gold", "claims on government", "of which central government",
               "claims on central bank", "claims on other depository corporations",
               "claims on other financial institutions", "claims on non-financial institutions", "other assets",
               "total assets",
               "reserve money", "currency issued", "deposits of financial corporations",
               "deposits of other depository corporations", "deposits of other financial corporations",
               "deposits of non-financial institutions",
               "deposits of financial corporations excluded from reserve money", "bond issue",
               "foreign liabilities", "deposits of government", "own capital", "other liabilities", "total liabilities"]
    df_data['year'] = columns
    df_data = df_data.iloc[:, -1:].join(df_data.iloc[:, :-1])
    df_data = df_data.set_index(df_data.columns[0]).T
    df_data = df_data.reset_index()
    df_data.rename(columns={df_data.columns[0]: "year"}, inplace=True)
    return df_data


def Money_and_Banking_Statistics__Official_reserve_assets__template(file,year,path):
    df = pd.read_excel(path + '/' + file)
    if year=='2020':
        df_data = df.iloc[2:19, 1:30]
        print(df_data)
        df_data = df_data.loc[:, df_data.iloc[0] == """亿美元 \n100million USD"""]
        df_data = df_data.reset_index(drop=True).dropna()
        df_data = df_data.reset_index(drop=True)
        df_years = df.iloc[1, 1:30].reset_index(drop=True).dropna()
        print(df_years)
        df_years = df_years.reset_index(drop=True).dropna()
        print(df_years)
        df_data.columns = df_years
        df_data.drop(df_data.index[0], inplace=True)
        df_columns = df.iloc[5:19, 0]
        df_columns = df_columns.fillna('Gold Weight')
        df_columns = df_columns[df_columns.str.contains(r'[a-zA-Z]', na=False)]
        df_columns = df_columns.reset_index(drop=True)
        print(df_columns)
        df_data['year'] = df_columns
        df_data = df_data.iloc[:, -1:].join(df_data.iloc[:, :-1])
        print(df_data)
        df_data = df_data.set_index(df_data.columns[0]).T
        df_data = df_data.reset_index()
        df_data.rename(columns={df_data.columns[0]: "year"}, inplace=True)
    else:
        df_data = df.iloc[4:20, 1:30]
        print(df_data)
        df_data = df_data.loc[:, df_data.iloc[0] == "100million USD"]
        df_data = df_data.reset_index(drop=True).dropna()
        df_data = df_data.reset_index(drop=True)
        df_years = df.iloc[2, 1:30].reset_index(drop=True).dropna()
        print(df_years)
        df_years = df_years.reset_index(drop=True).dropna()
        print(df_years)
        df_data.columns = df_years
        df_data.drop(df_data.index[0], inplace=True)
        df_columns = df.iloc[5:19, 0]
        df_columns = df_columns.fillna('Gold Weight')
        df_columns = df_columns[df_columns.str.contains(r'[a-zA-Z]', na=False)]
        df_columns = df_columns.reset_index(drop=True)
        print(df_columns)
        df_data['year'] = df_columns
        df_data = df_data.iloc[:, -1:].join(df_data.iloc[:, :-1])
        print(df_data)
        df_data = df_data.set_index(df_data.columns[0]).T
        df_data = df_data.reset_index()
        df_data.rename(columns={df_data.columns[0]: "year"}, inplace=True)
    return df_data
