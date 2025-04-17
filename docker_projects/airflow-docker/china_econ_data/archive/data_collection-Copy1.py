import requests
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
import json
from selenium.webdriver.common.by import By
from time import sleep
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def init_driver():
    options = ChromeOptions()
    #options.add_argument("--headless=new")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    options.add_experimental_option("perfLoggingPrefs", {"enableNetwork": True})
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def find_element_wait(driver, selector):
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
    element = driver.find_element(By.CSS_SELECTOR, selector)
    return element

def find_elements_wait(driver, selector):
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
    elements = driver.find_elements(By.CSS_SELECTOR, selector)
    return elements

web_driver = init_driver()
data_index_page = "http://www.pbc.gov.cn/en/3688247/3688975/index.html"
web_driver.get(data_index_page)
#WebDriverWait(web_driver, 30).until( EC.presence_of_element(By.CSS_SELECTOR, 'ul > li[class="class_A"]')_located((By.CSS_SELECTOR, 'ul > li[class="class_A"] > a')))
year_links = find_elements_wait(driver=web_driver, selector='ul > li[class="class_A"] > a')

year_list=[['Year'],['Link']]
for link in year_links:
    print(f"Year: {link.text}")
    print(f"Navigating to {link.get_attribute("href")}...")
    web_driver.get(link.get_attribute("href"))
    data_links = find_elements_wait(driver=web_driver, selector='div[class="ListR"] > a')
    year_list.append([link.text,link.get_attribute("href"))

    for data_link in data_links:
        print(f"
    {data_link.text}")
        print(f"Navigating to {data_link.get_attribute("href")}...")
        web_driver.get(data_link.get_attribute("href"))

        table_rows = find_elements_wait(driver=web_driver, selector='table[class="data_table"] tbody > tr')
        table_cells = find_elements_wait(driver=web_driver, selector='table[class="data_table"] tbody > tr > td')
        for cell in table_cells:
            if cell.get_attribute("class") != "data_xz":
                row_name = cell.text
                print(row_name)
            elif cell.get_attribute("class") == "data_xz" and cell.find_element(By.CSS_SELECTOR, 'a').text == "xls":
                #print(cell.find_element(By.CSS_SELECTOR, 'a'))
                xls_link = cell.find_element(By.CSS_SELECTOR, 'a').get_attribute('href')
                print(xls_link)
    web_driver.get(data_index_page)

web_driver.quit()
web_driver.close()

