import logging
import os
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
def logger_setup():
	format = '[%(levelname)s] - %(asctime)s - %(message)s'
	logging.basicConfig(
		level=logging.INFO,
		format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
		handlers=[
			logging.FileHandler("app.log"),
			logging.StreamHandler()])
	logger = logging.getLogger(__name__)
	return logger


def init_driver(download_folder=r".\download"):
	if not os.path.exists(download_folder):
		os.makedirs(download_folder)
	options = ChromeOptions()
	# options.add_argument("--headless=new")
	options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
	prefs = {
		"download.default_directory": os.path.abspath(download_folder),  # Download folder path
		"download.prompt_for_download": False,  # Disable "Save As" prompt
		"download.directory_upgrade": True,  # Automatically replace existing downloads
		"safebrowsing.enabled": False,  # Disable Safe Browsing (optional for Excel/PDF files)
		"plugins.always_open_pdf_externally": True  # Download PDFs instead of opening them
	}

	options.add_experimental_option("prefs", prefs)
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