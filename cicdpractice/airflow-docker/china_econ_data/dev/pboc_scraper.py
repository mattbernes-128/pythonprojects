from bs4 import BeautifulSoup
import requests
page_url = "http://www.pbc.gov.cn/en/3688006/index.html"
page_url = "http://www.pbc.gov.cn/en/3688247/3688975/5242368/5242421/index.html"
page_url = "http://www.pbc.gov.cn/en/3688247/3688975/index.html"
response = requests.get(page_url)
soup = BeautifulSoup(response.text, "html.parser")

for link in soup.find_all("a", href=True):
    if link["href"].endswith((".xlsx", ".xls", ".pdf")):
        print("Found file link:", link["href"])