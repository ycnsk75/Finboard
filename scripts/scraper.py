from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd

options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandboxes")
options.add_argument("--disable-dev-shm-usage")

service = Service('/path/to/chromedriver')
driver = webdriver.Chrome(service=service, options=options)

url = 'https://greenstocknews.com/stock-screener'
driver.get(url)

html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')

table = soup.find('table')
rows = table.find_all('tr')

header_row = rows[0]
columns = [th.text.strip() for th in header_row.find_all('th')]

data_rows = rows[1:]
data = []
for row in data_rows:
    row_data = [td.text.strip() for td in row.find_all('td', class_='stock-data')]
    data.append(row_data)

df = pd.DataFrame(data, columns=columns)
display(df)

driver.quit()