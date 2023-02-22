import time
import requests as req
from bs4 import BeautifulSoup as bs
import re
import pandas as pd

from google.cloud import bigquery

client = bigquery.Client()
date_check = []
while True:
    # Requesting for the website
    Web = req.get('https://www.x-rates.com/table/?from=USD&amount=1')
    soup = bs(Web.text, 'lxml')
    my_date = soup.find_all('span', class_="ratesTimestamp")
    date_ = my_date[0].text
    tabs = soup.find_all('table', class_="tablesorter ratesTable")
    my_list = []
    for tab in tabs:
        trows = tab.find_all('tr')
        for rows in trows:
            my_list.append(rows.text)
            #print(rows.text)     
    header = [my_list[0]]
    my_list = my_list[1:]
    res_header = [val.split('\n') for val in header]
    res_header_1 = res_header[0][1]
    res_header_2 = re.findall('\d+\.\d+',res_header[0][2])[0]
    res_header_3 = re.findall('\d+\.\d+',res_header[0][2])[0]
    res_rows = [val.split('\n') for val in my_list]
    res_rows_1 = [i[1] for i in res_rows]
    res_rows_2 = [i[2] for i in res_rows]
    res_rows_3 = [i[3] for i in res_rows]
    res_rows_1.insert(0, res_header_1)
    res_rows_2.insert(0, res_header_2)
    res_rows_3.insert(0, res_header_3)
    df = pd.DataFrame({'Date': date_, 'source_currency': res_rows_1, 'dollar_value': res_rows_2, 'inverse_dollar_value': res_rows_3})
    df = df.astype({'Date': 'object','source_currency': 'object', 'dollar_value': 'float64', 'inverse_dollar_value': 'float64'})
    dataset = client.dataset('sample')
    table = dataset.table('currency_rates')
    job_config = bigquery.job.LoadJobConfig()
    if len(date_check)==0:
        load_job = client.load_table_from_dataframe(df, table, job_config=job_config)
        date_check.insert(0, date_)
    elif date_ != date_check[0]:
        load_job = client.load_table_from_dataframe(df, table, job_config=job_config)
        date_check[0] = date_
    time.sleep(20)