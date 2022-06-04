import json
import time
import random
import urllib.request
from datetime import datetime
import requests
import bs4
import re
import json


date = datetime.now().strftime("%b-%d-%Y")
stopevents = requests.get('http://www.psudataeng.com:8000/getStopEvents/')

#convert html to json
soup = bs4.BeautifulSoup(stopevents.text,'lxml')

#get date
h1 = soup.find('h1').text
trip_date = re.sub(r'[a-zA-Z\s/]*','',h1)
trip_date = re.sub(r'^-','',trip_date)

#get list of trip numbers
h3 = soup.find_all('h3')
h3_text = [row.text for row in h3]
trip_number_list = [re.sub(r'[a-zA-Z\s]*','',num) for num in h3_text]

#get list of tables
table_list = soup.find_all('table')
results = []

#get data from table rows and map to headers, add to list
for index,table in enumerate (table_list):
    headers=[heading.text for heading in table.find_all('th')]
    table_rows = [row for row in table.find_all('tr')]
    table_rows.pop(0)
    result=[{headers[index]:cell.text for index,cell in enumerate(row.find_all('td'))}for row in table_rows]
    
    for element in result:
        element['trip_date'] = trip_date
        element['trip_number'] = trip_number_list[index]
    results.extend(result)

#save in json file
# with open(f'{date}_stop_event.json','w', encoding='utf-8') as f:
#         json.dump(results,f, ensure_ascii=False,indent=2)

# file = open(f'{date}.json')
# data = json.load(file)

