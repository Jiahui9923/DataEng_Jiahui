# 04/14/2022

import requests
import urllib3
import json

##urllib3 template! 
# http = urllib3.PoolManager()
# r = http.request('GET', 'http://www.hubertiming.com/results/2017GPTR10K')
# print(r.status)

url = requests.get("http://www.psudataeng.com:8000/getBreadCrumbData")

text = url.text
data = json.loads(text)

with open("days.json",'w') as code:
    code.write(text)

# # TO Get First_Data
# user = data[0]
# print(user['OPD_DATE'])
# address = user['ACT_TIME']
# print(address)


