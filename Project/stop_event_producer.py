#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import time
import random
import urllib.request
from datetime import datetime
import requests
import bs4
import re

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
with open(f'{date}_stop_event.json','w', encoding='utf-8') as f:
        json.dump(results,f, ensure_ascii=False,indent=2)

file = open(f'{date}_stop_event.json')
data = json.load(file)

if __name__ == '__main__':
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    for n in data:

        key = 1
        record_key = str(key)
        record_value = json.dumps(n,indent=4)
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)

        producer.poll(0)

    producer.flush()
    print("{} messages were produced to topic {}!".format(delivered_records, topic))