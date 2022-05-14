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
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================


from cgi import print_arguments
from email import header
from http.client import ImproperConnectionState
import json
import string
from tempfile import tempdir
import pandas as pd
import csv
from io import StringIO
import numpy as np

DBname = "postgres"
DBuser = "postgres"
DBpwd = "password"
TableName1 = "Trip"
TableName2 = "BreadCrumb"
CreateDB = False#True  #recreate DB table

def jsonTocsv():
    JSON_PATH = 'total_data.json'
    CSV_PATH = 'jsontocsv.csv'
    # data = json.loads(JSON_PATH)
    with open(JSON_PATH) as json_file:
        jsondata = json.load(json_file)
    data_file = open(CSV_PATH,'w',newline='')
    csv_writer = csv.writer(data_file)

    count = 0
    for data in jsondata:
        if count == 0:
            header = data.keys()
            csv_writer.writerow(header)
            count += 1
        csv_writer.writerow(data.values())

def getcsvdata():
    with open('jsontocsv.csv', mode='r') as fil:
        # dr = csv.DictReader(fil)
        df = fil.read() + '\n'

        # rowlist = []
        # for row in dr:
        #     rowlist.append(row)

    return df

def cop_from(rowlist):
    cf = StringIO(rowlist)
    print(cf.read())
    cursor.copy_from(f,dataname)

def assertion():
    df = pd.read_csv('jsontocsv.csv')


    for row in df.index:
        if ((pd.isna(df['GPS_LONGITUDE'][row]) == True) or (pd.isna(df['GPS_LATITUDE'][row]) == True)):
            df = df.drop(index=[row])
    for row in df.index:
        if (df['ACT_TIME'][row] <= 86400) == False:
            df.at[row,'ACT_TIME'] = df['ACT_TIME'][row] - 86400
    df['DIRECTION'] = df['DIRECTION'].fillna(value=0)

    # Existence: (5)
    for row in df.index:
        # Every C-Tran trip occured on a date
        assert(pd.isna(df['OPD_DATE'][row]) == False)

        # Every C-Tran trip need a vehicle_id
        assert(pd.isna(df['VEHICLE_ID'][row]) == False)

        # Every C-Tran bus trip need a gps_longitude and a gps_latitude
        assert((pd.isna(df['GPS_LONGITUDE'][row]) == False) and (pd.isna(df['GPS_LATITUDE'][row]) == False))
        # if df['GPS_HDOP'][row] > 10:
        #     print(df.iloc[row])

        # Every C-Tran bus trip will be located by satellite
        assert(pd.isna(df['GPS_SATELLITES'][row]) == False)
        # Each "ACT_TIME" should have one "Direction"
        assert(pd.isna(df['ACT_TIME'][row]) == False)



    # Limit: (6)
    for row in df.index:
        # The direction of the C-Tran bus should be between 0 - 359 degrees
        # df['DIRECTION'] = df['DIRECTION'].fillna(value=0)
        assert((df['DIRECTION'][row] <= 359) and (df['DIRECTION'][row] >= 0))
        
        # The C-Tran bus trip time should be within 24 hours
        assert(df['ACT_TIME'][row] <= 86400)


    # Summary: (2)
    # There should be less than 150 vehicle_id per day
    assert(len(pd.unique(df['VEHICLE_ID'])) <= 150 )


    # Statistical distribution: (4)
    # Most trips occur on weekdays

    # The horizontal dilution of precision (gps_hdop) should average around 0.8
    assert((df['GPS_HDOP'].mean() < 0.9) and (df['GPS_HDOP'].mean() > 0.7))

    # The C-Tran buses are usually connected to 12 satellites.
    assert((df['GPS_SATELLITES'].mean() < 13) and (df['GPS_SATELLITES'].mean() > 11))

    # The average speed of all C-Tran buses should be around 15.





def main():
    # jsonTocsv()
    # rowlist = getcsvdata()
    # cop_from(rowlist)
    assertion()
    # print(repr(rowlist))
    
if __name__ == '__main__':
    main()
