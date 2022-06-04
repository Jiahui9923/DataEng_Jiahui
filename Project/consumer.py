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

from confluent_kafka import Consumer
import json
import ccloud_lib
import pandas as pd
import io
import psycopg2
import csv
import io
import numpy as np

DBname = "postgres"
DBuser = "postgres"
DBpwd = "jiahui"
TableName1 = "Trip"
TableName2 = "BreadCrumb"
Unlogged = "UnloggedTable"
CreateDB = True  #recreate DB table
CreateUnloggedDB = True
DataValues = []
PrevData = [{'DIRECTION':'0','VELOCITY':'0','GPS_LONGITUDE':'0','GPS_LATITUDE':'0'}]

def dbconnect():
    connection = psycopg2.connect(
            host="localhost",
            database=DBname,
            user=DBuser,
            password=DBpwd,)
    connection.autocommit=True
    return connection

# creates trip and breadcrumb tables
def createTables(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
           DROP TABLE IF EXISTS {TableName2} ;
           DROP TABLE IF EXISTS {TableName1} ;
           DROP TYPE IF EXISTS service_type cascade;
           DROP TYPE IF EXISTS tripdir_type cascade;
           CREATE TYPE service_type as enum ('Weekday', 'Saturday', 'Sunday');
           CREATE TYPE tripdir_type as enum ('Out', 'Back');
           CREATE TABLE {TableName1} (
              trip_id integer,
              route_id integer,
              vehicle_id integer,
              service_key service_type,
              direction tripdir_type,
              PRIMARY KEY (trip_id)
           );
           CREATE TABLE {TableName2}(
              tstamp timestamp,
              latitude float,
              longitude float,
              direction integer,
              speed float,
              trip_id integer,
              FOREIGN KEY (trip_id) REFERENCES {TableName1}
           );
        """)
    print("Created BreadCrumb and Trip tables")

def createStagingTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
           DROP TABLE IF EXISTS {Unlogged};
           CREATE UNLOGGED TABLE {Unlogged} (
              EVENT_NO_TRIP           INTEGER,
              EVENT_NO_STOP           INTEGER,
              OPD_DATE                timestamp,
              VEHICLE_ID              INTEGER,
              METERS                  INTEGER,
              ACT_TIME                INTEGER,
              VELOCITY                integer,
              DIRECTION               integer,
              RADIO_QUALITY           text,
              GPS_LONGITUDE           decimal,
              GPS_LATITUDE            decimal,
              GPS_SATELLITES          INTEGER,
              GPS_HDOP                DECIMAL,
              SCHEDULE_DEVIATION      TEXT
           );
        """)
    print("Created staging table Unlogged")
        
def load_from_staging_table():
    conn = dbconnect()
    with conn.cursor() as cursor:
      #try:
        cursor.execute(f"""
            INSERT INTO {TableName1} (trip_id,vehicle_id)
            SELECT EVENT_NO_TRIP,VEHICLE_ID
            FROM {Unlogged}
            ON CONFLICT ON CONSTRAINT trip_pkey DO NOTHING;
            INSERT INTO {TableName2} (tstamp,latitude,longitude,direction,speed,trip_id)
            SELECT opd_date,gps_latitude,gps_longitude,direction,velocity,event_no_trip
            FROM {Unlogged};
        """)

def assertion(data):
    global DataValues
    global PrevData
    DataValues = []
    DataValues.append(data)
    pdf = pd.DataFrame(PrevData)
    df = pd.DataFrame(DataValues)
    
    for row in df.index:
        if ((pd.isna(df['GPS_LONGITUDE'][row]) == True) or (pd.isna(df['GPS_LATITUDE'][row]) == True)):
            df = df.drop(index=[row])
            
    for row in df.index:
        if (int(df['ACT_TIME'][row]) <= 86400) == False:
            df.at[row,'ACT_TIME'] = str(int(df['ACT_TIME'][row]) - 86400)
    df['DIRECTION'] = df['DIRECTION'].fillna(value=0)
    
    # Existence: (5)
    for row in df.index:
        # Every C-Tran trip occured on a date
        assert(pd.isna(df['OPD_DATE'][row]) == False)
        # Every C-Tran trip need a vehicle_id
        assert(pd.isna(df['VEHICLE_ID'][row]) == False)
        # Every C-Tran bus trip need a gps_longitude and a gps_latitude
        assert((pd.isna(df['GPS_LONGITUDE'][row]) == False) and (pd.isna(df['GPS_LATITUDE'][row])
== False))
        # if df['GPS_HDOP'][row] > 10:
        #     print(df.iloc[row])
        
        # Every C-Tran bus trip will be located by satellite
        assert(pd.isna(df['GPS_SATELLITES'][row]) == False)
        # Each "ACT_TIME" should have one "Direction"
        assert(pd.isna(df['ACT_TIME'][row]) == False)
        
    # Limit: (6)
    if df['VELOCITY'][0] == '':
        df['VELOCITY'][0] = pdf['VELOCITY'][0]
    if df['DIRECTION'][0] == '':
        df['DIRECTION'][0] = pdf['DIRECTION'][0]
    if df['GPS_LONGITUDE'][0] == '':
        df['GPS_LONGITUDE'][0] = pdf['GPS_LONGITUDE'][0]
  
    df['DIRECTION'] = df['DIRECTION'].ffill(axis=0) #transformation to fill empty string
    df['VELOCITY'] = df['VELOCITY'].ffill(axis=0)
    df['GPS_LONGITUDE'] = df['GPS_LONGITUDE'].ffill(axis=0)
    
    for row in df.index:
        # The direction of the C-Tran bus should be between 0 - 359 degrees
        try:
            assert((df['DIRECTION'][row] <= str(359)) and (df['DIRECTION'][row] >= str(0)))
        except AssertionError as msg:
            print(msg)
            df['DIRECTION'] = df['DIRECTION'].interpolate(method='linear',limit_direction='forward',axis=0)
        '''
        # The C-Tran bus trip time should be within 24 hours
        assert(int(df['ACT_TIME'][row]) <= 86400)
        # Summary: (2)
        # There should be less than 150 vehicle_id per day
        assert(len(pd.unique(df['VEHICLE_ID'])) <= 150 )
        '''
        # Statistical distribution: (4)
        # Most trips occur on weekdays
        # The horizontal dilution of precision (gps_hdop) should average around 0.8
        #assert((df['GPS_HDOP'].mean() < 0.9) and (df['GPS_HDOP'].mean() > 0.7))
        # The C-Tran buses are usually connected to 12 satellites.
        #assert((df['GPS_SATELLITES'].mean() < 13) and (df['GPS_SATELLITES'].mean() > 11))
        # The average speed of all C-Tran buses should be around 15.eDF = df[['OPD_DATE']]
    data = df.to_dict('records')
    PrevData = []
    PrevData.append(data[0])
    return data[0]

def main():
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)
    
    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)
    
    # Subscribe to topic
    consumer.subscribe([topic])
    
    # Connect to database
    conn = dbconnect()
    if CreateDB:
        createTables(conn)
    if CreateUnloggedDB:
        createStagingTable(conn)
        
    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                global DataValues
                if DataValues:
                    load_from_staging_table()
                    print("loaded from staging table to trip and breadcrumb table")
                    DataValues = []
                    
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
               record_key = msg.key()
               record_value = msg.value()
               data = json.loads(record_value)
               
               # data validation
               data = assertion(data)
               
               sec = int(data['ACT_TIME'])
               h = str(int(sec/3600))
               m = str(int((sec %3600)/60))
               s = str(int((sec%3600)%60))
               velocity = str(int(int(data['VELOCITY']) * 3600 * 0.00062137))
                                                          
               value = tuple([data['EVENT_NO_TRIP'],
                       data['EVENT_NO_STOP'],
                       data['OPD_DATE']+' '+h+':'+m+':'+s,
                       data['VEHICLE_ID'],
                       data['METERS'],
                       data['ACT_TIME'],
                       velocity,
                       data['DIRECTION'],
                       data['RADIO_QUALITY'],
                       data['GPS_LONGITUDE'],
                       data['GPS_LATITUDE'],
                       data['GPS_SATELLITES'],
                       data['GPS_HDOP'],
                       data['SCHEDULE_DEVIATION']])
               try:
                   conn = dbconnect()
                   cursor = conn.cursor()
                   cursor.execute(f"""
                       insert into {Unlogged} values{value};
                   """)
               except(Exception,psycopg2.Error) as error:
                   print("Error while connecting to PostgreSQL",error)
               finally:
                   cursor.close()
                   conn.close()

               total_count += 1
               print("Consumed record  total count is {}"
                      .format(total_count))
                     
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        
if __name__ == '__main__':
    main()




