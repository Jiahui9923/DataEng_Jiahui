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
Unlogged = "Unlogged_StopEvent"
CreateUnloggedDB = True
UpdateTrip = True
Prev_Data = ['0'] * 25

def dbconnect():
    connection = psycopg2.connect(
            host="localhost",
            database=DBname,
            user=DBuser,
            password=DBpwd,)
    connection.autocommit=True
    return connection

def createStagingTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
           DROP TABLE IF EXISTS {Unlogged};
           CREATE UNLOGGED TABLE {Unlogged} (
              vehicle_number          integer,
              leave_time              integer,
              train                   integer,
              route_number            integer,
              direction               tripdir_type,
              service_key             service_type,
              stop_time               integer,
              arrive_time             integer,
              dwell                   integer,
              location_id             integer,
              door                    integer,
              lift                    integer,
              ons                     integer,
              offs                    integer,
              estimated_load          integer,
              maximum_speed           integer,
              train_mileage           decimal,
              pattern_distance        decimal,
              location_distance       decimal,
              x_coordinate            decimal,
              y_coordinate            decimal,
              data_source             integer,
              schedule_status         integer,
              trip_date               text,
              trip_number             integer

           );

        """)
    print("Created staging table Unlogged_StopEvent")


def load_from_staging_table():
    conn = dbconnect()
    with conn.cursor() as cursor:
        cursor.execute(f"""
            UPDATE {TableName1} t SET route_id=s.route_number,direction=s.direction,service_key=s.service_key
            FROM (SELECT DISTINCT trip_number, route_number, direction, service_key from {Unlogged}) s
            WHERE t.trip_id=s.trip_number;

        """)


def assertion(data):
    # convert dict to dataframe

    global DataValues
    Temp_DataValues = []
    Temp_DataValues.append(data)
    df = pd.DataFrame(Temp_DataValues)

    print(df)
    
    # Every service_key is one of 'W', 'S' or 'U' values for 'Weekday','Saturday', and 'Sunday'
    assert ((df['service_key'] == 'W') | (df['service_key'] == 'S') | (df['service_key'] == 'U')).any()

    # convert dataframe to dict
    data = df.to_dict('records')[0]
    
    return data


def decoding_transformation(data):
    global Prev_Data
    
    #convert from dict to pandas dataframe
    df = pd.DataFrame.from_dict([data])
    
    # fill all empty values with values from Prev_Data
    for i in range(len(df.columns)):
        if (df.iloc[:,i] == "").any():
            df.iloc[:,i] = Prev_Data[i]

    # convert back to dict
    data = df.to_dict('records')[0]

    # decode data
    if data['direction'] == '0':
        data['direction'] = 'Out'
    elif data['direction'] == '1':
        data['direction'] = 'Back'
    else:
        if data['direction'] != 'Out' and data['direction'] != 'Back':
            print('error in direction transformation')
        
    if data['service_key'] == 'W':
        data['service_key'] = 'Weekday'
    elif data['service_key'] == 'S':
        data['service_key'] = 'Saturday'
    elif data['service_key'] == 'U':
        data['service_key'] = 'Sunday'
    else:
        
        print('error in service_key transformation')

    # convert dict to pandas dataframe
    df = pd.DataFrame.from_dict([data])
    
    # fill Prev_Data with current data
    for i in range(len(df.columns)):
        Prev_Data[i] = df.iloc[:,i]

    # convert back to dict
    data = df.to_dict('records')[0]
    return data


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

    # Create staging table
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

                global UpdateTrip
                if UpdateTrip:
                    load_from_staging_table()
                    print("loaded from staging table to trip table")

                    UpdateTrip = False

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
               
               # transformation
               data = decoding_transformation(data)
               
               # create tuple list of values
               value = tuple([data['vehicle_number'],
                       data['leave_time'],
                       data['train'],
                       data['route_number'],
                       data['direction'],
                       data['service_key'],
                       data['stop_time'],
                       data['arrive_time'],
                       data['dwell'],
                       data['location_id'],
                       data['door'],
                       data['lift'],
                       data['ons'],
                       data['offs'],
                       data['estimated_load'],
                       data['maximum_speed'],
                       data['train_mileage'],
                       data['pattern_distance'],
                       data['location_distance'],
                       data['x_coordinate'],
                       data['y_coordinate'],
                       data['data_source'],
                       data['schedule_status'],
                       data['trip_date'],
                       data['trip_number']])

               try:
                   conn = dbconnect()
                   cursor = conn.cursor()

                   cursor.execute(f"""
                   insert into {Unlogged} values{value};
                   """)

                   UpdateTrip = True
                   
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