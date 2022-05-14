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

DBname = "postgres"
DBuser = "postgres"
DBpwd = "password"
TableName1 = "Trip"
TableName2 = "BreadCrumb"
CreateDB = False#True  #recreate DB table

def dbconnect():
    connection = psycopg2.connect(
            host="localhost",
            database=DBname,
            user=DBuser,
            password=DBpwd,)
    connection.autocommit=True
    return connection
    
def createTables(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
           DROP TABLE IF EXISTS {TableName2} ;
           DROP TABLE IF EXISTS {TableName1} ;
           DROP TYPE IF EXISTS service_type;
           DROP TYPE IF EXISTS tripdir_type;
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
    conn.commit()


def createStringIoData(conn):

    
    return 1

    
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
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
               record_key = msg.key()
               record_value = msg.value()
               
               data = json.loads(record_value)

               Value1 = tuple([data['EVENT_NO_TRIP'],
                          0,
                          data['VEHICLE_ID'],
                          'Weekday',
                          'Out'])
               Value2 =tuple([data['OPD_DATE'],
                          data['GPS_LATITUDE'],
                          data['GPS_LONGITUDE'],
                          data['DIRECTION'],
                          float(data['VELOCITY']) * 3600 * 0.000621371,
                          data['EVENT_NO_TRIP']
                          ])
               try:
                   conn = dbconnect()
                   cursor = conn.cursor()
                   cursor.execute(f"""
                       INSERT INTO {TableName1} VALUES {Value1};
                       INSERT INTO {TableName2} VALUES {Value2};
                       """)
                   conn.commit()
                   
               except (Exception, psycopg2.Error) as error:
                   print("Error while connecting to PostgreSQL",error)
                   
               finally:
                   if conn:
                       cursor.close()
                       conn.close()
                       print("PostgreSQL connection is closed")
               total_count += 1
               print("Consumed record with key {} and value {}, \
                      and updated total count to {}"
                      .format(record_key, record_value,total_count))
                      
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
if __name__ == '__main__':
    main()
