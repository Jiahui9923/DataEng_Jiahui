import pandas as pd
import numpy as np
import datetime

from pyparsing import col
import warnings

import matplotlib.pyplot as plt

BREAD_FILE_PATH = 'breadcrumb.csv'
TRIP_FILE_PATH = 'trip.csv'
STOP_FILE_PATH = 'unlogged_stopevent.csv'
TABLE_FILE_PATH = 'unloggedtable.csv'

df_bread = pd.read_csv(BREAD_FILE_PATH,delimiter='|',header=None,low_memory=False,
                        names=['tstamp','latitude','longitude','direction','speed','trip_id'])
df_trip = pd.read_csv(TRIP_FILE_PATH,delimiter='|',header=None,
                        names=['trip_id','route_id','vehicle_id','service_key','direction'])
df_stop = pd.read_csv(STOP_FILE_PATH,delimiter='|',header=None,
                        names=['vehicle_number','leave_time','train','route_number','direction',
                                'service_key','stop_time','arrive_time','dwell','location_id','door',
                                'lift','ons','offs','estimated_load','maximum_speed','train_mileage',
                                'pattern_distance','location_distance','x_coordinate','y_coordinate',
                                'data_source','schedule_status','trip_date','trip_number'])
df_table = pd.read_csv(TABLE_FILE_PATH,delimiter='|',header=None,low_memory=False,
                        names=['event_no_trip','event_no_stop','opd_date','vehicle_id','meters',
                                'act_time','velocity','direction','radio_quality','gps_longitude',
                                'gps_latitude','gps_satellites','gps_hdop','schedule_deviation'])

# breadcrumb.csv
df_bread = df_bread.drop(index=[0,1])
df_bread = df_bread.drop(df_bread.tail(1).index)
df_bread = df_bread.reset_index(drop=True)

# trip.csv
df_trip = df_trip.drop(index=[0,1])
df_trip = df_trip.drop(df_trip.tail(1).index)
df_trip = df_trip.reset_index(drop=True)

# unlogged_stopevent.csv
df_stop = df_stop.drop(index=[0,1])
df_stop = df_stop.drop(df_stop.tail(1).index)
df_stop = df_stop.reset_index(drop=True)

# unloggedtable.csv
df_table = df_table.drop(df_table.head(2).index)
df_table = df_table.drop(df_table.tail(1).index)
df_table = df_table.reset_index(drop=True)


# Delete redundant columns
df_trip = df_trip.drop(columns=['route_id','service_key','direction'])
df_stop = df_stop.drop(columns=['dwell','door','lift','ons','offs','maximum_speed',
                                'data_source','schedule_status','trip_date'])
df_table = df_table.drop(columns=['opd_date','direction','radio_quality','gps_longitude',
                                'gps_latitude','gps_satellites','gps_hdop','schedule_deviation'])


# Rename
df_stop = df_stop.rename(columns={'vehicle_number':'vehicle_id','trip_number':'trip_id'})
df_table = df_table.rename(columns={'event_no_trip':'trip_id','event_no_stop':'stop_id'})

# Integration
df_new_trip = pd.merge(df_trip,df_stop,on=['trip_id'],how='right')

# df_new_trip = pd.concat([df_trip,df_table],keys=['trip','table'],axis=1)

# Optimization of data
df_new_trip = df_new_trip.drop(columns=['vehicle_id_x'])
df_new_trip = df_new_trip.rename(columns={'vehicle_id_y':'vehicle_id'})
df_new_trip.replace('\s+','',regex=True,inplace=True)
df_new_trip.to_csv('new_trip.csv')

print(df_new_trip)


