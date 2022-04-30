from asyncio.windows_events import NULL
from http.client import ImproperConnectionState
from operator import index
from pydoc import cram
from re import A
from tabnanny import check
from turtle import color
from attr import NOTHING
from matplotlib.pyplot import hist
from numpy import TooHardError, empty
import pandas as pd

from pylab import rcParams

from pandas import Series,DataFrame

from numpy.random import randn

import numpy as np

import matplotlib.pyplot as plt

# RUN CODE by VScode
# CSV_FILE_PATH = './Data_Validation/oregon_crash_2019.csv'/

CSV_FILE_PATH = './oregon_crash_2019.csv'

df = pd.read_csv(CSV_FILE_PATH)

# existence assertions: "Every crash occurred on a date"

#df_type_1 = df[df["Record Type"] == 1]
df_type_1 = df.iloc[lambda x: (x['Record Type'] == 1).tolist()]
df_date = df_type_1[['Crash Month', 'Crash Day', 'Crash Year']]
assert df_date.empty == False

# limit assertions: "Every crash occurred during year 2019"
df_year = df_type_1[['Crash Year']]
df_year_check = df_year[df_year['Crash Year'].ne(2019.0)]
assert df_year_check.empty == True

# intra-record assertions: "Every crash has a unique ID"
df_crash_id = df[df["Record Type"] == 1]
assert len(pd.unique(df_crash_id["Crash ID"])) == len(df_crash_id["Crash ID"])

# create 2+ inter-record check assertions: "Every vehicle listed in the crash data was part of a known crash"

# create 2+ summary assertions: "There were thousands of crashes but not millions"
assert len(df["Record Type"] == 1) <= 999999

# create 2+ statistical distribution assertions: "crashes are evenly/uniformly distributed throughout the months of the year"
df_day = df[df['Record Type'] == 1]
df_month = df_day['Crash Month']
assert ((df_month.mean() <= 7) & (df_month.mean() >= 6))

# Jiahui Part!
#######################################################

# existence assertions: "Every crash has a serial code"
df_serial = df[df['Record Type'] == 1]
for idx, row in df_serial.iterrows():
    assert pd.isna(row['Serial #']) == False

# limit assertions: "The maximum recorded type for each crash is 3"
for idx, row in df.iterrows():
    assert row['Record Type'] <= 3

# intra-record assertions: "Each participant is a assigned a unique ID"
df_crash_id = df[df["Record Type"] == 3]
assert len(pd.unique(df_crash_id["Participant ID"])) == len(df_crash_id["Participant ID"])

# inter-record check assertions(a): "Every crash requires three kinds of records"
df_type_count = df.groupby(['Crash ID'])['Record Type'].nunique().reset_index()
for idx,row in df_type_count.iterrows():
    assert row["Record Type"] ==3 

# inter-record check assertions(b): "Each Record Type = 3 needs a Participant ID"
df_type_3 = df[df["Record Type"] == 3]
for idx, row in df_type_3.iterrows():
    assert pd.isna(row['Participant ID']) == False

# inter-record check assertions(c): "Every Serial# listed in the crash data was part of a known crash"
for idx, row in df_type_1.iterrows():
    assert pd.isna(row['Serial #']) == False

# inter-record check assertions(d): "The participant ID of each crash is required to retain the vehicle information of type 2."

df_type_3_inter = pd.DataFrame(df_type_3,columns=['Vehicle ID','Participant ID'])
for idx, row in df_type_3_inter.iterrows():
    assert ((pd.isna(row['Vehicle ID']) == False) & (pd.isna(row['Participant ID']) == False))

# summary assertions(a): "Accidents usually involve about 1000 people a year"
# Get Assertion Error!

# df_participant = df[df["Record Type"] == 3]
# assert len(df_participant) <= 1000

# summary assertions(b): "There were hundreds of accidents instead of thousands of accidents."
df_crash_id_1 = df[df["Record Type"] == 1]
assert len(df_crash_id_1) <= 999

# statistical distribution assertions(a): "The average age of the accident participants was around 30"
# Get Assertion Error!
# assert (df_type_3['Age'].mean() <=35) & (df_type_3['Age'].mean() >= 25)

# statistical distribution assertions(b): "At least 80 percent of people have a valid driver's license when there's an accident"
# Get Assertion Error!

# df_type_3_driverid_1 = df_type_3[df_type_3["Driver License Status"] == 1]
# df_type_3_driverid_0 = df_type_3[df_type_3["Driver License Status"] == 0]
# df_ratio_driverid = len(df_type_3_driverid_1['Driver License Status']) / (len(df_type_3_driverid_1['Driver License Status']) + len(df_type_3_driverid_0['Driver License Status']) )
# assert df_ratio_driverid >= 0.8


# Export file:
df_crashes = df[df["Record Type"] == 1]
df_vehicles = df[df["Record Type"] == 2]
df_participants = df[df["Record Type"] == 3]

df_crashes.to_csv('crashes.csv')
df_vehicles.to_csv('vehicles.csv')
df_participants.to_csv('participants.csv')

# NOTE: Deprecated !

# print(len(df_type_3))
# print(len(df_type_3_driverid_1['Driver License Status']))
# print(len(df_type_3_driverid_0['Driver License Status']))
# print (df_ratio_driverid)
# print(len(df_type_3_driverid['Driver License Status']))

# print(df_type_count['Record Type'] )
# df['avg'] = df_type_count
# print(df_type_count.size())

# limit assertions

# print(df_month.mean())
# print(df_month.describe())
# print(len(pd.unique(df_day['Crash Month'])) )

# df = DataFrame(abs(randn(10,5)),columns=['A','B','C','D','E'],index = np.arange(0,100,10))

# df.plot(kind='bar')
# plt.show()

# print(df_month)

# print(len(df_crash_id["Crash ID"]))

# print(df_crash_id_only)
# print(len(pd.unique(df['Record Type'])))
# print(len(pd.unique(df_crash_id['Crash ID'])))

# print(df_year['Crash Year'])

# assert df_year['Crash Year'] == 2019

# print(df_year)

# pd.isna(df_date)
# for index, row in df.iterrows():
#     if row['Record Type'] == 1:
#         df2 = df.loc[index,'Crash Month','Crash Day' 'Crash Yeaer']
#         print(df2)
        
# for data in df['Record Type']:
#     if data == 1:

# assert flag == 0
