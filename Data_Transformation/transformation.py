import csv
from glob import glob
from os import stat
import re
from sqlite3 import Row
import string
from matplotlib.pyplot import flag
import pandas as pd
import numpy as np
from requests import head
from urllib3 import Retry
import io


CSV_FILE_PATH = './books.csv'
df = pd.read_csv(CSV_FILE_PATH)

# A. Filtering
df_a_method_1 = df.drop(['Edition Statement','Corporate Author','Corporate Contributors','Former owner','Engraver','Issuance type','Shelfmarks'],axis=1)
df_a_method_2 = pd.read_csv(CSV_FILE_PATH, usecols=['Identifier','Place of Publication','Date of Publication','Publisher','Title','Author','Contributors','Flickr URL'])

# B. Tidying Up the Data

df_1 = df_a_method_1
def tidy_data(item):
    # retval = re.sub('[^\d]{3}','',str(item))
    # numpy.nan
    retval_1 = re.sub('.*\?.*','',str(item))
    retval_1 = re.sub('\s','',str(item))
    retval_1 = re.sub('[^\d]','',str(item))
    try:
        retval_1 = re.search('^\d\d\d\d',retval_1).group()
    except AttributeError:
        retval_1 = re.search('^\d\d\d\d',retval_1)
    return retval_1

# def tidy_data_num(item):
#     retval_temp = re.('^\d\d\d',retval_1)
#     retval_1 = retval_temp.group()

# df_2 = df_1.loc[:,['Date of Publication']]

df_temp = df_1.loc[:,['Date of Publication']]
df_temp['Date of Publication'] = df_1['Date of Publication'].astype('string')
# print(df_2)
# print(df_temp['Date of Publication'].dtypes)
# print(type(df_temp))
df_number = df_temp.applymap(tidy_data)

df_number['Date of Publication'] = df_number['Date of Publication'].astype('string')
# print(df_test['Date of Publication'].dtypes)

df_number['Date of Publication'] = pd.to_numeric(df_number['Date of Publication'],errors='coerce')


df_1['Date of Publication'] = df_number['Date of Publication']

# print(df_1['Date of Publication'].head(10))




# C. Tidying with applymap()

TXT_FILE_PATH = './uniplaces.txt'
df_c = pd.read_table(TXT_FILE_PATH,header=None)

# Create a new dataframe
df_new = pd.DataFrame()
# flag = 0
col = ''
row = 0
state = ''

def state(item):
    # print(list(item))
    global col
    global df_new
    global row
    global state
    # print(item)
    if item.endswith('[edit]'):
        state_1 = re.search('[^\(\)]*(?=\[)',item)
        state = item
        # df_new.loc[row,'State'] = item
        # print(col_name)
        # df_new.columns = [title]
    else:
        if type(item) == str:
            df_new.loc[row,'State'] = state
            if re.search('[^\(\)]*(?=\()',item):
                city = re.search('[^\(\)]*(?=\()',item)[0]
                df_new.loc[row,'city'] = city
            else:
                city = np.nan
            
            if re.findall('(?<=\()[^\(\)]*(?=\))',item):
                un = re.findall('(?<=\()[^\(\)]*(?=\))',item)[0]
                df_new.loc[row,'university'] = un
                row = row + 1
            # print(type(un))
            # df_un[row_sec,city] = un
            else:
                un = np.nan
        
            # row_sec = row_sec + 1
        # df_new.loc[row,col] = item
        # row = row + 1
        # df_new = df_new.append(new_row,ignore_index= True)
    return item

def remove(item):
    retval = re.sub('(\[[^\)]*\])','',item)
    return retval

# (?<=\()[^\(\)]*(?=\))   
# df_temp['Date of Publication'] = df_1['Date of Publication'].astype('string')

df_c = df_c.applymap(state)
df_new = df_new.applymap(remove)

# df_new = df_new.astype('string')
# print(df_new)
# df_new = df_new.applymap(university)

# Write a new file
df_new.to_csv('test_file.csv')



# D. Decoding

CSV_PART_D_FILE_PATH = './trimet.csv'
df_d = pd.read_csv(CSV_PART_D_FILE_PATH)

# print(df_d)
occurrences = 0
idx_num = 0
idx_plus = 0
df_copy = df_d

def insert(df, i, df_add):
    df1 = df.iloc[:i, :]
    df2 = df.iloc[i:, :]
    df_d = pd.concat([df1, df_add, df2], ignore_index=True)
    return df_d


# print(type(df_d[4:5]))
for idx, row in df_d.iterrows():
    if row['OCCURRENCES'] > 1:
        occurrences = row['OCCURRENCES']
        idx_num = idx + idx_plus
        idx_plus = idx_plus + occurrences - 1
        copy_data = df_d[idx:idx+1]
        # print(copy_data)
        for i in range(occurrences-1):
            # print('h')
            df_copy = insert(df_copy, idx_num, copy_data)

        # print(type(copy_data))
        # print(idx,row['OCCURRENCES'])
    else:
        occurrences = 0
        idx_num = 0

# Verification Code 
# for idx, row in df_copy.iterrows():
#     if row['OCCURRENCES'] > 1:
#         # print(idx,row['OCCURRENCES'])
#         print(idx,row['OCCURRENCES'])
# df_copy.to_csv('test_decoding.csv')

# E. Filling

# df_d = df_d.fillna(value='Y')
df_d['VALID_FLAG'].fillna(value='Y')
# df_d = df_d.ffill(axis='VALID_FLAG',)

# for idx, row in df_d.iterrows():
#     if row['VALID_FLAG'] != 'Y':
#         print(row['VALID_FLAG'])

# df_d.to_csv('test_filling.csv')

# F. Interpolating

# .interpolate()
# df_d

df_d['ARRIVE_TIME'] = df_d['ARRIVE_TIME'].interpolate(method='slinear')


# print(df_d['ARRIVE_TIME'])
# print(df_d['ARRIVE_TIME'].head(137))

# G. More Transofrmations

# Link: https://towardsdatascience.com/8-ways-to-transform-pandas-dataframes-b8c168ce878f
# Link: https://towardsdatascience.com/transforming-data-in-python-with-pandas-melt-854221daf507

# H. Transformation Visualizations

csv = '''
VEHICLE_BREADCRUMB_ID,VEHICLE_NUMBER,EQUIPMENT_CLASS,ARRIVE_DATETIME,SERVICE_DATE,ARRIVE_TIME,LONGITUDE,LATITUDE,DWELL,DISTANCE,SPEED,SATELLITES,HDOP,OCCURRENCES,VALID_FLAG,LAST_USER,LAST_TIMESTAMP
4313659804,3411,B,29OCT2021:07:16:40,29OCT2021:00:00:00,26200,-122.799683,45.530268,0,121213.91,26,12,0.9,1,Y,TRANS,31OCT2021:06:06:40
4313659809,3411,B,29OCT2021:07:17:05,29OCT2021:00:00:00,26225,-122.799743,45.527793,0,122106.3,22,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313659813,3411,B,29OCT2021:07:17:25,29OCT2021:00:00:00,26245,-122.799228,45.526933,0,122490.16,17,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660064,3411,B,29OCT2021:08:01:32,29OCT2021:00:00:00,28892,-122.810535,45.548172,15,162555.78,7,12,0.7,6,Y,TRANS,31OCT2021:06:06:40
4313660104,3411,B,29OCT2021:08:05:09,29OCT2021:00:00:00,29109,-122.78541,45.543768,0,170433.08,28,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660282,3411,B,29OCT2021:08:23:59,29OCT2021:00:00:00,30239,-122.780947,45.509918,0,199179.8,13,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
'''
trimet = pd.read_csv(io.StringIO(csv))
(trimet[trimet['VALID_FLAG'] == 'Y']
    # .sort_values('SPEED')
)










