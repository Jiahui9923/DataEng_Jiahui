from cgi import print_arguments
from enum import Flag
from re import X
from turtle import st
from cv2 import DFT_REAL_OUTPUT, DrawMatchesFlags_DRAW_RICH_KEYPOINTS
import pandas as pd
import numpy as np
import datetime

from pyparsing import col
import warnings

import matplotlib.pyplot as plt

COVID_FILE_PATH = 'COVID_county_data.csv'
CENSUS_FILE_PATH = 'acs2017_census_tract_data.csv'


df_covid = pd.read_csv(COVID_FILE_PATH)
df_census = pd.read_csv(CENSUS_FILE_PATH,usecols=['State','County','TotalPop','Poverty','IncomePerCap'])

# print(df_census)
# PART A
# Transform the ACS Data
df_census['Poverty'] = df_census['Poverty'] * 0.01
df_census['Poverty'] = df_census['TotalPop'] * df_census['Poverty']
df_census['IncomePerCap'] = df_census['IncomePerCap'] * df_census['TotalPop']

# To Get New Table
# df_county_info = df_census.groupby(['State','County'],sort=False)['TotalPop'].sum().reset_index()
df_county_info = df_census.groupby(['State','County']).agg({'TotalPop':'sum','Poverty':'sum','IncomePerCap':'sum'})
# print(df_county_info)

# modify
df_county_info['IncomePerCap'] = df_county_info['IncomePerCap'] / df_county_info['TotalPop']
df_county_info['Poverty'] = df_county_info['Poverty'] / df_county_info['TotalPop']
df_county_info['IncomePerCap'] = df_county_info['IncomePerCap'].astype(int)
# df_county_info['Poverty'] = df_county_info['Poverty'].astype(float)

# convert groupby to df
df_county_info_new = pd.DataFrame(df_county_info)
df_county_info_new.reset_index(inplace=True)

df_county_info_new['ID'] = df_county_info_new.index

# print(df_county_info_new)

# Answer for table
# County info. & State info.
# print(df_county_info_new.loc[(df_county_info_new['County'] == 'Loudoun County') & (df_county_info_new['State'] == 'Virginia')])
# print(df_county_info_new.loc[(df_county_info_new['County'] == 'Washington County') & (df_county_info_new['State'] == 'Oregon')])
# print(df_county_info_new.loc[(df_county_info_new['County'] == 'Harlan County') & (df_county_info_new['State'] == 'Kentucky')])
# print(df_county_info_new.loc[(df_county_info_new['County'] == 'Malheur County') & (df_county_info_new['State'] == 'Oregon')])

# <most populous country in the USA>
# print(df_county_info_new.loc[df_county_info_new['TotalPop'].idxmax()])

# <least populous county in the USA>
# print(df_county_info_new.loc[df_county_info_new['TotalPop'].idxmin()])


# PART B 
# Transform the COVID Date

# Get a Unique ID
df_id = df_county_info_new.loc[:,['County','ID','State']]
df_id = df_id.rename(columns={'State':'state'})
df_id['county'] = df_id['County'].str.replace(r'[^\s]*$','',regex=True)
df_id['county'] = df_id['county'].str.strip()
df_id = df_id.drop("County",axis=1)

# Get COVID DATA
df_covid.date = pd.to_datetime(df_covid.date)
df_covid['month_year'] = pd.DatetimeIndex(df_covid['date']).to_period('M')
df_covid_new = df_covid.groupby(['month_year','county','state'],sort=False).agg({'cases':'sum','deaths':'sum'})

# convert groupby to df
df_covid_new = pd.DataFrame(df_covid_new)
df_covid_new.reset_index(inplace=True)

# print(df_id)
# df_covid_final = pd.merge(df_covid_new,df_id,how='left',on='county')
df_covid_final = df_covid_new.merge(df_id,how='left',on=['county','state'])

df_covid_final['ID'] = df_covid_final['ID'].astype('int',errors='ignore')
# print(df_covid_final)

# df_county_info['IncomePerCap'] = df_county_info['IncomePerCap'].astype(int)

# print(df_covid_new)
# print(df_covid_final['ID'].astype)
# print(df_id['ID'].astype)

# df_county_info_new

# Answer for table
print( df_covid_final.loc[(df_covid_new['month_year'] == '2021-02') & (df_covid_final['county'] == 'Malheur') & (df_covid_final['state'] == 'Oregon')])
# print( df_covid_final.loc[(df_covid_new['month_year'] == '2020-08') & (df_covid_final['county'] == 'Malheur') & (df_covid_final['state'] == 'Oregon')])
# print( df_covid_final.loc[(df_covid_new['month_year'] == '2021-01') & (df_covid_final['county'] == 'Malheur') & (df_covid_final['state'] == 'Oregon')])


# PART C
# Integrate COVID Date with ACS Data
df_total_cases = df_covid_final.groupby(['county','state'],sort=False).agg({'cases':'sum','deaths':'sum','ID':'mean'})
df_total_cases['cases'] = df_total_cases['cases'].astype(float)
df_total_cases['deaths'] = df_total_cases['deaths'].astype(float)
df_total_cases = pd.DataFrame(df_total_cases)
df_total_cases.reset_index(inplace=True)
# print(df_total_cases)
# df_county_info_new['new_county'] = df_county_info_new['County'].str.replace('[^\s]*$','')
# print(df_county_info_new)


warnings.filterwarnings("ignore")
df_acs_covid = df_county_info.merge(df_total_cases,how='left',on='ID')

# Test Warning:
# print(df_county_info)
# print(df_total_cases)
# print(df_acs_covid)

# Delete redundant columns
df_acs_covid = df_acs_covid.drop(['county','state'],axis=1)
# Using round to get number of cases & deaths
df_acs_covid['TotalCasesPer100K'] = round(df_acs_covid['cases'] / (df_acs_covid['TotalPop']/100000))
df_acs_covid['TotalDeathsPer100K'] = round(df_acs_covid['deaths'] / (df_acs_covid['TotalPop']/100000))

# Answer for table
# print( df_acs_covid.loc[(df_acs_covid['County'] == 'Washington County') & (df_acs_covid['State'] == 'Oregon')])
# print( df_acs_covid.loc[(df_acs_covid['County'] == 'Malheur County') & (df_acs_covid['State'] == 'Oregon')])
# print( df_acs_covid.loc[(df_acs_covid['County'] == 'Loudoun County') & (df_acs_covid['State'] == 'Virginia')])
# print( df_acs_covid.loc[(df_acs_covid['County'] == 'Harlan County') & (df_acs_covid['State'] == 'Kentucky')])


# PART D
# Analysis

# Oregon
df_ore = df_acs_covid[df_acs_covid['State'] == 'Oregon']
df_ore['Poverty'] = df_ore['Poverty'] * 100
# COVID total cases vs. % population in poverty
Ra = df_ore['TotalCasesPer100K'].corr(df_ore['Poverty'])
Rb = df_ore['TotalDeathsPer100K'].corr(df_ore['Poverty'])
Rc = df_ore['TotalCasesPer100K'].corr(df_ore['IncomePerCap'])
Rd = df_ore['TotalDeathsPer100K'].corr(df_ore['IncomePerCap'])

# ax1 = df_ore.plot.scatter(x='Poverty',y='TotalCasesPer100K',c='DarkBlue')
# ax2 = df_ore.plot.scatter(x='Poverty',y='TotalDeathsPer100K',c='DarkBlue')
# ax3 = df_ore.plot.scatter(x='IncomePerCap',y='TotalCasesPer100K',c='DarkBlue')
# ax4 = df_ore.plot.scatter(x='IncomePerCap',y='TotalDeathsPer100K',c='DarkBlue')

# print(plt.show())
# print(df_ore)
# print(Ra)
# print(Rb)
# print(Rc)
# print(Rd)


# USA
df_usa = df_acs_covid
df_usa['Poverty'] = df_usa['Poverty'] * 100
Rua = df_usa['TotalCasesPer100K'].corr(df_usa['Poverty'])
Rub = df_usa['TotalDeathsPer100K'].corr(df_usa['Poverty'])
Ruc = df_usa['TotalCasesPer100K'].corr(df_usa['IncomePerCap'])
Rud = df_usa['TotalDeathsPer100K'].corr(df_usa['IncomePerCap'])
Rue = df_usa['TotalCasesPer100K'].corr(df_usa['TotalPop'])

# print(df_usa)
# print(Rua)
# print(Rub)
# print(Ruc)
# print(Rud)
# print(Rue)



# Question of exploration

Rea = df_ore['TotalPop'].corr(df_ore['cases'])
ax_test = df_ore.plot.scatter(x='TotalPop',y='cases',c='DarkBlue')

print(plt.show())
print(Rea)







