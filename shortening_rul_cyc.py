# -*- coding: utf-8 -*-
"""
Created on Sat Apr 23 08:10:47 2022

@author: Prevas
"""
import pymysql
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import copy

# Connecting to the database

pw = input('Enter password: ')
connection = pymysql.connect(host='localhost',
                             port = 3307,
                             user='goransg',
                             password=pw,
                             database='scatec_data',
                             cursorclass=pymysql.cursors.DictCursor)

with connection:
    with connection.cursor() as cursor:
        # Create a new record
        sql = "SELECT * FROM mergeddata_v2 WHERE rowid_inv LIKE '%EG-ZA%'"
        cursor.execute(sql)      
        result = cursor.fetchall()
        
rawdata = pd.DataFrame(result)
RULs = []
for inv in np.unique(rawdata['ID']):
    RULs.extend(round((rawdata[(rawdata['ID'] == inv)].groupby((rawdata['ERR0607'] > 0).cumsum()).cumcount(ascending=False)+1)/(2*24),0))
rawdata['RUL'] = RULs
n_cycles = []
Cycle_ID = 828
for num in range(len(rawdata['RUL'])):
    try:
        if rawdata['RUL'][num] != 0 and rawdata['RUL'][num-1] == 0:
            Cycle_ID += 1
    except:
        pass
    n_cycles.append(Cycle_ID)
rawdata['Cycle_ID'] = n_cycles
original_uncut = copy.copy(rawdata)
maxcycles = rawdata.groupby(['ID'], sort=False)['Cycle_ID'].max()
for cyc in maxcycles.values:
    rawdata = rawdata[(rawdata['Cycle_ID'] != cyc)]
rawdata = rawdata.fillna(value=np.nan)
rawdata = rawdata.fillna(0)
for col in rawdata.columns:
    if rawdata[col].dtype in ('object', '<M8[ns]'):
        try:
            rawdata[col] = rawdata[col].astype('float64')
        except: 
            pass
#ts = rawdata['Ts']
ids = rawdata['ID']
rowidinv = rawdata['rowid_inv']
rawdata = rawdata.select_dtypes(exclude=['object', '<M8[ns]'])
#rawdata['Ts'] = ts
rawdata['rowid_inv'] = rowidinv
rawdata['ID'] = ids
rawdata = rawdata[rawdata.Cycle_ID != max(rawdata['Cycle_ID'])]
rawdata_prd = rawdata[(rawdata["Total_Active_Power_Measurement"]>0)]
rawdata_prd = rawdata_prd[(rawdata_prd['ERR0607']<=0)]

engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host="127.0.0.1:3307", db='scatec_data', user='goransg', pw=pw))
# Convert dataframe to sql table                                   
rawdata_prd.to_sql('inverter_data_short_v2', engine, index=True, if_exists = 'append')