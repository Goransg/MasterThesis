

# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 19:35:58 2022

@author: Gøran Sildnes Gedde-Dahl
@e-mail: goran.sildnes.gedde-dahl@nmbu.no
"""

import pymysql
import pandas as pd
import numpy as np
import math as m
import copy
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from os import listdir
from os.path import isfile, join
from time import strftime

def add_or_append_inverterdata(folder = 'Scatec data/Inverterdata/EG-ZA'):
    hostname="127.0.0.1:3307"
    dbname="scatec_data"
    uname="goransg"
    pwd=input('Enter password:')
    Cycle_ID = 0
    for csv_file in listdir(folder):
        try:
            SensorVals = pd.read_csv(folder+'/'+csv_file)
            SensorVals = SensorVals.drop(0)
            SensorVals = SensorVals.reset_index(drop = True)
            if 'Unnamed: 0' not in SensorVals.columns:
                pass
            else:    
                for col in SensorVals.columns:
                    if ("Unnamed" in col and col != "Unnamed: 0") or ("Avg1Hour" in col) or ("STATE" in col) or ("INVERTER_General_Status." in col) or 'HasAlarm' in col:
                        SensorVals = SensorVals.drop(col, axis = 1)
                newcols = []
                SensorVals = SensorVals.rename(columns={'Unnamed: 0':'Ts'})
                SensorVals['Ts'] = pd.to_datetime(SensorVals['Ts'])
                SensorVals = SensorVals.set_index('Ts')
                SensorVals = SensorVals.fillna(value=np.nan)
                SensorVals = SensorVals.fillna(0)
                print('Pre: ', SensorVals.shape)
                for col in SensorVals.columns:
                    if SensorVals[col].dtype in ('object', '<M8[ns]'):
                        try:
                            SensorVals[col] = SensorVals[col].astype('float64')
                        except: 
                            pass
                #ts = rawdata['Ts']
                SensorVals = SensorVals.select_dtypes(exclude=['object', '<M8[ns]'])
                #rawdata['Ts'] = ts
                SensorVals = SensorVals.resample('30min').mean()
                print('Post: ', SensorVals.shape)
                for t in SensorVals.columns:
                    t = t.replace('EG-ZA-Log1Min.','')
                    start = t.find('EG', t.find('EG'))
                    end = t.find('.',start)
                    newcols.append(t.replace(t[:end+1],'')[:64])
                    ID = t[start:end]
                SensorVals.columns = newcols
                SensorVals['ID'] = ID
                eamtags = []
                for num in range(len(SensorVals['ID'])):
                    eamtags.append(SensorVals['ID'][num].replace('TS','G-ISS'))
                SensorVals['EAMtag'] = eamtags
                SensorVals['rowid_inv'] = SensorVals['EAMtag'] + '.' + SensorVals.index.values.astype(str)
                timestamps = copy.copy(SensorVals.index.values)
                SensorVals['Ts'] = timestamps
                SensorVals = SensorVals.sort_values('rowid_inv')
                SensorVals = SensorVals.reset_index(drop = True)
                SensorVals['ERR0607'] = np.where(np.logical_and(SensorVals["ERR06"]>0, SensorVals['Error_Word_7']>0),1,0)
                #SensorVals['ERR0607'] = np.where(SensorVals["ERR06_07"]>0,1,0)
                SensorVals = SensorVals.fillna(value=np.nan)
                SensorVals = SensorVals.fillna(0)
                #ts = rawdata['Ts']
                rowidinv = SensorVals['rowid_inv']
                #rawdata['Ts'] = ts
                SensorVals['rowid_inv'] = rowidinv
                SensorVals['ERR0607'] = SensorVals['ERR0607'].astype('float64')
                SensorVals['Total_Active_Power_Measurement'] = SensorVals['Total_Active_Power_Measurement'].astype('float64')
                SensorVals = SensorVals.drop(['ERR00', 'ERR01', 'ERR02', 'ERR03', 'ERR04', 'ERR05', 'ERR06', 'ERR07', 'ERR08', 'ERR09', 'ERR10', 'ERR11', 'ERR12', 'ERR13', 'ERR14', 'ERR15', 'Error_Word_0','Error_Word_1','Error_Word_10','Error_Word_11','Error_Word_12','Error_Word_13','Error_Word_14','Error_Word_15','Error_Word_2','Error_Word_3','Error_Word_4','Error_Word_5','Error_Word_6','Error_Word_7','Error_Word_8','Error_Word_9'], axis = 1)
                SensorVals = SensorVals.drop([col for col in SensorVals.columns if ('ERR' in col and 'ERR0607' not in col) or 'Alarm' in col], axis = 1)
            # Create SQLAlchemy engine to connect to MySQL Database
                engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                				.format(host=hostname, db=dbname, user=uname, pw=pwd))
            # Convert dataframe to sql table                                   
                SensorVals.to_sql('inverter_data_v2', engine, index=True, if_exists = 'append')
                print(strftime("%m/%d/%Y %H:%M:%S") ,csv_file, 'Successfully added')
        except:
            print(csv_file, ' not readable')
            SensorVals = pd.DataFrame()
    
    return SensorVals

a = add_or_append_inverterdata()


def add_or_append_inverterdata_median(folder = 'Scatec data/Inverterdata/EG-AS'):
    hostname="127.0.0.1:3307"
    dbname="scatec_data"
    uname="goransg"
    pwd=input('Enter password:')
    for csv_file in listdir(folder):
        SensorVals = pd.read_csv(folder+'/'+csv_file)
        SensorVals = SensorVals.drop(0)
        SensorVals = SensorVals.reset_index(drop = True)
        for col in SensorVals.columns:
            if "Unnamed" in col and col != "Unnamed: 0":
                SensorVals = SensorVals.drop(col, axis = 1)
        newcols = []
        SensorVals = SensorVals.rename(columns={'Unnamed: 0':'Ts'})
        SensorVals['Ts'] = pd.to_datetime(SensorVals['Ts'])
        SensorVals = SensorVals.set_index('Ts')
        SensorVals = SensorVals.fillna(value=np.nan)
        SensorVals = SensorVals.fillna(0)
        print('Pre: ', SensorVals.shape)
        for col in SensorVals.columns:
            if SensorVals[col].dtype in ('object', '<M8[ns]'):
                try:
                    SensorVals[col] = SensorVals[col].astype('float64')
                except: 
                    pass
        #ts = rawdata['Ts']
        SensorVals = SensorVals.select_dtypes(exclude=['object', '<M8[ns]'])
        #rawdata['Ts'] = ts
        for t in SensorVals.columns:
            start = t.find('EG')
            end = t.find('.',start)
            newcols.append(t.replace(t[start:end+1],'')[:64])
            ID = t[start:end]
        SensorVals.columns = newcols
        errs = SensorVals[['ERR06', 'Error_Word_7']]
        errs = errs.resample('30min').mean()
        SensorVals = SensorVals.resample('30min').median()
        SensorVals['ERR06'] = errs['ERR06']
        SensorVals['Error_Word_7'] = errs['Error_Word_7']
        print('Post: ', SensorVals.shape)
        eamtags = []
        SensorVals['ID'] = ID
        for num in range(len(SensorVals['ID'])):
            eamtags.append(SensorVals['ID'][num].replace('TS','G-ISS'))
        SensorVals['EAMtag'] = eamtags
        SensorVals['rowid_inv'] = SensorVals['EAMtag'] + '.' + SensorVals.index.values.astype(str)
    # Create SQLAlchemy engine to connect to MySQL Database
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
        				.format(host=hostname, db=dbname, user=uname, pw=pwd))
    # Convert dataframe to sql table                                   
        SensorVals.to_sql('inverter_data_median', engine, index=True, if_exists = 'append')
        print(csv_file, 'Successfully added')
        
    return SensorVals

#a = add_or_append_inverterdata_median()

# =============================================================================
# engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
# 				.format(host=hostname, db=dbname, user=uname, pw=pwd))
# 
# engine.execute('ALTER TABLE weather_data ADD PRIMARY KEY (`rowid`);')
# =============================================================================
