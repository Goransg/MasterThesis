# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 19:35:58 2022

@author: Gøran Sildnes Gedde-Dahl
@e-mail: goran.sildnes.gedde-dahl@nmbu.no
"""

import numpy as np
import pymysql
import pandas as pd
import math as m
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from os import listdir
from os.path import isfile, join
import copy

def add_or_append_weatherdata(SensorVals):
    new_df = pd.DataFrame()
    ws_names = SensorVals.iloc[[0]].values[0]
    newcols = []
    for n in range(len(SensorVals.columns)):
        if SensorVals.columns[n] == 'Variable':
            newcols.append('TimeStamp')
        else:
            if '.' in SensorVals.columns[n]:
                end = SensorVals.columns[n].find('.')
            else: 
                end = len(SensorVals.columns[n])
            newcols.append(ws_names[n]+'.'+SensorVals.columns[n][:end])
    SensorVals.columns = newcols
    SensorVals = SensorVals.drop(0)
    SensorVals = SensorVals.drop(1)
    SensorVals = SensorVals.reset_index(drop = True)
    ws = []
    measures= []
    for col in SensorVals.columns:
        first_dot = col.find('.')
        if col == 'TimeStamp':
            measures.append('TimeStamp')
        elif col.count('.')>1:
            second_dot = col.find('.', first_dot+1)
            ws.append(col[:first_dot]) 
            measures.append(col[first_dot+1:second_dot])
        else:
            second_dot = len(col)
            ws.append(col[:first_dot]) 
            measures.append(col[first_dot+1:second_dot])
    ws = set(ws)
    measures = set(measures)
    for stat in ws:
        ws_df = pd.DataFrame()
        ws_df['WeatherStation'] = [stat for num in range(len(SensorVals))]
        for ms in measures:
            if ms == 'TimeStamp':
                ws_df['ts_ws'] = SensorVals['TimeStamp']
            elif stat[-1] != 'B': 
                try:
                    ws_df[ms] = SensorVals[stat+'.'+ms]
                    #print(stat,'.',ms, ' inne')
                except:
                    print(stat+'.'+ms)
        ws_df['rowid'] = ws_df['WeatherStation'] + '.' + ws_df['ts_ws']
                
        new_df = new_df.append(ws_df, ignore_index = True)
        
    
    # Credentials to database connection

    
    
    # Create SQLAlchemy engine to connect to MySQL Database
    hostname="127.0.0.1:3307"
    dbname="scatec_data"
    uname="root"
    pwd=input('Enter password:')
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
    				.format(host=hostname, db=dbname, user=uname, pw=pwd))
    new_df['ts_ws'] = pd.to_datetime(new_df['ts_ws'])
    
    # Convert dataframe to sql table                                   
    new_df.to_sql('weather_data', engine, index=False, if_exists = 'append')
    
    return new_df, ws_df
        

weatherfile = pd.read_csv('Scatec data/Weatherdata_EG-KO.csv')
b = copy.copy(weatherfile)
a, c = add_or_append_weatherdata(weatherfile)



def add_or_append_inv_to_ws(links):
    links = links.drop("Unnamed: 0", axis = 1)
    try:
        print(hostname)
    except:
        hostname="127.0.0.1:3307"
        dbname="scatec_data"
        uname="goransg"
        pwd=input('Enter password:')
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
    				.format(host=hostname, db=dbname, user=uname, pw=pwd))
                                 
    links.to_sql('inverter_metadata', engine, index=False, if_exists = 'append')

raw_data_connections = pd.read_csv('Scatec data/EG-KO_WS-INV.csv')

add_or_append_inv_to_ws(raw_data_connections)
   
    

# =============================================================================
# engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
# 				.format(host=hostname, db=dbname, user=uname, pw=pwd))
# 
# engine.execute('ALTER TABLE weather_data ADD PRIMARY KEY (`rowid`);')
# =============================================================================