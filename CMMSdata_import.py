
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  7 16:56:57 2022

@author: Gøran Sildnes Gedde-Dahl
@e-mail: goran.sildnes.gedde-dahl@nmbu.no
"""

import pymysql
import pandas as pd
import math as m
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import datetime


raw_data_cmms = pd.read_excel('Scatec data/CMMS_data.xlsx') 

def add_or_append_CMMSdata(WOs, intrvl):
    hostname="127.0.0.1:3306"
    dbname="scatec_data"
    uname="goransg"
    pwd=input('Enter password:')
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
    				.format(host=hostname, db=dbname, user=uname, pw=pwd))
    WOs['WO_Created_Time'] = pd.to_datetime(WOs['WO Created Time'])
    for num in range(len(WOs['WO_Created_Time'])):
        try:
            WOs['WO_Created_Time'][num] = WOs['WO_Created_Time'][num] - datetime.timedelta(minutes=WOs['WO_Created_Time'][num].minute % intrvl,
                                     seconds=WOs['WO_Created_Time'][num].second,
                                     microseconds=WOs['WO_Created_Time'][num].microsecond)
        except:
            print(WOs['WO_Created_Time'][num])   
                              
    WOs.to_sql('CMMS_data', engine, index=False, if_exists = 'append')

add_or_append_CMMSdata(raw_data_cmms, 15)
