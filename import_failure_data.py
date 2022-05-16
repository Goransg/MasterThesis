# -*- coding: utf-8 -*-
"""
Created on Fri Feb 25 13:39:25 2022

@author: gogd
"""

import pymysql
import pandas as pd
import math as m
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import datetime   

downtimes = pd.read_excel('Scatec data/downtime_I22.xlsx')
print(downtimes.columns)

downtimes.columns = ['Start_Time', 'End_Time', 'AlName', 
                     'Estimated_Production_Losses_(kWh)', 
                     'Estimated_Revenue_Losses_(USD)', 'Alarm_Message',
                     'Alarm_Time', 'Acked', 'Duration_dd_hh:mm:ss',
                     'Duration_s', 'Alarm_Source']
print(downtimes.columns)
hostname="127.0.0.1:3306"
dbname="scatec_data"
uname="goransg"
pwd=input('Enter password:')
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostname, db=dbname, user=uname, pw=pwd))


# Convert dataframe to sql table                                   
downtimes.to_sql('downtimes', engine, index=False, if_exists = 'append')

