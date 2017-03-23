import sys
import os
import datetime
import time
import glob
import configparser
import pandas as pd
import numpy as np
import argparse
from itertools import islice
#import matplotlib.pyplot as plt
import scipy.stats as sc
import math
import warnings

warnings.filterwarnings('ignore')
def time_zone_normalization(timezone):
    if math.isnan(timezone):
        return 0
    return timezone
def to_localized_data(date):
    print(date)
    return date

def group_creator_by_val(val):
    if val == group_creator_by_val.previous:
        tmp = ""
    else:
        group_creator_by_val.previous = val
        group_creator_by_val.count  +=1
    return group_creator_by_val.count
def date_group_creator_by_val(val):
    val = val.date()
    if val == date_group_creator_by_val.previous:
        tmp = ""
    else:
        date_group_creator_by_val.previous = val
        date_group_creator_by_val.count  +=1
    return date_group_creator_by_val.count
def count_dayly_record(val):
    global dateDict
    if(val in dateDict):
        tmp = dateDict[val]
        tmp = tmp + 1
        dateDict[val] = tmp
    else:
        dateDict[val] = 1
    return
def selectOverloadedRows(chargingData):
    overloadedRows = pd.DataFrame()
    for overloadedDate in overloadedDateSet:
        tmp = chargingData[chargingData['AndroidDate'] == overloadedDate]
        if(len(overloadedRows) == 0):
            overloadedRows = tmp
        else:
            overloadedRows.append(tmp)
    return overloadedRows
group_creator_by_val.count = 0 
group_creator_by_val.previous = None
date_group_creator_by_val.previous = None
date_group_creator_by_val.count = 0
overloadedDateSet = set()
#pd.set_option('display.max_columns', 10)
pd.options.display.max_columns = None
pd.options.display.width = 120
pd.set_option('display.max_rows', 500)
break_after_new_order = 0
charging_error = 0
big_changes = 0

dateDict = {}
nodeDict = {}
duplicateDict = {}

total_rows = 0
rowArray = [] 

def mainCycle(val):
    global break_after_new_order
    global charging_error
    global big_changes
    global total_rows
    global dateDict
    #LOAD DATA
    data = pd.read_csv(val, header=0, sep=';')
    print(data.head(10))
    a = data[data.duplicated(subset=['3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34'],keep=False)]
    if(len(a) > 0):
        print(len(a))
        print(a)
    total_rows = len(data)
    data['AndroidDate'] = pd.to_datetime(data['7'])
    data['uploadDate'] = pd.to_datetime(data['uploadDate'])
    data['StripedUploadDate'] = data['uploadDate'].apply(lambda A: A.date())
    data['StripedUploadDate'].apply(count_dayly_record)
    #print(data.head())
    #print(dateDict)
   
    if 'globalIndex' not in data.columns:
        print("Not present")
        return
    #print(data.head(100))
    #CREATE DATA FROM STRING

    #ORDER BY ANDROID DATE AND SERVERSIDE ROW
    data = data.sort_values(by=['AndroidDate', '1'], ascending=[True,True])
    #SELECT SUBSET:'pluggedState', 'voltage', 'temperature', 'percentage', 'health', 'chargingState','AndroidTime', 'ServerSideRow','AndroidTimezone'
    chargingData = data[['15','16','17','18','19','20','AndroidDate', '1','34','globalIndex','uploadDate','32','2']]
    chargingData.columns = ['pluggedState', 'voltage', 'temperature', 'percentage', 'health', 'chargingState','AndroidTime', 'ServerSideRow','AndroidTimezone','globalIndex','uploadDate','triggerCode','UploadTimestamp']
    #print(chargingData.head(200))


    #HANDLE NAN values (0) 
    chargingData['AndroidTimezone'] = chargingData['AndroidTimezone'].apply(time_zone_normalization)
    #ADD TIMEZONE TO ACTUAL ANDROID TIME
    chargingData['localizedDate'] = chargingData['AndroidTime'] + pd.to_timedelta(chargingData.AndroidTimezone, unit='h')
    #ADD DATE ONLY FIELD
    #print(chargingData[['localizedDate', 'AndroidTime', 'AndroidTimezone']])
    #ANDROID TIME TO INDEX
    chargingData.index = chargingData['localizedDate']
    chargingData.sort_index(inplace=True)
   
    #print(s.head(40))
print("START")
for fileName in glob.glob("/home/bilickiv/data/raw_dataset/userSpecificPreprocessed/Zz*.csv"):
    print(fileName)
    mainCycle(fileName)
    nodeDict[fileName] = total_rows
    
a = pd.Series(dateDict, name='Count')
a.to_csv('/home/bilickiv/data/raw_dataset/dayliStat.csv')
b = pd.Series(nodeDict, name='Count')
b.to_csv('/home/bilickiv/data/raw_dataset/nodeStat.csv')
print("END")