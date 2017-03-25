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
import matplotlib.pyplot as plt
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
def estimateSpeed(data):    
    percentage = data['percentage']
    timestamp = data['AndroidTime']
    speed = 0
    #print(timestamp)
    if(str(timestamp) != "NaT"):
        percentageDelta = 0
        if estimateSpeed.previousDate == None:
            estimateSpeed.previousDate = timestamp
            estimateSpeed.previousPercentage = percentage
        else:
            timeDelta = (timestamp - estimateSpeed.previousDate).seconds
            if(timeDelta == 0):
                timeDelta = 1/60
            else:
                timeDelta = timeDelta / 60
            percentageDelta = abs(percentage - estimateSpeed.previousPercentage)
            if(percentageDelta > 1):
            #print("DateDelta:" + str(timeDelta) + "---"+str(timestamp) +"-" + str(estimateSpeed.previousDate))
            #print("D:" + str(percentageDelta) +"A:" + str(percentage) + "P:" + str(estimateSpeed.previousPercentage))
                speed = percentageDelta / timeDelta
            estimateSpeed.previousDate = timestamp
            estimateSpeed.previousPercentage = percentage
    return speed
def charging_lambda(percentage):
    charging_trend.previous = 1000
    charging_trend.error = 0
    return percentage.apply(charging_trend)
def discharging_lambda(percentage):
    charging_trend.previous = 1000
    charging_trend.error = 0
    return percentage.apply(discharging_trend)
def discharging_trend(val):
    tmp = ""
    if charging_trend.previous >= val:
        tmp = True
    else:
        charging_trend.error  +=1
        tmp = False
    charging_trend.previous = val
    return tmp
def charging_trend(val):
    tmp = ""
    if val - charging_trend.previous > delta:
        tmp = True
    else:
        charging_trend.error  +=1
        tmp = False
    charging_trend.previous = val
    return tmp
def percentage_break_charging_tmp(percentage):
    return percentage.apply(percentage_break_charging)
def percentage_break_charging(val):
    tmp = ""
    if percentage_break_charging.previous == None:
        percentage_break_charging.previous = val
        tmp = False
    if(val > percentage_break_charging.previous):
        if (val - percentage_break_charging.previous) > charging_delta:
            tmp = True
        else:
            tmp = False
    else:
        if (percentage_break_charging.previous - val) > discharging_delta:
            tmp = True
        else:
            tmp = False
    percentage_break_charging.previous = val
    return tmp
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
def selectOverloadedRows(chargingData):
    overloadedRows = pd.DataFrame()
    for overloadedDate in overloadedDateSet:
        tmp = chargingData[chargingData['AndroidDate'] == overloadedDate]
        if(len(overloadedRows) == 0):
            overloadedRows = tmp
        else:
            overloadedRows.append(tmp)
    return overloadedRows
delta = 6
charging_delta = 20
discharging_delta = 4
percentage_break_charging.previous = None
estimateSpeed.previousDate = None
estimateSpeed.previousPercentage = None
charging_trend.previous = 1000
charging_trend.error = 0
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
total_rows = 0
def mainCycle(val):
    global break_after_new_order
    global charging_error
    global big_changes
    global total_rows



    
    #LOAD DATA
    data = pd.read_csv(val, header=0, sep=';')
    total_rows = len(data)
    if 'globalIndex' not in data.columns:
        print("Not present")
        return
    #print(data.head(10))
    #CREATE DATA FROM STRING
    data['AndroidDate'] = pd.to_datetime(data['9'])
    data['uploadDate'] = pd.to_datetime(data['0'])
    #ORDER BY ANDROID DATE AND SERVERSIDE ROW
    data = data.sort_values(by=['AndroidDate', '1'], ascending=[True,True])
    #SELECT SUBSET:'pluggedState', 'voltage', 'temperature', 'percentage', 'health', 'chargingState','AndroidTime', 'ServerSideRow','AndroidTimezone'
    chargingData = data[['17','18','19','20','21','22','AndroidDate', '3','36','globalIndex','uploadDate','34','4']]
    chargingData.columns = ['pluggedState', 'voltage', 'temperature', 'percentage', 'health', 'chargingState','AndroidTime', 'ServerSideRow','AndroidTimezone','globalIndex','uploadDate','triggerCode','UploadTimestamp']
    #print(chargingData.head(10))


    #HANDLE NAN values (0) 
    chargingData['AndroidTimezone'] = chargingData['AndroidTimezone'].apply(time_zone_normalization)
    #ADD TIMEZONE TO ACTUAL ANDROID TIME
    chargingData['localizedDate'] = chargingData['AndroidTime'] + pd.to_timedelta(chargingData.AndroidTimezone, unit='h')
    #ADD DATE ONLY FIELD
    chargingData['AndroidDate'] = chargingData['localizedDate'].apply(lambda A: A.date())
    #print(chargingData[['localizedDate', 'AndroidTime', 'AndroidTimezone']])
    #ANDROID TIME TO INDEX
    chargingData.index = chargingData['localizedDate']
    chargingData.sort_index(inplace=True)
    s = chargingData.apply(estimateSpeed, axis=1)
    s = s[s > 10]
    #print(s.head(40))
    if(len(s) > 0):
        big_changes = len(s)
        overloadedDateSet.update(s.index.date)
        #print(overloadedDateSet)
        #print(selectOverloadedRows(chargingData))
        dataToBeCorrected = selectOverloadedRows(chargingData)
        dataToBeCorrected = dataToBeCorrected.sort_values(by=['globalIndex', 'ServerSideRow'], ascending=[True,True])
        #print(dataToBeCorrected)
        #print(s.index.date)
        #print(chargingData.head(100))
        dataToBeCorrected['chargingStateGroup'] = dataToBeCorrected['chargingState'].apply(group_creator_by_val)
        dataToBeCorrected['serverDateStateGroup'] = dataToBeCorrected['uploadDate'].apply(date_group_creator_by_val)
        #print(dataToBeCorrected.head(100))
        #chargingData['wrongPercentage'] = chargingData.groupby('chargingStateGroup')['percentage'].transform(charging_lambda)
        dataToBeCorrected['dischargingTrend'] = dataToBeCorrected.groupby(['serverDateStateGroup','chargingStateGroup'])['percentage'].transform(discharging_lambda).astype('bool')
        dataToBeCorrected['chargingTrend'] = dataToBeCorrected.groupby(['serverDateStateGroup','chargingStateGroup'])['percentage'].transform(charging_lambda).astype('bool')
        #C. csoport - törés a százalékban (lemerülés nagyobb mint 5 töltődés nagyobb mint 20)
        dataToBeCorrected['percentageBreak'] = False
        dataToBeCorrected['percentageBreak'] = dataToBeCorrected.groupby(['chargingStateGroup'])['percentage'].transform(percentage_break_charging_tmp).astype('bool')
        dataToBeCorrected['chargingError']= 0
        dataToBeCorrected.ix[((dataToBeCorrected['chargingTrend'] == True) &(dataToBeCorrected['pluggedState'] == -1)),'chargingError']= 100
        #print(dataToBeCorrected[dataToBeCorrected['chargingError'] == 100].head(100))
        #print(dataToBeCorrected[['percentage','triggerCode','UploadTimestamp','chargingError','chargingStateGroup','serverDateStateGroup','chargingTrend','AndroidTimezone','percentage']].head(200))
        #print(dataToBeCorrected[dataToBeCorrected['chargingError'] == 100])
        #print("Charging error:" + str())
        charging_error = len(dataToBeCorrected[dataToBeCorrected['chargingError'] == 100])
        break_after_new_order = len(dataToBeCorrected[dataToBeCorrected.percentageBreak == True])
        #chargingData['wrongPercentage'] = chargingData.groupby('chargingStateGroup')['wrongPercentage'].transform(lambda x: x.max())
        #intNumber = chargingData.groupby('chargingStateGroup')
    #chargingData[['percentage']].plot()
#for fileName in glob.glob("/Users/bilickiv/tmpdata/duplicateFree/UGx3clFsMEsyNFRtZWh2bnozdWJ2dTRVS3hkM0pNZWVJaGNiZENjMEdpMD0.csv"):
#for fileName in glob.glob("/Users/bilickiv/tmpdata/duplicateFree/a05TTkJkQThjNWd5SFVYd0pEV1NKUHRsZ3c4TUE2NUdlOVJaVU1XamUrdz0.csv"):
print("START")
logArray = []
for fileName in glob.glob("/home/bilickiv/data/raw_dataset/duplicateFree/*.csv"):
    mainCycle(fileName)
    head, tail = os.path.split(fileName)
    log = {"0Rows":total_rows,"1CHERR": charging_error, "2B" : break_after_new_order,"3BC" : big_changes, "4File": tail}
    logArray.append(log)
    #print(log)
    break_after_new_order = 0
    charging_error = 0
    big_changes = 0
    total_rows = 0
    output = pd.DataFrame(logArray)    
    #print(output)
print("END")
output.to_csv("/home/bilickiv/data/raw_dataset/timeOverlapStat.csv")
print(output)