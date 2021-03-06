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
import scipy.stats as sc
import math
import warnings
break_after_new_order = 0
charging_error_unplugged = 0
charging_error_discharging = 0
big_changes = 0
total_rows = 0
chargingData = 0
logArray = []
def loadConfiguration():
    global userSpecificPreprocessedFolder
    global duplicateFree   
    global statFolder   
    global userSpecificFiles
    global fileStepCount

    parser = argparse.ArgumentParser()
    parser.add_argument("opsystem", help="runntime, 0=benti, 1=linux, 2=osx",type=int)
    parser.add_argument("chunk", help="chunk number, 0-12",type=int)                    
    args = parser.parse_args()
    fileStepCount = args.chunk
    print("Actual step:" + "----" + str(fileStepCount))
    if(args.opsystem == 2):
        actualEnvironment = "osx"
    if(args.opsystem == 0):
        actualEnvironment = "benti"
    if(args.opsystem == 1):
        actualEnvironment = "linux" 
    config = configparser.ConfigParser()
    config.read('timeOverlapHandler.txt')
    if(actualEnvironment == "osx"):
        statFolder = config['osx']['statFolder']              
        duplicateFree = config['osx']['duplicateFree']
    if(actualEnvironment == "benti"):
        statFolder = config['benti']['statFolder']              
        duplicateFree = config['benti']['duplicateFree']                        
    if(actualEnvironment == "linux"):
        statFolder = config['fict']['statFolder']            
        duplicateFree = config['fict']['duplicateFree']            
    return;

def loadchunks():
    global duplicateFree
    global fileStepCount
    global fileStep
    global total_rows
    global big_changes
    global charging_error_unplugged
    global charging_error_discharging
    global break_after_new_order
    global logArray
    global statFolder
    indexCounter = 0
    fileList = []
    for fileName in glob.glob(duplicateFree + "*.csv"):
        fileList.append(fileName)
    print(str(fileStep) + ":" + str(fileStepCount))
    start = fileStepCount * fileStep
    end = (fileStepCount + 1) * fileStep
    iter = islice(fileList, start, end, None)
    # loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
    for a in iter:
        print("Processing file:"  + a)
        log = mainCycle(a)
        head, tail = os.path.split(a)
        #log = {"0Rows":total_rows,"1CHERR": charging_error_unplugged, "2B" : break_after_new_order,"3BC" : big_changes, "4CD": charging_error_discharging, "5File": tail}
        logArray.append(log)
        #print(log)
        break_after_new_order = 0
        charging_error_discharging = 0        
        charging_error_unplugged = 0
        big_changes = 0
        total_rows = 0
        output = pd.DataFrame(logArray)    
        #print(log)
        #print(output)
        indexCounter = indexCounter + 1
        print("Loaded file (" + str(indexCounter) + "):" + a)
    output.to_csv(statFolder+str(start)+".csv")
    #print(output)
    return

def merge(old, new):
    
    return old
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
    if val - charging_trend.previous >= charging_delta:
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
    x = float(val)
    if math.isnan(x):
        return group_creator_by_val.count
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
def selectOverloadedRows(overloadedDateSet):
    global chargingData
    overloadedRows = pd.DataFrame()
    for overloadedDate in overloadedDateSet:
        tmp = chargingData[chargingData['AndroidDate'] == overloadedDate]
        #print(len(tmp))
       # print("Before:" + str(len(chargingData)))
        chargingData = chargingData[chargingData.AndroidDate != overloadedDate]
        overloadedRows = pd.concat([overloadedRows, tmp], ignore_index=True)
        #print("After:"+str(len(chargingData)))
        #print(len(overloadedRows))
    return overloadedRows
def detectChargingRuleErrors(data):
        #print(data.head())
        if(len(data) == 0):
            return
        #create groups of consecutive charging states, it increases the index for each group    
        data['chargingStateGroup'] = data['chargingState'].apply(group_creator_by_val)
        #create a group for each server side day
        data['serverDateStateGroup'] = data['uploadDate'].apply(date_group_creator_by_val)
        #print(dataToBeCorrected.head(100))
        #chargingData['wrongPercentage'] = chargingData.groupby('chargingStateGroup')['percentage'].transform(charging_lambda)
        #it defines the discharging boolean based on the chargingstategroup  the date and the trend detected with percentage change
        data['dischargingTrend'] = data.groupby(['serverDateStateGroup','chargingStateGroup'])['percentage'].transform(discharging_lambda).astype('bool')
        #it defines the charging boolean based on the chargingstategroup  the date and the trend detected with percentage change
        data['chargingTrend'] = data.groupby(['serverDateStateGroup','chargingStateGroup'])['percentage'].transform(charging_lambda).astype('bool')
        #C. csoport - törés a százalékban (lemerülés nagyobb mint 5 töltődés nagyobb mint 20)
        data['percentageBreak'] = False
        data['percentageBreak'] = data.groupby(['chargingStateGroup'])['percentage'].transform(percentage_break_charging_tmp).astype('bool')
        data['chargingErrorUnplugged']= 0
        data['chargingErrorDischarging']= 0
        #if it is charging (it was detected that it is charging) and it is unplugged then this is an error
        data.ix[((data['chargingTrend'] == True) & (data['chargingState'] != 2)),'chargingErrorUnplugged']= 100
        data.ix[((data['chargingTrend'] == True) &( data['pluggedState'] == -1)),'chargingErrorDischarging']= 100
        data.ix[((data['chargingTrend'] == True) & ( (data['pluggedState'] == -1) & (data['chargingState'] == 3) )),'chargingErrorStrictDischarging']= 100             
        #print(dataToBeCorrected[dataToBeCorrected['chargingError'] == 100].head(100))
        #print(dataToBeCorrected[['percentage','triggerCode','UploadTimestamp','chargingError','chargingStateGroup','serverDateStateGroup','chargingTrend','AndroidTimezone','percentage']].head(200))
        #print(dataToBeCorrected[dataToBeCorrected['chargingError'] == 100])
        #print("Charging error:" + str())
        charging_error_discharging = len(data[data['chargingErrorDischarging'] == 100])
        charging_error_unplugged = len(data[data['chargingErrorUnplugged'] == 100])
        charging_error_strict = len(data[data['chargingErrorStrictDischarging'] == 100])

        break_after_new_order = len(data[data.percentageBreak == True])
        log = {"1CHERR": charging_error_unplugged, "2B" : break_after_new_order,"4CD": charging_error_discharging, "5SC": charging_error_strict}

        #print("Charging error:" + str(charging_error_unplugged
        #))
        #print("Break after new order:" +str(break_after_new_order))
        return log

def detectChangeSpeedErrors(data):
    global break_after_new_order
    global charging_error_unplugged

    global total_rows
    global chargingData
    global overloadedDateSet
    print(len(data))
    s = data.apply(estimateSpeed, axis=1)
    s = s[s > 10]
    big_changes = len(s)
    #print(s.head(40))
    if(len(s) > 0):
        big_changes = len(s)
        #Creates a set with date objects
       
       # overloadedDateSet.update(s.index.date)
       
        #print(overloadedDateSet)
        #print(selectOverloadedRows(chargingData))
        #SELECT ROWS BY Android DATE IN overloadedDateSet
       
       # dataToBeCorrected = selectOverloadedRows(overloadedDateSet)
       
        #print("After return:" + str(len(chargingData)))
        #SORT the selected rows with the help of file name and db or file row
        
        #dataToBeCorrected = dataToBeCorrected.sort_values(by=['globalIndex', 'ServerSideRow'], ascending=[True,True])
        #correctdData = merge(chargingData,dataToBeCorrected)
        
        #print(chargingData[['localizedDate', 'AndroidDate', 'percentage', 'ServerSideRow']])        
        #print(dataToBeCorrected[['localizedDate', 'AndroidDate', 'percentage', 'ServerSideRow']])        
        #print("original")
        #detectChargingRuleErrors(chargingData)
        #print("ordered by serverside original")
        #chargingData = chargingData.sort_values(by=['globalIndex', 'ServerSideRow'], ascending=[True,True])
        #detectChargingRuleErrors(chargingData)        
        #print("removed")
        #detectChargingRuleErrors(dataToBeCorrected)
        #print("removed by time")
        #dataToBeCorrected = dataToBeCorrected.sort_values(by=['AndroidTime', 'globalIndex', 'ServerSideRow'], ascending=[True,True,True])
        #detectChargingRuleErrors(dataToBeCorrected)
    return big_changes
def evaluate(fileName):
    global chargingData
    global break_after_new_order
    global charging_error_unplugged
    global charging_error_discharging
    global big_changes
    global total_rows

    chargingData = chargingData.sort_values(by=['localizedDate','globalIndex', 'row1', 'row2', 'row3'], ascending=[True,True,True,True,True])        
    big_changes_a = detectChangeSpeedErrors(chargingData)
    log_a = detectChargingRuleErrors(chargingData)
    charging_error_unplugged_a = log_a['1CHERR']
    break_after_new_order_a = log_a['2B']
    charging_error_discharging_a = log_a['4CD']
    charging_error_strict_a = log_a['5SC']

    chargingData = chargingData.sort_values(by=['globalIndex', 'globalIndex', 'row1', 'row2', 'row3'], ascending=[True,True, True, True, True])
    big_changes_s = detectChangeSpeedErrors(chargingData)
    log_s = detectChargingRuleErrors(chargingData)
    charging_error_unplugged_s = log_s['1CHERR']
    break_after_new_order_s = log_s['2B']
    charging_error_discharging_s = log_s['4CD']
    charging_error_strict_s = log_s['5SC']

    summaryLog = {"0TR":total_rows,"1ABC":big_changes_a,"2SBC":big_changes_s, "3ACEU":charging_error_unplugged_a,"4SCEU":charging_error_unplugged_s, "5ACED":charging_error_discharging_a,"6SCED":charging_error_discharging_s, "7AP":break_after_new_order_a,"8SP":break_after_new_order_s,"9ASC":charging_error_strict_a,"10SSC":charging_error_strict_s,  "11F": fileName}            
    return summaryLog           
def mainCycle(val):
    global break_after_new_order
    global charging_error_unplugged

    global big_changes
    global total_rows
    global chargingData
    #LOAD DATA
    data = pd.read_csv(val, header=0, sep=';')
    #print(data.head())
    total_rows = len(data)
    if 'globalIndex' not in data.columns:
        print("Not present globalIndex")
        return
    print(data.head(10))
    #CREATE DATE FROM STRING
    data['AndroidDate'] = pd.to_datetime(data['9'])
    #Server date to date
    data['uploadDate'] = pd.to_datetime(data['0'])
    #ORDER BY ANDROID DATE AND SERVERSIDE ROW
    data = data.sort_values(by=['AndroidDate', '3'], ascending=[True,True])
    #SELECT SUBSET:'pluggedState', 'voltage', 'temperature', 'percentage', 'health', 'chargingState','AndroidTime', 'ServerSideRow','AndroidTimezone'
    chargingData = data[['17','18','19','20','21','22','AndroidDate', '3','36','globalIndex','uploadDate','34','4','row1','row2','row3']]
    chargingData.columns = ['pluggedState', 'voltage', 'temperature', 'percentage', 'health', 'chargingState','AndroidTime', 'ServerSideRow','AndroidTimezone','globalIndex','uploadDate','triggerCode','UploadTimestamp','row1','row2','row3']
    #print(chargingData.head(10))


    #HANDLE NAN values (0) 
    chargingData['AndroidTimezone'] = chargingData['AndroidTimezone'].apply(time_zone_normalization)
    #ADD TIMEZONE TO ACTUAL ANDROID TIME
    chargingData['localizedDate'] = chargingData['AndroidTime'] + pd.to_timedelta(chargingData.AndroidTimezone, unit='h')
    #ADD DATE ONLY FIELD
    chargingData['AndroidDate'] = chargingData['localizedDate'].apply(lambda A: A.date())
    #print(chargingData[['localizedDate', 'AndroidDate', 'AndroidTimezone']].head())
    #ANDROID TIME TO INDEX
    chargingData.index = chargingData['localizedDate']
    #print(chargingData.head())
    chargingData.sort_index(inplace=True)
    log = evaluate(val)
    #print(break_after_new_order)
    #print(charging_error_unplugged
    #)
    #print(big_changes)
    #print(total_rows)

    #gives a set with speed and Android date
   
        #print(s.index.date)
        #print(chargingData.head(100))
        #create group by the new order and the cahrging state (each statechange increases an index which will be the group)
    return log
        #chargingData['wrongPercentage'] = chargingData.groupby('chargingStateGroup')['wrongPercentage'].transform(lambda x: x.max())
        #intNumber = chargingData.groupby('chargingStateGroup')
    #chargingData[['percentage']].plot()
#for fileName in glob.glob("/Users/bilickiv/tmpdata/duplicateFree/UGx3clFsMEsyNFRtZWh2bnozdWJ2dTRVS3hkM0pNZWVJaGNiZENjMEdpMD0.csv"):
#for fileName in glob.glob("/Users/bilickiv/tmpdata/duplicateFree/a05TTkJkQThjNWd5SFVYd0pEV1NKUHRsZ3c4TUE2NUdlOVJaVU1XamUrdz0.csv"):

warnings.filterwarnings('ignore')
fileStep = 500
fileStepCount = 0
fileList = []
indexEntries = {}
hashIds = set()
userSpecificPreprocessedFolder = ""
statFolder = ""
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


print("START")

loadConfiguration()
loadchunks()

print("END")
#output.to_csv("/Users/bilickiv/tmpdata/stat.csv")
#print(output)