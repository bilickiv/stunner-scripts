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
from matplotlib import pyplot
from pandas import Series
from pandas import DataFrame
from pandas import TimeGrouper
from matplotlib import pyplot

fileList = []
indexEntries = {}
hashIds = set()
userSpecificPreprocessedFolder = ""
userSpecificPreprocessedCausalityReports = ""
userSpecificPreprocessedTimePeriodReports = ""
actualEnvironment = "osx"
errorLogCollector = pd.DataFrame()
timePeriodCollector = pd.DataFrame()
summaryLogCollector = pd.DataFrame()
summarylogRow = []
androidPeriodCount = 0
serverPeriodCount = 0
fileStep = 500
fileStepCount = 0
originalSize = 0
shrinkedSize = 0
def loadchunks():
    global fileStepCount
    global fileStep
    indexCounter = 0
    fileList = []
    for fileName in glob.glob(userSpecificPreprocessedFolder + "ZzgrSUk3cGRyTThLcU5QanJ5bm4rdndVcUMwMXJvbE1lTkRWUjJDaGxYOD0.csv"):
        fileList.append(fileName)
    print(str(fileStep) + ":" + str(fileStepCount))
    start = fileStepCount * fileStep
    end = (fileStepCount + 1) * fileStep
    iter = islice(fileList, start, end, None)
    # loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
    for a in iter:
        mainCycle(a)
        indexCounter = indexCounter + 1
        print("Loaded file (" + str(indexCounter) + "):" + a)
    return
def mainCycle(fname):
    #for fname in glob.glob(userSpecificPreprocessedFolder+"*.csv"):
    #print(data.head(10))
   # print("Staring file:" + filename)
   # createOverlapAnalysis(data)
    checkChargingRules(fname)
    #startFinePeriodAnalysis(fname)

    #print(errorLogCollector.head(10))  
    #summarylogRow.append([filename,errorLogCollector['HashID'].iloc[0],len(data.index),len(errorLogCollector.index),androidPeriodCount,serverPeriodCount,int(errorLogCollector['Delta'].max()),int(errorLogCollector['Delta'].median())]) 
    #summaryLogCollector = pd.DataFrame(summarylogRow)
    #summaryLogCollector.to_csv(userSpecificPreprocessedCausalityReports+"summary.csv", sep='\t', encoding='utf-8')
   # print(summaryLogCollector.tail(10))
   # print("F: " + filename +  "Hash:"+errorLogCollector['HashID'].iloc[0] +" L:" + str(len(data.index)) + " CR:" + str(len(errorLogCollector.index)) + " A/U:" + str(androidPeriodCount)+"/"+str(serverPeriodCount) + "Max:" + str(int(errorLogCollector['Delta'].max())) + "Med:" + str(int(errorLogCollector['Delta'].median())))
    #errorLogCollector.to_csv(userSpecificPreprocessedCausalityReports+filename+".csv", sep='\t', encoding='utf-8')
def startFinePeriodAnalysis(fname):
    head, tail = os.path.split(fname)
    filename = tail.split('.')[0] 
    data = pd.read_csv(fname, header=0, sep=';')
    deltas = {3, 5, 10, 20, 30, 60, 120,240,1440,2880}
    for delta in deltas:
        initDirectories(userSpecificPreprocessedTimePeriodReports+str(delta))
        createFinePeriodAnalisys(data,delta,filename)
    return;
def initDirectories(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
    return;
def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
def createFinePeriodAnalisys(data, delta, fname):
    global timePeriodCollector
    global userSpecificPreprocessedTimePeriodReports    
    count = 0
    rowlist = []
    previousDate = ""
    startDate = ""    
    df = data[['uploadDate','7','globalIndex','1','3','fileName']]
    #print(df.tail(10))
    # Android time
    df = df.sort_values(by=['7'], ascending=[True])
    first = True  
    for index, row in df.iterrows():
        actualTimestamp =  row['7']
        hashId = row['3']
        if(not pd.isnull(actualTimestamp)):
            if(previousDate == ""):
                previousDate =  actualTimestamp
                startDate = actualTimestamp
            else:
                first = False
                previousDate = previousDate.split(".")[0]
                actualTimestamp = actualTimestamp.split(".")[0]
                startDate = startDate.split(".")[0]
                a = datetime.datetime.strptime(previousDate,'%Y-%m-%d %H:%M:%S')
                b = datetime.datetime.strptime(actualTimestamp,'%Y-%m-%d %H:%M:%S')
                c = datetime.datetime.strptime(startDate,'%Y-%m-%d %H:%M:%S')
                localdelta = int(abs(( a - b ).total_seconds())/60)
                #end of current period
                if(localdelta > delta):
                    periodLength = int(abs(( a - c ).total_seconds())/60)
                    errorALog = {"1StartDate": startDate, "2EndDate" : previousDate,"3Count" : count, "4HashID": hashId, "5PeriodLength": periodLength}
                    rowlist.append(errorALog)
                    previousDate = actualTimestamp
                    startDate = actualTimestamp
                    count = 0
                #it continues
                else:
                    count = count + 1
                    previousDate = actualTimestamp
    if(count != 0):
        previousDate = previousDate.split(".")[0]
        startDate = startDate.split(".")[0]
        a = datetime.datetime.strptime(previousDate,'%Y-%m-%d %H:%M:%S')
        c = datetime.datetime.strptime(startDate,'%Y-%m-%d %H:%M:%S')
        periodLength = int(abs(( a - c ).total_seconds())/60)
        errorALog = {"1StartDate": startDate, "2EndDate" : previousDate,"3Count" : count, "4HashID": hashId, "5PeriodLength": periodLength}
        rowlist.append(errorALog)
    if(first):
        errorALog = {"1StartDate": startDate, "2EndDate" : previousDate,"3Count" : 1, "4HashID": hashId, "5PeriodLength": 0}
        rowlist.append(errorALog)
    timePeriodCollector = pd.DataFrame(rowlist)
    timePeriodCollector.to_csv(userSpecificPreprocessedTimePeriodReports+str(delta)+"/"+fname+".csv", sep='\t', encoding='utf-8')    
    return;        
def checkChargingRules(fname):
    head, tail = os.path.split(fname)
    filename = tail.split('.')[0] 
    data = pd.read_csv(fname, header=0, sep=';')
    #severTimestamp, androidTimestamp, logfileid, logrow, hash, fileHash, pluggedState, voltage, temperature, percentage, health, chargingState 
    df = data[['uploadDate','7','globalIndex','1','3','fileName','15','16','17','18','19','20']]
    df = df.sort_values(by=['7'], ascending=[True])    
    print(df.tail())
    voltage = data[['7','18','20','15','17','16']]
    voltage['7'] = pd.to_datetime(voltage['7'])
    voltage.index = voltage['7']
    del voltage['7']
    print(voltage.head(100))
    #voltage.plot(x='7', y='16',style='k.')
    voltage['2015-11-05':'2015-11-07'].plot()
    pyplot.show()
    groups = voltage.groupby(pd.TimeGrouper(freq='D'))
    days = pd.DataFrame()
    for name, group in groups:
        print(name.day)
        print(group.values)
        days[name.day] = group.values
    month.plot(subplots=True, legend=False)
    pyplot.show()
    for index, row in df.iterrows():
        voltageLevel = row['16']
        temperature = row['17']
        percentage = row['18']
        health = row['19']
        chargingState = row['20']
    print(df.tail(10))    
    return;
def createTimeAnalysis(data):
    global errorLogCollector
    global androidPeriodCount
    global serverPeriodCount
    global originalSize
    global shrinkedSize
    androidPeriodCount = 0
    serverPeriodCount = 0
    endUDate = "1970-01-01"
    endADate = "1970-01-01"
    errorLog = {}
    fileNumber = data[0].str.replace('/home/bilickiv/data/raw_dataset/unzipped_dataset/','')
    fileNumber = fileNumber.str.replace('.csv','')
    fileNumber = fileNumber.str.replace('SQL:/home/bilickiv//data/raw_dataset/old_data/datacollector-','')
    fileNumber = fileNumber.str.replace('.sql.blob','')
    fileNumber = fileNumber.str.replace('2015-01-13','13')
    fileNumber = fileNumber.str.replace('2015-05-21','21')
    fileNumber = pd.to_numeric(fileNumber, errors='coerce')
    #fileNumber = fileNumber.to_numeric()            
    data['duplicated'] = data.duplicated(subset=[3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24] , keep='first')
    print(data.tail(100))
    data['globalIndex'] = fileNumber
    tmp = data.loc[data['duplicated'] == False]
    shrinkedSize = len(tmp.index)
    originalSize = len(data.index)
    df = tmp[['globalIndex',1,2,7,3]]
    # server date, file name, row
    df = df.sort_values(by=[2, 'globalIndex', 1], ascending=[True, True, True])    
    #df = df.sort_values(by=['globalIndex',1], ascending=[True,True])
   #print(df.head(1000))
    countA = 0
    countU = 0    
    tmpdate = ""
    firstUDate = ""
    firstADate = ""
    rowlist = []    
    for index, row in df.iterrows():
        hashId = row[3]
        firstUDate = ""
        firstADate = ""
        countU = countU + 1
        countA = countA + 1
        #print(row)
        #print(str(row['globalIndex'])+":"+str(row[1]))
        dTmp = row[2]
        if(RepresentsInt(dTmp)):
            tmpdate = datetime.datetime.fromtimestamp(int(dTmp)/1000).strftime('%Y-%m-%d %H:%M:%S')
        else:
            tmpdate = dTmp

        if(firstUDate == ""):
            firstUDate =  tmpdate           
        if(tmpdate >= endUDate):
            endUDate = tmpdate
        else:
            endUDate = endUDate.split(".")[0]
            tmpdate = tmpdate.split(".")[0]
            a = datetime.datetime.strptime(endUDate,'%Y-%m-%d %H:%M:%S')
            b = datetime.datetime.strptime(tmpdate,'%Y-%m-%d %H:%M:%S')
            delta = abs(( a - b ).seconds)/60
            #print(str(delta))
            #print("U:" + firstDate + ":" + tmpdate +":" + str(count))
            errorULog = {"Type":'U',"StartDate": firstUDate, "EndDate" : endUDate,"Count" : countU, "Error": 'Y', "HashID": hashId , "Delta" : delta, "Original" : originalSize, "Shrinked" : shrinkedSize}
            rowlist.append(errorULog)
            serverPeriodCount = serverPeriodCount + 1
            #print(rowlist)
            firstUDate = tmpdate
            endUDate = tmpdate
            #print(count)
            #print(row)        
            countU = 0
        tmpRow7 =  row[7]
        if(not pd.isnull(tmpRow7)):
            if(firstADate == ""):
                firstADate =  row[7]                      
            if(row[7] >= endADate):
                endADate = row[7]            
            else:
                thisDate = row[7]
                #print(endADate)
                #print(thisDate)
                endADate = endADate.split(".")[0]
                thisDate = thisDate.split(".")[0]
                a = datetime.datetime.strptime(endADate,'%Y-%m-%d %H:%M:%S')
                b = datetime.datetime.strptime(thisDate,'%Y-%m-%d %H:%M:%S')
                delta = abs(( a - b ).seconds)/60
                #print(str(delta))
                errorALog = {"Type":'A',"StartDate": firstADate, "EndDate" : endADate,"Count" : countA, "Error": 'Y', "HashID": hashId, "Delta" : delta, "Original" : originalSize, "Shrinked" : shrinkedSize }
                rowlist.append(errorALog)
                androidPeriodCount = androidPeriodCount + 1
    
                #print(rowlist)
                #print(count)
                countA = 0
                firstADate = row[7]
                endADate = row[7]

    if(countA != 0):
        errorALog = {"Type":'A',"StartDate": firstADate, "EndDate" : endADate,"Count" : countA, "Error": 'N', "HashID": hashId, "Delta" : 0, "Original" : originalSize, "Shrinked" : shrinkedSize }
        #print("Without error:"+str(errorALog))
        androidPeriodCount = androidPeriodCount + 1        
        rowlist.append(errorALog)
        #print(rowlist)
        
    if(countU != 0):
        errorULog = {"Type":'U',"StartDate": firstUDate, "EndDate" : endUDate,"Count" : countU, "Error": 'N', "HashID": hashId, "Delta" : 0, "Original" : originalSize, "Shrinked" : shrinkedSize }
        #print("Without error:"+str(errorULog))
        rowlist.append(errorULog)
        serverPeriodCount = serverPeriodCount + 1
        
    #print(rowlist)    
    errorLogCollector = pd.DataFrame(rowlist)
    #print(errorLogCollector.head(10))
    return;
def createOverlapAnalysis(data):
    global errorLogCollector
    global androidPeriodCount
    androidPeriodCount = 0
    endADate = "1970-01-01"
    errorLog = {}
    #print(data.tail(10))
    #severTimestamp, androidTimestamp, logfileid, logrow, hash, fileHash
    df = data[['uploadDate','7','globalIndex','1','3','fileName']]
    # server date, android timestamp
    df = df.sort_values(by=['uploadDate', '7'], ascending=[True, True])    
    countA = 0
    tmpdate = ""
    firstADate = ""
    rowlist = []    
    for index, row in df.iterrows():
        hashId = row['3']
        countA = countA + 1
        tmpAndroid =  row['7']
        if(not pd.isnull(tmpAndroid)):
            if(firstADate == ""):
                firstADate =  row['7']                      
            if(row['7'] >= endADate):
                endADate = row['7']            
            else:
                thisDate = row['7']
                endADate = endADate.split(".")[0]
                thisDate = thisDate.split(".")[0]
                a = datetime.datetime.strptime(endADate,'%Y-%m-%d %H:%M:%S')
                b = datetime.datetime.strptime(thisDate,'%Y-%m-%d %H:%M:%S')
                delta = int(abs(( a - b ).seconds)/60)
                errorALog = {"1StartDate": firstADate, "2EndDate" : endADate,"3Count" : countA, "4Error": 'Y', "5HashID": hashId}
                rowlist.append(errorALog)
                androidPeriodCount = androidPeriodCount + 1
                countA = 0
                firstADate = row['7']
                endADate = row['7']
    if(countA != 0):
        errorALog = {"1StartDate": firstADate, "2EndDate" : endADate,"3Count" : countA, "4Error": 'N', "5HashID": hashId}
        androidPeriodCount = androidPeriodCount + 1        
        rowlist.append(errorALog)
    errorLogCollector = pd.DataFrame(rowlist)
    return;    
def loadConfiguration():
    global userSpecificPreprocessedFolder
    global userSpecificPreprocessedCausalityReports 
    global userSpecificPreprocessedTimePeriodReports     
    global userSpecificFiles
    global fileStepCount

    parser = argparse.ArgumentParser()
    parser.add_argument("opsystem", help="runntime, 0=benti, 1=linux, 2=osx",type=int)
    parser.add_argument("chunk", help="chunk number, 0-12",type=int)                    
    args = parser.parse_args()
    fileStepCount = args.chunk
    #print("Actul step:" + "----" + str(fileStepCount))
    if(args.opsystem == 2):
        actualEnvironment = "osx"
    if(args.opsystem == 0):
        actualEnvironment = "benti"
    if(args.opsystem == 1):
        actualEnvironment = "linux" 
    config = configparser.ConfigParser()
    config.read('causalityPeriodExtractor.txt')
    if(actualEnvironment == "osx"):
        userSpecificPreprocessedFolder = config['osx']['userSpecificPreprocessedFolder']              
        userSpecificPreprocessedCausalityReports = config['osx']['userSpecificPreprocessedCausalityReports']
        userSpecificPreprocessedTimePeriodReports = config['osx']['userSpecificPreprocessedTimePeriodReports']
    if(actualEnvironment == "benti"):
        userSpecificPreprocessedFolder = config['benti']['userSpecificPreprocessedFolder']              
        userSpecificPreprocessedCausalityReports = config['benti']['userSpecificPreprocessedCausalityReports']
        userSpecificPreprocessedTimePeriodReports = config['benti']['userSpecificPreprocessedTimePeriodReports']
                        
    if(actualEnvironment == "linux"):
        userSpecificPreprocessedFolder = config['fict']['userSpecificPreprocessedFolder']            
        userSpecificPreprocessedCausalityReports = config['fict']['userSpecificPreprocessedCausalityReports']
        userSpecificPreprocessedTimePeriodReports = config['fict']['userSpecificPreprocessedTimePeriodReports']            
    return;


#path = "/Volumes/Backup/research/data/*.csv"
loadConfiguration()
loadchunks()
#removeFiles()
#loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")


    #loadFile(fname)
          

              
#loadFile("../402036")      
      