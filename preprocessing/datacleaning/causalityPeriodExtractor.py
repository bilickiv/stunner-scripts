import sys
import os
import datetime
import time
import glob
import configparser
import pandas as pd
import numpy as np
import argparse

indexEntries = {}
hashIds = set()
userSpecificPreprocessedFolder = ""
userSpecificPreprocessedCausalityReports = ""
actualEnvironment = "osx"
errorLogCollector = pd.DataFrame(columns=('Type','StartDate', 'EndDate', 'Count', 'Error', 'HashId'))
summaryLogCollector = pd.DataFrame(columns=('F','L', 'CR', 'A','U'))
summarylogRow = []
androidPeriodCount = 0
serverPeriodCount = 0

def createTimeAnalysis(data):
    global errorLogCollector
    global androidPeriodCount
    global serverPeriodCount
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
    fileNumber = pd.to_numeric(fileNumber, errors='coerce')
    #fileNumber = fileNumber.to_numeric()            
    data['globalIndex'] = fileNumber
    df = data[['globalIndex',1,2,7,3]]
    df = df.sort_values(by=['globalIndex',1], ascending=[True,True])
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
        
        #print(str(row['globalIndex'])+":"+str(row[1]))
        if(isinstance( row[2], int )):
            tmpdate = datetime.datetime.fromtimestamp(row[2]/1000).strftime('%Y-%m-%d %H:%M:%S')
        else:
            tmpdate = row[2]

        if(firstUDate == ""):
            firstUDate =  tmpdate           
        if(tmpdate >= endUDate):
            endUDate = tmpdate
        else:
            #print("U:" + firstDate + ":" + tmpdate +":" + str(count))
            errorULog = {"Type":'U',"StartDate": firstUDate, "EndDate" : endUDate,"Count" : countU, "Error": 'Y', "HashID": hashId }
            rowlist.append(errorULog)
            serverPeriodCount = serverPeriodCount + 1
            #print(rowlist)
            firstUDate = tmpdate
            endUDate = tmpdate
            #print(count)
            #print(row)        
            countU = 0    
        if(firstADate == ""):
            firstADate =  row[7]                      
        if(row[7] >= endADate):
            endADate = row[7]            
        else:
            errorALog = {"Type":'A',"StartDate": firstADate, "EndDate" : endADate,"Count" : countA, "Error": 'Y', "HashID": hashId }
            rowlist.append(errorALog)
            androidPeriodCount = androidPeriodCount + 1
 
            #print(rowlist)
            #print(count)
            countA = 0
            firstADate = row[7]
            endADate = row[7]

    if(countA != 0):
        errorALog = {"Type":'A',"StartDate": firstADate, "EndDate" : endADate,"Count" : countA, "Error": 'N', "HashID": hashId }
        #print("Without error:"+str(errorALog))
        androidPeriodCount = androidPeriodCount + 1        
        rowlist.append(errorALog)
        #print(rowlist)
        
    if(countU != 0):
        errorULog = {"Type":'U',"StartDate": firstUDate, "EndDate" : endUDate,"Count" : countU, "Error": 'N', "HashID": hashId }
        #print("Without error:"+str(errorULog))
        rowlist.append(errorULog)
        serverPeriodCount = serverPeriodCount + 1
        
    #print(rowlist)    
    errorLogCollector = pd.DataFrame(rowlist)
    #print(errorLogCollector.head(10))

def removeFiles():
    global userSpecificPreprocessedCausalityReports
    for fl in glob.glob(userSpecificPreprocessedCausalityReports+"*.csv"):
        os.remove(fl)     
    return;             
def loadConfiguration():
    global userSpecificPreprocessedFolder
    global userSpecificPreprocessedCausalityReports      
    global userSpecificFiles
    parser = argparse.ArgumentParser()
    parser.add_argument("opsystem", help="runntime, 1=linux, 2=osx",type=int)
    parser.add_argument("chunk", help="chunk number, 0-12",type=int)                    
    args = parser.parse_args()
    if(args.opsystem == 2):
        actualEnvironment = "osx"
    else:
        actualEnvironment = "linux" 
    config = configparser.ConfigParser()
    config.read('causalityPeriodExtractor.txt')
    if(actualEnvironment == "osx"):
        userSpecificPreprocessedFolder = config['osx']['userSpecificPreprocessedFolder']              
        userSpecificPreprocessedCausalityReports = config['osx']['userSpecificPreprocessedCausalityReports']              
    else:
        userSpecificPreprocessedFolder = config['fict']['userSpecificPreprocessedFolder']            
        userSpecificPreprocessedCausalityReports = config['fict']['userSpecificPreprocessedCausalityReports']            
    return;


#path = "/Volumes/Backup/research/data/*.csv"
loadConfiguration()
#removeFiles()
#loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
for fname in glob.glob(userSpecificPreprocessedFolder+"*.csv"):
    global summarylogRow
#for fname in glob.glob(userSpecificPreprocessedFolder+"*.csv"):
    head, tail = os.path.split(fname)
    filename = tail.split('.')[0] 
    data = pd.read_csv(fname, header=-1, sep=';')
    #print(data.head(10))
    createTimeAnalysis(data)
    #print(errorLogCollector.head(10))  
    summarylogRow.append([filename,len(data.index),len(errorLogCollector.index),androidPeriodCount,serverPeriodCount]) 
    summaryLogCollector = pd.DataFrame(summarylogRow)
    summaryLogCollector.to_csv(userSpecificPreprocessedCausalityReports+"summary.csv", sep='\t', encoding='utf-8')

    print(summaryLogCollector.tail(10))
    print("F: " + filename +  " L:" + str(len(data.index)) + " CR:" + str(len(errorLogCollector.index)) + " A/U:" + str(androidPeriodCount)+"/"+str(serverPeriodCount))
    errorLogCollector.to_csv(userSpecificPreprocessedCausalityReports+filename+".csv", sep='\t', encoding='utf-8')

    #loadFile(fname)
          

              
#loadFile("../402036")      
      