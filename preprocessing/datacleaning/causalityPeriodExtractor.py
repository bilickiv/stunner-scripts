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

fileList = []
indexEntries = {}
hashIds = set()
userSpecificPreprocessedFolder = ""
userSpecificPreprocessedCausalityReports = ""
actualEnvironment = "osx"
errorLogCollector = pd.DataFrame(columns=('Type','StartDate', 'EndDate', 'Count', 'Error', 'HashId', 'Delta'))
summaryLogCollector = pd.DataFrame(columns=('F','L', 'CR', 'A','U', 'Max', 'Med'))
summarylogRow = []
androidPeriodCount = 0
serverPeriodCount = 0
fileStep = 500
fileStepCount = 0

def loadchunks():
    fileList = []
    for fileName in glob.glob(userSpecificPreprocessedFolder + "*.csv"):
        fileList.append(fileName)
    print(str(fileStep))
    start = fileStepCount * fileStep
    end = (fileStepCount + 1) * fileStep
    iter = islice(fileList, start, end, None)
    # loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
    for a in iter:
        mainCycle(a)
        print("Loaded file:" + a)
    return
def mainCycle(fname):
    #for fname in glob.glob(userSpecificPreprocessedFolder+"*.csv"):
    global summarylogRow
#for fname in glob.glob(userSpecificPreprocessedFolder+"*.csv"):
    head, tail = os.path.split(fname)
    filename = tail.split('.')[0] 
    data = pd.read_csv(fname, header=-1, sep=';')
    #print(data.head(10))
    print("Staring file:" + filename)
    createTimeAnalysis(data)
    #print(errorLogCollector.head(10))  
    #summarylogRow.append([filename,errorLogCollector['HashID'].iloc[0],len(data.index),len(errorLogCollector.index),androidPeriodCount,serverPeriodCount,int(errorLogCollector['Delta'].max()),int(errorLogCollector['Delta'].median())]) 
    #summaryLogCollector = pd.DataFrame(summarylogRow)
    #summaryLogCollector.to_csv(userSpecificPreprocessedCausalityReports+"summary.csv", sep='\t', encoding='utf-8')
   # print(summaryLogCollector.tail(10))
    print("F: " + filename +  "Hash:"+errorLogCollector['HashID'].iloc[0] +" L:" + str(len(data.index)) + " CR:" + str(len(errorLogCollector.index)) + " A/U:" + str(androidPeriodCount)+"/"+str(serverPeriodCount) + "Max:" + str(int(errorLogCollector['Delta'].max())) + "Med:" + str(int(errorLogCollector['Delta'].median())))
    errorLogCollector.to_csv(userSpecificPreprocessedCausalityReports+filename+".csv", sep='\t', encoding='utf-8')

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
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
        print(row)
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
            errorULog = {"Type":'U',"StartDate": firstUDate, "EndDate" : endUDate,"Count" : countU, "Error": 'Y', "HashID": hashId , "Delta" : delta}
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
                errorALog = {"Type":'A',"StartDate": firstADate, "EndDate" : endADate,"Count" : countA, "Error": 'Y', "HashID": hashId, "Delta" : delta }
                rowlist.append(errorALog)
                androidPeriodCount = androidPeriodCount + 1
    
                #print(rowlist)
                #print(count)
                countA = 0
                firstADate = row[7]
                endADate = row[7]

    if(countA != 0):
        errorALog = {"Type":'A',"StartDate": firstADate, "EndDate" : endADate,"Count" : countA, "Error": 'N', "HashID": hashId, "Delta" : 0 }
        #print("Without error:"+str(errorALog))
        androidPeriodCount = androidPeriodCount + 1        
        rowlist.append(errorALog)
        #print(rowlist)
        
    if(countU != 0):
        errorULog = {"Type":'U',"StartDate": firstUDate, "EndDate" : endUDate,"Count" : countU, "Error": 'N', "HashID": hashId, "Delta" : 0 }
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
    parser.add_argument("opsystem", help="runntime, 0=benti, 1=linux, 2=osx",type=int)
    parser.add_argument("chunk", help="chunk number, 0-12",type=int)                    
    args = parser.parse_args()
    fileStepCount = args.chunk
    print("Actul step:" + "----" + str(fileStepCount))
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
    if(actualEnvironment == "benti"):
        userSpecificPreprocessedFolder = config['benti']['userSpecificPreprocessedFolder']              
        userSpecificPreprocessedCausalityReports = config['benti']['userSpecificPreprocessedCausalityReports']                        
    if(actualEnvironment == "linux"):
        userSpecificPreprocessedFolder = config['fict']['userSpecificPreprocessedFolder']            
        userSpecificPreprocessedCausalityReports = config['fict']['userSpecificPreprocessedCausalityReports']            
    return;


#path = "/Volumes/Backup/research/data/*.csv"
loadConfiguration()
loadchunks()
#removeFiles()
#loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")


    #loadFile(fname)
          

              
#loadFile("../402036")      
      