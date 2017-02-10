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
duplicateFree = ""
actualEnvironment = "osx"
errorLogCollector = pd.DataFrame(columns=('Type','StartDate', 'EndDate', 'Count', 'Error', 'HashId', 'Delta'))
summaryLogCollector = pd.DataFrame(columns=('F','L', 'CR', 'A','U', 'Max', 'Med'))
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
    for fileName in glob.glob(userSpecificPreprocessedFolder + "*.csv"):
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
    global summarylogRow
    head, tail = os.path.split(fname)
    filename = tail.split('.')[0] 
    data = pd.read_csv(fname, header=-1, sep=';')
    #print(data.head(10))
   # print("Staring file:" + filename)
    removeDuplicates(data, filename)

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
def removeDuplicates(data, filename):
    fileNumber = data[0].str.replace('/home/bilickiv/data/raw_dataset/unzipped_dataset/','')
    fileNumber = fileNumber.str.replace('.csv','')
    fileNumber = fileNumber.str.replace('SQL:/home/bilickiv//data/raw_dataset/old_data/datacollector-','')
    fileNumber = fileNumber.str.replace('.sql.blob','')
    fileNumber = fileNumber.str.replace('2015-01-13','13')
    fileNumber = fileNumber.str.replace('2015-05-21','21')
    fileNumber = pd.to_numeric(fileNumber, errors='coerce')
    data['globalIndex'] = fileNumber
    data['duplicated'] = data.duplicated(subset=[3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24] , keep='first')
    tmp = data.loc[data['duplicated'] == False]
    tmp['fileName'] = filename
    correctUploadDate(tmp)
    #print(tmp.tail(100))
    tmp.to_csv(duplicateFree+filename+".csv", sep=';', encoding='utf-8')
    return        
def correctUploadDate(data):
    #print(data.tail(100))
    for index, row in data.iterrows():
        dTmp = row[2]
        if(RepresentsInt(dTmp)):
            tmpdate = datetime.datetime.fromtimestamp(int(dTmp)/1000).strftime('%Y-%m-%d %H:%M:%S')
        else:
            tmpdate = dTmp
        #data.set_value('uploaDate', tmpdate, index)
        data.loc[index,'uploaDate'] = tmpdate
    return

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

def removeFiles():
    global duplicateFree
    for fl in glob.glob(duplicateFree+"*.csv"):
        os.remove(fl)     
    return;             
def loadConfiguration():
    global userSpecificPreprocessedFolder
    global duplicateFree      
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
    config.read('duplicationCleaner.txt')
    if(actualEnvironment == "osx"):
        userSpecificPreprocessedFolder = config['osx']['userSpecificPreprocessedFolder']              
        duplicateFree = config['osx']['duplicateFree']
    if(actualEnvironment == "benti"):
        userSpecificPreprocessedFolder = config['benti']['userSpecificPreprocessedFolder']              
        duplicateFree = config['benti']['duplicateFree']                        
    if(actualEnvironment == "linux"):
        userSpecificPreprocessedFolder = config['fict']['userSpecificPreprocessedFolder']            
        duplicateFree = config['fict']['duplicateFree']            
    return;


#path = "/Volumes/Backup/research/data/*.csv"
loadConfiguration()
loadchunks()
#removeFiles()
#loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")


    #loadFile(fname)
          

              
#loadFile("../402036")      
      