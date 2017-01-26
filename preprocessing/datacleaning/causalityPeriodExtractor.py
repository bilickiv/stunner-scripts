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
def createTimeAnalysis(data):
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
    #print(data.head(10))
    countA = 0
    countU = 0    
    tmpdate = ""
    firstUDate = ""
    firstADate = ""    
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
            errorULog = {"Type":['U'],"StartDate": [firstUDate], "EndDate" : [endUDate],"Count" : [countU], "Error": ['Y'], "HashID": [hashId] }
            tmpDf = pd.DataFrame.from_dict(errorULog)
            errorLogCollector.append(tmpDf)
            print(errorULog)
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
            errorALog = {"Type":['A'],"StartDate": [firstADate], "EndDate" : [endADate],"Count" : [countA], "Error": ['Y'], "HashID": [hashId] }
            tmpDf = pd.DataFrame.from_dict(errorALog)
            errorLogCollector.append(tmpDf)
            print(errorALog)
            #print(count)
            countA = 0
            firstADate = row[7]
            endADate = row[7]

    if(countA != 0):
        errorALog = {"Type":['A'],"StartDate": [firstADate], "EndDate" : [endADate],"Count" : [countA], "Error": ['N'], "HashID": [hashId] }
        print("Without error:"+str(errorALog))
        tmpDf = pd.DataFrame.from_dict(errorALog)
        errorLogCollector.append(tmpDf)
        
    if(countU != 0):
        errorULog = {"Type":['U'],"StartDate": [firstUDate], "EndDate" : [endUDate],"Count" : [countU], "Error": ['N'], "HashID": [hashId] }
        print("Without error:"+str(errorULog))
        tmpDf = pd.DataFrame.from_dict(errorULog)
        errorLogCollector.append(tmpDf)   
    print(errorLogCollector.head(100))    


def removeFiles():
    global userSpecificPreprocessedCausalityReports
    for fl in glob.glob(userSpecificPreprocessedCausalityReports+"*.csv"):
        os.remove(fl)     
    return;             
def loadConfiguration():
    global userSpecificPreprocessedFolder 
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
print("Loading configfile:" + "----" + str(datetime.datetime.now()))
loadConfiguration()
print("Actual envirnment:" + "----" + actualEnvironment)
#removeFiles()
print("Loading files from:" + userSpecificPreprocessedFolder + "----" + str(datetime.datetime.now()))
#loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
for fname in glob.glob(userSpecificPreprocessedFolder+"*.csv"):
    print("Loading file:" + fname + "----" + str(datetime.datetime.now()))
    data = pd.read_csv(fname, header=-1, sep=';')
    createTimeAnalysis(data)
    #loadFile(fname)
          

              
#loadFile("../402036")      
      