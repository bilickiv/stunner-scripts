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
def createTimeAnalysis(data):
    uploadDate = "1970-01-01"
    mdate = "1970-01-01"
    data.sort_values(by=[0,1], ascending=[True,True])
    print(data.head(10))
    count = 0
    tmpdate = ""
    for index, row in data.iterrows():
        count=+1
        if(isinstance( row[2], int )):
            tmpdate = datetime.datetime.fromtimestamp(row[2]/1000).strftime('%Y-%m-%d %H:%M:%S')
        else:
            tmpdate = row[2]
        if(tmpdate >= uploadDate):
            uploadDate = tmpdate
        else:
            print("Error actual:" + uploadDate + "next one:" + tmpdate)
            print(count)
            print(row)
            
            count = 0    
            uploadDate = tmpdate
                    
        if(row[7] >= mdate):
                mdate = row[7]
        else:
            print("Error actual:" + mdate + "next one:" + row[7])
            print(row)
            print(count)
            count = 0
            mdate = row[7]

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
      