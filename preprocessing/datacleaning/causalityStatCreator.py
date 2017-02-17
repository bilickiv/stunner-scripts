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
actualEnvironment = "osx"
userSpecificPreprocessedTimePeriodSummary = ""
userSpecificPreprocessedTimePeriodReports = ""
totalNumberPerDeltaReportCollector = [] #= pd.DataFrame(columns=('Hashid','Filename','Delta', 'TotalEvents','PeriodCount', 'Median length'))
fileStep = 9000
fileStepCount = 0
def startDelta():
    deltas = {'3', '5', '10', '20', '30', '60', '1440'}
    for delta in deltas:
        loadchunks(delta)
    return;
def saveLogs():
    global userSpecificPreprocessedTimePeriodSummary  
    global totalNumberPerDeltaReportCollector
    tmp = pd.DataFrame(totalNumberPerDeltaReportCollector,columns=('Hashid','Filename','Delta', 'TotalEvents','PeriodCount', 'Median length'))    
    print(tmp.head(10))
    tmp = tmp.sort_values(by=[2, 3], ascending=[False, False]) 
    #print(summaryLogCollector.head(10))
    tmp.to_csv(userSpecificPreprocessedTimePeriodSummary+"totalNumberPerDeltaReport-summary.csvv", sep='\t', encoding='utf-8')     
    return;

def loadchunks(delta):
    global summarylogRow
    global userSpecificPreprocessedTimePeriodReports
    print("Loading delata" + str(delta))
    fileList = []
    for fileName in glob.glob(userSpecificPreprocessedTimePeriodReports+str(delta)+"/" + "*.csv"):
        fileList.append(fileName)
    print(str(fileStep))
    start = fileStepCount * fileStep
    end = (fileStepCount + 1) * fileStep
    iter = islice(fileList, start, end, None)
    # loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
    for a in iter:
        mainCycle(a, delta)
        print("Loaded file:" + a)   
    return;

def mainCycle(fname, delta):
    global summarylogRow
    head, tail = os.path.split(fname)
    filename = tail.split('.')[0] 
    data = pd.read_csv(fname, header=0, sep="\t")
    if not data.empty:
        totalNumberPerDeltaReport(filename,data, delta)
    else:
        print("Empty file:" + str(fname))

    #print("Staring file:" + filename)
    #print(data.tail(100))
    #f = filename
    #hashId = data['5HashID'].iloc[0]
    #l = data['3Count'].sum()
    #cr = len(data.index)
    #a = data.shape[0]
    #summarylogRow.append([f,hashId,l,cr,a]) 
    #print("F: " + f +  " Hash:"+ str(hashId) +" L:" + str(l) + " CR:" + str(cr) + " AS:" + str(a))
    return;
def totalNumberPerDeltaReport(filename,data, delta):
    global totalNumberPerDeltaReportCollector
   # print(data.tail(10))
    f = filename
    hashId = data['4HashID'].iloc[0]
    l = data['3Count'].sum()
    cr = len(data.index)
    median = data['3Count'].median()
    #a = data.shape[0]
# 'Hashid','Filename','Delta', 'TotalEvents','PeriodCount', 'Median length'
    totalNumberPerDeltaReportCollector.append([hashId,f,delta,l,cr,median])
   # print(totalNumberPerDeltaReportCollector)
    return;

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False


def loadConfiguration():
    global userSpecificPreprocessedTimePeriodReports
    global userSpecificPreprocessedTimePeriodSummary      
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
    config.read('causalityStatCreator.txt')
    if(actualEnvironment == "osx"):
        userSpecificPreprocessedTimePeriodReports = config['osx']['userSpecificPreprocessedTimePeriodReports']
        userSpecificPreprocessedTimePeriodSummary = config['osx']['userSpecificPreprocessedTimePeriodSummary']
    if(actualEnvironment == "benti"):
        userSpecificPreprocessedTimePeriodReports = config['benti']['userSpecificPreprocessedTimePeriodReports']
        userSpecificPreprocessedTimePeriodSummary = config['benti']['userSpecificPreprocessedTimePeriodSummary']        
    if(actualEnvironment == "linux"):
        userSpecificPreprocessedTimePeriodReports = config['fict']['userSpecificPreprocessedTimePeriodReports']
        userSpecificPreprocessedTimePeriodSummary = config['fict']['userSpecificPreprocessedTimePeriodSummary'] 
    return;


loadConfiguration()
startDelta()

      