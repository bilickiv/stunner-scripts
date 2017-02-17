
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

fileList = []
indexEntries = {}
hashIds = set()
userSpecificPreprocessedCausalityReports = ""
actualEnvironment = "osx"
errorLogCollector = pd.DataFrame(columns=('Type','StartDate', 'EndDate', 'Count', 'Error', 'HashId', 'Delta'))
summaryLogCollector = pd.DataFrame(columns=('F','L', 'CR', 'A','U', 'Max', 'Med'))
summarylogRow = []
androidPeriodCount = 0
serverPeriodCount = 0
fileStep = 9000
fileStepCount = 0

def loadchunks():
    global timeperiodsummary
    fileList = []
    for fileName in glob.glob(timeperiodsummary + "*.csvv"):
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
    global timeperiodsummary
    head, tail = os.path.split(fname)
    filename = tail.split('.')[0] 
    data = pd.read_csv(fname, header=0, sep="\t")
    print(data.tail(10))
    deltas = {3, 5, 10, 20, 30, 60, 120,240,1440,2880}
    for delta in deltas:
        ax = data[(data.Delta == int(delta))].filter(items=['PeriodCount']).plot(kind='hist', alpha=0.5,  bins=[10, 20, 30, 40, 50, 100,1000,2000,4000,10000,100000],title=str(delta)+' minute long follow time', logy = False, logx = True)
        fig = ax.get_figure()
        fig.savefig(timeperiodsummary + str(delta)+' minute.png')                     
    ax4 = data[(data.Delta == 1440)].filter(items=['PeriodCount']).plot(kind='hist', alpha=0.5,  bins=[1,2,3,4,5,6,7,8,9,10, 20, 30, 40, 50, 100,1000],title='One day long  follow time', logy = False, logx = True)
    fig4 = ax4.get_figure()
    fig4.savefig(timeperiodsummary + 'one day.png')
    #errorLogCollector.to_csv(userSpecificPreprocessedCausalityReports+filename+".csv", sep='\t', encoding='utf-8')

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False


def loadConfiguration():
    global timeperiodsummary      
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
    config.read('causalityStatAnalysis.txt')
    if(actualEnvironment == "osx"):
        timeperiodsummary = config['osx']['timeperiodsummary']
    if(actualEnvironment == "benti"):
        timeperiodsummary = config['benti']['timeperiodsummary']                        
    if(actualEnvironment == "linux"):
        timeperiodsummary = config['fict']['timeperiodsummary']            
    return;


#path = "/Volumes/Backup/research/data/*.csv"
loadConfiguration()
loadchunks()
#removeFiles()
#loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")


    #loadFile(fname)
          

              
#loadFile("../402036")      
      