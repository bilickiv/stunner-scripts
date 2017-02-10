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
    global summarylogRow
    fileList = []
    for fileName in glob.glob(userSpecificPreprocessedCausalityReports + "*.csv"):
        fileList.append(fileName)
    print(str(fileStep))
    start = fileStepCount * fileStep
    end = (fileStepCount + 1) * fileStep
    iter = islice(fileList, start, end, None)
    # loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
    for a in iter:
        mainCycle(a)
        print("Loaded file:" + a)
    summaryLogCollector = pd.DataFrame(summarylogRow)
    #print(summaryLogCollector.head(10))
    summaryLogCollector = summaryLogCollector.sort_values(by=[2, 3], ascending=[False, False]) 
    #print(summaryLogCollector.head(10))
 
    summaryLogCollector.to_csv(userSpecificPreprocessedCausalityReports+"summary.csv", sep='\t', encoding='utf-8')        
    return
def mainCycle(fname):
    global summarylogRow
    head, tail = os.path.split(fname)
    filename = tail.split('.')[0] 
    data = pd.read_csv(fname, header=0, sep="\t")
    print("Staring file:" + filename)
    #print(data.tail(100))
    f = filename
    hashId = data['HashID'].iloc[0]
    l = data['Count'].sum()
    cr = len(data.index)
    a = data[data.Type == "A"].shape[0]
    med = int(data['Delta'].median())
    m = int(data['Delta'].max())
    summarylogRow.append([f,hashId,l,cr,a,m,med]) 
    print("F: " + f +  " Hash:"+ str(hashId) +" L:" + str(l) + " CR:" + str(cr) + " AS:" + str(a) + " Max:" + str(int(m)) + " Med:" + str(med))

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False


def loadConfiguration():
    global userSpecificPreprocessedCausalityReports      
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
        userSpecificPreprocessedCausalityReports = config['osx']['userSpecificPreprocessedCausalityReports']
    if(actualEnvironment == "benti"):
        userSpecificPreprocessedCausalityReports = config['benti']['userSpecificPreprocessedCausalityReports']                        
    if(actualEnvironment == "linux"):
        userSpecificPreprocessedCausalityReports = config['fict']['userSpecificPreprocessedCausalityReports']            
    return;


loadConfiguration()
loadchunks()

      