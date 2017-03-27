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
warnings.filterwarnings('ignore')
fileStep = 500
fileStepCount = 0
fileList = []
indexEntries = {}
hashIds = set()
userSpecificPreprocessedFolder = ""
duplicateFreetimestamp = ""
def loadConfiguration():
    global userSpecificPreprocessedFolder
    global duplicateFree   
    global duplicateFreetimestamp   
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
    config.read('converter.txt')
    if(actualEnvironment == "osx"):
        duplicateFreetimestamp = config['osx']['duplicateFree-timestamp']              
        duplicateFree = config['osx']['duplicateFree']
    if(actualEnvironment == "benti"):
        duplicateFreetimestamp = config['benti']['duplicateFree-timestamp']              
        duplicateFree = config['benti']['duplicateFree']                        
    if(actualEnvironment == "linux"):
        duplicateFreetimestamp = config['fict']['duplicateFree-timestamp']            
        duplicateFree = config['fict']['duplicateFree']            
    return;
def timeToTimeStamp(val):
    try:
        x = float(val)
        if math.isnan(x):
            return val
    except ValueError:
        val = pd.to_datetime(val)
        val = time.mktime(val.timetuple())
    else:
        return val
    return val
def loadchunks():
    global duplicateFree
    global fileStepCount
    global fileStep
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
        mainCycle(a)
        indexCounter = indexCounter + 1
        print("Loaded file (" + str(indexCounter) + "):" + a)
    return
def mainCycle(fname):
    #for fname in glob.glob(userSpecificPreprocessedFolder+"*.csv"):
    global summarylogRow
    global duplicateFreetimestamp
    head, tail = os.path.split(fname)
    filename = tail.split('.')[0] 
    data = pd.read_csv(fname, header=0, sep=';')
    print("processing:" + fname)
    data['0'] =  data['0'].apply(timeToTimeStamp)
    data['1'] =  data['1'].apply(timeToTimeStamp)
    data['4'] =  data['4'].apply(timeToTimeStamp)
    data['9'] =  data['9'].apply(timeToTimeStamp)
    data['uploadDate'] =  data['uploadDate'].apply(timeToTimeStamp)
    head, tail = os.path.split(fname)
    data = data.sort_values(by=['globalIndex', '3'], ascending=[True,True])
    data.to_csv(duplicateFreetimestamp+str(tail), sep=';', encoding='utf-8')
    return
print("START")
loadConfiguration()
loadchunks()




