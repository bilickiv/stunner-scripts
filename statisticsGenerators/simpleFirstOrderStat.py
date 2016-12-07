import sys
import json
import os
import datetime
import time
import glob
import configparser
import pandas as pd
import numpy as np
userSpecificPreprocessedFolder = ""
actualEnvironment = "osx"
userSpecificPreprocessedSubsetFolder = ""
simpleFirstOrderStat  = ""
cumulativeStat15m = {}
cumulativeStat1h = {}
cumulativeStat3h = {}
cumulativeStat1d = {}
def removeFiles():
    global userSpecificPreprocessedFolder
    for fl in glob.glob(simpleFirstOrderStat+"*.csv"):
        os.remove(fl)     
    return;   
def loadConfiguration():
    global userSpecificPreprocessedFolder 
    global userSpecificPreprocessedSubsetFolder
    global simpleFirstOrderStat
    config = configparser.ConfigParser()
    config.read('simpleFirstOrderStat.txt')
    if(actualEnvironment == "osx"):
        userSpecificPreprocessedFolder = config['osx']['userSpecificPreprocessedFolder']
        userSpecificPreprocessedSubsetFolder = config['osx']['userSpecificPreprocessedSubsetFolder']              
        simpleFirstOrderStat = config['osx']['simpleFirstOrderStat']              
                      
    else:
        userSpecificPreprocessedFolder = config['fict']['userSpecificPreprocessedFolder'] 
        userSpecificPreprocessedSubsetFolder = config['fict']['userSpecificPreprocessedSubsetFolder']                               
        simpleFirstOrderStat = config['fict']['simpleFirstOrderStat']                               
    return;
def createStat(name, frequency):
    global cumulativeStat15m
    global cumulativeStat1h
    global cumulativeStat3h
    global cumulativeStat1d
    global simpleFirstOrderStat
    u_cols = ['hashid', 'publicIP', 'localIP', 'timestamp', 'latitude', 'longitude','discoveryResultCode', 'connectionMode','bandwidth','ssid','macAddress','rssi','carrier','simCountryIso','networkType','roaming','timeZone']
    statColumns = ['publicIP', 'localIP', 'latitude', 'longitude','discoveryResultCode', 'connectionMode','bandwidth','ssid','macAddress','rssi','carrier','simCountryIso','networkType','roaming','timeZone']
   #a2gva2htYXFDQ25WUDZVKy9oVXJZWFh0MEJzZk8zcjZaS28vMGo4SmY5TT0.csv
    startTime = datetime.datetime.now()
    fileNameArray = name.split("/")
    fileName = fileNameArray[-1]
    fileArray = fileName.split(".")
    fileWithoutExtension = fileArray[0]
    #file = open(simpleFirstOrderStat+fileWithoutExtension+".csv", "w")
    df = pd.read_csv(name, sep=';', names = u_cols,header = None)
    origLen = (len(df.index))
    df[['timestamp']] = pd.to_datetime(df[['timestamp']].stack()).unstack()
    df.index = df['timestamp']
   #print(df.head())
    #a = df['localIP'].resample('D',lambda x: x.nunique())
    #print(a)
    #lIP = df.groupby(pd.Grouper(key='timestamp', freq=frequency))#.apply(lambda x: x.nunique())
    results = {}
    for i, val in enumerate(statColumns):
        tmpDf = df[['timestamp',val]].groupby(pd.Grouper(key='timestamp', freq=frequency))
        pIP = tmpDf.apply(lambda x: x[val].nunique())
        results[val] = pIP
    e = pd.DataFrame(results)
   # print(len(e.index))
    e = e[(e.localIP != 0)]
    #print(len(e.index))
    print(e.describe(include = 'all'))
   # print('-------------------------')
    #print(e.head())
    #print('-------------------------')
    if(frequency == "15 T"):
        e.to_csv(simpleFirstOrderStat+"/15minute/"+fileWithoutExtension+".csv",sep=";")
        cumulativeStat15m[fileWithoutExtension] = e.describe(include = 'all')

    if(frequency == "H"):
        e.to_csv(simpleFirstOrderStat+"/hour/"+fileWithoutExtension+".csv",sep=";")
        cumulativeStat1h[fileWithoutExtension] = e.describe(include = 'all')

    if(frequency == "3 H"):
        e.to_csv(simpleFirstOrderStat+"/3hour/"+fileWithoutExtension+".csv",sep=";")
        cumulativeStat3h[fileWithoutExtension] = e.describe(include = 'all')

    if(frequency == "D"):
        e.to_csv(simpleFirstOrderStat+"/day/"+fileWithoutExtension+".csv",sep=";")
        cumulativeStat1d[fileWithoutExtension] = e.describe(include = 'all')
        
    #print(pIP)
   # c = gp['localIP'].count()
    #z = gp.apply(lambda x: x.nunique(),axis=3)
    endTime = (datetime.datetime.now() - startTime).total_seconds() 
    print("Imorted " +str(origLen)+" Freq: " + frequency +"  "+ str(len(e.index)) + ":row saved in: " + str(endTime) +"seconds")   
    return;
                



if(str(sys.argv[1]) == "osx"):
    actualEnvironment = "osx"
else:
    actualEnvironment = "linux"   
print("Actul envirnment:" + "----" + actualEnvironment)
#path = "/Volumes/Backup/research/data/*.csv"


print("Loading configfile:" + "----" + str(datetime.datetime.now()))
loadConfiguration()
print("Loading files from:" + userSpecificPreprocessedSubsetFolder + "----" + str(datetime.datetime.now()))
#loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
for fname in glob.glob(userSpecificPreprocessedSubsetFolder+"*.csv"):
    print("Loading file:" + fname + "----" + str(datetime.datetime.now()))
    createStat(fname, "15 T")
    createStat(fname, "H")
    createStat(fname, "3 H")
    createStat(fname, "D")
df15m = pd.DataFrame(cumulativeStat15m)
df1h = pd.DataFrame(cumulativeStat1h)
df3h = pd.DataFrame(cumulativeStat3h)
df1d = pd.DataFrame(cumulativeStat1d)

df15m.to_csv(simpleFirstOrderStat+"/15minute/stat.csv",sep=";")
df1h.to_csv(simpleFirstOrderStat+"/hour/stat.csv",sep=";")
df3h.to_csv(simpleFirstOrderStat+"/3hour/stat.csv",sep=";")
dfd.to_csv(simpleFirstOrderStat+"/day/stat.csv",sep=";")





