import sys
import json
import os
import datetime
import time
import glob
import configparser
import pandas as pd
import numpy as np
from itertools import islice
import argparse


actualEnvironment = "osx"
simpleFirstOrderStat  = ""
cumulativeStat15m = {}
cumulativeStat1h = {}
cumulativeStat3h = {}
cumulativeStat1d = {}
def loadConfiguration():
    global simpleFirstOrderStat
    config = configparser.ConfigParser()
    config.read('simpleFirstOrderStat.txt')
    if(actualEnvironment == "osx"):
        simpleFirstOrderStat = config['osx']['simpleFirstOrderStat']              
                      
    else:
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
    fileNameArray = name.split("/")
    fileName = fileNameArray[-1]
    fileArray = fileName.split(".")
    fileWithoutExtension = fileArray[0]
    #file = open(simpleFirstOrderStat+fileWithoutExtension+".csv", "w")
    df = pd.read_csv(name, sep=';', names = u_cols,header = None)
    #print(len(e.index))
    #print(df.describe(include = 'all'))
   # print('-------------------------')
    #print(e.head())
    #print('-------------------------')
    if(frequency == "15 T"):
        cumulativeStat15m[fileWithoutExtension] = df.describe(include = 'all')

    if(frequency == "H"):
        cumulativeStat1h[fileWithoutExtension] = df.describe(include = 'all')

    if(frequency == "3 H"):
        cumulativeStat3h[fileWithoutExtension] = df.describe(include = 'all')

    if(frequency == "D"):
        cumulativeStat1d[fileWithoutExtension] = df.describe(include = 'all')
        
    #print(pIP)
   # c = gp['localIP'].count()
    #z = gp.apply(lambda x: x.nunique(),axis=3)
    return;
                

parser = argparse.ArgumentParser()
parser.add_argument("opsystem", help="runntime, 1=linux, 2=osx",
                    type=int)
parser.add_argument("chunk", help="chunk number, 0-12",
                    type=int)                    
args = parser.parse_args()
if(args.opsystem == 2):
    actualEnvironment = "osx"
else:
    actualEnvironment = "linux"
print("Actul envirnment:" + "----" + actualEnvironment)
fileStepCount =  args.chunk  
print("Actul step:" + "----" + str(fileStepCount))
#path = "/Volumes/Backup/research/data/*.csv"


loadConfiguration()
#loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
for fileName in glob.glob(simpleFirstOrderStat+"/15minute/"+"*.csv"):
    createStat(fileName, "15 T")
    print("15 T - Loaded file:" + fileName)
for fileName in glob.glob(simpleFirstOrderStat+"/hour/"+"*.csv"):
    createStat(fileName, "H")
    print("H - Loaded file:" + fileName)
for fileName in glob.glob(simpleFirstOrderStat+"/3hour/"+"*.csv"):
    createStat(fileName, "3 H")
    print("3H - Loaded file:" + fileName)
for fileName in glob.glob(simpleFirstOrderStat+"/day/"+"*.csv"):
    createStat(fileName, "D")     
    print("D - Loaded file:" + fileName)         

df15m = pd.DataFrame(cumulativeStat15m)
df1h = pd.DataFrame(cumulativeStat1h)
df3h = pd.DataFrame(cumulativeStat3h)
df1d = pd.DataFrame(cumulativeStat1d)

df15m.to_csv(simpleFirstOrderStat+"/15minute/stat.csv",sep=";")
df1h.to_csv(simpleFirstOrderStat+"/hour/stat.csv",sep=";")
df3h.to_csv(simpleFirstOrderStat+"/3hour/stat.csv",sep=";")
dfd.to_csv(simpleFirstOrderStat+"/day/stat.csv",sep=";")





