
import glob
import configparser
from itertools import islice
import argparse
import pandas as pd



actualEnvironment = "osx"
simpleFirstOrderStat = ""
cumulativeStat15m = []
cumulativeStat1h = []
cumulativeStat3h = []
cumulativeStat1d = []
fileList = []
fileStep = 10000
fileStepCount = 0


def loadConfiguration():
    global simpleFirstOrderStat
    config = configparser.ConfigParser()
    config.read('firstOrderAggregator.txt')
    if(actualEnvironment == "osx"):
        simpleFirstOrderStat = config['osx']['simpleFirstOrderStat']
    if(actualEnvironment == "univ-osx"):
        simpleFirstOrderStat = config['univ-osx']['simpleFirstOrderStat']
    if(actualEnvironment == "linux"):
        simpleFirstOrderStat = config['fict']['simpleFirstOrderStat']
    return


def createStat(name, frequency):
    global cumulativeStat15m
    global cumulativeStat1h
    global cumulativeStat3h
    global cumulativeStat1d
    global simpleFirstOrderStat
    u_cols = ['hashid', 'publicIP', 'localIP', 'timestamp', 'latitude', 'longitude', 'discoveryResultCode', 'connectionMode',
              'bandwidth', 'ssid', 'macAddress', 'rssi', 'carrier', 'simCountryIso', 'networkType', 'roaming', 'timeZone']
    statColumns = ['publicIP', 'localIP', 'latitude', 'longitude', 'discoveryResultCode', 'connectionMode',
                   'bandwidth', 'ssid', 'macAddress', 'rssi', 'carrier', 'simCountryIso', 'networkType', 'roaming', 'timeZone']
   # a2gva2htYXFDQ25WUDZVKy9oVXJZWFh0MEJzZk8zcjZaS28vMGo4SmY5TT0.csv
    fileNameArray = name.split("/")
    fileName = fileNameArray[-1]
    fileArray = fileName.split(".")
    fileWithoutExtension = fileArray[0]
    #file = open(simpleFirstOrderStat+fileWithoutExtension+".csv", "w")
    df = pd.read_csv(name, sep=';', header=0)
    #print(df.describe(include = 'all'))
   # print('-------------------------')
    # print('-------------------------')
    if(frequency == "15 T"):
        tmpseries = df.max().to_dict()
        tmpseries['ID']=fileWithoutExtension
        tmpDf = pd.DataFrame(tmpseries, index=[0])
        #print(tmpDf.head())
        cumulativeStat15m.append(tmpDf)
        #cumulativeStat15m.append(tmpseries) 

    if(frequency == "H"):
        tmpseries = df.max().to_dict()
        tmpseries['ID']=fileWithoutExtension
        tmpDf = pd.DataFrame(tmpseries, index=[0])
        #print(tmpDf.head())
        cumulativeStat1h.append(tmpDf)
        #cumulativeStat1h[fileWithoutExtension] = df.max()

    if(frequency == "3 H"):
        tmpseries = df.max().to_dict()
        tmpseries['ID']=fileWithoutExtension
        tmpDf = pd.DataFrame(tmpseries, index=[0])
        #print(tmpDf.head())
        cumulativeStat3h.append(tmpDf)
        #cumulativeStat3h[fileWithoutExtension] = df.max()

    if(frequency == "D"):
        tmpseries = df.max().to_dict()
        tmpseries['ID']=fileWithoutExtension
        tmpDf = pd.DataFrame(tmpseries, index=[0])
        #print(tmpDf.head())
        cumulativeStat1d.append(tmpDf)
       # print(cumulativeStat1d)
        #cumulativeStat1d[fileWithoutExtension] = df.max()

    # print(pIP)
   # c = gp['localIP'].count()
    #z = gp.apply(lambda x: x.nunique(),axis=3)
    return


def load15min():
    fileList = []
    print("Loading from:" + simpleFirstOrderStat + "15minute/" + "*.csv")
    for fileName in glob.glob(simpleFirstOrderStat + "15minute/" + "*.csv"):
        fileList.append(fileName)
    print(str(fileStep))
    start = fileStepCount * fileStep
    end = (fileStepCount + 1) * fileStep
    iter = islice(fileList, start, end, None)
    # loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
    for a in iter:
        createStat(a, "15 T")
        print("15 T - Loaded file:" + a)
    df15m = pd.concat(cumulativeStat15m)
    df15m.to_csv(simpleFirstOrderStat + "agg/15minute-stat.csv", sep=";")
    return


def load1hour():
    fileList = []
    for fileName in glob.glob(simpleFirstOrderStat + "hour/" + "*.csv"):
        fileList.append(fileName)
    print(str(fileStep))
    start = fileStepCount * fileStep
    end = (fileStepCount + 1) * fileStep
    iter = islice(fileList, start, end, None)
    # loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
    for a in iter:
        createStat(a, "H")
        print("H - Loaded file:" + a)
    df1h = pd.concat(cumulativeStat1h)
    df1h.to_csv(simpleFirstOrderStat + "agg/hour-stat.csv", sep=";")
    return


def load3hour():
    fileList = []
    for fileName in glob.glob(simpleFirstOrderStat + "3hour/" + "*.csv"):
        fileList.append(fileName)
    print(str(fileStep))
    start = fileStepCount * fileStep
    end = (fileStepCount + 1) * fileStep
    iter = islice(fileList, start, end, None)
    # loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
    for a in iter:
        createStat(a, "3 H")
        print("3 H - Loaded file:" + a)
    df1h = pd.concat(cumulativeStat3h)
    df1h.to_csv(simpleFirstOrderStat + "agg/3hour-stat.csv", sep=";")
    return


def loadday():
    global cumulativeStat1d
    fileList = []
    for fileName in glob.glob(simpleFirstOrderStat + "day/" + "*.csv"):
        fileList.append(fileName)
    print(str(fileStep))
    print(simpleFirstOrderStat + "day/" + "*.csv")
    print(len(fileList))
    start = fileStepCount * fileStep
    end = (fileStepCount + 1) * fileStep
    print(start)
    print(end)
    iter = islice(fileList, start, end, None)
    # loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
    for a in iter:
        createStat(a, "D")
        print("D - Loaded file:" + a)
    df1d = pd.concat(cumulativeStat1d)
    print(df1d.head())
    df1d.to_csv(simpleFirstOrderStat + "agg/day-stat.csv", sep=";")
    return
parser = argparse.ArgumentParser()
parser.add_argument("opsystem", help="runntime, 1=linux, 2=osx",
                    type=int)
parser.add_argument("chunk", help="chunk number, 0-12",
                    type=int)
args = parser.parse_args()
if(args.opsystem == 2):
    actualEnvironment = "osx"
if(args.opsystem == 1):
    actualEnvironment = "univ-osx"
if(args.opsystem == 0):
    actualEnvironment = "linux"

print("Actul envirnment:" + "----" + actualEnvironment)
fileStepCount = args.chunk
print("Actul step:" + "----" + str(fileStepCount))
#path = "/Volumes/Backup/research/data/*.csv"


loadConfiguration()
loadday()
load3hour()
load1hour()
load15min()
