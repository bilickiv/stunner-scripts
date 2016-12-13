
import glob
import configparser
from itertools import islice
import argparse
import pandas as pd



actualEnvironment = "osx"
simpleFirstOrderStat = ""
cumulativeStat15m = pd.DataFrame()
cumulativeStat1h = pd.DataFrame()
cumulativeStat3h = pd.DataFrame()
cumulativeStat1d = pd.DataFrame()
fileList = []
fileStep = 500
fileStepCount = 0


def loadConfiguration():
    global simpleFirstOrderStat
    config = configparser.ConfigParser()
    config.read('firstOrderAggregator.txt')
    if(actualEnvironment == "osx"):
        simpleFirstOrderStat = config['osx']['simpleFirstOrderStat']
    else:
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
        tmpseries = df.max()
        tmpseries.append(fileWithoutExtension)
        cumulativeStat15m.append(tmpseries) 

    if(frequency == "H"):
        cumulativeStat1h[fileWithoutExtension] = df.max()

    if(frequency == "3 H"):
        cumulativeStat3h[fileWithoutExtension] = df.max()

    if(frequency == "D"):
        tmpseries = df.max().to_dict()
        tmpseries['ID']=fileWithoutExtension
        tmpDf = pd.DataFrame.from_dict(tmpseries,orient='columns', index='ID')
        cumulativeStat1d.append(tmpDf, ignore_index=True)
        print(cumulativeStat1d)
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
    df15m = pd.DataFrame(cumulativeStat15m)
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
    df1h = pd.DataFrame(cumulativeStat1h)
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
    df1h = pd.DataFrame(cumulativeStat3h)
    df1h.to_csv(simpleFirstOrderStat + "agg/3hour-stat.csv", sep=";")
    return


def loadday():
    fileList = []
    for fileName in glob.glob(simpleFirstOrderStat + "day/" + "*.csv"):
        fileList.append(fileName)
    print(str(fileStep))
    start = fileStepCount * fileStep
    end = (fileStepCount + 1) * fileStep
    iter = islice(fileList, start, end, None)
    # loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
    for a in iter:
        createStat(a, "D")
        print("D - Loaded file:" + a)
    df1d = pd.DataFrame(cumulativeStat1d)
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
else:
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
