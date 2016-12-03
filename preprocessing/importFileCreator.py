import sys
import json
import os
import datetime
import time
import glob
import configparser

indexEntries = {}
hashIds = set()
userSpecificPreprocessedFolder = ""
userSpecificPreprocessedSubsetFolder = ""
userSpecificFiles = ""
userSpecificFilesWindows = ""
actualEnvironment = "osx"

def removeFiles():
    global userSpecificPreprocessedFolder
    for fl in glob.glob(userSpecificPreprocessedFolder+"*.csv"):
        os.remove(fl)     
    return; 
    for fl in glob.glob(userSpecificPreprocessedSubsetFolder+"*.csv"):
        os.remove(fl)     
    return;             
def loadConfiguration():
    global userSpecificPreprocessedFolder 
    global userSpecificFiles
    global userSpecificFilesWindows
    global userSpecificPreprocessedSubsetFolder
    config = configparser.ConfigParser()
    config.read('importFileCreatorConfig.txt')
    if(actualEnvironment == "osx"):
        userSpecificPreprocessedFolder = config['osx']['userSpecificPreprocessedFolder']              
        userSpecificPreprocessedSubsetFolder = config['osx']['userSpecificPreprocessedSubsetFolder']              
        userSpecificFiles = config['osx']['userSpecificFiles']
        userSpecificFilesWindows = config['osx']['userSpecificFilesWindows']
    else:
        userSpecificPreprocessedFolder = config['fict']['userSpecificPreprocessedFolder']            
        userSpecificPreprocessedSubsetFolder = config['fict']['userSpecificPreprocessedSubsetFolder']            
        userSpecificFiles = config['fict']['userSpecificFiles']
        userSpecificFilesWindows = config['fict']['userSpecificFilesWindows']
    return;
def getJsonData(jsonSource, jsonKey):
    try:
        resultString = jsonSource[jsonKey]
        resultString = str(resultString).encode('utf-8') 
    except (KeyError,TypeError) as e: 
        #print 'Decoding JSON has failed publicIP' + str(counter)
        resultString = 'N/A'
    return removeByte(str(resultString))
def getDeepJsonData(jsonSource, jsonTree,jsonKey):
    try:
        resultString = jsonSource[jsonTree][jsonKey]
        resultString = str(resultString).encode('utf-8') 
    except (KeyError,TypeError) as e: 
        #print 'Decoding JSON has failed publicIP' + str(counter)
        resultString = 'N/A'
    return removeByte(str(resultString))
def parseWindowsPhoneLog(unifiedLine):
    csvData = unifiedLine.split(';')
   # print unifiedLine    
    importString = csvData[0] #UploadTimestam
   # print importString
    tmp = datetime.datetime.fromtimestamp(float(importString+'.0')/1000).strftime('%Y-%m-%d %H:%M:%S')
    importString = str(tmp) + ';' + csvData[1] #Device hash
    importString = importString + ';' + csvData[2] #SW napespace
    tmp = csvData[3]
    tmp = tmp.encode('utf-8')       
    try:
            data = json.loads(tmp)
            importString = importString + ';' + getJsonData(data,'publicIP')
            importString = importString + ';' + getJsonData(data,'localIP')
            tmp1 = getJsonData(data,'date')
            tmp = datetime.datetime.fromtimestamp(float(tmp1+'.0')/1000).strftime('%Y-%m-%d %H:%M:%S')
            importString = importString + ';' + tmp
            importString = importString + ';' + getJsonData(data,'latitude')
            importString = importString + ';' + getJsonData(data,'longitude')
            importString = importString + ';' + getJsonData(data,'androidVersion')
            importString = importString + ';' + getJsonData(data,'resultCode')
            importString = importString + ';' + getJsonData(data,'discoveryId')
            importString = importString + ';' + getDeepJsonData(data,'batteryInfo','technology')
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','present')
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','pluggedState')            
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','voltage')
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','temperature')
            importString = importString + ';' + getDeepJsonData(data,'batteryInfo','batteryLevel')
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','health')
            importString = importString + ';' + getDeepJsonData(data,'batteryInfo','charging')
            importString = importString + ';' + getDeepJsonData(data,'wifiInfo','bandwidth')
            importString = importString + ';' + getDeepJsonData(data,'wifiInfo','ssid')
            importString = importString + ';' + getDeepJsonData(data,'wifiInfo','macAddress')
            importString = importString + ';' + getDeepJsonData(data,'wifiInfo','rssi')
            importString = importString + ';' + getDeepJsonData(data,'mobileNetInfo','carrier')
            importString = importString + ';' + getDeepJsonData(data,'mobileNetInfo','netType') 
            importString = importString + ';' + getDeepJsonData(data,'mobileNetInfo','roaming')
            importString = importString + ';' + getDeepJsonData(data,'uptimeInfoDTO','shutDownTimestamp')
            importString = importString + ';' + getDeepJsonData(data,'uptimeInfoDTO','turnOnTimestamp') 
            importString = importString + ';' + getDeepJsonData(data,'uptimeInfoDTO','uptime')                  
            importString = importString + ';' + getJsonData(data,'triggerCode')
            importString = importString + ';' + getJsonData(data,'appVersion')
            importString = importString + ';' + getJsonData(data,'timeZone')                                                                                                                                                                                                                                           
    except (KeyError,TypeError,ValueError) as e: 
            print('ERROR: ' + str(e)) 
            print (tmp )  
    return importString 
def parseAndroidLog(unifiedLine):
        csvData = unifiedLine.split(';')
        sourceFile = csvData[0] #Source file
        source_row = csvData[1] #source row       
        upload_date = csvData[2] #uploaad date
        deviceHash = replaceProblematicChars(csvData[3]) #device hash
        platform = csvData[4] #platform

        #tmp = datetime.datetime.fromtimestamp(float(importString+'.0')/1000).strftime('%Y-%m-%d %H:%M:%S')
        importString = sourceFile + ';'  + str(source_row) +';'+ upload_date + ';' + deviceHash + ';' + platform  #Device hash
        tmp = replaceProblematicChars(csvData[5])
        #tmp = tmp.encode('utf-8')       
        try:
            data = json.loads(tmp)
            importString = importString + ';' + getJsonData(data,'publicIP')
            importString = importString + ';' + getJsonData(data,'localIP')
            tmp1 = getJsonData(data,'timeStamp')
            tmp = datetime.datetime.fromtimestamp(float(tmp1+'.0')/1000).strftime('%Y-%m-%d %H:%M:%S')
            importString = importString + ';' + tmp
            importString = importString + ';' + getJsonData(data,'latitude')
            importString = importString + ';' + getJsonData(data,'longitude')
            importString = importString + ';' + getJsonData(data,'androidVersion')
            importString = importString + ';' + getJsonData(data,'discoveryResultCode')
            importString = importString + ';' + getJsonData(data,'connectionMode')
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','technology')
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','present')
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','pluggedState')            
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','voltage')
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','temperature')
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','percentage')
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','health')
            importString = importString + ';' + getDeepJsonData(data,'batteryDTO','chargingState')
            importString = importString + ';' + getDeepJsonData(data,'wifiDTO','bandwidth')
            importString = importString + ';' + getDeepJsonData(data,'wifiDTO','ssid')
            importString = importString + ';' + getDeepJsonData(data,'wifiDTO','macAddress')
            importString = importString + ';' + getDeepJsonData(data,'wifiDTO','rssi')
            importString = importString + ';' + getDeepJsonData(data,'mobileDTO','carrier')
            importString = importString + ';' + getDeepJsonData(data,'mobileDTO','simCountryIso')
            importString = importString + ';' + getDeepJsonData(data,'mobileDTO','networkType')                                                                                                                                                                                                            
            importString = importString + ';' + getDeepJsonData(data,'mobileDTO','roaming')
            importString = importString + ';' + getDeepJsonData(data,'uptimeInfoDTO','shutDownTimestamp')
            importString = importString + ';' + getDeepJsonData(data,'uptimeInfoDTO','turnOnTimestamp') 
            importString = importString + ';' + getDeepJsonData(data,'uptimeInfoDTO','uptime')              
            importString = importString + ';' + getJsonData(data,'triggerCode')
            importString = importString + ';' + getJsonData(data,'appVersion')
            importString = importString + ';' + getJsonData(data,'timeZone')                                                                                                                                                                                                                                           
        except (KeyError,TypeError,ValueError) as e: 
            print( 'ERROR in parse Android: ' + str(e) + " : " + deviceHash)
            print( tmp)
        #                             pIP      lIP     mdate   lat    long     aVer    dRes    cMod   volt    temp    perc     health  chSta   BW     ssid    MA       rssi    CA     NT       Roaming
        #    importString = importString + 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;' + 'N/A;'                                                                     
            
        return importString;
    
def parseAndroidFilteredLog(unifiedLine):
        csvData = unifiedLine.split(';')      
        deviceHash = replaceProblematicChars(csvData[3]) #device hash
        platform = csvData[4] #platform

        #tmp = datetime.datetime.fromtimestamp(float(importString+'.0')/1000).strftime('%Y-%m-%d %H:%M:%S')
        importString = deviceHash  #Device hash
        tmp = replaceProblematicChars(csvData[5])
        #tmp = tmp.encode('utf-8')       
        try:
            data = json.loads(tmp)
            importString = importString + ';' + getJsonData(data,'publicIP')
            importString = importString + ';' + getJsonData(data,'localIP')
            tmp1 = getJsonData(data,'timeStamp')
            tmp = datetime.datetime.fromtimestamp(float(tmp1+'.0')/1000).strftime('%Y-%m-%d %H:%M:%S')
            importString = importString + ';' + tmp
            importString = importString + ';' + getJsonData(data,'latitude')
            importString = importString + ';' + getJsonData(data,'longitude')
            importString = importString + ';' + getJsonData(data,'discoveryResultCode')
            importString = importString + ';' + getJsonData(data,'connectionMode')
            importString = importString + ';' + getDeepJsonData(data,'wifiDTO','bandwidth')
            importString = importString + ';' + getDeepJsonData(data,'wifiDTO','ssid')
            importString = importString + ';' + getDeepJsonData(data,'wifiDTO','macAddress')
            importString = importString + ';' + getDeepJsonData(data,'wifiDTO','rssi')
            importString = importString + ';' + getDeepJsonData(data,'mobileDTO','carrier')
            importString = importString + ';' + getDeepJsonData(data,'mobileDTO','simCountryIso')
            importString = importString + ';' + getDeepJsonData(data,'mobileDTO','networkType')                                                                                                                                                                                                            
            importString = importString + ';' + getDeepJsonData(data,'mobileDTO','roaming')
            importString = importString + ';' + getJsonData(data,'timeZone')                                                                                                                                                                                                                                           
        except (KeyError,TypeError,ValueError) as e: 
            print( 'ERROR in parse Android: ' + str(e) + " : " + deviceHash)
            print( tmp)
        #                             pIP      lIP     mdate   lat    long     aVer    dRes    cMod   volt    temp    perc     health  chSta   BW     ssid    MA       rssi    CA     NT       Roaming
        #    importString = importString + 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;' + 'N/A;'                                                                     
            
        return importString;


def removeByte(original):
    result = original.replace("b'","").replace("'","")
    return result
def replaceProblematicChars(inputString):
    inputString = inputString.replace('=\n','=')
    inputString = inputString.replace('=\\n','=')
    inputString = inputString.replace('=\\\n','=')
    inputString = inputString.replace('=\\\\n','=')
    inputString = inputString.replace('=\\\\\\\\n','=')
    inputString = inputString.replace('\n','')
    inputString = inputString.replace('\t','')    
    inputString = inputString.replace('"\\\\\\\\"Vamos Ecuador\\\\\\\\""','"Vamos Ecuador"')    
    inputString = inputString.replace('ZAIN IQ\\n','ZAIN IQ')
    inputString = inputString.replace('\\\\','')
    inputString = inputString.replace('\\\\\\\\','')
    inputString = r''+inputString
    niceString = inputString.replace('\\','')
    niceString = inputString.replace('""O2 - UK""','"O2 - UK"')
    
    return niceString

def loadFile(name):
    global userSpecificPreprocessedFolder
    global userSpecificPreprocessedSubsetFolder
    startTime = datetime.datetime.now()
    fileNameArray = name.split("/")
    fileName = fileNameArray[-1]
    fileArray = fileName.split(".")
    fileWithoutExtension = fileArray[0]
    file = open(userSpecificPreprocessedFolder+fileWithoutExtension+".csv", "w")
    filteredFile = open(userSpecificPreprocessedSubsetFolder+fileWithoutExtension+".csv", "w")

    counter = 0
    with open(name, "r") as ins:
        for line in ins:
            counter = counter + 1
            importString = parseAndroidLog(line)
            filteredImportString = parseAndroidFilteredLog(line)
            file.write(importString+'\n');
            filteredFile.write(filteredImportString+'\n');
            filteredImportString = ''
            importString = ''
            output = ''
    endTime = (datetime.datetime.now() - startTime).total_seconds() 
    print( str(counter) + ":row saved in: " + str(endTime) +"seconds")   
    file.close()
    filteredFile.close()   
    return;
if(str(sys.argv[1]) == "osx"):
    actualEnvironment = "osx"
else:
    actualEnvironment = "linux"   
print("Actul envirnment:" + "----" + actualEnvironment)
#path = "/Volumes/Backup/research/data/*.csv"
print("Loading configfile:" + "----" + str(datetime.datetime.now()))
loadConfiguration()
print("Removing old files :" + "----" + str(datetime.datetime.now()))
removeFiles()
print("Loading files from:" + userSpecificFiles + "----" + str(datetime.datetime.now()))
#loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
for fname in glob.glob(userSpecificFiles+"*.imp"):
    print("Loading file:" + fname + "----" + str(datetime.datetime.now()))
    loadFile(fname)
          

              
#loadFile("../402036")      
      