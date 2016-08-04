import sys
import json
import os
import datetime
import time

reload(sys)  
sys.setdefaultencoding('utf8')
firstLine = ''
secondLine = ''
thirdLine = ''
indexString = ''
counter = 0 
def loadBlobData(name):

    insideOfTheIndex = 'false'
    file = open(name+".blobcsv", "w") 
  
    with open(name, "r") as ins:
        for line in ins:
           splittedString = line.split(';') 
           #print(splittedString[0])
           #print(splittedString[1])
           niceString = replaceProblematicChars(splittedString[1])
           if(niceString.find('timeStamp') != -1):
                jsonRep = parseAndroidJson(niceString)
           else:
                correctedWPString = correctWPJSon(niceString)
                jsonRep = parseWPJson(correctedWPString)
                #print('WINDOWS')
           #print(jsonRep)
           file.write(splittedString[0] + ';' + jsonRep)          
    file.close()
    return
def correctWPJSon(inputString):
    parsedWPString = inputString.replace('"{"','{', 100)
    parsedWPString = parsedWPString.replace('"}"','}', 100)
    parsedWPString = parsedWPString.replace('id "','"id"', 100)
    parsedWPString = parsedWPString.replace('batteryLevel ":"','"batteryLevel":', 100)
    parsedWPString = parsedWPString.replace('dischargeTime ":"','"dischargeTime":', 100)
    parsedWPString = parsedWPString.replace('charging ":"','"charging":', 100)
    parsedWPString = parsedWPString.replace('bandwidth ":"','"bandwidth":', 100)
    parsedWPString = parsedWPString.replace('ssid ":"','"ssid":', 100)
    parsedWPString = parsedWPString.replace('carrier ":"','"carrier":', 100)
    parsedWPString = parsedWPString.replace('netType ":"','"netType":', 100)
    parsedWPString = parsedWPString.replace('isRoaming ":"','"isRoaming":', 100)
    return parsedWPString    
def parseWPJson(unifiedLine):
    importString = ''
    tmp = unifiedLine.encode('utf-8')       
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
            #print 'ERROR: ' + str(e)
            #print tmp
            pass   
    return importString     
def parseAndroidJson(jsonString):
    importString = ''
    try:
        data = json.loads(jsonString)
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
        importString = importString + ';' + getDeepJsonData(data,'mobileDTO','networkType')                                                                                                                                                                                                            
        importString = importString + ';' + getDeepJsonData(data,'mobileDTO','roaming')
        importString = importString + ';' + getDeepJsonData(data,'uptimeInfoDTO','shutDownTimestamp')
        importString = importString + ';' + getDeepJsonData(data,'uptimeInfoDTO','turnOnTimestamp') 
        importString = importString + ';' + getDeepJsonData(data,'uptimeInfoDTO','uptime')            
        importString = importString + ';' + getJsonData(data,'triggerCode')
        importString = importString + ';' + getJsonData(data,'appVersion')
        importString = importString + ';' + getJsonData(data,'timeZone')            
    except (KeyError,TypeError,ValueError) as e: 
        #print 'ERROR: ' + str(e)
        pass
        # print tmp
    return importString
def replaceProblematicChars(inputString):
    inputString = inputString.replace('=\n','=')
    inputString = inputString.replace('=\\n','=')
    inputString = inputString.replace('=\\\n','=')
    inputString = inputString.replace('=\\\\n','=')
    inputString = inputString.replace('=\\\\\\\\n','=')
    inputString = inputString.replace('\n','')
    inputString = inputString.replace('\t','')
    inputString = inputString.replace("\'",'')
    inputString = r''+inputString
    niceString = inputString.replace('\\','')
    return niceString

def getJsonData(jsonSource, jsonKey):
    try:
        resultString = jsonSource[jsonKey]
        resultString = str(resultString).encode('utf-8') 
    except (KeyError,TypeError) as e: 
        #print 'Decoding JSON has failed publicIP' + str(counter)
        resultString = 'N/A'
    return str(resultString)
def getDeepJsonData(jsonSource, jsonTree,jsonKey):
    try:
        resultString = jsonSource[jsonTree][jsonKey]
        resultString = str(resultString).encode('utf-8') 
    except (KeyError,TypeError) as e: 
        #print 'Decoding JSON has failed publicIP' + str(counter)
        resultString = 'N/A'
    return str(resultString)
      
import glob

#path = "/Volumes/Backup/research/Arch/old_data/*.blob"
path = "/home/bilickiv/raw_dataset/old_data/*.blob"

for fname in glob.glob(path):
    print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
    #loadFile(fname)
    loadBlobData(fname)
    print("Finished loading file:" + fname + "----" + unicode(datetime.datetime.now()))


              
#loadFile("../402036")      
      