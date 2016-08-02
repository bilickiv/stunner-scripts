import sys
import json
import os
import datetime
import time

reload(sys)  
sys.setdefaultencoding('utf8')
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
            importString = importString + ';' + getJsonData(data,'triggerCode')
            importString = importString + ';' + getJsonData(data,'appVersion')
            importString = importString + ';' + getJsonData(data,'timeZone')                                                                                                                                                                                                                                           
    except (KeyError,TypeError,ValueError) as e: 
            print 'ERROR: ' + str(e)
            print tmp   
    return importString 
def parseAndroidLog(unifiedLine):
        csvData = unifiedLine.split(';')
        if(len(csvData) != 4):
            print len(csvData)
            print unifiedLine
        importString = csvData[0] #UploadTimestamp
        tmp = datetime.datetime.fromtimestamp(float(importString+'.0')/1000).strftime('%Y-%m-%d %H:%M:%S')
        importString = str(tmp) + ';' + csvData[1] #Device hash
        importString = importString + ';' + csvData[2] #SW napespace
        tmp = csvData[3]
        tmp = tmp.encode('utf-8')       
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
            importString = importString + ';' + getDeepJsonData(data,'mobileDTO','networkType')                                                                                                                                                                                                            
            importString = importString + ';' + getDeepJsonData(data,'mobileDTO','roaming')
            importString = importString + ';' + getJsonData(data,'triggerCode')
            importString = importString + ';' + getJsonData(data,'appVersion')
            importString = importString + ';' + getJsonData(data,'timeZone')                                                                                                                                                                                                                                           
        except (KeyError,TypeError,ValueError) as e: 
            print 'ERROR: ' + str(e)
            print tmp
        #                             pIP      lIP     mdate   lat    long     aVer    dRes    cMod   volt    temp    perc     health  chSta   BW     ssid    MA       rssi    CA     NT       Roaming
        #    importString = importString + 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;'+ 'N/A;' + 'N/A;'                                                                     
            
        return importString;
    






def loadFile(name):
      startTime = datetime.datetime.now()
      first = 'true'
      output = ''
      importString = ''
      file = open(name+".imp", "w")
      counter = 0
      with open(name, "r") as ins:
        for line in ins:
            if(line.find('hu.uszeged.wlab.stunner.windowsphone') != -1):
                print 'Windows phone'
                print line
                output = line.replace('=\n','=')
                importString = fname + ';' +parseWindowsPhoneLog(output)
                counter = counter + 1
                file.write(importString+'\n')
                importString = ''
                output = ''
                first = 'true'                               
            else:

                if (first == 'true'):
                    output = line
                    first = 'false'
                else:
                    counter = counter + 1
                    output = output + line
                    output = output.replace('=\n','=')
                    output = output.replace('=\\n','=')
                    output = output.replace('\n','')
                    output = output.rstrip('\n')
                    testTextArray = output.splitlines()
                    testConcat = str(testTextArray).replace('\\n','').replace("']",'').replace("['",'')
                    testConcat = testConcat.replace('\\x','')
                    testConcat = testConcat.replace('\\"','"')
                    importString = fname + ';' + parseAndroidLog(testConcat)
                    file.write(importString+'\n');
                    importString = ''
                    output = ''
                    first = 'true'
            #importString = importString + ';'
            # importString = importString.encode('utf-8')
              ##print importString

      endTime = (datetime.datetime.now() - startTime).total_seconds() 
      print str(counter) + ":row saved in: " + unicode(endTime) +"seconds"   
      file.close()   
      return;
      
import glob
path = "*.csv"
for fname in glob.glob(path):
    print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
    loadFile(fname)
          

              
#loadFile("../402036")      
      