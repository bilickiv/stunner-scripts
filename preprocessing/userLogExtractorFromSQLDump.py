import glob
import sys
import datetime
import pdb
import configparser
import os
import base64

indexEntries = {}
indexFile = ""
userSpecificFiles = ""
userSpecificFilesWindows = ""
actualEnvironment = "osx"

def loadConfiguration():
    global indexFile
    global blobFile
    global userSpecificFiles
    global userSpecificFilesWindows
    config = configparser.ConfigParser()
    config.read('userLogExtractorFromSQLDumpConfig.txt')
    if(actualEnvironment == "osx"):
        indexFile = config['osx']['indexFile']
        blobFile = config['osx']['blobFile']
        userSpecificFiles = config['osx']['userSpecificFiles']
        userSpecificFilesWindows = config['osx']['userSpecificFilesWindows']
    else:
        indexFile = config['fict']['indexFile']
        blobFile = config['fict']['blobFile']
        userSpecificFiles = config['fict']['userSpecificFiles']
        userSpecificFilesWindows = config['fict']['userSpecificFilesWindows']
    return;
def saveLine(idString, content):
        global userSpecificFiles
        csvData = idString.split(';')
        idStr = csvData[1]
        tmp = replaceProblematicChars(idStr)
        fileName = base64.b64encode(tmp.encode(encoding='utf_8'))
        tmp = str(fileName).replace("b'","").replace("'","").replace("=","")
        #print(tmp)
        file = open(userSpecificFiles+tmp+".imp", "a+", encoding="utf-8")
        file.write(content+'\n')
        file.close()
        return;

def saveWindowsLine(idString, content):
        global userSpecificFiles
        global userSpecificFilesWindows
        tmp = replaceProblematicChars(idString)
        fileName = base64.b64encode(tmp.encode(encoding='utf_8'))
        tmp = str(fileName).replace("b'","").replace("'","").replace("=","")
        #print(tmp)
        file = open(userSpecificFilesWindows+tmp+".imp", "a+", encoding="utf-8")
        file.write(content+'\n')
        file.close()
        return;

def loadIndexFile():
    global indexFile
    index = 0
    startTime = datetime.datetime.now()
    with open(indexFile, "r", encoding="utf-8") as ins:
        for line in ins:
            #print(line)
            csvData = line.split('\t')
            indexEntries[csvData[4]] = line
            index = index + 1
            if(index % 10000 == 0):
                print(index)
    endTime = (datetime.datetime.now() - startTime).total_seconds() 
    print(str(index) + ":rows loaded in: " + str(endTime) +"seconds")   
    #  file.close()   
    return;
def loadBlobFile():
    global blobFile
    index = 0
    startTime = datetime.datetime.now()
    with open(blobFile, "r", encoding="utf-8") as ins:
        for line in ins:
            #print(line)
            csvData = line.split(';')
            tmpid = csvData[0]
            if(index % 10000 == 0):
                print(index)
            try:
                otherPart = indexEntries[tmpid]
                print(otherPart)
                print(line)
                index = index + 1
            except:
                 print("Error" + tmpid)                
    endTime = (datetime.datetime.now() - startTime).total_seconds() 
    print(str(index) + ":rows loaded in: " + str(endTime) +"seconds")   
    #  file.close()   
    return;
if(str(sys.argv[1]) == "osx"):
    actualEnvironment = "osx"
else:
    actualEnvironment = "linux"    

#load configuration
loadConfiguration()
#add the newly uploaded files to the log
print("Start loading indexfile  ("+str(datetime.datetime.now())+")")
loadIndexFile()
print("Finished loading indexfile  ("+str(datetime.datetime.now())+")")
loadBlobFile()


