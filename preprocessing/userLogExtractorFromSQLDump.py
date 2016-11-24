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
    otherPart = ""
    with open(blobFile, "r", encoding="utf-8") as ins:
        for line in ins:
            #print(line)
            csvData = line.split(';')
            tmpid = csvData[0]
            if(index % 1000000 == 0):
                print(index)
            index = index + 1    
            try:
                otherPart = indexEntries[tmpid]
                tmpOtherPart = otherPart.split('\t')
                print("1    "+otherPart)
                print("2    "+str(tmpOtherPart)
                print("3    "+replaceProblematicChars(csvData[1]))
                print("4    "+tmpOtherPart[2])
                print("5    "+tmpOtherPart[3])
                print("6    "+tmpOtherPart[1])                
                #file;timestamp;id;op;json
                dataString = "SQL;" + tmpOtherPart[2] + ";" + tmpOtherPart[3] + ";" + tmpOtherPart[1] + ";"+ replaceProblematicChars(csvData[1])
                if(tmpOtherPart[1] == "hu.uszeged.wlab.stunner.windowsphone"):
                        saveWindowsLine(tmpOtherPart[3],dataString)
                else:
                        saveLine(tmpOtherPart[3],dataString)
             #   print(dataString)
            #   print(line)
            except:
                 print("Error" + tmpid + str(sys.exc_info()[0]))                
    endTime = (datetime.datetime.now() - startTime).total_seconds() 
    print(str(index) + ":rows loaded in: " + str(endTime) +"seconds")   
    #  file.close()   
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
#        file = open(userSpecificFiles+fileName+".imp", "a+", encoding="utf-8")
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
#        file = open(userSpecificFiles+fileName+".imp", "a+", encoding="utf-8")
        file.write(content+'\n')
        file.close()
        return;
def replaceProblematicChars(inputString):
    inputString = inputString.replace('=\n','=')
    inputString = inputString.replace('=\\n','=')
    inputString = inputString.replace('=\\\n','=')
    inputString = inputString.replace('=\\\\n','=')
    inputString = inputString.replace('=\\\\\\\\n','=')
    inputString = inputString.replace('\n','')
    inputString = inputString.replace('\t','')
    inputString = inputString.replace("\'",'')
    inputString = inputString.replace('ZAIN IQ\\n','ZAIN IQ')

    
    inputString = r''+inputString
    niceString = inputString.replace('\\','')
    niceString = inputString.replace('""O2 - UK""','"O2 - UK"')
    
    return niceString
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


