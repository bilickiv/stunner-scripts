import glob
import sys
import datetime
import pdb
import configparser
import os
import base64

indexEntries = {}
indexFile1 = ""
blobFile1 = ""
indexFile2 = ""
blobFile2 = ""
userSpecificFiles = ""
userSpecificFilesWindows = ""
actualEnvironment = "osx"

def loadConfiguration():
    global indexFile1
    global blobFile1
    global indexFile2
    global blobFile2    
    global userSpecificFiles
    global userSpecificFilesWindows
    config = configparser.ConfigParser()
    config.read('userLogExtractorFromSQLDumpConfig.txt')
    if(actualEnvironment == "osx"):
        indexFile1 = config['osx']['indexFile1']
        blobFile1 = config['osx']['blobFile1']
        indexFile2 = config['osx']['indexFile2']
        blobFile2 = config['osx']['blobFile2']        
        userSpecificFiles = config['osx']['userSpecificFiles']
        userSpecificFilesWindows = config['osx']['userSpecificFilesWindows']
    else:
        indexFile1 = config['fict']['indexFile1']
        blobFile1 = config['fict']['blobFile1']
        indexFile2 = config['fict']['indexFile2']
        blobFile2 = config['fict']['blobFile2']        
        userSpecificFiles = config['fict']['userSpecificFiles']
        userSpecificFilesWindows = config['fict']['userSpecificFilesWindows']
    return;
def saveLine(idStr, content):
        global userSpecificFiles
        try:
            tmp = replaceProblematicChars(idStr)
            fileName = base64.b64encode(tmp.encode(encoding='utf_8'))
            tmp = str(fileName).replace("b'","").replace("'","").replace("=","")
            #print(tmp)
            file = open(userSpecificFiles+tmp+".imp", "a+", encoding="utf-8")
            file.write(content+'\n')
            file.close()
        except:
            print("Error in saveLine", sys.exc_info()[0])
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

def loadIndexFile(round):
    global indexFile1
    global indexFile2
    global indexEntries 
    indexEntries.clear()
    indexFile = ""
    if(round == "first"):
        indexFile = indexFile1
    else:
        indexFile = indexFile2
    index = 0
    startTime = datetime.datetime.now()
    with open(indexFile, "r", encoding="utf-8") as ins:
        for line in ins:
            #print(line)
            csvData = line.split('\t')
            indexEntries[csvData[4]] = line
            index = index + 1
            if(index % 1000000 == 0):
                print(index)
    endTime = (datetime.datetime.now() - startTime).total_seconds() 
    print(str(index) + ":rows loaded in: " + str(endTime) +"seconds")   
    #  file.close()   
    return;
def loadBlobFile(round):
    global blobFile1
    global blobFile2
    blobFile = ""
    if(round == "first"):
        blobFile = blobFile1
    else:
        blobFile = blobFile2
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
                #file;timestamp;id;op;json
                dataString = "SQL;" + tmpOtherPart[2] + ";" + tmpOtherPart[3] + ";" + tmpOtherPart[1] + ";"+ replaceProblematicChars(csvData[1])
                if(tmpOtherPart[1] == "hu.uszeged.wlab.stunner.windowsphone"):
                        saveWindowsLine(tmpOtherPart[3],dataString)
                else:
                        saveLine(tmpOtherPart[3],dataString)
             #   print(dataString)
            #   print(line)
            except:
                 print("Error in loadBlobFile:" + tmpid ,sys.exc_info()[0])                
    endTime = (datetime.datetime.now() - startTime).total_seconds() 
    print(str(index) + ":rows loaded in: " + str(endTime) +"seconds")   
    #  file.close()   
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
loadIndexFile("first")
print("Finished loading indexfile  ("+str(datetime.datetime.now())+")")
loadBlobFile("first")
loadIndexFile("second")
print("Finished loading indexfile  ("+str(datetime.datetime.now())+")")
loadBlobFile("second")

