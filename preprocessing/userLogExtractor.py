import glob
import sys
import datetime
import pdb
import configparser
import os
import base64

uploadedFiles = []
actualListOfFiles = []
fileToBeUploaded = []
rawFiles = ""
userSpecificFiles = ""
actualEnvironment = "osx"

def loadConfiguration():
    global rawFiles
    global userSpecificFiles
    config = configparser.ConfigParser()
    config.read('userLogExtractorConfig.txt')
    if(actualEnvironment == "osx"):
        rawFiles = config['osx']['rawFiles']
        userSpecificFiles = config['osx']['userSpecificFiles']
    else:
        rawFiles = config['fict']['rawFiles']
        userSpecificFiles = config['fict']['userSpecificFiles']
    return;


def loadListOfFiles():
    global userSpecificFiles
    global rawFiles
    print("Searching for files in:" rawFiles)
    for fname in glob.glob(rawFiles+"*.csv"):
        print("Processing file:" + fname)
        loadFile(fname)
    return;


def saveTestLine(idString, content):
        csvData = idString.split(';')
        idStr = csvData[1]
        print(replaceProblematicChars(idStr))
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

def loadFile(name):
        deviceId = ""  
        startTime = datetime.datetime.now()
        first = 'true'
        output = ''
        importString = ''
        #file = open(name+".imp", "w")
        counter = 0
        with open(name, "r", encoding="utf-8") as ins:
            for line in ins:
                if(line.find('hu.uszeged.wlab.stunner.windowsphone') != -1):
                    print('Windows phone')
                    print(line)
                    output = line.replace('=\n','=')
                    importString = name + ';' + output
                    counter = counter + 1
                    #file.write(importString+'\n')
                    csvData = output.split(';')
                    idStr = csvData[1]
                    saveWindowsLine(idStr,output)
                    importString = ''
                    output = ''
                    first = 'true'                               
                else:
                    if (first == 'true'):
                        deviceId = line
                        #print(deviceId)
                        output = line
                        first = 'false'
                    else:
                        counter = counter + 1
                        output = output + line
                        niceString = replaceProblematicChars(output)                    
                        importString = name + ';' + niceString
                        #file.write(importString+'\n');
                        saveLine(deviceId, importString)
                        importString = ''
                        output = ''
                        first = 'true'
        endTime = (datetime.datetime.now() - startTime).total_seconds() 
        print(str(counter) + ":row saved in: " + str(endTime) +"seconds")   
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
#load configuration
loadConfiguration()

#add the newly uploaded files to the log
print("Start generating log file  ("+str(datetime.datetime.now())+")")
loadListOfFiles()

