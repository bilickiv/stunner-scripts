import glob
import sys
import datetime
import pdb
import configparser
import os

uploadedFiles = []
actualListOfFiles = []
fileToBeUploaded = []
logFileLocation = ""
filelistLocation = ""
serverAddress = ""
actualEnvironment = "osx"
def loadConfiguration():
    global logFileLocation
    global filelistLocation
    global serverAddress
    config = configparser.ConfigParser()
    config.read('uploadConfig.txt')
    if(actualEnvironment == "osx"):
        logFileLocation = config['osx']['logFile']
        filelistLocation = config['osx']['filelistLocation']
        serverAddress = config['osx']['serverAddress']
    else:
        logFileLocation = config['fict']['logFile']
        filelistLocation = config['fict']['filelistLocation']
        serverAddress = config['fict']['serverAddress']         
    return;

def uploadedFilesLog():
    global logFileLocation
    global uploadedFiles
    startTime = datetime.datetime.now()
    counter = 0
    with open(logFileLocation, "r") as ins:
        for line in ins:
            uploadedFiles.append(line) 
    endTime = (datetime.datetime.now() - startTime).total_seconds() 
    return;


def loadListOfFiles():
    global filelistLocation
    global actualListOfFiles
    for fname in glob.glob(filelistLocation+"*.*"):
        actualListOfFiles.append(fname)


def appendFilesLog():
    global logFileLocation
    global fileToBeUploaded
    startTime = datetime.datetime.now()
    logFile = open(logFileLocation, "a+")
    counter = 0
    for line, value in enumerate(fileToBeUploaded):
           # print("To append:"+value) 
            logFile.writelines(str(value))
            logFile.writelines("\n")                              
    endTime = (datetime.datetime.now() - startTime).total_seconds() 
    logFile.close()   
    return;
def uploadFiles():
    global fileToBeUploaded
    global serverAddress
    for line, value in enumerate(fileToBeUploaded):
        startTime = datetime.datetime.now()
        os.system("scp "+value+" "+serverAddress)
        print("Uploading ("+str(startTime)+"):"+value) 
    return;

def findTheMissingFiles():
    found = False
    global actualListOfFiles
    global uploadedFiles
    global fileToBeUploaded
    for realRow, realFile in enumerate(actualListOfFiles):
        for logRow, logFile in enumerate(uploadedFiles):
            if(realFile.rstrip() == logFile.rstrip()):
                found = True
        if(not(found)):
            print("New:" + realFile) 
            fileToBeUploaded.append(realFile.rstrip())
        found = False
    return;   


#print 'Number of arguments:', len(sys.argv), 'arguments.'
print( 'Argument List:', str(sys.argv[1]))
#print 'Argument List:', str(sys.argv[2])
if(str(sys.argv[1]) == "osx"):
    actualEnvironment = "osx"
else:
    actualEnvironment = "linux"        
#Load the configuration file
loadConfiguration()
#load the list of already uploaded feils from log file
uploadedFilesLog()

#load the list of files from the directory
loadListOfFiles()

#select the fies which has not been uploaded
findTheMissingFiles()

#upload the missing files
uploadFiles()

#add the newly uploaded files to the log
appendFilesLog()

#print( "Files in directory:" + str(actualListOfFiles))
#print( "Files not in already uploaded list" + str(fileToBeUploaded))
#path = "./sample-data/*.imp"
#cnx = mysql.connector.connect(**config)
#for fname in glob.glob(path):
#    print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
#    uploadFile(fname)
#    print("Finished loading file:" + fname + "----" + unicode(datetime.datetime.now()))
#uploadCSVFile('./sample-data/devices-measurements.csv')
#cnx.close()

