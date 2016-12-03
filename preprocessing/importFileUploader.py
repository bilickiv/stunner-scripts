import sys
import json
import os
import datetime
import time
import glob
import configparser
import pymysql.cursors



userSpecificPreprocessedFolder = ""
actualEnvironment = "osx"
userSpecificPreprocessedSubsetFolder = ""


def loadConfiguration():
    global userSpecificPreprocessedFolder 
    global userSpecificPreprocessedSubsetFolder
    
    config = configparser.ConfigParser()
    config.read('importFileUploaderConfig.txt')
    if(actualEnvironment == "osx"):
        userSpecificPreprocessedFolder = config['osx']['userSpecificPreprocessedFolder']
        userSpecificPreprocessedSubsetFolder = config['osx']['userSpecificPreprocessedSubsetFolder']              
                      
    else:
        userSpecificPreprocessedFolder = config['fict']['userSpecificPreprocessedFolder'] 
        userSpecificPreprocessedSubsetFolder = config['fict']['userSpecificPreprocessedSubsetFolder']                               
    return;

def uploadWithOtherDriver(name):
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "LOAD DATA LOCAL INFILE '"+name+"' INTO TABLE DATA FIELDS TERMINATED BY ';'"
            cursor.execute(sql)

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()
    finally:
        print('Finished')    
    return
def uploadFiltered(name):
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "LOAD DATA LOCAL INFILE '"+name+"' INTO TABLE DATAPART FIELDS TERMINATED BY ';'"
            cursor.execute(sql)

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()
    finally:
        print('Finished')    
    return
                

if(str(sys.argv[1]) == "osx"):
    actualEnvironment = "osx"
else:
    actualEnvironment = "linux"   
print("Actul envirnment:" + "----" + actualEnvironment)
#path = "/Volumes/Backup/research/data/*.csv"

connection = pymysql.connect(host='10.6.14.36',
                             user='root',
                             password='',
                             db='stunner',
                             charset='utf8mb4',
                             local_infile=True,
                             cursorclass=pymysql.cursors.Cursor)

print("Loading configfile:" + "----" + str(datetime.datetime.now()))
loadConfiguration()
print("Loading files from:" + userSpecificPreprocessedFolder + "----" + str(datetime.datetime.now()))
#loadFile(userSpecificFiles+"a2hFd3IrTHpIVHZJb1NhaU45R0xIT0h6KzloSTA1VzV4dmJmYnRVaDFhVT0.imp")
for fname in glob.glob(userSpecificPreprocessedSubsetFolder+"*.imp"):
    print("Loading file:" + fname + "----" + str(datetime.datetime.now()))
    uploadFiltered(fname)
cnx.close()
