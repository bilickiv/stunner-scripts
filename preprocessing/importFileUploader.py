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

def loadConfiguration():
    global userSpecificPreprocessedFolder 
    config = configparser.ConfigParser()
    config.read('importFileUploaderConfig.txt')
    if(actualEnvironment == "osx"):
        userSpecificPreprocessedFolder = config['osx']['userSpecificPreprocessedFolder']              
    else:
        userSpecificPreprocessedFolder = config['fict']['userSpecificPreprocessedFolder']            
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
def uploadFile(name):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    query = "LOAD DATA LOCAL INFILE '"+name+"' INTO TABLE STUDYDATA FIELDS TERMINATED BY ';'"
    cursor.execute( query )
    print( cursor.rowcount)
    cnx.commit()
    
    return
                

if(str(sys.argv[1]) == "osx"):
    actualEnvironment = "osx"
else:
    actualEnvironment = "linux"   
print("Actul envirnment:" + "----" + actualEnvironment)
#path = "/Volumes/Backup/research/data/*.csv"

config = {
'user': 'root',
'password': '',
'host': '10.6.14.36',
'port' : 3306,
'database': 'test',
'raise_on_warnings': True,
}


connection = pymysql.connect(host='10.6.14.37',
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
for fname in glob.glob(userSpecificPreprocessedFolder+"*.imp"):
    print("Loading file:" + fname + "----" + str(datetime.datetime.now()))
    uploadWithOtherDriver(fname)
cnx.close()
