import sys
import os
import datetime
import time
import glob
import configparser
import pandas as pd
import numpy as np
import argparse
from itertools import islice
import pymysql

duplicateFree = ""
fileStep = 500
fileStepCount = 0
connectionFiltered = pymysql.connect(host='10.6.14.36',
                             user='root',
                             password='',
                             db='stunner',
                             charset='utf8mb4',
                             local_infile=True,
                             cursorclass=pymysql.cursors.Cursor)                             
def loadConfiguration():
    global duplicateFree      
    global fileStep
    global fileStepCount

    parser = argparse.ArgumentParser()
    parser.add_argument("opsystem", help="runntime, 0=benti, 1=linux, 2=osx",type=int)
    parser.add_argument("chunk", help="chunk number, 0-12",type=int)                    
    args = parser.parse_args()
    fileStepCount = args.chunk
    #print("Actul step:" + "----" + str(fileStepCount))
    if(args.opsystem == 2):
        actualEnvironment = "osx"
    if(args.opsystem == 0):
        actualEnvironment = "benti"
    if(args.opsystem == 1):
        actualEnvironment = "linux" 
    config = configparser.ConfigParser()
    config.read('duplicationCleaner.txt')
    if(actualEnvironment == "osx"):
        duplicateFree = config['osx']['duplicateFree']
    if(actualEnvironment == "benti"):
        duplicateFree = config['benti']['duplicateFree']                        
    if(actualEnvironment == "linux"):
        duplicateFree = config['fict']['duplicateFree']            
    return;

def uploadFilteredFile(name):
    try:
        with connectionFiltered.cursor() as cursor:
            # Create a new record
            sql = "LOAD DATA LOCAL INFILE '" + name + \
            "'  IGNORE INTO TABLE FILTEREDDATA FIELDS TERMINATED BY ';' IGNORE 1 LINES"
            cursor.execute(sql)
        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connectionFiltered.commit()
    finally:
        print('Finished')    
    return    

def loadchunks():
    global fileStepCount
    global fileStep
    global duplicateFree
    indexCounter = 0
    fileList = []
    for fileName in glob.glob(duplicateFree + "*.csv"):
        fileList.append(fileName)
    print(str(fileStep) + ":" + str(fileStepCount))
    start = fileStepCount * fileStep
    end = (fileStepCount + 1) * fileStep
    iter = islice(fileList, start, end, None)
    print("Loading files between:" + str(start) + " and " + str(end))
    for a in iter:
        print("Loading file:" + a)
        uploadFilteredFile(a)
        print("Finished loading file:" + a )        
    return

loadConfiguration()
loadchunks()
              
#loadFile("../402036")      
      
