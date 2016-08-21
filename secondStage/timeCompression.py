# -*- coding: utf-8 -*-
# The goal of this csrip is to generate user specific data files

import sys
import json
import os
from datetime import datetime
import pymysql.cursors
import base64

reload(sys)  
sys.setdefaultencoding('utf8')
config = {
'user': 'root',
'password': '',
'host': '10.6.14.36',
'database': 'test'
}


def loadGivenEndUser(file, cursor, userId, hashOfTheUser):
    result = ''
    counter = 0
    discoveryResult = -8
    startDate = None
    endDate = None
    difference = 0
    samewas = False
    first = True
    totalRows = 0
    checkRows = 0
    saved = False
    query = "select CAST(mdate as DATETIME),aggDiscResult, *  from DATA where hashid like '"+userId+"' order by CAST(mdate as DATETIME);"
    ret = cursor.execute( query )
    results = cursor.fetchall()
    count = 1
    print "---------------------------------------------------------------------"
    print "The count of total rows" + str(len(results)) + " For user: " + userId 
    print "The filename: " + str(hashOfTheUser)
    totalRows = len(results)
    index = 0
    file.write("First row;" + userId + ";" + str(len(results))  +'\n')
    if(len(results) == 0):
        print "Zero length:" + userId
    if(len(results) == 1):
        for row in results:
            startDate = row[0]
            endDate = row[0] 
            discoveryResult = int(row[1])
        result = str(count) + ";" + str(endDate-startDate) + ";" + str(startDate) + ";" + str(endDate) + ";" + str(discoveryResult)
        checkRows = checkRows + count            
        file.write(result + '\n')
        print "One row " + userId
    else:
        for row in results:
            index = index + 1
            #print discoveryResult
            if(startDate is None):
                startDate = row[0]
            if(endDate is None):
                endDate = row[0]            
            if(discoveryResult == int(row[1])):
             #   print "It was same" +str(discoveryResult)
                endDate = row[0]                  
                count = count + 1
                saved = False
            else:
                if(discoveryResult == -8):
                    discoveryResult = int(row[1])
                    startDate = row[0]
                    endDate = row[0]
                    saved = False
                    #print "First cycle"
                else:
                    saved = True
              #      print "Saving it" +str(discoveryResult)
                    result = str(count) + ";" + str(endDate-startDate) + ";" + str(startDate) + ";" + str(endDate) + ";" + str(discoveryResult)            
                    checkRows = checkRows + count            
                    file.write(result + '\n')
                    counter = counter + 1
                    result = ''
                    discoveryResult = int(row[1])
                    startDate = row[0]
                    endDate = row[0]
                   # print str(index) + "..." + str(totalRows) 
                    if(index == totalRows):
                        count = 1
                       # print "LAST SAVE"
                        result = str(count) + ";" + str(endDate-startDate) + ";" + str(startDate) + ";" + str(endDate) + ";" + str(discoveryResult)            
                        checkRows = checkRows + count            
                        file.write(result + '\n')
                        counter = counter + 1
            # for column in row:
            #     result = result +  str(column) + ';'
                count = 1
        if(not saved):
        # print "All the same: " + userId
            result = str(count) + ";" + str(endDate-startDate) + ";" + str(startDate) + ";" + str(endDate) + ";" + str(discoveryResult)            
            file.write(result + '\n')
            counter = counter + 1 
            checkRows = checkRows + count            

        
    print 'Saved lines: ' + str(counter) + ' original rows: ' + str(totalRows) + ' countedRows' + str(checkRows)
    if(totalRows != checkRows):
        print "+++++++++++++++++++++++++ ERROR +++++++++++++++++++++++++++++++++++++++++++"
    file.flush()
    return

def loadUserList():
    cnx = pymysql.connect(**config)
    cursor = cnx.cursor()
    query = "select distinct hashid from DATA"#" where hashId like '%gqVdRgqBInW1CcUXXi%'"
    ret = cursor.execute( query )
    results = cursor.fetchall()
    for row in results:
        fname = row[0]
        print fname + row[0]
        hashOfTheUser = base64.standard_b64encode(fname)
        # Now print fetched result
      #  print 'Creating log for user:' + hashOfTheUser
        file = open('../results/userLog/'+hashOfTheUser+".imp", "w")
        loadGivenEndUser(file, cursor, fname, hashOfTheUser)
        file.close()      
    print cursor.rowcount
    cnx.commit()
    return
      
import glob
#path = "*.imp"
cnx = pymysql.connect(**config)
#for fname in glob.glob(path):print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
 #   uploadFile(fname)
loadUserList() 
cnx.close()
          

              
#loadFile("../402036")      
      