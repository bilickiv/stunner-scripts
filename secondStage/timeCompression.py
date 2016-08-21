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


def loadGivenEndUser(file, cursor, userId):
    result = ''
    counter = 0
    discoveryResult = -8
    startDate = None
    endDate = None
    difference = 0
    samewas = False
    first = True
    firstInGroup = False
    query = "select CAST(mdate as DATETIME),aggDiscResult, *  from DATA where hashid like '"+userId+"' order by CAST(mdate as DATETIME);"
    ret = cursor.execute( query )
    results = cursor.fetchall()
    count = 1
    for row in results:
        if(startDate is None):
            startDate = row[0]
        if(endDate is None):
            endDate = row[0]            
        if(discoveryResult == int(row[1])):
            if(firstInGroup is True):
                 firstInGroup = False
            else:
                  endDate = row[0]
            count = count + 1
        else:
            firstInGroup = True
            if(not first):
                result = str(count) + ";" + str(endDate-startDate) + ";" + str(startDate) + ";" + str(endDate) + ";" + discoveryResult            
                file.write(result + '\n')
            else:
                first = False
            result = ''
            discoveryResult = int(row[1])
            startDate = row[0]
            endDate = row[0]
           # for column in row:
           #     result = result +  str(column) + ';'
            count = 1 
  
    print 'Saved lines:' + str(counter)
    file.flush()
    return

def loadUserList():
    cnx = pymysql.connect(**config)
    cursor = cnx.cursor()
    query = "select distinct hashid from DATA where hashid like '%SDb4mR9XVlq0jBPLE/hUHC8T/GVaRaxnscXuDJA9VUw=%'"
    ret = cursor.execute( query )
    results = cursor.fetchall()
    for row in results:
        fname = row[0]
        hashOfTheUser = base64.standard_b64encode(fname)
        # Now print fetched result
        print 'Creating log for user:' + hashOfTheUser
        file = open('../results/userLog/'+hashOfTheUser+".imp", "w")
        loadGivenEndUser(file, cursor, fname)
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
      