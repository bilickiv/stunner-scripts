# -*- coding: utf-8 -*-
# The goal of this csrip is to generate user specific data files

import sys
import json
import os
import datetime
import mysql.connector
import base64

reload(sys)  
sys.setdefaultencoding('utf8')
config = {
'user': 'root',
'password': '',
'host': '10.6.14.36',
'database': 'test',
'raise_on_warnings': True,
}


def loadGivenEndUser(file, cursor, userId):
    result = ''
    query = "select *  from RAWDATA where hashid like '"+userId+"';"
    ret = cursor.execute( query )
    results = cursor.fetchall()
    for row in results:
        for column in row:
            result = result +  column + ';'
        #file.writelines(row)
        print (row)
        file.write(result + '\n')
        result = ''    
    return
    file.flush()

def loadUserList():
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    query = "select distinct hashid from RAWDATA"
    ret = cursor.execute( query )
    results = cursor.fetchall()
    for row in results:
        fname = row[0]
        hashOfTheUser = base64.standard_b64encode(fname)
        # Now print fetched result

        file = open('./results/userLog/'+hashOfTheUser+".imp", "w")
        loadGivenEndUser(file, cursor, fname)
        file.close()      
    print cursor.rowcount
    cnx.commit()
    return
      
import glob
#path = "*.imp"
cnx = mysql.connector.connect(**config)
#for fname in glob.glob(path):print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
 #   uploadFile(fname)
loadUserList() 
cnx.close()
          

              
#loadFile("../402036")      
      