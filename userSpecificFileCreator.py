# -*- coding: utf-8 -*-
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
    query = "select *  from RAWDATA where hashid like '"+userId+"';"
    ret = cursor.execute( query )
    results = cursor.fetchall()
    for row in results:
        #file.writelines(row)
        print (row)
    return

def loadUserList():
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    query = "select distinct hashid from RAWDATA"
    ret = cursor.execute( query )
    results = cursor.fetchall()
    for row in results:
        fname = row[0]
        tmp = base64.standard_b64encode(fname)
        # Now print fetched result
        print tmp
        file = open('./details/'+tmp+".imp", "w")
        loadGivenEndUser(file, cursor, fname)
        file.close()      
    print cursor.rowcount
    cnx.commit()
    return

def uploadFile(name):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    query = "LOAD DATA LOCAL INFILE '"+name+"' INTO TABLE RAWDATA FIELDS TERMINATED BY ';'"
    ret = cursor.execute( query )
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
      