import sys
import json
import os
import datetime
import mysql.connector

reload(sys)  
sys.setdefaultencoding('utf8')
config = {
'user': 'root',
'password': '',
'host': '10.6.14.36',
'database': 'test',
'raise_on_warnings': True,
}
def uploadFile(name):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    query = "LOAD DATA LOCAL INFILE '"+name+"' IGNORE INTO TABLE TMPRAWDATA FIELDS TERMINATED BY ';'"
    ret = cursor.execute( query )
    print cursor.rowcount
    cnx.commit()
    
    return
def uploadCSVFile(name):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    query = "LOAD DATA LOCAL INFILE '"+name+"' IGNORE INTO TABLE TMPDEVICES FIELDS TERMINATED BY ','"
    ret = cursor.execute( query )
    print cursor.rowcount
    cnx.commit()
    
    return      
import glob
path = "./sample-data/*.imp"
cnx = mysql.connector.connect(**config)
for fname in glob.glob(path):
    print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
#    uploadFile(fname)
    print("Finished loading file:" + fname + "----" + unicode(datetime.datetime.now()))
uploadCSVFile('./sample-data/devices-measurements.csv')
cnx.close()
          

              
#loadFile("../402036")      
      