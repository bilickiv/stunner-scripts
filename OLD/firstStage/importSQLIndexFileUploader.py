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
    query = "LOAD DATA LOCAL INFILE '"+name+"' INTO TABLE TMPINDEX FIELDS TERMINATED BY '\t'"
    ret = cursor.execute( query )
    print cursor.rowcount
    cnx.commit()
    
    return
      
import glob
path = "/Volumes/Backup/research/Arch/old_data/*.index"
#path = "/home/bilickiv/raw_dataset/old_data/*.index"
cnx = mysql.connector.connect(**config)
for fname in glob.glob(path):
    print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
    uploadFile(fname)
cnx.close()
          

              
#loadFile("../402036")      
      