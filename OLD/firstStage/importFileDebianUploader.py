import sys
import json
import os
import datetime
import pymysql.cursors

reload(sys)  
sys.setdefaultencoding('utf8')
config = {
'user': 'root',
'host': '127.0.0.1',
'database': 'test',
}
def uploadFile(name):
    cnx = pymysql.connect(**config)
    cursor = cnx.cursor()
    query = "LOAD DATA LOCAL INFILE '"+name+"' INTO TABLE RAWDATA FIELDS TERMINATED BY ';'"
    ret = cursor.execute( query )
    print cursor.rowcount
    cnx.commit()
    
    return
      
import glob
path = "./sample-data/*.imp"
cnx = pymysql.connect(**config)
for fname in glob.glob(path):
    print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
    uploadFile(fname)
cnx.close()
          

              
#loadFile("../402036")      
      
