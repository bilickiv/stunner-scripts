import sys
import json
import os
import datetime
import pymysql

reload(sys)  
sys.setdefaultencoding('utf8')
connection = pymysql.connect(host='10.6.14.37',
                             user='root',
                             password='',
                             db='stunner',
                             charset='utf8mb4',
                             local_infile=True,
                             cursorclass=pymysql.cursors.Cursor)
def uploadFile(name):
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "LOAD DATA LOCAL INFILE '" + name + \
                "'  INTO TABLE DATA FIELDS TERMINATED BY ';'"
            cursor.execute(sql)
        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()
    finally:
        print('Finished')    
    return
import glob
path = "/home/bilickiv/data/raw_dataset/userSpecificPreprocessed/*.csv"
for fname in glob.glob(path):
    print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
#    uploadFile(fname)
    print("Finished loading file:" + fname + "----" + unicode(datetime.datetime.now()))
    uploadFile(fname)
          

              
#loadFile("../402036")      
      
