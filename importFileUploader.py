import sys
import os
import datetime
#import mysql.connector
import pymysql.cursors

reload(sys)  
sys.setdefaultencoding('utf8')
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
                             cursorclass=pymysql.cursors.Cursor)
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
    query = "LOAD DATA LOCAL INFILE '"+name+"' INTO TABLE DATA FIELDS TERMINATED BY ';'"
    cursor.execute( query )
    print cursor.rowcount
    cnx.commit()
    
    return
      
import glob
print (sys.version)
#path = "/Users/bilickiv/developer/data/*.imp"
path = "/home/bilickiv/data/raw_dataset/processed_data/*.imp"
#cnx = mysql.connector.connect(**config)
for fname in glob.glob(path):
    print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
    uploadWithOtherDriver(fname)
cnx.close()
          

              
#loadFile("../402036")      
      