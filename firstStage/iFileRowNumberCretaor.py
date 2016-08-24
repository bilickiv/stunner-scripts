import sys
import json
import os
import datetime
import time

reload(sys)  
sys.setdefaultencoding('utf8')

def loadFile(name):
      startTime = datetime.datetime.now()
      first = 'true'
      output = ''
      importString = ''
      file = open(name+".imp.up", "w")
      counter = 0
      with open(name, "r") as ins:
        for line in ins:
            counter = counter + 1
            file.write(str(counter)+';'+line+'\n');
      file.close()   
      return;
      
import glob
path = "/home/bilickiv/data/raw_dataset/processed_data/*.csv.imp"
#path = "/home/bilickiv/raw_dataset/new_data/*.csv"
for fname in glob.glob(path):
    print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
    loadFile(fname)
    print("Finished loading file:" + fname + "----" + unicode(datetime.datetime.now()))


              
#loadFile("../402036")      
      