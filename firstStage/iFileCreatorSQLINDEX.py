import sys
import json
import os
import datetime
import time

reload(sys)  
sys.setdefaultencoding('utf8')





def loadIndexData(name):
    insideOfTheIndex = 'false'
    file = open(name+".index", "w")    
    with open(name, "r") as ins:
        for line in ins:
                if(line.find('COPY uploaded_data') != -1):
                    insideOfTheIndex = 'true'
                    print(line)
                if(insideOfTheIndex == 'true' and (line == '\\.' or len(line) < 5)):
                    print('End of first section')
                    file.close()
                    return
                if(insideOfTheIndex == 'true'):
                    if(line.find('wlab') != -1):
	                    #parseIndexLine(line)
                        file.write(line);        
                    
    file.close()
    return
import glob
path = "/home/bilickiv/raw_dataset/old_data/*.sql"
for fname in glob.glob(path):
    print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
    #loadFile(fname)
    loadIndexData(fname)
    print("Finished loading file:" + fname + "----" + unicode(datetime.datetime.now()))


              
#loadFile("../402036")      
      