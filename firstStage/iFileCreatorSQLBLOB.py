import sys
import json
import os
import datetime
import time

reload(sys)  
sys.setdefaultencoding('utf8')
firstLine = ''
secondLine = ''
thirdLine = ''
indexString = ''
counter = 0 
def loadBlobData(name):

    insideOfTheIndex = 'false'
    file = open(name+".blob", "w") 
  
    with open(name, "r") as ins:
        for line in ins:
                if(line.find('-- Data for Name: BLOBS; Type: BLOBS; Schema: -; Owner:') != -1):
                    insideOfTheIndex = 'true'
                    print(line)
                if(insideOfTheIndex.find('true') and line.find('COMMIT;') !=-1):
                    insideOfTheIndex = 'false'
                    print('End of the section')
                    return
                if(insideOfTheIndex == 'true'):
                    if(line.find('SELECT lo_open(') != -1):
	                    #parseIndexLine(line)
                        parseFirstLine(line);        
                    if(line.find('SELECT pg_catalog.lowrite(') != -1):
                        #parseIndexLine(line)
                        parseSecondLine(line, file);       
                    if(line.find('SELECT lo_close(0);') != -1):
                        #parseIndexLine(line)
                        parseThirdLine(line);                          
                    
    file.close()
    return

def parseFirstLine(line):
    global firstLine 
    global secondLine 
    global thirdLine
    global indexString 
    if(firstLine.find('processed') != -1):
        print('Error with first line' + line)
    else:
        #print('Processing first line')
        indexString =  extractDataFromFirstLine(line)
        #print(indexString)
    firstLine = 'processed'
    #print(line)
    return
def parseSecondLine(line,file):
    global firstLine
    global secondLine
    global thirdLine
    global counter
    if(line.find('eventdatas') != -1):
        secondLine = 'processed'
        return  
    if(firstLine.find('processed') != -1):
        #print ('Processing Second line')
        jsonString =  extractDataFromSecondLine(line)
        if(len(indexString) > 3):
            file.write(indexString + ';' +jsonString+'\n');
            counter = counter + 1
            if(counter % 10000 == 0):
                print counter
        else:
            print('Error with indexString' + line)    
        #parsedString = parseAndroidLog(jsonString)
        #print(jsonString)  
    else:
        print('Order Error' + line)
    #print(line)
    secondLine = 'processed'
    return

def parseThirdLine(line):
    global firstLine 
    global secondLine
    global thirdLine  
    if(secondLine.find('processed') == -1):
        print('Order Error')
    #    print(line)
    secondLine = ''
    firstLine = ''
    return
def extractDataFromFirstLine(line):
    endPosition = line.find(')')
    starPosition = line.find('lo_create(')+10    
    tmpIndex = line[starPosition:endPosition]
    return tmpIndex
def extractDataFromSecondLine(line):
    starPosition = line.find('lowrite(0,')+12
    tmpIndex = line[starPosition:len(line)-4]
    return tmpIndex    


      
import glob

#path = "/Volumes/Backup/research/Arch/old_data/datacollector-2015-05-21.sql"
path = "/home/bilickiv/raw_dataset/old_data/*.sql"

for fname in glob.glob(path):
    print("Loading file:" + fname + "----" + unicode(datetime.datetime.now()))
    #loadFile(fname)
    loadBlobData(fname)
    print("Finished loading file:" + fname + "----" + unicode(datetime.datetime.now()))


              
#loadFile("../402036")      
      