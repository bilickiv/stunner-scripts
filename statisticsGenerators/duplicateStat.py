import sys
import os
import datetime
import time
import glob
import configparser
import pandas as pd
import numpy as np
import argparse
from itertools import islice
#import matplotlib.pyplot as plt
import scipy.stats as sc
import math
import warnings

warnings.filterwarnings('ignore')
dateDict = {}
nodeDict = []
duplicateDict = {}

total_rows = 0
rowArray = [] 
def count_dayly_record(val):
    global dateDict
    if(val in dateDict):
        tmp = dateDict[val]
        tmp = tmp + 1
        dateDict[val] = tmp
    else:
        dateDict[val] = 1
    return
def mainCycle(val):
    global dateDict
    #LOAD DATA
    data = pd.read_csv(val, header=-1, sep=';')
    #print(data.head(1))
    data['duplicated'] = data.duplicated(subset=[5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36] , keep='first')
    #print(data.head())
    data[0] = pd.to_datetime(data[0])
    data['StripedUploadDate'] = data[0].apply(lambda A: A.date())
    tmp = data.loc[data['duplicated'] == False]
    c = data.loc[data['duplicated'] == True]
    c['StripedUploadDate'].apply(count_dayly_record)
    result = {"DeviceId":str(data.iloc[0][5]),"Original":len(data),"Duplicated":len(c)}
    nodeDict.append(result) 
    return
print("START")
for fileName in glob.glob("/home/bilickiv/data/raw_dataset/userSpecificPreprocessed/*.csv"):
    print(fileName)
    mainCycle(fileName)
    
a = pd.Series(dateDict, name='Count')
a.to_csv('/home/bilickiv/data/raw_dataset/dayliStat.csv')
b = pd.DataFrame(nodeDict)
b.to_csv('/home/bilickiv/data/raw_dataset/nodeStat.csv')
print("END")