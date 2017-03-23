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
def time_zone_normalization(timezone):
    if math.isnan(timezone):
        return 0
    return timezone
def to_localized_data(date):
    print(date)
    return date

def group_creator_by_val(val):
    if val == group_creator_by_val.previous:
        tmp = ""
    else:
        group_creator_by_val.previous = val
        group_creator_by_val.count  +=1
    return group_creator_by_val.count
def date_group_creator_by_val(val):
    val = val.date()
    if val == date_group_creator_by_val.previous:
        tmp = ""
    else:
        date_group_creator_by_val.previous = val
        date_group_creator_by_val.count  +=1
    return date_group_creator_by_val.count
def count_dayly_record(val):
    global dateDict
    if(val in dateDict):
        tmp = dateDict[val]
        tmp = tmp + 1
        dateDict[val] = tmp
    else:
        dateDict[val] = 1
    return
def selectOverloadedRows(chargingData):
    overloadedRows = pd.DataFrame()
    for overloadedDate in overloadedDateSet:
        tmp = chargingData[chargingData['AndroidDate'] == overloadedDate]
        if(len(overloadedRows) == 0):
            overloadedRows = tmp
        else:
            overloadedRows.append(tmp)
    return overloadedRows
group_creator_by_val.count = 0 
group_creator_by_val.previous = None
date_group_creator_by_val.previous = None
date_group_creator_by_val.count = 0
overloadedDateSet = set()
#pd.set_option('display.max_columns', 10)
pd.options.display.max_columns = None
pd.options.display.width = 120
pd.set_option('display.max_rows', 500)
break_after_new_order = 0
charging_error = 0
big_changes = 0

dateDict = {}
nodeDict = {}
duplicateDict = {}

total_rows = 0
rowArray = [] 

def mainCycle(val):
    global break_after_new_order
    global charging_error
    global big_changes
    global total_rows
    global dateDict
    #LOAD DATA
    data = pd.read_csv(val, header=-1, sep=';')
    print(data.head(1))
    a = data[data.duplicated(subset=[3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34],keep=False)]
    if(len(a) > 0):
        print("Duplicate")
        print(len(a))
    #print(s.head(40))
print("START")
for fileName in glob.glob("/home/bilickiv/data/raw_dataset/userSpecificPreprocessed/Zz*.csv"):
    print(fileName)
    mainCycle(fileName)
    nodeDict[fileName] = total_rows
    
a = pd.Series(dateDict, name='Count')
a.to_csv('/home/bilickiv/data/raw_dataset/dayliStat.csv')
b = pd.Series(nodeDict, name='Count')
b.to_csv('/home/bilickiv/data/raw_dataset/nodeStat.csv')
print("END")