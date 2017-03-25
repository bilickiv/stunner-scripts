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
import scipy.stats as sc
import math
import warnings
warnings.filterwarnings('ignore')
def timeToTimeStamp(val):
    try:
        int(val)
    except ValueError:
        val = pd.to_datetime(val)
        val = time.mktime(val.timetuple())
    else:
        return val
    return val

for fileName in glob.glob("/home/bilickiv/data/raw_dataset/duplicateFree/*.csv"):
    data = pd.read_csv(fileName, header=0, sep=';')
    print("processing:" + fileName)
    data['0'] =  data['0'].apply(timeToTimeStamp)
    data['1'] =  data['1'].apply(timeToTimeStamp)
    data['4'] =  data['4'].apply(timeToTimeStamp)
    data['9'] =  data['9'].apply(timeToTimeStamp)
    data['uploadDate'] =  data['uploadDate'].apply(timeToTimeStamp)
    head, tail = os.path.split(fileName)
    data.to_csv("/home/bilickiv/data/raw_dataset/duplicateFree-timestamp/"+str(tail), sep='\t', encoding='utf-8')