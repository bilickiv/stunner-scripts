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
for fileName in glob.glob("/home/bilickiv/data/raw_dataset/duplicateFree/*.csv"):
    data = pd.read_csv(fileName, header=0, sep=';')
    print("processing:" + fileName)
    data['0'] =  pd.to_datetime(data['0'])
    data['0'] = data['0'].astype('int64')//10**9
    data['1'] =  pd.to_datetime(data['1'])
    data['1'] = data['1'].astype('int64')//10**9
    data['4'] =  pd.to_datetime(data['4'])
    data['4'] = data['4'].astype('int64')//10**9
    data['9'] =  pd.to_datetime(data['9'])
    data['9'] = data['9'].astype('int64')//10**9
    data['uploadDate'] =  pd.to_datetime(data['uploadDate'])
    data['uploadDate'] = data['uploadDate'].astype('int64')//10**9
    head, tail = os.path.split(fileName)
    data.to_csv("/home/bilickiv/data/raw_dataset/duplicateFree-timestamp/"+str(tail), sep='\t', encoding='utf-8')