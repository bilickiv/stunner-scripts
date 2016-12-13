import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pylab import *

df = pd.read_csv('/Users/bilickiv/tmpdata/simpleFirstOrderStat/agg/day-stat.csv', header=0, sep=';')
print(df.head())
df.plot()
show()