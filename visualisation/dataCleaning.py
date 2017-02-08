import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
from pandas.tools.plotting import scatter_matrix

variance = pd.read_csv('/Users/Vilmos/tmpdata/data_cleaning/variance.txt', header=0, sep=' ')
print(variance.head[10])

ax1 =  variance.filter(items=['sum(count)']).plot(kind='hist', title='15 minute long window',logy = True, logx = True)
fig1 = ax1.get_figure()
fig1.savefig('/Users/Vilmos/tmpdata/data_cleaning/x.png')
fig1.show()