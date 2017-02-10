import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
from pylab import *
from pandas.tools.plotting import scatter_matrix
import matplotlib.ticker as ticker
import numpy as np
from scipy.stats import kendalltau
import seaborn as sns
sns.set_style("whitegrid")

df15MinuteHist = pd.read_csv('/Users/bilickiv/tmpdata/simpleFirstOrderStat/agg/day-stat.csv', header=0, sep=';')
#sns.jointplot(x, y, kind="hex", stat_func=kendalltau, color="#4CB391")
sns.boxplot(data=df15MinuteHist.filter(items=['publicIP','localIP']))
#df15MinuteHist.hist()

#df15AggMinuteHist = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/15minute-stat.csv', header=0, sep=';')
#df15AggMinuteHist.plot(kind='hist',  bins=[0,1,2,3,4, 5, 10, 20, 30,100,200,400], title='15 minute long windows',logy = True, logx = True)


#dfDayHist = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/day-stat.csv', header=0, sep=';')
#dfDayHist.filter(items=['publicIP','localIP']).plot(kind='hist', alpha=0.5,  bins=[0, 5, 10, 20, 30, 40, 50, 100, 200,300, 400,1000],title='One day  long windows', logy = True, logx = True)

#df15MinuteKDE = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/15minute-stat.csv', header=0, sep=';')
#df15MinuteKDE.filter(items=['publicIP','localIP']).plot(kind='kde', alpha=0.5,   title='15 minute long windows', logx = True)

#dfDayKDE = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/day-stat.csv', header=0, sep=';')
#dfDayKDE.filter(items=['publicIP','localIP']).plot(kind='kde', alpha=0.5,  title='One day  long windows', logx = True)

#df15MinuteBoxplot = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/15minute-stat.csv', header=0, sep=';')
#df15MinuteBoxplot.filter(items=['publicIP','localIP','connectionMode','discoveryResultCode','networkType','carrier']).plot(kind='box',   title='15 minute long windows', logy = True)

#dfDayBoxplot = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/day-stat.csv', header=0, sep=';')
#dfDayBoxplot.filter(items=['publicIP','localIP','connectionMode','discoveryResultCode','networkType','carrier']).plot(kind='box',  title='One day  long windows', logy = True)

#show()