import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pylab import *
from pandas.tools.plotting import scatter_matrix

df15MinuteHist = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/day-stat.csv', header=0, sep=';')
fig = plt.figure()
ax = plt.gca()
ax.plot(df15MinuteHist['publicIP'] ,df15MinuteHist['localIP'], 'o', c='blue', alpha=0.05, markeredgecolor='none')
ax.set_yscale('log')
ax.set_xscale('log')
#df15MinuteHist.plot.scatter(x='publicIP', y='localIP',logx = True, logy = True);
#scatter_matrix(df15MinuteHist, alpha=0.2, figsize=(6, 6), diagonal='kde')
#df15MinuteHist.filter(items=['publicIP','localIP']).plot(kind='hist', alpha=0.5,  bins=[0,1,2,3,4, 5, 10, 20, 30,100,200,400], title='15 minute long windows',logy = True, logx = True)
print(df15MinuteHist.describe())
#df15MinuteHist.hist()

df15AggMinuteHist = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/15minute-stat.csv', header=0, sep=';')
ax1 =  df15AggMinuteHist.filter(items=['publicIP','localIP']).plot(kind='hist',  bins=[0,1,2,3,4, 5, 10, 20, 30,100,200,400], title='15 minute long window',logy = True, logx = True)
fig1 = ax1.get_figure()
fig1.savefig('/Users/Vilmos/tmpdata/simpleFirstOrderStat/figures/PubvsLocalIP15minhist.png')

dfDayHist = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/day-stat.csv', header=0, sep=';')
ax2 =  dfDayHist.filter(items=['publicIP','localIP']).plot(kind='hist', alpha=0.5,  bins=[0, 5, 10, 20, 30, 40, 50, 100, 200,300, 400,1000],title='One day  long window', logy = True, logx = True)
fig2 = ax2.get_figure()
fig2.savefig('/Users/Vilmos/tmpdata/simpleFirstOrderStat/figures/PubvsLocalIPDayhist.png')

#df15MinuteKDE = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/15minute-stat.csv', header=0, sep=';')
#df15MinuteKDE.filter(items=['publicIP','localIP']).plot(kind='kde', alpha=0.5,   title='15 minute long windows', logx = True)

dfDayKDE = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/day-stat.csv', header=0, sep=';')
ax3 =  dfDayKDE.filter(items=['publicIP','localIP']).plot(kind='kde', alpha=0.5,  title='One day  long window', logx = True)
fig3 = ax3.get_figure()
fig3.savefig('/Users/Vilmos/tmpdata/simpleFirstOrderStat/figures/PubvsLocalIPDayKDEt.png')

df15MinuteBoxplot = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/15minute-stat.csv', header=0, sep=';')
ax4 = df15MinuteBoxplot.filter(items=['publicIP','localIP','connectionMode','discoveryResultCode','networkType','carrier']).plot(kind='box',   title='15 minute long window', logy = True,figsize=(20,8))
fig4 = ax4.get_figure()
fig4.savefig('/Users/Vilmos/tmpdata/simpleFirstOrderStat/figures/boxAll15min.png')

dfDayBoxplot = pd.read_csv('/Users/Vilmos/tmpdata/simpleFirstOrderStat/agg/day-stat.csv', header=0, sep=';')
ax5 = dfDayBoxplot.filter(items=['publicIP','localIP','connectionMode','discoveryResultCode','networkType','carrier']).plot(kind='box',  title='One day  long window', logy = True, figsize=(20,8))
fig5 = ax5.get_figure()
fig5.savefig('/Users/Vilmos/tmpdata/simpleFirstOrderStat/figures/boxAllDay.png')
show()