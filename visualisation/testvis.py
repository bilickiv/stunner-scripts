import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pylab import *

df = pd.read_csv('/Users/bilickiv/tmpdata/simpleFirstOrderStat/day/enlFcThIWHo3blFBMGx0VnUxbTgrcjZKMExZZGhJeDJrN3VTTTBBOXdZOD0.csv', header=0, sep=';')
print(df.head())
df.plot()
show()