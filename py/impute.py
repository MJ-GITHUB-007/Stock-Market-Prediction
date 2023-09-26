import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose

import os

curr_path = os.getcwd()
curr_ptr = 6

data = pd.read_csv(os.path.join(curr_path, 'data.csv'))
Index_list = data['Index'].value_counts().index
curr_Index = Index_list[curr_ptr]
data = data[data['Index'] == Index_list[curr_ptr]]
data['Date'] = pd.to_datetime(data['Date'])

st = data.iloc[0]['Date'].date()
ed = data.iloc[-1]['Date'].date()

date_range = pd.date_range(start=st, end=ed, freq='D')
data = pd.merge(data, pd.DataFrame({'Date': date_range}), on='Date', how='right')
data.fillna({'Index': curr_Index, 'Open': 0, 'High': 0, 'Low': 0, 'Close': 0, 'Adj Close': 0, 'Volume':0, 'CloseUSD':0}, inplace=True)

data.index = data.pop('Date')
print(data)
print(Index_list)

plt.plot(data['CloseUSD'])
plt.title('OPEN')
plt.xlabel('date')
plt.ylabel('value')
plt.show()
exit()

results = seasonal_decompose(data['CloseUSD'])
results.plot()
plt.show()
