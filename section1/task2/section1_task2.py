#!/usr/bin/env python
# coding: utf-8

# In[ ]:


### Required
#### 1. **Use Binance Python SDK** to get public data


## Write a script (.py file)

## (a) get candle data(aka kline, histo)
## (b) get transactions(aka trades)
## (c) get market depth(aka orderbook)

#Format the data the way you like using pandas, save them seperately as csv.


# In[1]:


from binance.client import Client
import pandas as pd
from datetime import datetime


## ref:https://stackoverflow.com/questions/43318383/converting-unix-timestamp-length-13-string-to-readable-date-in-python
def convert_to_date(timestamp_with_ms):
    '''convert timestamp in milliseconds to date'''
    dt = datetime.fromtimestamp(timestamp_with_ms / 1000)
    formatted_time = dt.isoformat(sep=' ', timespec='milliseconds')
    return formatted_time

def save_to_csv(df, filename):
    '''save dataframe to csv file'''
    # write dataframe to a csv with header without index
    df.to_csv(filename+'.csv', header=True, index=False)


# In[2]:


##(a) get historical kline data from any date range
client = Client("", "")

print("Fetch 12hour klines for ETHBTC from 31 Dec, 2019 to 1 May, 2020")
# return: list of OHLCV values (Open, High, Low, Close, Volume) return: list of OHLCV values
klines = client.get_historical_klines("ETHBTC", Client.KLINE_INTERVAL_12HOUR, "31 Dec, 2019", "1 May, 2020")


##load data to dataframe
df = pd.DataFrame(klines,columns=["Open time","Open","High", "Low","Close","Volume","Close time", \
                                "Quote asset volume","Number of trades","Taker buy base asset volume",\
                                "Taker buy quote asset volume","Ignore"])
#drop column
df = df.drop(columns=["Ignore"])

# conver timstamp to date
df['Open time'] = df['Open time'].apply(lambda x: convert_to_date(x))
df["Close time"] = df["Close time"].apply(lambda x: convert_to_date(x))

# save to csv
filename = 'klines'
save_to_csv(df, filename)


# In[8]:


df.head()


# In[3]:


## (b) get transactions(aka trades)

print("Get recent trades for BNBBTC")
trades = client.get_recent_trades(symbol='BNBBTC', limit=500)
df_trades = pd.DataFrame(trades)
df_trades["time"] = df_trades["time"].apply(lambda x: convert_to_date(x))

# save to csv
filename = 'transactions'
save_to_csv(df_trades, filename)


# In[9]:


df_trades.head()


# In[4]:


## (c) get market depth
# bids:buyers, asks:sellers

print("Get order book for BNBBTC")
depth = client.get_order_book(symbol='BNBBTC', limit=100)

# formating data for dataframe
format_ls = ['bids',"asks"]
for i in format_ls:
    for idx, e in enumerate(depth[i]):
        e.append(i)

# build data frame
df1 = pd.DataFrame(depth["bids"], columns=["Price", "QTY", "Bids or Asks"])
df2 = pd.DataFrame(depth["asks"], columns=["Price", "QTY", "Bids or Asks"])
df_order_book = df1.append(df2)


# save to csv
filename='order_book'
save_to_csv(df_order_book, filename)


# In[10]:


df_order_book.head()


# In[11]:


df_order_book.tail()


# In[5]:


#### Optional

#### 1. Trade in Binance by its python SDK

#- [ ] setup a binance account
#- [ ] setup its trading api(you need to create a api key, api secret pair to trade)
#- [ ] check python SDK, create a test order.

from binance.client import Client

# Created at Binance Not Binance.us.
# They are different, so your frist attempt using the key and secret from Binance.us don't work
api_key='FbMo8RNCKlQl2yHsZ8nFWLeT2apXEZPagRZhIs3PoJrePErGQtBgIcGsKfB2q3Ld'
api_secret='YXMWxgfNYy1sJN6ofoA03LgX5aChY8BEeGsar3VxfjdmvN6SOAPrLKZvU0ryXz9o'

## Check the length of key and secret are correct
# print(len(api_key),len(api_secret)), output 64 64

client = Client(api_key, api_secret)
## check if client works
# client.get_account()

# place a test market buy order
order = client.create_test_order(
    symbol='BNBBTC',
    side=Client.SIDE_BUY,
    type=Client.ORDER_TYPE_MARKET,
    quantity=100)
