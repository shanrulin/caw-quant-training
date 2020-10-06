#!/usr/bin/env python
# coding: utf-8

# In[4]:


### Required

#### 1. **Write a function** to download histohour data, parameters:

#fsym: BTC, tsym: USDT, start_time="2017-04-01", end_time="2020-04-01", e='binance'


import requests
import json
from datetime import datetime, timedelta
import pandas as pd

def histohour_data(fsym, tsym, start_time, end_time, e):
    fsym=fsym
    tsym=tsym
    e=e
    
    ### deal with timestamp and data points needed
    date_string_start = start_time 
    
    date_start = datetime.strptime(date_string_start, "%Y-%m-%d") # for calculation, to get total data points 
    timestamp_start = datetime.timestamp(date_start) # for update starting timestamp

    date_string_end = end_time 

    date_end = datetime.strptime(date_string_end, "%Y-%m-%d") # for calculation, to get total data points 
    timestamp_end = datetime.timestamp(date_end)

    ## get the time difference in btw
    td = date_start - date_end
    td_hours = int(round(td.total_seconds() / 3600))
    ## total data points from two dates
    remain=td_hours+1


    ### extract data
    url='https://min-api.cryptocompare.com/data/v2/histohour'
    limit=2000 # data limit from CryptoCompare. The number of total data downloaded is limit+1
    all_data=[] #store data, start w/ empty list
    
    
    if remain>(limit+1):
        while remain>(limit+1):
            ##setting
            parameters = {'fsym': fsym, 'tsym': tsym, 'limit':limit, 'toTs':timestamp_start, 'e':e}
        
            ## request data
            r_temp = requests.get(url, params=parameters)
        
            ## extend data list
            temp_ls=r_temp.json()['Data']['Data']
            temp_ls.extend(all_data)
            all_data=temp_ls
            #print("all_data:",len(all_data)) 
        
            ### update new starting point
            timestamp_temp=r_temp.json()['Data']['Data'][0]['time']
            ## timestamp to datetime
            date_form=datetime.fromtimestamp(timestamp_temp)
            ##subtract 1 hour
            update_date=date_form-timedelta(hours=1)
            ## date to timestamp
            timestamp_start = datetime.timestamp(update_date)
        
            ## update remain data needed
            remain=remain-(limit+1)
            
    ## when data ponts needed < (limit+1)
    limit=remain-1  
    #setting
    parameters = {'fsym': fsym, 'tsym': tsym, 'limit':limit, 'toTs':timestamp_start, 'e':e}
    
    r_temp = requests.get(url, params=parameters)
    temp_ls=r_temp.json()['Data']['Data']
    temp_ls.extend(all_data)
    all_data=temp_ls

    
    ### formating data    

    ##load data to dataframe
    df = pd.DataFrame(all_data)
    ## change timestamp to datetime
    df['time']=df['time'].apply(lambda x: datetime.fromtimestamp(x))
    ## drop two coulmns
    df2=df.drop(columns=['conversionType', 'conversionSymbol'])
    ## change column order
    new_cols_order=[ 'close', 'high', 'low', 'open', 'volumefrom', 'volumeto','time']
    df2=df2[new_cols_order]
    ## change column name
    df2.columns =[ 'close', 'high', 'low', 'open', 'volume', 'baseVolume','datetime']
    
    # write dataframe to a csv with header without index
    df2.to_csv(fsym+'-'+tsym+'-1h.csv', header=True, index=False)


# ### Write a class

# In[72]:


### Optional

#### 1. Modularize your code

import requests
import json
from datetime import datetime, timedelta
import pandas as pd


class Extract_data:
    
    def histohour_data(self, fsym, tsym, start_time, end_time, e):
        """Download histohour data from CryptoCompare"""
    
        fsym=fsym
        tsym=tsym
        e=e
    
        ### deal with timestamp and data points needed
        date_string_start = start_time 
    
        date_start = datetime.strptime(date_string_start, "%Y-%m-%d") # for calculation, to get total data points 
        timestamp_start = datetime.timestamp(date_start) # for update starting timestamp

        date_string_end = end_time 

        date_end = datetime.strptime(date_string_end, "%Y-%m-%d") # for calculation, to get total data points 
        timestamp_end = datetime.timestamp(date_end)

        ## get the time difference in btw
        td = date_start - date_end
        td_hours = int(round(td.total_seconds() / 3600))
        ## total data points from two dates
        remain=td_hours+1


        ### extract data
        url='https://min-api.cryptocompare.com/data/v2/histohour'
        limit=2000 # data limit from CryptoCompare. The number of total data downloaded is limit+1
        all_data=[] #store data, start w/ empty list
    
    
        if remain>(limit+1):
            while remain>(limit+1):
                ##setting
                parameters = {'fsym': fsym, 'tsym': tsym, 'limit':limit, 'toTs':timestamp_start, 'e':e}
        
                ## request data
                r_temp = requests.get(url, params=parameters)
        
                ## extend data list
                temp_ls=r_temp.json()['Data']['Data']
                temp_ls.extend(all_data)
                all_data=temp_ls
                #print("all_data:",len(all_data)) 
        
                ### update new starting point
                timestamp_temp=r_temp.json()['Data']['Data'][0]['time']
                ## timestamp to datetime
                date_form=datetime.fromtimestamp(timestamp_temp)
                ##subtract 1 hour
                update_date=date_form-timedelta(hours=1)
                ## date to timestamp
                timestamp_start = datetime.timestamp(update_date)
        
                ## update remain data needed
                remain=remain-(limit+1)
            
        ## when data ponts needed < (limit+1)
        limit=remain-1  
        #setting
        parameters = {'fsym': fsym, 'tsym': tsym, 'limit':limit, 'toTs':timestamp_start, 'e':e}
    
        r_temp = requests.get(url, params=parameters)
        temp_ls=r_temp.json()['Data']['Data']
        temp_ls.extend(all_data)
        all_data=temp_ls

    
        ### formating data    

        ##load data to dataframe
        df = pd.DataFrame(all_data)
        ## change timestamp to datetime
        df['time']=df['time'].apply(lambda x: datetime.fromtimestamp(x))
        ## drop two coulmns
        df2=df.drop(columns=['conversionType', 'conversionSymbol'])
        ## change column order
        new_cols_order=[ 'close', 'high', 'low', 'open', 'volumefrom', 'volumeto','time']
        df2=df2[new_cols_order]
        ## change column name
        df2.columns =[ 'close', 'high', 'low', 'open', 'volume', 'baseVolume','datetime']
    
        # write dataframe to a csv with header without index
        df2.to_csv(fsym+'-'+tsym+'-1h.csv', header=True, index=False)

        
#### 2. Add one more data endpoint

    def top_N_list(self,tsym2, n):
        """Download Toplist by Market Cap Full Data"""
        
        # The currency symbol
        tsym2=tsym2
        # Top N coins by their market cap
        n=n # range from 10 to 100
    
        parameters = {'tsym': tsym2, 'limit':n}
        url2="https://min-api.cryptocompare.com/data/top/mktcapfull"
    
        ## request data
        r_temp = requests.get(url2, params=parameters)
    
        data=r_temp.json()['Data']
        #print(json.dumps(data, indent=4))
        length=len(r_temp.json()['Data'])
        #print(length)
    
        ls=[] #store data
        for i in range(length):
            coin=data[i]['CoinInfo']['FullName']
            rating=data[i]['CoinInfo']['Rating']['Weiss']
            temp_data=[coin,rating]
            #print(temp_data)
            ls.append(temp_data)
        # Create the pandas DataFrame 
        df = pd.DataFrame(ls, columns = ['Coin Full Name', 'Rating']) 
    
        # write dataframe to a csv with header without index
        n_str=str(n)
        df.to_csv('Top_'+n_str+'_list.csv', header=True, index=False)
        

