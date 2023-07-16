#!/usr/bin/python

import time, datetime, sys, os, requests
os.environ[ 'MPLCONFIGDIR' ] = '/tmp/'
import pandas as pd
pd.options.mode.chained_assignment = None
os.chdir("/home/ec2-user/data")

# Only run on weekdays
st_day = datetime.datetime.today().weekday()
if (st_day == 6) | (st_day == 0):
    sys.exit()

print("SHORT - START: ", datetime.datetime.now().time())

dt_ymd = datetime.datetime.now() + datetime.timedelta(days=-1)
ud = dt_ymd.strftime('%Y-%m-%d').replace("-","")
#ud = '20211112'
print(ud)
#sys.exit()

import requests, csv
ulist = ['CNMS']
sd = pd.DataFrame()

for u in ulist:
    print(u,' ',ud)
    url = "https://cdn.finra.org/equity/regsho/daily/"+u+"shvol"+ud+".txt"
    #print(url)
    c = requests.get(url)
    c = c.text.replace(",","").replace("|",",").replace("'","")
    #print(c)
    fn = 'short/'+u+'.txt'
    f = open(fn, 'w+')
    f.write(c)
    f.close()
    x = pd.read_csv(fn,dtype='str',header=0,skipfooter=1,encoding='utf_8',quoting=3,engine='python')
    #print(x.head())
    sd = sd.append(x, ignore_index=True, sort=False)
    time.sleep(2)

#print(sd.head())
#print(sd.tail())
#print(sd.info())
#sys.exit()

sd.dropna(inplace=True)
#print(sd.head())
sd = sd.rename(columns={'Date':'date','Symbol':'symbol','ShortVolume':'short','ShortExemptVolume':'short_exempt','TotalVolume':'long','Market':'market'})
#print(sd.head())

sd = sd[(~sd['symbol'].str.contains("/")) & (~sd['short'].str.contains("/")) & (~sd['long'].str.contains("/"))]
sd['long'] = sd.long.astype('float')
sd['short'] = sd.short.astype('float')
sd['short_exempt'] = sd.short_exempt.astype('float')
sd = sd[(sd['long']>0) & (sd['short']>=0)]

sh = sd.groupby(['symbol','date'])[['short','short_exempt','long']].agg('sum')
sh = sh.reset_index()

def datecon(x):
    x = str(x)
    x = x[:4]+'-'+x[4:6]+'-'+x[6:8]
    return x

sh['date'] = sh['date'].apply(datecon)
sh['date'] = pd.to_datetime(sh['date'])

sh = sh[['symbol','date','short','short_exempt','long']]

shp = pd.read_pickle('short.pkl')
shp = shp[['symbol','date','short','short_exempt','long']]

sh = shp.append(sh, ignore_index=True, sort=False)
sh.drop_duplicates(inplace=True)
sh = sh.sort_values('date')
sh.to_pickle('short.pkl')

print("SHORT - END:   ", datetime.datetime.now().time())
