 # -*- coding: utf-8 -*-

import mysql
import mysql.connector
import iFinDPy
import datetime
import pandas as pd
import math
import statistics
import scipy.stats as stats
import scipy
from scipy.stats import pearsonr
import sys
import matplotlib.pyplot as plt


#同花顺下载数据

from iFinDPy import *
#用户在使用时请修改成自己的账号和密码
thsLogin = THS_iFinDLogin("mxtz038","701253")

# 香港市场股票与美国市场股票要分开
if(thsLogin == 0 or thsLogin == -201):
    pass;
else:    
 print("登录失败") 

def load_sector_mv(sector_ID,date):
    code_data = THS_BD(sector_ID,'ths_mv_total_block',date)
    gics_one = code_data.data
    return gics_one;

def sector_mv_his_HK(sector_ID,start_date,end_date):
    HK_trade_date = THS_DateQuery('HKEX','dateType:0,period:D,dateFormat:0',start_date,end_date)['tables']['time']
    data = pd.DataFrame()
    for date in HK_trade_date:
        data1 = load_sector_mv(sector_ID,date)
        data = data.append(data1)
    data.insert(0,'time',HK_trade_date)
    data['sector_daily_return']=data['ths_mv_total_block'].pct_change()
    return data;

#sss = sector_mv_his_HK('011003003003018043','2020-12-01','2020-12-08')
#print(sss)

#'161004_20302010'
def sector_mv_his_US(sector_ID,start_date,end_date):
    US_trade_date = THS_DateQuery('AMEX','dateType:0,period:D,dateFormat:0',start_date,end_date)['tables']['time']
    data = pd.DataFrame()
    for date in US_trade_date:
        data1 = load_sector_mv(sector_ID,date)
        data = data.append(data1)
    data.insert(0,'time',US_trade_date)
    data['sector_daily_return']=data['ths_mv_total_block'].pct_change() 
    return data;

US_airline = sector_mv_his_US('161004_20302010','2020-12-01','2020-12-07')
print(US_airline)

def sector_compared(HK_sector_ID,US_sector_ID,start_date,end_date):
    HK_sector = sector_mv_his_HK(HK_sector_ID,start_date,end_date).fillna(0)
    US_sector = sector_mv_his_US(US_sector_ID,start_date,end_date).fillna(0)
    sector_comb_mv = pd.merge(HK_sector,US_sector,on=['time'],suffixes=('_HK','_US'))
    sector_comb_return = sector_comb_mv[['time','sector_daily_return_HK','sector_daily_return_US']]
    return sector_comb_return;

def sector_rolling_corr(HK_sector_ID,US_sector_ID,start_date,end_date,rolling_window):
    HK_sector = sector_mv_his_HK(HK_sector_ID,start_date,end_date).fillna(0)['sector_daily_return'].reset_index(drop=True)
    US_sector = sector_mv_his_US(US_sector_ID,start_date,end_date).fillna(0)['sector_daily_return'].reset_index(drop=True)
    correlation = HK_sector.rolling(rolling_window).corr(US_sector)
    HK_sector = sector_mv_his_HK(HK_sector_ID,start_date,end_date)
    US_sector = sector_mv_his_US(US_sector_ID,start_date,end_date)
    sector_comb_mv = pd.merge(HK_sector,US_sector,on=['time'],suffixes=('_HK','_US'))
    trade_date = sector_comb_mv['time']
    corr2 = pd.concat([trade_date,correlation],axis=1)
    corr2.columns=['time','rolling_corr']
    return corr2;

def sector_corr(HK_sector_ID,US_sector_ID,start_date,end_date):
    HK_sector = sector_mv_his_HK(HK_sector_ID,start_date,end_date).fillna(method='pad',inplace=True)
    US_sector = sector_mv_his_US(US_sector_ID,start_date,end_date).fillna(method='pad',inplace=True)
    correlation = HK_sector['sector_daily_return'].corr(US_sector['sector_daily_return'])
    return correlation;

#corr = sector_rolling_corr('011003003003018043','161004_20302010','2020-11-25','2020-12-07',5)
#print(corr)
 # -*- coding: utf-8 -*-

import mysql
import mysql.connector
import iFinDPy
import datetime
import pandas as pd
import math
import statistics
import scipy.stats as stats
import scipy
from scipy.stats import pearsonr
import sys
import matplotlib.pyplot as plt


#同花顺下载数据

from iFinDPy import *
#用户在使用时请修改成自己的账号和密码
thsLogin = THS_iFinDLogin("mxtz038","701253")

# 香港市场股票与美国市场股票要分开
if(thsLogin == 0 or thsLogin == -201):
    pass;
else:    
 print("登录失败") 
#corr.to_excel(path)

path_sector_id = 'S:/INV/50 Quant/Sector_ID.xlsx'
sector_id_table = pd.read_excel(path_sector_id,sheet_name='US_GICS_1')
print(sector_id_table)
sector_id_list = sector_id_table['GICS_1_ID'].tolist()
print(sector_id_list)

start_date = '2020-12-01'
end_date = '2020-12-08'
sector_ID = '161004_20303010'
daily_mv = sector_mv_his_US(sector_ID,start_date,end_date)
print(daily_mv)






daily_change = pd.DataFrame()
for sector_ID in sector_id_list:
    daily_mv = sector_mv_his_US(sector_ID,start_date,end_date)
    daily_change = daily_change.append(daily_mv)
print(daily_change)