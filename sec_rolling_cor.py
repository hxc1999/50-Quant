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

path3 = 'S:/INV/50 Quant/sec_daily_return_table_year.xlsx'
sec_daily_return_table = pd.read_excel(path3).drop(['Number'],axis=1)
trade_date = sec_daily_return_table['time'].reset_index(drop=True)
sec_daily_return_table.index = sec_daily_return_table['time']
sec_daily_return_table = sec_daily_return_table.drop(['time'],axis=1)
print(sec_daily_return_table)
corr1 = sec_daily_return_table.corr().sum().sum()/444
print(corr1)


index = 0
sample_size = 10
corr1_sum_list = []
corr1_average_list=[]

while index < (len(sec_daily_return_table) - sample_size + 1):
    sample_df = sec_daily_return_table.iloc[index:(index+sample_size)].reset_index(drop=True)
    #contruct correlation matrix
    corr1_sum = (sample_df.corr().sum().sum() - len(sample_df.columns)) / 2 
    corr1_average = corr1_sum / (len(sample_df.columns) * (len(sample_df.columns) -1) /2)
    #print(corr1_sum, corr1_average)
    corr1_sum_list.append(corr1_sum)
    corr1_average_list.append(corr1_average)
    index = index +1

print(corr1_sum_list, corr1_average_list)
data_table = pd.DataFrame()
data_table['Corr_Sum'] = corr1_sum_list
data_table['Corr_Average'] = corr1_average_list
data_table['time']=trade_date.iloc[sample_size-1:].tolist()
print(data_table)
data_table.index = data_table['time']
data_table2 = data_table.iloc[:,0:2]
print(data_table2)

path4 = 'S:/INV/50 Quant/sec_rolling_corr_table_year.xlsx'
data_table2.to_excel(path4)


    