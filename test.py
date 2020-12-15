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

def Load_Sec_Daily_Return_HK(ths_ticker, start_date, end_date):
    stock_ticker = ths_ticker             #'0939.HK,2388.HK'
    indicator1 = 'ths_stock_code_hks;ths_chg_ratio_hks'
    indicator_para1 = ';'  #选择GICS的参数（行业分级）以及选择股票价格的复权（不复权）
    params = 'Days:Tradedays,Fill:Previous,Interval:D' #选择数据间隔、填充以及交易日
    st_date = start_date
    ed_date = end_date
    data1 = THS_DateSerial(stock_ticker,indicator1,indicator_para1,params,st_date,ed_date,True)
    data2 = THS_Trans2DataFrame(data1) #输出后复权的数据 以DataFrame的形式
    data2.columns = ['time','thscode','ths_stock_code_hks',ths_ticker]
    return data2;


ss = Load_Sec_Daily_Return_HK('0939.hk','2020-12-07','2020-12-11')
print(ss)


