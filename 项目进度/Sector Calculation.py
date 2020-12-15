# Stragetic 1 #
"""
-*- coding: utf-8 -*-
Created on Fri Nov 27 15:27:08 2020
"""
from iFinDPy  import *
import numpy as np
import pandas as pd
import datetime
import time
#取消panda科学计数法,保留4位有效小数位.
pd.set_option('float_format', lambda x:'%.2f' % x)
#设置中文对齐,数值等宽对齐.
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 500) 

#获取股票数据库,thscode可以做参数传入
def get_stock(date):
    #基础数据-股票-所属GICS行业;股票代码;股票简称;上市交易所;总市值;收盘价;涨跌幅;成交量;区间涨跌幅;区间成交量-iFinD数据接口
    stock_datas = THS_BD('300033.SZ','ths_the_gics_industry_stock;ths_stock_code_stock;ths_stock_short_name_stock;ths_listing_exchange_stock;ths_market_value_stock;ths_close_price_stock;ths_chg_ratio_stock;ths_vol_stock;ths_int_chg_ratio_stock;ths_vol_int_stock','100,2020-11-27;;;;2020-11-27;2020-11-27,100,2020-11-27;2020-11-27;2020-11-27,100;2020-11-27,2020-11-27,100;2020-11-27,2020-11-27')

    #获取指标数据
    result = stock_datas.data;
    print(result)
    return;

#获取GICS数据,板块ID可以做参数传入
def get_gics_data():
    #获取GICS行业数据
    stock_datas = THS_DP('block','2020-11-27;001005010','date:Y,thscode:Y,security_name:Y')
    #获取指标数据
    result = stock_datas.data;
    codes = ','.join(result['THSCODE'])
    allcode_list = codes.split(",")
    for i in allcode_list:
        print(i);
        #通过个股数据，计算总市值
        code_data = THS_BD(i,'open,high,low','Interval:D,CPS:1,baseDate:1900-01-01,Currency:YSHB,fill:Previous','2019-08-07','2020-08-07',True)
                
    return;

if __name__ == '__main__':
    #登录
    login = THS_iFinDLogin("账号","密码");
    if (login == 0 or login == -201):
        pass;
    else:
        print("登录失败，错误码：",thsLogin); 
    date = time.strftime("%Y-%m-%d", time.localtime());
    result = get_stock(date)
    out = get_gics_data()
    time.sleep(3)
    #Restlt = get_historic_high_stock_history_data()

