# -*- coding: utf-8 -*-

import mysql
import mysql.connector
import iFinDPy
import datetime
import pandas as pd

#def getYesterday():
    #today = datetime.date.today()
    #today=datetime.date.today() 
    #oneday=datetime.timedelta(days=1) 
    #yesterday=today-oneday  
    #return yesterday

#同花顺下载数据

from iFinDPy import *
#用户在使用时请修改成自己的账号和密码
thsLogin = THS_iFinDLogin("mxtz038","701253")

# 香港市场股票与美国市场股票要分开
if(thsLogin == 0 or thsLogin == -201):
    stock_ticker = '0001.HK'
    indicator1 = 'ths_stock_code_hks;ths_stock_short_name_hks;ths_corp_cn_name_hks;ths_corp_name_en_hks;ths_the_gics_industry_hk;ths_listed_exchange_hks;ths_close_price_hks'
    indicator_para1 = ';;;;104;;101'
    indicator2 = 'ths_close_price_hks'
    indicator_para2 = '100'
    params = 'Days:Tradedays,Fill:Previous,Interval:D'
    st_date = '2020-11-23'
    ed_date = '2020-11-23'
    data1 = THS_DateSerial(stock_ticker,indicator1,indicator_para1,params,st_date,ed_date,True)
    data2 = THS_DateSerial(stock_ticker,indicator2,indicator_para2,params,st_date,ed_date,True)
    data3 = THS_Trans2DataFrame(data1)
    print(data3.head(5))
    data4 = THS_Trans2DataFrame(data2)
    print(data4.head(5))
    data5 = pd.merge(data3,data4,how='outer',on=['time','thscode'],suffixes=('_Rehabilitation','_Non')) 
    print(data5.head(10))   
    path_folder = 'S:/INV/50 Quant/'
    path_security_name = 
    #data5.to_excel(path)

else:    
 print("登录失败") 