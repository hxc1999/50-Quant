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
    stock_ticker = '2318.HK,1530.HK'
    indicator = 'ths_stock_code_hks;ths_stock_short_name_hks;ths_corp_cn_name_hks;ths_corp_name_en_hks;ths_the_gics_industry_hk;ths_listed_exchange_hks'
    indicator_para = ';;;;104;'
    st_date = '2020-11-23'
    ed_date = '2020-11-25'
    data = THS_DateSerial(stock_ticker,indicator,indicator_para,'',st_date,ed_date)
    print(type(data)) 
    data3 = THS_Trans2DataFrame(data)
    print(type(data3))
    path = 'S:/INV/50 Quant/hklist3.xlsx'
    data3.to_excel(path)

else:    
 print("登录失败") 

    
#database stuff
db = mysql.connector.connect(host="localhost",user="root",passwd="Tplhk123",auth_plugin="caching_sha2_password",database="testdatabase")
mycursor=db.cursor()

mycursor.execute("show databases;")