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

# 下载公司基础信息数据
if(thsLogin == 0 or thsLogin == -201):
    #根据股票池筛选，选择出股票序列
    #Code_list = THS_DataPool('block','2015-04-25;001005030','date:Y,thscode:Y,security_name:Y')
    #thsData = THS_Trans2DataFrame(Code_list)
    #codes=thsData.THSCODE
    #codeList = ','.join(codes)
    # print(codes)
    # codes.to_csv('code.csv')

    #根据给定股票代码，输出公司基础信息数据
    codeList = '0001.HK,0002.HK,0003.HK,0004.HK,0005.HK,0006.HK,0007.HK,0008.HK,0009.HK,0010.HK'
    indicator = 'ths_stock_code_hks;ths_stock_short_name_hks;ths_corp_cn_name_hks;ths_corp_name_en_hks;ths_the_gics_industry_hk;ths_listed_exchange_hks'
    indicator_para = ';;;;104;'
    st_date = '2020-11-26'
    ed_date = '2020-11-26'
    data = THS_DateSerial(codeList,indicator,indicator_para,'',st_date,ed_date)
    print(type(data)) 
    data3 = THS_Trans2DataFrame(data)
    print(data3.head(5))
    path = 'S:/INV/50 Quant/HK_Sec_table.xlsx'
    data3.to_excel(path)

else:    
 print("登录失败") 