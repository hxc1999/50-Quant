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

    #根据给定股票代码，批量输出公司基础信息数据
    path = 'S:/INV/50 Quant/HK_Sec_table.xlsx' #可手动或批量从同花顺中获得公司代码存储于excel中，未来可添加或修改
    HK_Sec_table = pd.read_excel(path)
    print(HK_Sec_table)
    stock_list = HK_Sec_table['thscode']#提取股票代码
    print(stock_list)
    stock_ticker = stock_list.tolist() #将股票代码存入列表内，作为变量投入数据提取公式中
    print(stock_ticker)
    indicator1 = 'ths_stock_code_hks;ths_stock_short_name_hks;ths_corp_cn_name_hks;ths_corp_name_en_hks;ths_the_gics_industry_hk;ths_listed_exchange_hks;ths_close_price_hks' #指标
    indicator_para1 = ';;;;104;;101'  #选择GICS的参数（行业分级）以及选择股票价格的复权（后复权）
    indicator2 = 'ths_close_price_hks'
    indicator_para2 = '100' #股票价格不复权
    params = 'Days:Tradedays,Fill:Previous,Interval:D' #选择数据间隔、填充以及交易日
    st_date = '2020-11-10'
    ed_date = '2020-11-23'
    data1 = THS_DateSerial(stock_ticker,indicator1,indicator_para1,params,st_date,ed_date,True)
    data2 = THS_DateSerial(stock_ticker,indicator2,indicator_para2,params,st_date,ed_date,True)
    data3 = THS_Trans2DataFrame(data1) #输出后复权的数据 以DataFrame的形式
    print(data3.head(5))
    data4 = THS_Trans2DataFrame(data2)  # 输出不复权的数据 以DataFrame的形式
    print(data4.head(5))
    data5 = pd.merge(data3,data4,how='outer',on=['time','thscode'],suffixes=('_Rehabilitation','_Non')) #合并表格,包括复权以及不复权的价格
    print(data5.head(10))   


   

else:    
 print("登录失败") 