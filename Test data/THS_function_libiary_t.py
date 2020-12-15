# -*- coding: utf-8 -*-

import mysql
import mysql.connector
import iFinDPy
import datetime
import pandas as pd


#同花顺下载数据

from iFinDPy import *
#用户在使用时请修改成自己的账号和密码
thsLogin = THS_iFinDLogin("mxtz038","701253")

# 香港市场股票与美国市场股票要分开
if(thsLogin == 0 or thsLogin == -201):
    pass;

else:    
 print("登录失败") 

#定义港股价格函数
def load_security_price_data_HK(ths_ticker, start_date, end_date):
    stock_ticker = ths_ticker             #'0939.HK,2388.HK'
    indicator1 = 'ths_stock_code_hks;ths_close_price_hks;ths_open_price_hks;ths_high_d_hks;ths_low_d_hks;ths_vol_hks;ths_amt_d_hks'
    indicator_para1 = ';100;100;100;100;1;'  #选择GICS的参数（行业分级）以及选择股票价格的复权（不复权）
    indicator2 = 'ths_close_price_hks'
    indicator_para2 = '101' #股票价格复权
    params = 'Days:Tradedays,Fill:Previous,Interval:D' #选择数据间隔、填充以及交易日
    st_date = start_date
    ed_date = end_date
    data1 = THS_DateSerial(stock_ticker,indicator1,indicator_para1,params,st_date,ed_date,True)
    data2 = THS_DateSerial(stock_ticker,indicator2,indicator_para2,params,st_date,ed_date,True)
    data3 = THS_Trans2DataFrame(data1) #输出后复权的数据 以DataFrame的形式
    data4 = THS_Trans2DataFrame(data2)  # 输出不复权的数据 以DataFrame的形式
    data5 = pd.merge(data3,data4,how='outer',on=['time','thscode'],suffixes=('_Non','_Rehabilitation')) #合并表格,包括复权以及不复权的价格
    return data5;

# 定义美股价格函数
def load_security_price_data_US(ths_ticker, start_date, end_date):
    stock_ticker_US = ths_ticker             #'ZNH.N'
    indicator1_US = 'ths_stock_code_uss;ths_close_price_uss;ths_open_price_uss;ths_high_price_uss;ths_low_uss;ths_vol_uss'
    indicator2 = 'ths_close_price_uss'
    indicator_para2 = '101' #股票价格复权
    indicator_para1_US = ';100;100;100;100;'  #选择GICS的参数（行业分级）以及选择股票价格的复权（不复权）
    params_US = 'Days:Tradedays,Fill:Previous,Interval:D' #选择数据间隔、填充以及交易日
    st_date = start_date
    ed_date = end_date
    data1_US = THS_DateSerial(stock_ticker_US,indicator1_US,indicator_para1_US,params_US,st_date,ed_date)
    data2 = THS_DateSerial(stock_ticker_US,indicator2,indicator_para2,params_US,st_date,ed_date,True)
    data3_US = THS_Trans2DataFrame(data1_US) #输出后复权的数据 以DataFrame的形式  
    data4 = THS_Trans2DataFrame(data2)  # 输出不复权的数据 以DataFrame的形式
    data5 = pd.merge(data3_US,data4,how='outer',on=['time','thscode'],suffixes=('_Non','_Rehabilitation')) #合并表格,包括复权以及不复权的价格
    return data5;


#下载全部港股航空股
Code_list = THS_DataPool('block','2020-12-01;011003003003018043','date:Y,thscode:Y,security_name:Y')
thsData = THS_Trans2DataFrame(Code_list)
codes=thsData.THSCODE
codeList_HK_all = ','.join(codes)
    
indicator = 'ths_stock_code_hks;ths_stock_short_name_hks;ths_corp_cn_name_hks;ths_corp_name_en_hks;ths_the_gics_industry_hk;ths_listed_exchange_hks;ths_amt_m_hks;ths_market_value_hks'
indicator_para = ';;;;104;;;HKD'
params = 'Days:Tradedays,Fill:Previous,Interval:D'
st_date = '2020-12-03'
ed_date = '2020-12-03'
data = THS_DateSerial(codeList_HK_all,indicator,indicator_para,params,st_date,ed_date) 
HK_all_airline = THS_Trans2DataFrame(data)
print(HK_all_airline)

#path_HK_airline = 'S:/INV/50 Quant/Historical Data folder/HK_airline.xlsx'
#HK_all_airline.to_excel(path_HK_airline)  # 输出excel表单

#下载全部美股航空股
Code_list_US = THS_DataPool('block','2020-12-01;161004_20302010','date:Y,thscode:Y,security_name:Y')
thsData_US = THS_Trans2DataFrame(Code_list_US)
codes_US=thsData_US.THSCODE
codeList_US_all = ','.join(codes_US)
print(codeList_US_all)
    
indicator_US = 'ths_stock_code_uss;ths_stock_short_name_uss;ths_corp_cn_name_uss;ths_corp_name_en_uss;ths_the_gics_industry_uss;ths_listed_exchange_uss;ths_vol_m_uss;ths_market_value_uss'
indicator_para_US = ';;;;103;;;USD'
params_US = 'Days:Tradedays,Fill:Previous,Interval:D'
st_date = '2020-12-03'
ed_date = '2020-12-03'
data_US = THS_DateSerial(codeList_US_all,indicator_US,indicator_para_US,params_US,st_date,ed_date) 
US_all_airline = THS_Trans2DataFrame(data_US)
print(US_all_airline)

#path_HK_airline = 'S:/INV/50 Quant/Historical Data folder/HK_airline.xlsx'
#HK_all_airline.to_excel(path_HK_airline)  # 输出excel表单

#Check function 
load_ZHNN = load_security_price_data_US('ZNH.N','2020-12-03','2020-12-03')
load_0933 = load_security_price_data_HK('1055.HK','2020-12-03','2020-12-03')
print(load_0933)
print(load_ZHNN)

# create US code dictionary
securities_list_values_US = US_all_airline['thscode'].tolist()
securities_list_key_US = US_all_airline['ths_stock_code_uss'].tolist()
securities_list_dict_US = dict(zip(securities_list_key_US,securities_list_values_US))
print(securities_list_dict_US)

# create HK code dictionary
securities_list_values_HK = HK_all_airline['thscode'].tolist()
securities_list_key_HK = HK_all_airline['ths_stock_code_hks'].tolist()
securities_list_dict_HK = dict(zip(securities_list_key_HK,securities_list_values_HK))
print(securities_list_dict_HK)

start_date = '2019-11-01'
end_date = '2020-12-03'

print(securities_list_values_US)





airline_HK = pd.DataFrame()
for key, value in securities_list_dict_HK.items():
    airline_HK= load_security_price_data_HK(value,start_date,end_date)
    excel_filepath = 'S:/INV/50 Quant/Historical Data folder/airline'
    excel_header = key
    write = pd.ExcelWriter(excel_filepath + '\\'+excel_header + '_Hist_price_Tb.xlsx')
    airline_HK.to_excel(write)
    write.save()
     
airline_US = pd.DataFrame()
for key, value in securities_list_dict_US.items():
    airline_US= load_security_price_data_US(value,start_date,end_date)
    excel_filepath = 'S:/INV/50 Quant/Historical Data folder/airline'
    excel_header = key
    write = pd.ExcelWriter(excel_filepath + '\\'+excel_header + '_Hist_price_Tb.xlsx')
    airline_US.to_excel(write)
    write.save()

