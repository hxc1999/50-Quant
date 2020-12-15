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

#load setock list for specific sector listed in HK, sector params ---'Date;Sector ID'
def load_sector_stock_table_HK(sector_params,start_date,end_date):
    Code_list = THS_DataPool('block',sector_params,'date:Y,thscode:Y,security_name:Y')  # '2020-12-01;011003003003018043' sample
    thsData = THS_Trans2DataFrame(Code_list)
    codes=thsData.THSCODE
    codeList_HK_all = ','.join(codes)
    indicator = 'ths_stock_code_hks;ths_stock_short_name_hks;ths_corp_cn_name_hks;ths_corp_name_en_hks;ths_the_gics_industry_hk;ths_listed_exchange_hks;ths_amt_m_hks;ths_market_value_hks'
    indicator_para = ';;;;104;;;HKD'
    params = 'Days:Tradedays,Fill:Previous,Interval:D'
    st_date = start_date
    ed_date = end_date
    data = THS_DateSerial(codeList_HK_all,indicator,indicator_para,params,st_date,ed_date) 
    HK_all = THS_Trans2DataFrame(data)
    return HK_all;

#load setock list for specific sector listed in HK, sector params ---'Date;Sector ID'
def load_sector_stock_table_US(sector_params,start_date,end_date):
    Code_list_US = THS_DataPool('block',sector_params,'date:Y,thscode:Y,security_name:Y')   #'2020-12-01;161004_20302010'
    thsData_US = THS_Trans2DataFrame(Code_list_US)
    codes_US=thsData_US.THSCODE
    codeList_US_all = ','.join(codes_US)
    print(codeList_US_all)
    indicator_US = 'ths_stock_code_uss;ths_stock_short_name_uss;ths_corp_cn_name_uss;ths_corp_name_en_uss;ths_the_gics_industry_uss;ths_listed_exchange_uss;ths_vol_m_uss;ths_market_value_uss'
    indicator_para_US = ';;;;103;;;USD'
    params_US = 'Days:Tradedays,Fill:Previous,Interval:D'
    st_date = start_date
    ed_date = end_date
    data_US = THS_DateSerial(codeList_US_all,indicator_US,indicator_para_US,params_US,st_date,ed_date) 
    US_all= THS_Trans2DataFrame(data_US)
    return US_all;

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

#load setock dict for specific sector listed in HK, sector params ---'Date;Sector ID'
def load_sector_stock_dict_HK(sector_params,start_date,end_date):
    Code_list = THS_DataPool('block',sector_params,'date:Y,thscode:Y,security_name:Y')  # '2020-12-01;011003003003018043' sample
    thsData = THS_Trans2DataFrame(Code_list)
    codes=thsData.THSCODE
    codeList_HK_all = ','.join(codes)
    indicator = 'ths_stock_code_hks;ths_stock_short_name_hks;ths_corp_cn_name_hks;ths_corp_name_en_hks;ths_the_gics_industry_hk;ths_listed_exchange_hks;ths_amt_m_hks;ths_market_value_hks'
    indicator_para = ';;;;104;;;HKD'
    params = 'Days:Tradedays,Fill:Previous,Interval:D'
    st_date = start_date
    ed_date = end_date
    data = THS_DateSerial(codeList_HK_all,indicator,indicator_para,params,st_date,ed_date) 
    HK_all = THS_Trans2DataFrame(data)
    securities_list_values_HK = HK_all_airline['thscode'].tolist()
    securities_list_key_HK = HK_all_airline['ths_stock_code_hks'].tolist()
    securities_list_dict_HK = dict(zip(securities_list_key_HK,securities_list_values_HK))
    return securities_list_dict_HK;

#load setock dict for specific sector listed in HK, sector params ---'Date;Sector ID'
def load_sector_stock_dict_US(sector_params,start_date,end_date):
    Code_list_US = THS_DataPool('block',sector_params,'date:Y,thscode:Y,security_name:Y')   #'2020-12-01;161004_20302010'
    thsData_US = THS_Trans2DataFrame(Code_list_US)
    codes_US=thsData_US.THSCODE
    codeList_US_all = ','.join(codes_US)
    #print(codeList_US_all)
    indicator_US = 'ths_stock_code_uss;ths_stock_short_name_uss;ths_corp_cn_name_uss;ths_corp_name_en_uss;ths_the_gics_industry_uss;ths_listed_exchange_uss;ths_vol_m_uss;ths_market_value_uss'
    indicator_para_US = ';;;;103;;;USD'
    params_US = 'Days:Tradedays,Fill:Previous,Interval:D'
    st_date = start_date
    ed_date = end_date
    data_US = THS_DateSerial(codeList_US_all,indicator_US,indicator_para_US,params_US,st_date,ed_date) 
    US_all= THS_Trans2DataFrame(data_US)
    securities_list_values_US = US_all['thscode'].tolist()
    securities_list_key_US = US_all['ths_stock_code_uss'].tolist()
    securities_list_dict_US = dict(zip(securities_list_key_US,securities_list_values_US))
    return securities_list_dict_US;

# export single securities his price table from certain HK stock dict
def export_single_sec_his_HK_mass(path_save,securities_list_dict_HK,start_date,end_date):
    list_HK = pd.DataFrame()
    for key, value in securities_list_dict_HK.items():
        list_HK= load_security_price_data_HK(value,start_date,end_date)
        excel_filepath = path_save
        excel_header = key
        write = pd.ExcelWriter(excel_filepath + '\\'+excel_header + '_Hist_price_Tb.xlsx')
        list_HK.to_excel(write)
        write.save()

# export single securities his price table from certain US stock dict
def export_single_sec_his_US_mass(path_save,securities_list_dict_US,start_date,end_date):    
    list_US = pd.DataFrame()
    for key, value in securities_list_dict_US.items():
        list_US= load_security_price_data_US(value,start_date,end_date)
        excel_filepath = path_save
        excel_header = key
        write = pd.ExcelWriter(excel_filepath + '\\'+excel_header + '_Hist_price_Tb.xlsx')
        list_US.to_excel(write)
        write.save()


def load_sector_mv(sector_ID,date):
    code_data = THS_BD(sector_ID,'ths_mv_total_block',date)
    gics_one = code_data.data
    return gics_one;

ss = load_sector_mv('161004_20302010','2020-12-07')
print(ss)

