import pandas as pd
import numpy as np
import mysql
import math
import statistics
import scipy.stats as stats
import scipy
from scipy.stats import pearsonr


def header(msg):
    print('-'*50)
    print('[ '+msg+' ]')

def calc_daily_return_list(A_adj_close_list):
    A_daily_return_list = []
    index = 0
    while index < (len(A_adj_close_list)-1):
        A_daily_return_list.append(A_adj_close_list[index+1]/A_adj_close_list[index] -1)
        index = index + 1
    return(A_daily_return_list)

def daily_rolling_correlation(combined_daily_rtn_df, sample_period):
    correlation_list_df = pd.DataFrame()
    correlation_list = []
    date_list=[]
    index = 0
    while index < (len(combined_daily_rtn_df) - sample_period+1):
        series_A = []
        series_B = []
        series_A = combined_daily_rtn_df['A Daily Return'].to_list()
        series_A = series_A[index:(index+sample_period)]
        series_B = combined_daily_rtn_df['B Daily Return'].to_list()
        series_B = series_B[index:(index+ sample_period)]
        corr,_ = pearsonr(series_A, series_B)
        #print(series_A)
        #print(series_B)
        #sample_df = combined_daily_rtn_df['A Daily Return',
        # 'B Daily Return'].loc[index:(index+sample_period)]
        correlation_list.append(corr)
        index = index + 1
    date_list = combined_daily_rtn_df['Date'].to_list()
    correlation_list_df['Date'] = date_list[(sample_period-1):(len(correlation_list)+sample_period-1)]
    correlation_list_df['Corr Coef'] = correlation_list
    return(correlation_list_df)

def daily_cross_correlation(combined_daily_rtn_df, sample_period, lag_period):
    correlation_list = []
    correlation_list_df = pd.DataFrame()
    index = 0
    while index < (len(combined_daily_rtn_df) - sample_period - lag_period +1):
        series_A = []
        series_B = []
        series_A = combined_daily_rtn_df['A Daily Return']
        series_A = series_A[index:(index+sample_period)]
        series_B = combined_daily_rtn_df['B Daily Return']
        series_B = series_B[( index+ lag_period):(index+ sample_period + lag_period)]
        corr,_ = pearsonr(series_A, series_B)
        #sample_df = combined_daily_rtn_df['A Daily Return','B Daily Return'].loc[index:(index+sample_period)]
        correlation_list.append(corr)
        index = index + 1
    date_list = combined_daily_rtn_df['Date'].to_list()
    correlation_list_df['Date'] = date_list[sample_period:(len(correlation_list)+sample_period)]
    correlation_list_df['AutoCorr Coef'] = correlation_list
    return(correlation_list_df)


############################### read stock list ##################################
HK_stock_list = ['00293', '00670', '00753', '01055']
US_stock_list = ['ZNH', 'VLRS', 'UAL', 'SKYW', 'SAVE', 'RYAAY', 'PAC', 'MESA', 'LUV', 'JBLU', 'HA', 'GOL', 'DAL', 'CPA', 'CEA', 'AZUL', 'ALK', 'AAL']

################## Initialize Sector Data Table ####################
HK_sector_stock_price_df = pd.DataFrame()
HK_sector_stock_daily_rtn_df = pd.DataFrame()
US_sector_stock_price_df = pd.DataFrame()
US_sector_stock_daily_rtn_df = pd.DataFrame()
empty_column = [0.0000] * len(HK_sector_stock_price_df)
HK_sector_stock_price_df["Date"] = empty_column
empty_column = [0.0000] * len(US_sector_stock_price_df)
US_sector_stock_price_df["Date"] = empty_column
for x in HK_stock_list:
    HK_sector_stock_price_df[x] = empty_column 
for x in US_stock_list:
    US_sector_stock_price_df[x] = empty_column 

################## Load HK Sector Table Data ####################
for x in HK_stock_list:
    #load data from excel
    path = "D:/50 Quant/Historical Data/" + x + "_Hist_price_Tb.xlsx"
    hist_price_df = pd.read_excel(path)
    HK_sector_stock_price_df[x] = hist_price_df["ths_close_price_hks_Rehabilitation"] 
    HK_sector_stock_price_df["Date"] = hist_price_df["time"]

#HK_sector_stock_price_df['Cust_Index_HK'] = HK_sector_stock_price_df.mean(axis = 1)
#print(HK_sector_stock_price_df)

################## Load US Sector Table Data ####################
for x in US_stock_list:
    #load data from excel
    path = "D:/50 Quant/Historical Data/" + x + "_Hist_price_Tb.xlsx"
    hist_price_df = pd.read_excel(path)
    US_sector_stock_price_df[x] = hist_price_df["ths_close_price_uss_Rehabilitation"] 
    US_sector_stock_price_df["Date"] = hist_price_df["time"]

#US_sector_stock_price_df['Cust_Index_US'] = US_sector_stock_price_df.mean(axis = 1)
#US_sector_stock_daily_rtn_df['Cust_Index_US'] = US_sector_stock_daily_rtn_df.mean(axis =1)
#print(US_sector_stock_price_df)

############################## Merge Common Date #######################
combined_daily_price_df = pd.merge(HK_sector_stock_price_df, US_sector_stock_price_df, on="Date")
####### recombine new date with old price
print(combined_daily_price_df)

############################# reseparate and calculate index ####################
HK_sector_stock_price_df = pd.DataFrame()
HK_sector_stock_rtn_df = pd.DataFrame()
for x in HK_stock_list:
    #load data from excel
    HK_sector_stock_price_df[x] = combined_daily_price_df[x] 
    HK_sector_stock_price_df["Date"] = combined_daily_price_df['Date']
    HK_sector_stock_rtn_df[x] = calc_daily_return_list(combined_daily_price_df[x]) 
    
HK_sector_stock_rtn_df['Cust_Index_HK'] = HK_sector_stock_rtn_df.mean(axis = 1)
HK_sector_stock_rtn_df["Date"] = combined_daily_price_df['Date'].to_list()[1:]
print(HK_sector_stock_rtn_df)

US_sector_stock_price_df = pd.DataFrame()
US_sector_stock_rtn_df = pd.DataFrame()
for x in US_stock_list:
    #load data from excel
    US_sector_stock_price_df[x] = combined_daily_price_df[x] 
    US_sector_stock_price_df["Date"] = combined_daily_price_df['Date']
    US_sector_stock_rtn_df[x] = calc_daily_return_list(combined_daily_price_df[x]) 
    
US_sector_stock_rtn_df['Cust_Index_US'] = US_sector_stock_rtn_df.mean(axis = 1)
US_sector_stock_rtn_df["Date"] = combined_daily_price_df['Date'].to_list()[1:]
#print(US_sector_stock_rtn_df)

#print(combined_daily_price_df)
combined_daily_rtn_df = pd.DataFrame()
combined_daily_rtn_df["Date"] = combined_daily_price_df['Date'].to_list()[1:]
combined_daily_rtn_df["A Daily Return"] = HK_sector_stock_rtn_df['Cust_Index_HK'] 
combined_daily_rtn_df["B Daily Return"] = US_sector_stock_rtn_df['Cust_Index_US']
#print(combined_daily_rtn_df)

###################################### Analysis Part #################################################
##### rolling correlation 
path = "D:/50 Quant/Result/"

sample_period_list = [5,10,30]
for x in sample_period_list:
    sample_period = x
    lag_period = 0
    rolling_return_correlation_df = daily_rolling_correlation(combined_daily_rtn_df, sample_period)
    header("Rolling Correlation Between A and B")
#    print(str(sample_period) + "day")
    filename = str(sample_period)+"D_Rolling_Correlation_Lag_"+str(lag_period)+".xlsx"
    rolling_return_correlation_df.to_excel(path+filename)

##### rolling auto correlation 1 day lag 
sample_period_list = [5,10,30]
for x in sample_period_list:
    sample_period = x
    lag_period = 1 # A （t) vs B (t+1) if positive it's leading
    rolling_return_cross_correlation_df = daily_cross_correlation(combined_daily_rtn_df, sample_period, lag_period)
    header("Rolling Auto Correlation Between A and B")
    filename = str(sample_period)+"D_Auto_Correlation_Lag_"+str(lag_period)+".xlsx"
    rolling_return_cross_correlation_df.to_excel(path+filename)
    
##### rolling auto correlation -1 day lag 
sample_period_list = [5,10,30]
A_Daily_Return = combined_daily_rtn_df['A Daily Return']
B_Daily_Return = combined_daily_rtn_df['B Daily Return']
combined_daily_rtn_df['A Daily Return'] = B_Daily_Return
combined_daily_rtn_df['B Daily Return'] = A_Daily_Return

for x in sample_period_list:
    sample_period = x
    lag_period = 1 # A （t) vs B (t+1) if positive it's leading
    rolling_return_cross_correlation_df = daily_cross_correlation(combined_daily_rtn_df, sample_period, lag_period)
    header("Rolling Auto Correlation Between A and B")
    filename = str(sample_period)+"D_Auto_Correlation_Lag_"+str(-lag_period)+".xlsx"
    rolling_return_cross_correlation_df.to_excel(path+filename)

print("end of program")
##### daily autorrelation (1-2-3)
##### reversal analysis




