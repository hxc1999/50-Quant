import datetime
import pandas as pd
import numpy as np
import mysql.connector


################################# functions ######################################

def create_sec_tb(connection, mycursor):
    #DROP TABLE IF EXISTS securities_tb, sec_tb
    SQLStatement = """DROP TABLE IF EXISTS sec_tb;"""
    mycursor.execute(SQLStatement)
    SQLStatement = """DROP TABLE IF EXISTS securities_tb;"""
    mycursor.execute(SQLStatement)
    
    #create sec_tb (ths_id, comp_short_name_chi, comp_long_name_chi, comp_long_name_eng, industry, exchanges)
    SQLStatement = """CREATE TABLE sec_tb(
        ths_id VARCHAR(10) UNIQUE NOT NULL,
        comp_short_name_chi VARCHAR(50) NOT NULL,
        comp_long_name_chi VARCHAR(50) NOT NULL,
        comp_long_name_eng VARCHAR(50) NOT NULL,
        industry VARCHAR(50) NOT NULL,
        exchanges VARCHAR(50) NOT NULL
        );"""
        
    mycursor.execute(SQLStatement)
    connection.commit()
    print("CREATED NEW TABLE: sec_tb")    

def populate_sec_tb(connection, mycursor):
    #path = 'S:/INV/50 Quant/HK_Sec_Table.xlsx'
    path = 'D:/50 Quant/HK_Sec_Table.xlsx'
    S_table = pd.read_excel(path,sheet_name=0,header=0,converters={'ths_stock_code_hks':str})
    S_table = S_table.iloc[:,3:]    #take ths_stock_code_hks	ths_stock_short_name_hks	ths_corp_cn_name_hks	ths_corp_name_en_hks	ths_the_gics_industry_hk	ths_listed_exchange_hks
    one_column = S_table.iloc[:,0]
    #print(len(one_column))
    print(S_table.iloc[0,0])
    index = 0
    while index < len(one_column):
        ths_id = str(S_table.iloc[index,0])
        comp_short_name_chi = S_table.iloc[index,1]
        comp_long_name_chi = S_table.iloc[index,2]
        comp_long_name_eng = S_table.iloc[index,3]
        industry = S_table.iloc[index,4]
        exchanges = S_table.iloc[index,5]
        index += 1
        
        SQLStatement = """INSERT INTO sec_tb (ths_id, comp_short_name_chi, comp_long_name_chi, comp_long_name_eng, industry, exchanges) VALUES (%s,%s,%s,%s,%s,%s) 
        ON DUPLICATE KEY UPDATE 
        comp_short_name_chi = %s, 
        comp_long_name_chi = %s, 
        comp_long_name_eng = %s, 
        industry = %s, 
        exchanges = %s
        ;"""
        Values = (ths_id,
        comp_short_name_chi,
        comp_long_name_chi,
        comp_long_name_eng,
        industry,
        exchanges,
        comp_short_name_chi,
        comp_long_name_chi,
        comp_long_name_eng,
        industry,
        exchanges)
        
        mycursor.execute(SQLStatement,Values)
        connection.commit()
    print("INITIALIZED sec_tb")    


def create_hist_data_tb(connection, mycursor):
    SQLStatement = "SELECT ths_id FROM sec_tb;"
    mycursor.execute(SQLStatement)
    ths_id_list = mycursor.fetchall()
    for x in ths_id_list:
        tablename = x[0]+"_hist_data"
        SQLStatement = "DROP TABLE IF EXISTS " + tablename+ ";"
        mycursor.execute(SQLStatement)
        SQLStatement = """CREATE TABLE """ + tablename + """(
            ths_id VARCHAR(10) NOT NULL,
            data_Date Date UNIQUE NOT NULL,
            adj_Close DECIMAL(15,4) NOT NULL
            );"""
        mycursor.execute(SQLStatement)
        connection.commit()
        print("CREATED NEW TABLE: " + tablename)

def populate_hist_data_tb(connection, mycursor,sec_table_name):
    path = "D:/50 Quant/" + sec_table_name
    S_table = pd.read_excel(path,sheet_name=0,header=0,converters={'ths_stock_code_hks':str})
    S_table = S_table.iloc[:,3:]    #take ths_stock_code_hks	ths_stock_short_name_hks	ths_corp_cn_name_hks	ths_corp_name_en_hks	ths_the_gics_industry_hk	ths_listed_exchange_hks
    sec_list = S_table.iloc[:,0]

    #for each security, load their historical excel file

    i = 0
    while i < len(sec_list):
        #load excel data file
        data_path = "D:/50 Quant/Historical Data/" + sec_list[i]+"_hist_data_Tb.xlsx"
        hist_data_table = pd.read_excel(data_path)

        #set variables for SQL statement
        ths_id = sec_list[i]
        date_col_index = 1
        adj_close_col_index = 10
        date_column = hist_data_table.iloc[:,date_col_index]    
        adj_close_column = hist_data_table.iloc[:,adj_close_col_index]
        
        j = 0
        while j < len(date_column):
            #print(ths_id + "\t" + date_column[j] + '\t' +str(adj_close_column[j]))
            mysql_table_name = str(ths_id) + "_hist_data"
            SQLStatement = """INSERT INTO """ + mysql_table_name + """(ths_id, data_date, adj_close) VALUES ('""" + str(ths_id) +"""', '"""+str(date_column[j])+"""',"""+str(adj_close_column[j])+""") ON DUPLICATE KEY UPDATE ths_id = '"""+str(ths_id)+"""', data_date ='"""+str(date_column[j])+"""',adj_close = """+str(adj_close_column[j])+""";"""
            mycursor.execute(SQLStatement)
            connection.commit()
            j = j+1
        i=i+1
        print("POPULATED "+ths_id+"_hist_data_tb")

def update_hist_data(connection, mycursor, ths_id, update_date, adj_close_data):    
    mysql_table_name = str(ths_id) + "_hist_data"
    SQLStatement = """INSERT INTO """ + mysql_table_name + """(ths_id, data_date, adj_close) VALUES ('""" + str(ths_id) +"""', '"""+str(update_date)+"""',"""+str(adj_close_data)+""") ON DUPLICATE KEY UPDATE ths_id = '"""+str(ths_id)+"""', data_date ='"""+str(update_date)+"""',adj_close = """+str(adj_close_data)+""";"""
    mycursor.execute(SQLStatement)
    connection.commit()
    print_statement = "UPDATE\t" + ths_id + "\t" + update_date + "\t" + adj_close_data
    print(print_statement)

def load_hist_data_series(connection, mycursor, ths_id, start_date, end_date):
    mysql_table_name = str(ths_id) + "_hist_data"
    #SELECT * from 00001_hist_data WHERE data_Date BETWEEN {ts '2008-12-20 00:00:00'} AND {ts '2020-11-20 00:00:00'}
    SQLStatement = """SELECT * FROM """ + mysql_table_name + """ WHERE data_date BETWEEN {ts '""" + start_date + """ 00:00:00'} AND {ts '"""+str(end_date) + """ 00:00:00'};"""
    mycursor.execute(SQLStatement)
    SQLResponse = list(mycursor.fetchall())
    
    print_statement = "LOAD\t" + ths_id+"\t"+str(start_date)+"\t"+str(end_date)
    print(print_statement)
    return SQLResponse


###################### start of main code #################################
connection = mysql.connector.connect(
    host="localhost",
    #    host = "10.124.11.21",
    user="root",
    passwd="Tplhk123",
    #auth_plugin="caching_sha2_password",
    database="TestDB"
)
mycursor = connection.cursor()

#create database for the first

create_sec_tb(connection, mycursor)
populate_sec_tb(connection, mycursor)
create_hist_data_tb(connection, mycursor)
populate_hist_data_tb(connection, mycursor,"HK_Sec_Table.xlsx")
update_hist_data(connection, mycursor, "00001","2020-11-10", "666.66")
#return list of ths_id, date, adj_close 
return_series = load_hist_data_series(connection, mycursor, "00001", "2020-01-01","2020-11-12") 
print(return_series)


