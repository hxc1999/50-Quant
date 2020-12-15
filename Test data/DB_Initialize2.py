import mysql
import mysql.connector
import datetime
import pandas as pd
import numpy as np


def initialize_hist_price_tb(mycursor, ths_id):
    print("inside initialize_hist_price_tb " + ths_id)
    #create sec_hist_price_tb
    tablename = ths_id + "_tb"
    SQLStatement = """DROP TABLE IF EXISTS """+ tablename + """;"""
    mycursor.execute(SQLStatement)

    SQLStatement = """CREATE TABLE """ + tablename + """(
        ths_id VARCHAR(5) UNIQUE NOT NULL,
        price_Date Datetime NOT NULL,
        Adj_Close DECIMAL(10,4) NOT NULL
    );"""
    mycursor.execute(SQLStatement)

def iniitalize_sec_tb(mycursor):
    print("inside initialize_sec_tb")
    path = 'S:/INV/50 Quant/A50.xlsx'
    S_table = pd.read_excel(path)
    S_table = S_table.iloc[:,3:]    #take ths_stock_code_hks	ths_stock_short_name_hks	ths_corp_cn_name_hks	ths_corp_name_en_hks	ths_the_gics_industry_hk	ths_listed_exchange_hks
    ths_id = S_table[i,0]
    comp_short_name_chi = S_table[i,1]
    comp_long_name_chi = S_table[i,2]
    comp_long_name_eng = S_table[i,3]
    industry = S_table[i,4]
    exchanges = S_table[i,5]
    
    
    
    print(s2.iloc[0,0])

try:
    connection = mysql.connector.connect(
        host="localhost",
    #    host = "10.124.11.21",
        user="root",
        passwd="Tplhk123",
        #auth_plugin="caching_sha2_password",
        database="TestDB"
    )

    mycursor= connection.cursor()

    mycursor.execute("SHOW DATABASES;")
    Sqlresponse = mycursor.fetchall()
    print(Sqlresponse)

    
   

    #read from csv for list of securities



 
    print("end")




except:
    print("Error somewhere")
finally:
    if (connection.is_connected()):
        connection.close()
        mycursor.close()
        print("MySQL connection is closed")