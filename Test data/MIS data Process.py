# -*- coding: utf-8 -*-
"""
Created on Sat Oct  3 18:37:44 2020

@author: lwhe
"""


import pandas as pd
from pandas import DataFrame,Series
path = 'C:/Users/lwhe/Desktop/发票/Python/MIS2.xlsx'
MIS_BOND = pd.read_excel(path,sheet_name=5)
print(MIS_BOND)
afs = MIS_BOND[25:286].reset_index(drop=True)
print(afs)
htm = MIS_BOND[341:1088].reset_index(drop=True)
print(htm)
others = MIS_BOND[1087:1088].reset_index(drop=True)
others3=others.iloc[:,[2,3]]
print(others3)
bond_list_raw= afs.append(htm).append(others).reset_index(drop=True)
print(bond_list_raw)
bond_list =bond_list_raw.iloc[:,[3,2,23,25,30,48]]
print(bond_list)
bond_list.columns = ["ISIN","Securities Name","Position","Cost Price",
                     "MV 000HKD","Purchase Time"]
print(bond_list)
bond2 = bond_list.iloc[:,[0,2,4]]
print(bond2)
bond_Counterparty_riskQ = pd.pivot_table(bond2,index="ISIN",
                                         values=["Position","MV 000HKD"],
                                         aggfunc='sum')
print(bond_Counterparty_riskQ)
MV1 = bond_Counterparty_riskQ['MV 000HKD'].sum()
MV2 = bond2['MV 000HKD'].sum()
print(MV1,MV2)
#bond_Counterparty_riskQ.to_excel(r'C:/Users/lwhe/Desktop/发票/Python/bond_Counterparty_riskQ.xlsx')
bond_list["Purchase Cost"]=bond_list["Position"]*bond_list["Cost Price"]
print(bond_list)
bond_monthly= pd.pivot_table(bond_list,index=["ISIN","Securities Name"],
                                         values=["Position","MV 000HKD","Purchase Cost"],
                                         aggfunc='sum')
print(bond_monthly)
MV3 = bond_monthly['MV 000HKD'].sum()
print(MV1,MV2,MV3)
#bond_monthly.to_excel(r'C:/Users/lwhe/Desktop/发票/Python/bond_monthly.xlsx')










