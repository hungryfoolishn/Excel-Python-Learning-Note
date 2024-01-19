# -*- coding: utf-8 -*-

from tkinter import *
from tkinter import filedialog

import pandas as pd

下载文件 = pd.read_csv('C:/Users/stayhungary/Desktop/Public and Private Sector Securities 20230526174402.csv')
ISIN编码为空 = pd.read_excel('C:/Users/stayhungary/Desktop/ISIN编码.xlsx')
mask =ISIN编码为空.iloc[:,-1]
print(下载文件)
print('mask',mask)
print('111',下载文件.loc[:, 'ISIN编号'])
df = 下载文件[下载文件.iloc[:, 1].isin(mask)]
print(df)

# q = input("请输入储存路径(C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总)：")
# with pd.ExcelWriter('{}/月相关汇总表.xlsx'.format(b1)) as writer1:
#     df.to_excel(writer1, sheet_name='sheet1', index=True)