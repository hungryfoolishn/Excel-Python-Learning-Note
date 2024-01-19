# -*- coding: utf-8 -*-

from tkinter import *
from tkinter import filedialog

import pandas as pd
import json

下载文件 = pd.read_json(r'C:\Users\stayhungary\Desktop\0802.json')
# 下载文件['record_code']=下载文件['record_code'].astype('object')
print(下载文件)

df= pd.DataFrame(下载文件)

# df = pd.DataFrame(df, columns=['record_code','out_station_time','car_no','total_weight','limit_weight','overrun','axis','site_name','direction','overrun_rate','photo1','photo2','photo3','vedio','insert_time','city','county','has_car_no','area_county','allow'])
# df['record_code']=df['record_code'].astype('object')

# df['record_code']=df['record_code'].astype('int64')
print(df)
type(df)
with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\平湖数字路政数据.xlsx') as writer1:
    df.to_excel(writer1, sheet_name='地市', index=True)
