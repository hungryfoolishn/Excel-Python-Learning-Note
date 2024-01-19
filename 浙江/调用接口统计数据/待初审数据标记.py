import pymysql
import pandas as pd
from datetime import datetime
from urllib import parse

day = datetime.now().date()  # 获取当前系统时间

today = datetime.now()
# import time
#
#
# def sleeptime(hour, min, sec):
#     return 3600 * hour + 60 * min + sec
#
#
# print('暂停：', sleeptime(0, 0, 20), '秒')
# seconds = sleeptime(0, 0, 0)
# time.sleep(seconds)
from datetime import datetime

ks = datetime.now()
print('运行开始时间', ks)
import time
import pandas as pd
import requests
import base64
import datetime
import pymysql
import json
import csv

now = day - datetime.timedelta(days=0)
print('starttime', now)
starttime='2023-09-01'
endtime='2023-10-01'

# 获取数据
def get_df_from_db(sql):
    # if index == 3:
    #     print (sql['where'])
    #     return
    data = {
        "type": "query",
        "tableName": sql['tableName'],
        "where": (base64.b64encode(sql['where'].encode())).decode(),
        "columns": sql['columns'],
        "isEncry": "1"
    }
    url = 'https://yhxc.jtyst.zj.gov.cn:7443/zc-interface/db/excuteSql'
    headers = {'content-type': 'application/json'}
    res = requests.post(url, json=data, headers=headers)
    res = json.loads(res.text)
    data = res['data']
    data=pd.DataFrame(data)
    return  data

# 获取数据
def update_df_from_db(sql):
    # if index == 3:
    #     print (sql['where'])
    #     return
    data = {
        "type": "update",
        "tableName": sql['tableName'],
        "where": (base64.b64encode(sql['where'].encode())).decode(),
        "columns": sql['columns'],
        "isEncry": "1"
    }
    url = 'https://yhxc.jtyst.zj.gov.cn:7443/zc-interface/db/excuteSql'
    headers = {'content-type': 'application/json'}
    res = requests.post(url, json=data, headers=headers)

    return  res

##原始表获取
# sql = "select  out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_21 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'".format(
#     starttime, endtime)
# t_bas_pass_data_21 = get_df_from_db(sql)
file_name = r"G:\智诚\2023日常给出数据\其他任务\异常数据标记模板.xlsx"
df_区县编码 = pd.read_excel(file_name)
# i=0
# for car_no,out_station_time in df_区县编码[i]:
#     print(car_no,out_station_time)
#     i+=1
#

row_indexs = []
for index, row in df_区县编码.iterrows():
    row_indexs.append(index)
    print(row['car_no'])
    sql = {
        "tableName": "t_bas_over_data_31   ",
        "where": " car_no='{}' and out_station_time ='{}' ".format(row['car_no'],row['out_station_time']),
        "columns": "is_unusual = 3 "
    }
    print(sql)
    update_df_from_db(sql)




