import time
import pandas as pd
import requests
import base64
import datetime
import pymysql
import json
import csv

# 当前时间  必填
cur_time = '2023-08-17'
cur_time = datetime.datetime.strptime(cur_time, '%Y-%m-%d')
# 昨天时间
yesterday_time = cur_time - datetime.timedelta(days=1)

cur_time = str(cur_time)[:10]
yesterday_time = str(yesterday_time)[:10]

# 查询各区县围栏启用数量
sql_fence = {
    'tableName' : 't_bas_fence_control',
    'where': 'status = 1 AND is_deleted = 0 GROUP BY county_code ORDER BY county_code',
}
sql_31 ={
    "tableName":"t_bas_over_data_31   ",
    "where":"  allow IS NULL AND is_unusual = 0 and area_county =330781 and out_station_time between '2023-01-01 00:00:00' and '2023-09-01 00:00:00'  ",
    "columns":"area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight"
}

sql_71 ={
    "tableName":"t_bas_over_data_71   ",
    "where":"  area_county =330781 and out_station_time between '2023-01-01 00:00:00' and '2023-09-01 00:00:00'  ",
    "columns":"area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight"
}

sql_21 ={
    "tableName":"t_bas_over_data_21  ",
    "where":"  area_county =330781 and out_station_time between '2023-01-01 00:00:00' and '2023-09-01 00:00:00'  ",
    "columns":"area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight"
}
# 获取数据
def get_data(sql):
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


def save_data(data):

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\工作簿081921.xlsx') as writer1:
        data.to_excel(writer1, sheet_name='地市', index=True)


if __name__ == "__main__":
    data1=get_data(sql_21)
    save_data(data1)
    time.sleep(1)
