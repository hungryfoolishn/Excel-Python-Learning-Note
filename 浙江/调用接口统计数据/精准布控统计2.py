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
sql_310315 ={
    "tableName":"t_bas_over_data_31_20230315   ",
    "where":"  allow IS NULL AND is_unusual = 0 and area_county =330781 and out_station_time between '2023-01-01 00:00:00' and '2023-09-01 00:00:00'  ",
    "columns":"area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight"
}
sql_310415 ={
    "tableName":"t_bas_over_data_31_20230415   ",
    "where":"  allow IS NULL AND is_unusual = 0 and area_county =330781 and out_station_time between '2023-01-01 00:00:00' and '2023-09-01 00:00:00'  ",
    "columns":"area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight"
}
sql_310626 ={
    "tableName":"t_bas_over_data_31_20230626   ",
    "where":"  allow IS NULL AND is_unusual = 0 and area_county =330781 and out_station_time between '2023-01-01 00:00:00' and '2023-09-01 00:00:00'  ",
    "columns":"area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight"
}

sql_310801 ={
    "tableName":"t_bas_over_data_31_20220801   ",
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

sql_pass ={
    "tableName":"t_bas_pass_data_31  ",
    "where":"  area_county =330781 and out_station_time between '2023-08-18 00:00:00' and '2023-08-18 23:59:59' GROUP BY out_station "
}

sql = "SELECT out_station,count(1) as pass_num,sum(is_truck) as truck_num FROM t_bas_pass_data_31" \
      " WHERE out_station_time >= '{} 00:00:00' AND out_station_time < '{} 00:00:00' GROUP BY out_station".format(yesterday_time,yesterday_time)
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

def get_data2(sql):
    # if index == 3:
    #     print (sql['where'])
    #     return
    data = {
        "type": "query",
        "tableName": sql['tableName'],
        "where": (base64.b64encode(sql['where'].encode())).decode(),
        "columns": 'out_station,count(1) as pass_num,sum(is_truck) as truck_num ',
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

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\工作簿0819pass.xlsx') as writer1:
        data.to_excel(writer1, sheet_name='地市', index=True)


if __name__ == "__main__":
    data1=get_data2(sql_pass)
    save_data(data1)
    time.sleep(1)
