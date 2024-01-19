# coding: utf-8


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

start_time='2023-07-01'
end_time='2023-08-31'


##--交通现场查处数明细

sql_月报明细={
    "type": "query",
    "tableName":"t_bas_over_data_31 a left join t_code_area b on a.area_county = b.county_code  ",
    "where":" out_station_time between '2023-08-01 00:00:00' and '2023-08-31 23:59:59' and allow is null GROUP BY  b.city, b.county,a.area_county ",
    "columns": "b.city, b.county,a.area_county, count(1) as 超限数,sum(IF( a.total_weight between 100 and 300, 1, 0 ))  as 百吨王数,sum(IF( a.overrun_rate between 100 and 500 and a.total_weight between 0 and 99.999, 1, 0 )) as 超限100数,sum(IF(a.total_weight between 80.0001 and 300 , 1, 0 )) as 总重80吨以上数,sum(IF( a.car_no LIKE '%牌%', 1, 0 )) as 未识别车牌数"
}
sql_台州月报明细={
    "type": "query",
    "tableName": "t_bas_over_data_31 a  left join t_code_area b on a.area_county = b.county_code",
    "where": "  out_station_time between '2023-07-01 00:00' and '2023-07-31 23:59:59' and allow is  null and city ='台州' and is_unusual=0 group by area_county",
    "columns": "county, sum(IF( a.total_weight between 70 and 300 , 1, 0 )) 总重70吨以上数,sum(IF( a.overrun_rate between 70 and 500 , 1, 0 ))  超限70以上数 "
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



if __name__ == "__main__":
    data1=get_data(sql_月报明细)
    # data1 = pd.DataFrame(data1,
    #                   columns=['record_code', 'out_station_time', 'car_no', 'total_weight', 'limit_weight', 'overrun',
    #                            'axis', 'site_name', 'direction', 'overrun_rate', 'photo1', 'photo2', 'photo3', 'vedio',
    #                            'insert_time', 'city', 'county', 'has_car_no', 'area_county', 'allow'])

    # data2=get_data(sql_80吨以上非现查处数)
    # data3=get_data(sql_交警现场)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\0908月报明细2.0.xlsx') as writer1:
        data1.to_excel(writer1, sheet_name='sql_80吨以上交通现场', index=True)
        # data2.to_excel(writer1, sheet_name='sql_80吨以上非现查处数', index=True)
        # data3.to_excel(writer1, sheet_name='sql_80吨以上交警现场', index=True)


