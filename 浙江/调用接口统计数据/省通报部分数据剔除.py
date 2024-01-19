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
starttime='2023-10-01'
endtime='2023-11-01'

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

sql={
    "tableName":"t_bas_over_data_31   ",
    "where":"  allow IS NULL AND is_unusual = 0 and overrun_rate >= 100 and area_county  in (330122,330183,330681)  and total_weight <100 "
            "and out_station_time between '{} 00:00:00' and '{} 00:00:00' and car_no not like '%浙%' ".format(starttime, endtime),
    "columns":"record_code"
}
超限100浙牌 = get_df_from_db(sql)
超限100浙牌 = pd.DataFrame(超限100浙牌)
print( 超限100浙牌)



sql={
    "tableName":"t_bas_over_data_31   ",
    "where":"  allow IS NULL AND is_unusual = 0 and area_city =330100 and out_station_time between '{} 00:00:00' and '{} 00:00:00' and total_weight >= 100 ".format(starttime, endtime),
    "columns":"record_code"
}
百吨王 = get_df_from_db(sql)
百吨王 = pd.DataFrame(百吨王)



# sql = """SELECT a.record_code
# FROM t_bas_over_data_31 a LEFT JOIN
# t_bas_over_data_collection_31 b
# on a.record_code = b.record_code
# where a.is_unusual = 0
# and a.out_station_time >= '{} 00:00:00' and  a.out_station_time <'{} 00:00:00'
# and a.allow is null  and a.area_province=330000 and a.overrun_rate >100
# and a.total_weight >80
# and b.`status` =15
# """.format(starttime, endtime)
# 超限100初审不通过全省 = get_df_from_db(sql)
# 超限100初审不通过全省 = pd.DataFrame(超限100初审不通过全省)




sql={
    "tableName":"t_bas_over_data_31 a LEFT JOIN t_bas_over_data_collection_31 b  on a.record_code = b.record_code   ",
    "where":"  a.allow IS NULL AND a.is_unusual = 0 and a.area_county in (330122,330183,330681) and b.status not in (3,4,5,6,12,13)  and a.overrun_rate <100 and a.out_station_time between '{} 00:00:00' and '{} 00:00:00' and a.total_weight >80  ".format(starttime, endtime),
    "columns":"a.record_code"
}
初审不通过80吨以上 = get_df_from_db(sql)
初审不通过80吨以上 = pd.DataFrame(初审不通过80吨以上)

wide_table31 = pd.concat([超限100浙牌, 百吨王, 初审不通过80吨以上])
record_code更新=wide_table31['record_code'].to_list()
record_code更新=tuple(record_code更新)
print(record_code更新)


sql={
    "tableName":"t_bas_over_data_31   ",
    "where":" record_code in {}".format(record_code更新),
    "columns":"is_unusual = 2 "
}
update_df_from_db(sql)



# wide_table31.to_excel(r'C:\Users\liu.wenjie\Desktop\月度更新.xlsx')
# '''数据更新'''
# db = pymysql.connect(host="172.19.116.150", port=11806, user='zjzhzcuser', password='F4dus0ee',
#                      database='db_manage_overruns')
# # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
# #                  database='jingdong_ceshi')
# mycursor = db.cursor()
# sql = "UPDATE t_bas_over_data_31 set is_unusual = 2 where record_code in {}".format(record_code更新)
# mycursor.execute(sql)
# db.commit()
# db.close()

