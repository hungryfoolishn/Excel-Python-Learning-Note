import time

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
# 查询各区县总预警数量
sql_warning = {
    'tableName' : 't_bas_fence_control_warning',
    'where': "warning_time <= '{} 00:00:00' GROUP BY county_code ORDER BY county_code".format(cur_time),
}
# 查询各区县昨日预警数量
sql_warning_yesterday = {
    'tableName' : 't_bas_fence_control_warning',
    'where': "warning_time > '{} 00:00:00' AND warning_time <= '{} 23:59:59' GROUP BY county_code ORDER BY county_code".format(yesterday_time,yesterday_time),
}
# 查询各区县拦截数量
sql_result = {
    'tableName' : 't_bas_fence_control_result',
    'where': "intercept_time <= '{} 00:00:00' GROUP BY county_code ORDER BY county_code".format(cur_time),
}
# 查询各区县昨日拦截数量
sql_result_yesterday = {
    'tableName' : 't_bas_fence_control_result',
    'where': "intercept_time > '{} 00:00:00' AND intercept_time <= '{} 23:59:59' GROUP BY county_code ORDER BY county_code".format(yesterday_time,yesterday_time),
}

DBName = 'db_manage_overruns'
# 创建连接
db = pymysql.connect(
    host="192.168.1.229",
    port=3306,
    user="root",
    passwd="zcits123456",
    db=DBName,
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True
)
cursor = db.cursor()

# 执行sql 查询数据
def get_df_from_db(sql):
    cursor.execute(sql)
    db_data = cursor.fetchall()
    return db_data

# 写入到excel中
def set_data(index, data):

    city_list = []
    county_list = []
    count_list = []
    for i in data:
        county_code = i['county_code']
        count = i['count(*)']

        sql_area = 'SELECT * FROM t_code_area WHERE county_code = {}'.format(county_code)
        db_area = get_df_from_db(sql_area)
        city = db_area[0]['city']
        county = db_area[0]['county']
        # print(city,county,count)
        city_list.append(city)
        county_list.append(county)
        count_list.append(count)

        if index == 1:
            print("各区县围栏启用数量",city,county,count)
        elif index == 2:
            print("各区县总预警数量",city,county,count)
        elif index == 3:
            print("各区县昨日预警数量",city,county,count)
        elif index == 4:
            print("各区县总拦截数量",city,county,count)
        elif index == 5:
            print("各区县昨日拦截数量",city,county,count)

    # if index == 1:
    #     passfile = open('围栏使用统计.csv', mode='w', newline='')
    #     passhead = ['地市', '区县', '围栏启用数']
    #     writepass = csv.DictWriter(passfile, passhead)
    #     writepass.writeheade()
    #
    #
    #     data_csv = []
    #     for i in range(len(city_list)):
    #         data_csv.append(city_list[i], county_list[i],count_list[i])
    #     writepass.writerow(data_csv)


# 获取数据
def get_data(index, sql):
    # if index == 3:
    #     print (sql['where'])
    #     return
    data = {
        "type": "query",
        "tableName": sql['tableName'],
        "where": (base64.b64encode(sql['where'].encode())).decode(),
        "columns": 'county_code, count(*)',
        "isEncry": "1"
    }
    url = 'https://yhxc.jtyst.zj.gov.cn:7443/zc-interface/db/excuteSql'
    headers = {'content-type': 'application/json'}
    res = requests.post(url, json=data, headers=headers)
    res = json.loads(res.text)
    data = res['data']
    set_data(index, data)




get_data(1, sql_fence)
time.sleep(1)
get_data(2, sql_warning)
time.sleep(1)
get_data(3, sql_warning_yesterday)
time.sleep(1)
get_data(4, sql_result)
time.sleep(1)
get_data(5,sql_result_yesterday)
time.sleep(1)