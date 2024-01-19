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

sql_pass ={
    "tableName":"t_bas_over_data_collection_31 ",
    "where":"  out_station_time  between '2023-01-01 00:00:00' and '2023-08-25 23:59:59' and vehicle_city =330100 and status =13"
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
    db_data=pd.DataFrame(db_data)
    return db_data


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
        "columns": '    "columns":"area_city as 地市,area_county as 区县,out_station_time as 检测时间,car_no as 车牌号,total_weight as 总重,limit_weight as 限重,overrun as 超重,overrun_rate as 超限率,axis as 轴数,a.status as 状态,site_name as 检测站点,law_judgment as 判定需处罚,make_copy as 外省抄告,link_man 所属运输企业名称,phone 联系电话, vehicle_county 车籍地 record_code"',
        "isEncry": "1"
    }
    url = 'https://lwjc.jtyst.zj.gov.cn:7443/zc-interface/db/excuteSql'
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
    sql_area = 'SELECT city,county,city_code,county_code FROM t_code_area '
    db_area = get_df_from_db(sql_area)
    print(db_area)
    t_bas_over_data_collection_31=pd.read_excel(r'C:\Users\stayhungary\Desktop\工作簿1232.xlsx',sheet_name='地市')
    t_bas_over_data_collection_31 = t_bas_over_data_collection_31.fillna(0, inplace=False)
    print(t_bas_over_data_collection_31)
    t_bas_over_data_collection_31['车籍地'] = t_bas_over_data_collection_31['车籍地'].astype('int')
    t_bas_over_data_collection_31['车籍地'] = t_bas_over_data_collection_31['车籍地'].astype('string')
    t_bas_over_data_collection_31['区县'] = t_bas_over_data_collection_31['区县'].astype('string')
    t_bas_over_data_collection_31['处罚区县'] = t_bas_over_data_collection_31['处罚区县'].astype('string')
    df1= pd.merge(t_bas_over_data_collection_31, db_area, left_on=['车籍地'],right_on=['county_code'],
                        how='left')
    df1.drop_duplicates(subset=['record_code'], keep='first', inplace=True)
    df2= pd.merge(t_bas_over_data_collection_31, db_area, left_on=['区县'],right_on=['county_code'],
                        how='left')
    df2.drop_duplicates(subset=['record_code'], keep='first', inplace=True)
    df3= pd.merge(t_bas_over_data_collection_31, db_area, left_on=['处罚区县'],right_on=['county_code'],
                        how='left')
    df3.drop_duplicates(subset=['record_code'], keep='first', inplace=True)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\工作簿0825.xlsx') as writer1:
        df1.to_excel(writer1, sheet_name='地市', index=True)
        df2.to_excel(writer1, sheet_name='地市2', index=True)
        df3.to_excel(writer1, sheet_name='地市3', index=True)
    # data1=get_data2(sql_pass)
    # save_data(data1)
    # time.sleep(1)
