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
end_time='2023-10-30'


##--交通现场查处数明细

sql_交通现场查处数明细={
    "tableName":" t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  LEFT JOIN t_bas_over_data_collection_sign d on d.record_code=c.record_code ",
    "where":"  a.record_type = 99 AND a.insert_type = 5 AND a.close_case_time >= '{} 00:00:00' AND a.close_case_time <= '{} 23:59:59' AND a.area_city = '330100'  ".format(start_time,end_time),
    "columns":"*"
}


##--交通现场查处数

sql_交通现场查处数={
    "tableName":" t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  LEFT JOIN t_bas_over_data_collection_sign d on d.record_code=c.record_code",
    "where":" c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time >= '{} 00:00:00' AND c.close_case_time <= '{} 23:59:59' AND c.area_city = 330100 and total_weight >80 GROUP BY c.area_city,c.area_county  ".format(start_time,end_time),
    "columns":"a.city,c.area_city,a.county, c.area_county,count( DISTINCT ( CASE_NUM ) ) AS 交通现场查处数 ,sum(d.punish_money) 交通现场处罚金额, sum(d.traffic_police_deduct)交通现场扣分 "
}


##--交警现场明细
sql_交警现场明细 ={
    "tableName":"  t_bas_police_road_site c left join t_code_area as b on c.area_county=b.county_code   ",
    "where":"   c.punish_time >= '{} 00:00:00' AND c.punish_time < '{} 23:59:59' and area_city = '330100' ".format(start_time,end_time),
    "columns":"*"
}

##--交警现场
sql_交警现场 ={
    "tableName":"   t_bas_police_road_site a LEFT JOIN t_code_area b ON a.area_county = b.county_code   ",
    "where":"   a.punish_time >= '{} 00:00:00' AND a.punish_time < '{} 23:59:59' and a.area_city = '330100' and case_status=2 and total_weight>80000 GROUP BY area_county  ".format(start_time,end_time),
    "columns":"area_county,count(DISTINCT case_number) as 交警现场查处数, sum(a.punish_money) 交警现场处罚金额 ,sum(a.traffic_police_deduct) 交警现场扣分 "
}


##80吨以上非现查处数明细
sql_80吨以上非现查处数明细 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"  a.law_judgment=1 and b.city='杭州' and c.close_case_time>='{} 00:00:00' and c.close_case_time<'{} 23:59:59'  ".format(start_time,end_time),
    "columns":"b.city 地市,b.county  区县,a.out_station_time  检测时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
              "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time"
}

##80吨以上非现查处数
sql_80吨以上非现查处数 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"  a.law_judgment=1 and b.city='杭州' and c.close_case_time>='{} 00:00:00' and c.close_case_time<'{} 23:59:59' and total_weight >80 GROUP by b.city ,b.county ".format(start_time,end_time),
    "columns":" a.area_county,b.city as 地市,b.county as 区县,count(DISTINCT case_num) 交通非现查处数,SUM(c.punish_money) 交通非现处罚金额   "
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
    data1=get_data(sql_交通现场查处数)
    data2=get_data(sql_80吨以上非现查处数)
    data3=get_data(sql_交警现场)
    data = pd.merge(data1, data2, on=['area_county'], how='outer')
    data = pd.merge(data, data3, on=['area_county'], how='outer')
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\工作簿0831pass4.0.xlsx') as writer1:
        data.to_excel(writer1, sheet_name='data', index=True)
        data1.to_excel(writer1, sheet_name='sql_80吨以上交通现场', index=True)
        data2.to_excel(writer1, sheet_name='sql_80吨以上非现查处数', index=True)
        data3.to_excel(writer1, sheet_name='sql_80吨以上交警现场', index=True)


