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
end_time='2023-10-10'


##--交通现场查处数明细

sql_交通现场查处数明细={
    "tableName":" t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  LEFT JOIN t_bas_over_data_collection_sign d on d.record_code=c.record_code ",
    "where":"  a.record_type = 99 AND a.insert_type = 5 AND a.close_case_time >= '{} 00:00:00' AND a.close_case_time <= '{} 23:59:59' AND a.area_city = '330100'  ".format(start_time,end_time),
    "columns":"*"
}


##--交通现场查处数

sql_交通现场查处数={
    "tableName":" t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  LEFT JOIN t_bas_over_data_collection_sign d on d.record_code=c.record_code",
    "where":" c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time >= '{} 00:00:00' AND c.close_case_time <= '{} 23:59:59' AND c.area_city = 330100 GROUP BY c.area_city,c.area_county  ".format(start_time,end_time),
    "columns":"a.city,c.area_city,a.county, c.area_county,count( DISTINCT ( CASE_NUM ) ) AS 交通现场查处数 ,sum(d.punish_money) 交通现场处罚金额"
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
    "where":"   a.punish_time >= '{} 00:00:00' AND a.punish_time < '{} 23:59:59' and a.area_city = '330100' and case_status=2  GROUP BY area_county  ".format(start_time,end_time),
    "columns":"area_county,count(DISTINCT case_number) as 交警现场查处数, sum(a.punish_money) 交警现场处罚金额"
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
    "where":"  a.law_judgment=1 and b.city='杭州' and c.close_case_time>='{} 00:00:00' and c.close_case_time<'{} 23:59:59'  GROUP by b.city ,b.county ".format(start_time,end_time),
    "columns":" b.city as 地市,b.county as 区县,count(DISTINCT case_num) 交通非现查处数,SUM(c.punish_money) 交通非现处罚金额 "
}

##80吨以上非现查处数明细
sql_双百处罚情况 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"  a.law_judgment=1 and b.city='杭州' and a.valid_time between '2023-01-01 00:00:00' and '2023-08-31 23:59:59' and a.overrun_rate between 100 and 500  group by a.area_county ",
    "columns":"b.county  区县, count(1) as 入库数 , sum(IF( close_case_time is not null, 1, 0 )) as 处罚数, sum(IF( a.make_copy =1, 1, 0 )) as 外省抄告数"
}


q案件 = '2023-01-01'
s案件 = '2023-09-30'

sql_非现入库明细 = {
    "tableName": "t_bas_over_data_collection_31 a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code ",
    "where": " law_judgment = 1  and b.city='杭州' AND valid_time between '{} 00:00:00'  AND '{} 23:59:59'  and a.status !=5   ".format(
        q案件, s案件),
    "columns":"b.city 地市,b.county  区县,a.out_station_time  检测时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
              "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time"
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
    data1=get_data(sql_非现入库明细)
    # data2=get_data(sql_80吨以上非现查处数)
    # data3=get_data(sql_交警现场)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\杭州截止9月入库案件明细.xlsx') as writer1:
        data1.to_excel(writer1, sheet_name='非现入库明细', index=True)
        # data2.to_excel(writer1, sheet_name='sql_80吨以上非现查处数', index=True)
        # data3.to_excel(writer1, sheet_name='sql_80吨以上交警现场', index=True)


