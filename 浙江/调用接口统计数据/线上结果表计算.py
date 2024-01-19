# -*- coding:utf-8 -*-
import pandas as pd
import requests
import base64
import datetime
import pymysql
import json
from urllib import parse




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

def delete_data(sql):
    # if index == 3:
    #     print (sql['where'])
    #     return
    data = {
        "type": "delete",
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

def insert_data(sql):
    # if index == 3:
    #     print (sql['where'])
    #     return
    data = {
        "type": "insert",
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

def update_data(sql):
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
    res = json.loads(res.text)
    data = res['data']
    data=pd.DataFrame(data)
    return  data


# {
#     "type": "update",
#     "tableName": "t_bas_over_data_41   ",
#     "where": "record_code in ('2919_563481','2925_564455','2919_569440','2911_1376909','2919_586481','2919_586818','2919_587530','2919_587941','2919_589892','2919_589936','2911_1377840','2911_1378193','2911_1378413','2911_1378534','2919_597773','2919_599271','2919_657425','2919_695407','2925_747619','2921_647297','2911_5728','2911_8565','2911_75164','2919_768265')",
#     "columns": "is_unusual=2"
# }
def data_sours(c,j):
    import pandas as pd
    from datetime import datetime
    day = datetime.now().date()  # 获取当前系统时间

    today = datetime.now()

    from datetime import datetime

    ks = datetime.now()
    print('运行开始时间', ks)

    import datetime

    starttime = day - datetime.timedelta(days=c)
    print('starttime', starttime)
    endtime = day - datetime.timedelta(days=j)
    print('endtime', endtime)

    """ 引入原始表 """

    ##原始表获取

    # sql = "select  out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_21 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'".format(
    #     starttime, endtime)
    # t_bas_pass_data_21 = get_df_from_db(sql)
    sql_31 = {
        "tableName": "t_bas_over_data_31   ",
        "where": "  allow IS NULL AND is_unusual = 0 and area_county in (330122,330329,330482,330781,330212) and out_station_time between '{} 00:00:00' and '{} 00:00:00'  ".format(
            starttime, endtime),
        "columns": "area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight"
    }

    sql_71 = {
        "tableName": "t_bas_over_data_71   ",
        "where": "  area_county in (330122,330329,330482,330781,330212) and out_station_time between '{} 00:00:00' and '{} 00:00:00'  ".format(
            starttime, endtime),
        "columns": "area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight"
    }

    sql_21 = {
        "tableName": "t_bas_over_data_21  ",
        "where": "  area_county in (330122,330329,330482,330781,330212) and out_station_time between '{} 00:00:00' and '{} 00:00:00'  ".format(
            starttime, endtime),
        "columns": "area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight"
    }

    sql_pass = {
        "tableName": "t_bas_pass_data_31  ",
        "where": "  area_county in (330122,330329,330482,330781,330212) and out_station_time between '{} 00:00:00' and '{} 00:00:00'  GROUP BY out_station ".format(
            starttime, endtime)
    }
    sql_station = {
        "tableName": "t_sys_station  ",
        "where": "  is_deleted = 0  and station_status = 0 and station_type in (21,31,71)  ",
        "columns": "station_code,station_status,station_type,is_check_station,station_name, area_county"
    }

    sql_area = {
        "tableName": "t_code_area  ",
        "where": "  is_deleted = 0  ",
        "columns": "city_code,county_code,city,county"
    }

    pass_truck_num = get_data2(sql_pass)

    pass_truck_num['truck_num'] = pass_truck_num['truck_num'].astype('int')

    # sql = "SELECT out_station,count(1) as pass_num,sum(is_truck) as truck_num FROM t_bas_pass_data_31 WHERE out_station_time >= '{} 00:00:00' AND out_station_time < '{} 00:00:00' GROUP BY out_station".format(
    #     starttime, endtime)
    # pass_truck_num = get_df_from_db(sql)
    # pass_truck_num = pd.DataFrame(pass_truck_num)
    # pass_truck_num.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\pass_truck_num.xlsx')

    t_bas_pass_data_31 = get_data(sql_31)
    t_bas_pass_data_31['total_weight'] = t_bas_pass_data_31['total_weight'].astype('float')
    t_bas_pass_data_31['limit_weight'] = t_bas_pass_data_31['limit_weight'].astype('float')
    # sql = "select  area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight from t_bas_over_data_31 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00' AND allow IS NULL AND is_unusual = 0 ".format(
    #     starttime, endtime)
    # t_bas_pass_data_31 = get_df_from_db(sql)

    # and area_county = 330781
    t_bas_pass_data_71 = get_data(sql_71)
    t_bas_pass_data_71['total_weight'] = t_bas_pass_data_71['total_weight'].astype('float')
    t_bas_pass_data_71['limit_weight'] = t_bas_pass_data_71['limit_weight'].astype('float')
    t_bas_pass_data_71['out_station'] = t_bas_pass_data_71['out_station'].astype('string')
    # sql = "select  area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight from t_bas_pass_data_71 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'  ".format(
    #     starttime, endtime)
    # t_bas_pass_data_71 = get_df_from_db(sql)
    t_bas_pass_data_21 = get_data(sql_21)
    t_bas_pass_data_21 = pd.DataFrame(t_bas_pass_data_21,
                                 columns=['area_city', 'area_county', 'out_station', 'out_station_time',
                                          'car_no',
                                          'total_weight',
                                          'limit_weight'])
    t_bas_pass_data_21['car_no'] = t_bas_pass_data_21['car_no'].astype('string')
    t_bas_pass_data_21['total_weight'] = t_bas_pass_data_21['total_weight'].astype('float')
    t_bas_pass_data_21['limit_weight'] = t_bas_pass_data_21['limit_weight'].astype('float')
    t_bas_pass_data_21['out_station'] = t_bas_pass_data_21['out_station'].astype('string')
    # sql = "select out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_71 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00' ".format(
    #     starttime, endtime)
    # t_bas_pass_data_71 = get_df_from_db(sql)
    t_sys_station = get_data(sql_station)
    t_sys_station = pd.DataFrame(t_sys_station,
                                 columns=['station_code', 'station_status', 'station_type', 'is_check_station',
                                          'station_name',
                                          'area_county'])
    t_code_area = get_data(sql_area)
    t_code_area = pd.DataFrame(t_code_area,
                               columns=['city_code', 'county_code', 'city', 'county'])
    # sql1 = "select  station_code,station_status,station_type,is_check_station,station_name, area_county from t_sys_station where is_deleted = 0   and station_type in (31,71,21) "
    # t_sys_station = get_df_from_db2(sql1)
    # sql1 = "select city_code,county_code,city,county FROM t_code_area  "
    # t_code_area = get_df_from_db2(sql1)

    ##站点区域表连接
    U_station_area = pd.merge(t_sys_station, t_code_area, left_on='area_county', right_on='county_code', how='left')
    # U_station_area.to_excel('C:/Users/Administrator/Desktop/U_station_area.xlsx')

    U_station_area.rename(
        columns={'city': 'city_name', 'county': 'county_name'}, inplace=True)
    U_station_area = pd.DataFrame(U_station_area,
                                  columns=['city_code', 'city_name', 'county_code', 'county_name', 'station_code',
                                           'station_name', 'station_status', 'station_type', 'is_check_station'])

    ##各表car_no空值填充
    # U_pass21_area_station = t_bas_pass_data_21
    # U_pass21_area_station['car_no'] = U_pass21_area_station['car_no'].fillna(value=0)
    # # U_pass21_area_station['car_no'] = U_pass21_area_station['car_no'].apply(lambda car_no: car_no[:2])
    U_pass31_area_station = t_bas_pass_data_31
    # U_pass31_area_station['car_no'] = U_pass31_area_station['car_no'].fillna(value=0)

    i = {
        "330100": 1.1,
        "330200": 1,
        "330300": 1,
        "330400": 1.1,
        "330500": 1,
        "330600": 1,
        "330700": 1,
        "330800": 1.1,
        "330900": 1.1,
        "331000": 1.1,
        "331100": 1.1,
        "330122": 1.091,
        "330183": 1.1,
        "330329": 1.2,
        "330523": 1.03,
        "330603": 1.1,
        "330604": 1.2,
        "330624": 1.2,
        "330681": 1.1,
        "330703": 1.2,
        "330782": 1.1
    }

    for item in i.items():
        key = item[0]
        value = item[1]
        U_pass31_area_station.loc[
            ((U_pass31_area_station['area_county'] == key) | (U_pass31_area_station['area_city'] == key)) & (
                    U_pass31_area_station['total_weight'] < 100), 'limit_weight'] = U_pass31_area_station[
            'limit_weight'].map(lambda x: float(x) * value).round(0)

    U_pass31_area_station['limit_weight'] = U_pass31_area_station['limit_weight'].fillna(value=1)
    U_pass31_area_station.loc[U_pass31_area_station['limit_weight'] < 1, 'limit_weight'] = 9
    U_pass31_area_station['total_weight'] = U_pass31_area_station['total_weight'].astype('float')
    U_pass31_area_station['limit_weight'] = U_pass31_area_station['limit_weight'].astype('float')
    U_pass31_area_station['overrun_rate'] = (U_pass31_area_station['total_weight'] - U_pass31_area_station[
        'limit_weight']) * 100 / U_pass31_area_station['limit_weight']

    # U_pass31_area_station['car_no'] = U_pass31_area_station['car_no'].apply(lambda car_no: car_no[:2])
    U_pass71_area_station = t_bas_pass_data_71
    U_pass71_area_station['car_no'] = U_pass71_area_station['car_no'].fillna(value=0)
    U_pass21_area_station = t_bas_pass_data_21
    U_pass21_area_station['car_no'] = U_pass21_area_station['car_no'].fillna(value=0)
    # # U_pass41_area_station['car_no'] = U_pass41_area_station['car_no'].apply(lambda car_no: car_no[:2])
    # U_pass71_area_station = t_bas_pass_data_71
    # U_pass71_area_station['car_no'] = U_pass71_area_station['car_no'].fillna(value=0)
    # # U_pass71_area_station['car_no'] = U_pass71_area_station['car_no'].apply(lambda car_no: car_no[:2])

    U_pass31_area_station = pd.DataFrame(U_pass31_area_station,
                                         columns=['out_station', 'out_station_time', 'car_no', 'total_weight',
                                                  'limit_weight',
                                                  'overrun_rate'])

    U_pass71_area_station = pd.DataFrame(U_pass71_area_station,
                                         columns=['out_station', 'out_station_time', 'car_no', 'total_weight',
                                                  'limit_weight',
                                                  'overrun_rate'])

    # 31表
    # 建立关键字段
    wide_table_31 = pass_truck_num
    wide_table_31 = pd.merge(wide_table_31, t_sys_station, left_on='out_station', right_on='station_code',
                             how='left')
    wide_table_31['pass_num'] = wide_table_31['pass_num'].astype('float')
    wide_table_31['truck_num'] = wide_table_31['truck_num'].astype('float')
    wide_table_31 = pd.DataFrame(wide_table_31)

    T0_5T = U_pass31_area_station[
        ((0 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 5))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T0_5T')

    U_pass31_area_station = U_pass31_area_station[(U_pass31_area_station.total_weight >= 2.5)]

    over_num = U_pass31_area_station[(U_pass31_area_station['overrun_rate'] > 0)].groupby(['out_station'])[
        'car_no'].count().reset_index(name='over_num')
    wide_table_31 = pd.merge(wide_table_31, over_num, on=['out_station'], how='left')
    wide_table_31 = wide_table_31.fillna(value=0)
    wide_table_31['over_rate'] = (wide_table_31['over_num'] / wide_table_31['pass_num'] * 100).round(2)
    wide_table_31['truck_over_rate'] = (wide_table_31['over_num'] / wide_table_31['truck_num'] * 100).round(2)
    wide_table_31['no_over'] = wide_table_31['truck_num'] - wide_table_31['over_num']
    no_car = \
        U_pass31_area_station[(U_pass31_area_station['car_no'].str.len() <= 5)].groupby(['out_station'])[
            'car_no'].count().reset_index(name='no_car')
    wide_table_31 = pd.merge(wide_table_31, no_car, on=['out_station'], how='left')

    ##超限程度分布
    C0_10X = U_pass31_area_station[
        ((0 < U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 10))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C0_10X')
    C10_20X = U_pass31_area_station[
        ((10 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 20))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C10_20X')
    C20_30X = U_pass31_area_station[
        ((20 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 30))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C20_30X')
    C30_40X = U_pass31_area_station[
        ((30 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 40))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C30_40X')
    C40_50X = U_pass31_area_station[
        ((40 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 50))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C40_50X')
    C50_60X = U_pass31_area_station[
        ((50 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 60))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C50_60X')
    C60_70X = U_pass31_area_station[
        ((60 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 70))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C60_70X')
    C70_80X = U_pass31_area_station[
        ((70 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 80))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C70_80X')
    C80_90X = U_pass31_area_station[
        ((80 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 90))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C80_90X')
    C90_100X = U_pass31_area_station[
        ((90 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 100))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C90_100X')
    C100X = U_pass31_area_station[(100 <= U_pass31_area_station['overrun_rate'])].groupby(['out_station'])[
        'car_no'].count().reset_index(name='C100X')

    ##聚合超限程度
    wide_table_31 = pd.merge(wide_table_31, C0_10X, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C10_20X, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C20_30X, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C30_40X, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C40_50X, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C50_60X, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C60_70X, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C70_80X, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C80_90X, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C90_100X, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C100X, on=['out_station'], how='left')

    ##吨位分布
    T0_5T = T0_5T
    T5_10T = U_pass31_area_station[
        ((5 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 10))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T5_10T')
    T10_20T = U_pass31_area_station[
        ((10 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 20))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T10_20T')
    T20_30T = U_pass31_area_station[
        ((20 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 30))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T20_30T')
    T30_40T = U_pass31_area_station[
        ((30 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 40))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T30_40T')
    T40_50T = U_pass31_area_station[
        ((40 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 50))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T40_50T')
    T50_60T = U_pass31_area_station[
        ((50 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 60))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T50_60T')
    T60_70T = U_pass31_area_station[
        ((60 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 70))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T60_70T')
    T70_80T = U_pass31_area_station[
        ((70 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 80))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T70_80T')
    T80_90T = U_pass31_area_station[
        ((80 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 90))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T80_90T')
    T90_100T = U_pass31_area_station[
        ((90 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 100))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T90_100T')
    T100T = U_pass31_area_station[(100 <= U_pass31_area_station['total_weight'])].groupby(['out_station'])[
        'car_no'].count().reset_index(name='T100T')

    ##聚合吨位分布
    wide_table_31 = pd.merge(wide_table_31, T0_5T, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T5_10T, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T10_20T, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T20_30T, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T30_40T, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T40_50T, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T50_60T, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T60_70T, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T70_80T, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T80_90T, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T90_100T, on=['out_station'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T100T, on=['out_station'], how='left')

    wide_table_31 = wide_table_31.fillna(value=0)

    wide_table_31.rename(
        columns={'over_num': 'overrun_num',
                 'statistics_time': 'statistics_date', 'no_over': 'overrun_0', 'no_car': 'no_car_num',
                 'C0_10X': 'overrun_0_10', 'C10_20X': 'overrun_10_20', 'C20_30X': 'overrun_20_30',
                 'C30_40X': 'overrun_30_40', 'C40_50X': 'overrun_40_50', 'C50_60X': 'overrun_50_60',
                 'C60_70X': 'overrun_60_70', 'C70_80X': 'overrun_70_80', 'C80_90X': 'overrun_80_90',
                 'C90_100X': 'overrun_90_100',
                 'C100X': 'overrun_100', 'T0_5T': 'total_weight_0_5', 'T5_10T': 'total_weight_5_10',
                 'T10_20T': 'total_weight_10_20', 'T20_30T': 'total_weight_20_30',
                 'T30_40T': 'total_weight_30_40', 'T40_50T': 'total_weight_40_50', 'T50_60T': 'total_weight_50_60',
                 'T60_70T': 'total_weight_60_70', 'T70_80T': 'total_weight_70_80', 'T80_90T': 'total_weight_80_90',
                 'T90_100T': 'total_weight_90_100', 'T100T': 'total_weight_100'}, inplace=True)

    wide_table_31 = pd.DataFrame(wide_table_31,
                                 columns=['out_station', 'pass_num', 'truck_num',
                                          'overrun_num', 'over_rate', 'truck_over_rate', 'no_car_num', 'overrun_0',
                                          'overrun_0_10', 'overrun_10_20', 'overrun_20_30', 'overrun_30_40',
                                          'overrun_40_50', 'overrun_50_60', 'overrun_60_70',
                                          'overrun_70_80', 'overrun_80_90', 'overrun_90_100', 'overrun_100',
                                          'total_weight_0_5', 'total_weight_5_10', 'total_weight_10_20',
                                          'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50',
                                          'total_weight_50_60', 'total_weight_60_70', 'total_weight_70_80',
                                          'total_weight_80_90',
                                          'total_weight_90_100', 'total_weight_100'])
    # print(wide_table_31)
    # though_area_31.to_excel('C:/Users/Administrator/Desktop/though_area_31.xlsx')
    # 71表
    # 建立关键字段
    U_pass71_area_station['limit_weight'] = U_pass71_area_station['limit_weight'].fillna(value=1)
    U_pass71_area_station.loc[U_pass71_area_station['limit_weight'] < 1, 'limit_weight'] = 5
    U_pass71_area_station['total_weight'] = U_pass71_area_station['total_weight'].astype('float')
    U_pass71_area_station['limit_weight'] = U_pass71_area_station['limit_weight'].astype('float')
    U_pass71_area_station['overrun_rate'] = (U_pass71_area_station['total_weight'] - U_pass71_area_station[
        'limit_weight']) * 100 / (U_pass71_area_station['limit_weight'] + 0.00001)

    wide_table_71 = U_pass71_area_station.groupby(
        ['out_station'])[
        'car_no'].count().reset_index(name='pass_num')
    wide_table_71 = pd.merge(wide_table_71, t_sys_station, left_on='out_station', right_on='station_code',
                             how='left')
    wide_table_71 = pd.DataFrame(wide_table_71)

    T0_5T = U_pass71_area_station[
        ((0 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 5))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T0_5T')

    U_pass71_area_station = U_pass71_area_station[(U_pass71_area_station.total_weight >= 2.5)]

    truck_num = U_pass71_area_station.groupby(['out_station'])['car_no'].count().reset_index(
        name='truck_num')
    wide_table_71 = pd.merge(wide_table_71, truck_num, on=['out_station'], how='left')
    over_num = U_pass71_area_station[(U_pass71_area_station['overrun_rate'] > 0)].groupby(['out_station'])[
        'car_no'].count().reset_index(name='over_num')
    wide_table_71 = pd.merge(wide_table_71, over_num, on=['out_station'], how='left')
    wide_table_71 = wide_table_71.fillna(value=0)
    wide_table_71['over_rate'] = (wide_table_71['over_num'] / wide_table_71['pass_num'] * 100).round(2)
    wide_table_71['truck_over_rate'] = (wide_table_71['over_num'] / wide_table_71['truck_num'] * 100).round(2)
    wide_table_71['no_over'] = wide_table_71['truck_num'] - wide_table_71['over_num']
    no_car = \
        U_pass71_area_station[(U_pass71_area_station['car_no'].str.len() <= 5)].groupby(['out_station'])[
            'car_no'].count().reset_index(name='no_car')
    wide_table_71 = pd.merge(wide_table_71, no_car, on=['out_station'], how='left')

    ##超限程度分布
    C0_10X = U_pass71_area_station[
        ((0 < U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 10))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C0_10X')
    C10_20X = U_pass71_area_station[
        ((10 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 20))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C10_20X')
    C20_30X = U_pass71_area_station[
        ((20 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 30))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C20_30X')
    C30_40X = U_pass71_area_station[
        ((30 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 40))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C30_40X')
    C40_50X = U_pass71_area_station[
        ((40 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 50))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C40_50X')
    C50_60X = U_pass71_area_station[
        ((50 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 60))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C50_60X')
    C60_70X = U_pass71_area_station[
        ((60 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 70))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C60_70X')
    C70_80X = U_pass71_area_station[
        ((70 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 80))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C70_80X')
    C80_90X = U_pass71_area_station[
        ((80 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 90))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C80_90X')
    C90_100X = U_pass71_area_station[
        ((90 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 100))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C90_100X')
    C100X = U_pass71_area_station[(100 <= U_pass71_area_station['overrun_rate'])].groupby(['out_station'])[
        'car_no'].count().reset_index(name='C100X')

    ##聚合超限程度
    wide_table_71 = pd.merge(wide_table_71, C0_10X, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C10_20X, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C20_30X, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C30_40X, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C40_50X, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C50_60X, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C60_70X, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C70_80X, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C80_90X, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C90_100X, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C100X, on=['out_station'], how='left')

    ##吨位分布
    T0_5T = T0_5T
    T5_10T = U_pass71_area_station[
        ((5 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 10))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T5_10T')
    T10_20T = U_pass71_area_station[
        ((10 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 20))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T10_20T')
    T20_30T = U_pass71_area_station[
        ((20 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 30))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T20_30T')
    T30_40T = U_pass71_area_station[
        ((30 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 40))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T30_40T')
    T40_50T = U_pass71_area_station[
        ((40 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 50))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T40_50T')
    T50_60T = U_pass71_area_station[
        ((50 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 60))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T50_60T')
    T60_70T = U_pass71_area_station[
        ((60 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 70))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T60_70T')
    T70_80T = U_pass71_area_station[
        ((70 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 80))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T70_80T')
    T80_90T = U_pass71_area_station[
        ((80 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 90))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T80_90T')
    T90_100T = U_pass71_area_station[
        ((90 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 100))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T90_100T')
    T100T = U_pass71_area_station[(100 <= U_pass71_area_station['total_weight'])].groupby(['out_station'])[
        'car_no'].count().reset_index(name='T100T')

    ##聚合吨位分布
    wide_table_71 = pd.merge(wide_table_71, T0_5T, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T5_10T, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T10_20T, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T20_30T, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T30_40T, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T40_50T, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T50_60T, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T60_70T, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T70_80T, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T80_90T, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T90_100T, on=['out_station'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T100T, on=['out_station'], how='left')

    wide_table_71 = wide_table_71.fillna(value=0)

    wide_table_71.rename(
        columns={'over_num': 'overrun_num',
                 'statistics_time': 'statistics_date', 'no_over': 'overrun_0', 'no_car': 'no_car_num',
                 'C0_10X': 'overrun_0_10', 'C10_20X': 'overrun_10_20', 'C20_30X': 'overrun_20_30',
                 'C30_40X': 'overrun_30_40', 'C40_50X': 'overrun_40_50', 'C50_60X': 'overrun_50_60',
                 'C60_70X': 'overrun_60_70', 'C70_80X': 'overrun_70_80', 'C80_90X': 'overrun_80_90',
                 'C90_100X': 'overrun_90_100',
                 'C100X': 'overrun_100', 'T0_5T': 'total_weight_0_5', 'T5_10T': 'total_weight_5_10',
                 'T10_20T': 'total_weight_10_20', 'T20_30T': 'total_weight_20_30',
                 'T30_40T': 'total_weight_30_40', 'T40_50T': 'total_weight_40_50', 'T50_60T': 'total_weight_50_60',
                 'T60_70T': 'total_weight_60_70', 'T70_80T': 'total_weight_70_80', 'T80_90T': 'total_weight_80_90',
                 'T90_100T': 'total_weight_90_100', 'T100T': 'total_weight_100'}, inplace=True)

    wide_table_71 = pd.DataFrame(wide_table_71,
                                 columns=['out_station', 'pass_num', 'truck_num',
                                          'overrun_num', 'over_rate', 'truck_over_rate', 'no_car_num', 'overrun_0',
                                          'overrun_0_10', 'overrun_10_20', 'overrun_20_30', 'overrun_30_40',
                                          'overrun_40_50', 'overrun_50_60', 'overrun_60_70',
                                          'overrun_70_80', 'overrun_80_90', 'overrun_90_100', 'overrun_100',
                                          'total_weight_0_5', 'total_weight_5_10', 'total_weight_10_20',
                                          'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50',
                                          'total_weight_50_60', 'total_weight_60_70', 'total_weight_70_80',
                                          'total_weight_80_90',
                                          'total_weight_90_100', 'total_weight_100'])
    # 21表
    # 建立关键字段
    U_pass21_area_station['limit_weight'] = U_pass21_area_station['limit_weight'].fillna(value=1)
    U_pass21_area_station.loc[U_pass21_area_station['limit_weight'] < 1, 'limit_weight'] = 5
    U_pass21_area_station['total_weight'] = U_pass21_area_station['total_weight'].astype('float')
    U_pass21_area_station['limit_weight'] = U_pass21_area_station['limit_weight'].astype('float')
    U_pass21_area_station['overrun_rate'] = (U_pass21_area_station['total_weight'] - U_pass21_area_station[
        'limit_weight']) * 100 / (U_pass21_area_station['limit_weight'] + 0.00001)

    wide_table_21 = U_pass21_area_station.groupby(
        ['out_station'])[
        'car_no'].count().reset_index(name='pass_num')
    wide_table_21 = pd.merge(wide_table_21, t_sys_station, left_on='out_station', right_on='station_code',
                             how='left')
    wide_table_21 = pd.DataFrame(wide_table_21)

    T0_5T = U_pass21_area_station[
        ((0 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 5))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T0_5T')

    U_pass21_area_station = U_pass21_area_station[(U_pass21_area_station.total_weight >= 2.5)]

    truck_num = U_pass21_area_station.groupby(['out_station'])['car_no'].count().reset_index(
        name='truck_num')
    wide_table_21 = pd.merge(wide_table_21, truck_num, on=['out_station'], how='left')
    over_num = U_pass21_area_station[(U_pass21_area_station['overrun_rate'] > 0)].groupby(['out_station'])[
        'car_no'].count().reset_index(name='over_num')
    wide_table_21 = pd.merge(wide_table_21, over_num, on=['out_station'], how='left')
    wide_table_21 = wide_table_21.fillna(value=0)
    wide_table_21['over_rate'] = (wide_table_21['over_num'] / wide_table_21['pass_num'] * 100).round(2)
    wide_table_21['truck_over_rate'] = (wide_table_21['over_num'] / wide_table_21['truck_num'] * 100).round(2)
    wide_table_21['no_over'] = wide_table_21['truck_num'] - wide_table_21['over_num']
    no_car = \
        U_pass21_area_station[(U_pass21_area_station['car_no'].str.len() <= 5)].groupby(['out_station'])[
            'car_no'].count().reset_index(name='no_car')
    wide_table_21 = pd.merge(wide_table_21, no_car, on=['out_station'], how='left')

    ##超限程度分布
    C0_10X = U_pass21_area_station[
        ((0 < U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 10))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C0_10X')
    C10_20X = U_pass21_area_station[
        ((10 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 20))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C10_20X')
    C20_30X = U_pass21_area_station[
        ((20 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 30))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C20_30X')
    C30_40X = U_pass21_area_station[
        ((30 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 40))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C30_40X')
    C40_50X = U_pass21_area_station[
        ((40 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 50))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C40_50X')
    C50_60X = U_pass21_area_station[
        ((50 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 60))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C50_60X')
    C60_70X = U_pass21_area_station[
        ((60 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 70))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C60_70X')
    C70_80X = U_pass21_area_station[
        ((70 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 80))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C70_80X')
    C80_90X = U_pass21_area_station[
        ((80 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 90))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C80_90X')
    C90_100X = U_pass21_area_station[
        ((90 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 100))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='C90_100X')
    C100X = U_pass21_area_station[(100 <= U_pass21_area_station['overrun_rate'])].groupby(['out_station'])[
        'car_no'].count().reset_index(name='C100X')

    ##聚合超限程度
    wide_table_21 = pd.merge(wide_table_21, C0_10X, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, C10_20X, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, C20_30X, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, C30_40X, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, C40_50X, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, C50_60X, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, C60_70X, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, C70_80X, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, C80_90X, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, C90_100X, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, C100X, on=['out_station'], how='left')

    ##吨位分布
    T0_5T = T0_5T
    T5_10T = U_pass21_area_station[
        ((5 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 10))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T5_10T')
    T10_20T = U_pass21_area_station[
        ((10 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 20))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T10_20T')
    T20_30T = U_pass21_area_station[
        ((20 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 30))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T20_30T')
    T30_40T = U_pass21_area_station[
        ((30 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 40))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T30_40T')
    T40_50T = U_pass21_area_station[
        ((40 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 50))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T40_50T')
    T50_60T = U_pass21_area_station[
        ((50 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 60))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T50_60T')
    T60_70T = U_pass21_area_station[
        ((60 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 70))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T60_70T')
    T70_80T = U_pass21_area_station[
        ((70 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 80))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T70_80T')
    T80_90T = U_pass21_area_station[
        ((80 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 90))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T80_90T')
    T90_100T = U_pass21_area_station[
        ((90 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 100))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T90_100T')
    T100T = U_pass21_area_station[(100 <= U_pass21_area_station['total_weight'])].groupby(['out_station'])[
        'car_no'].count().reset_index(name='T100T')

    ##聚合吨位分布
    wide_table_21 = pd.merge(wide_table_21, T0_5T, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, T5_10T, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, T10_20T, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, T20_30T, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, T30_40T, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, T40_50T, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, T50_60T, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, T60_70T, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, T70_80T, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, T80_90T, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, T90_100T, on=['out_station'], how='left')
    wide_table_21 = pd.merge(wide_table_21, T100T, on=['out_station'], how='left')

    wide_table_21 = wide_table_21.fillna(value=0)

    wide_table_21.rename(
        columns={'over_num': 'overrun_num',
                 'statistics_time': 'statistics_date', 'no_over': 'overrun_0', 'no_car': 'no_car_num',
                 'C0_10X': 'overrun_0_10', 'C10_20X': 'overrun_10_20', 'C20_30X': 'overrun_20_30',
                 'C30_40X': 'overrun_30_40', 'C40_50X': 'overrun_40_50', 'C50_60X': 'overrun_50_60',
                 'C60_70X': 'overrun_60_70', 'C70_80X': 'overrun_70_80', 'C80_90X': 'overrun_80_90',
                 'C90_100X': 'overrun_90_100',
                 'C100X': 'overrun_100', 'T0_5T': 'total_weight_0_5', 'T5_10T': 'total_weight_5_10',
                 'T10_20T': 'total_weight_10_20', 'T20_30T': 'total_weight_20_30',
                 'T30_40T': 'total_weight_30_40', 'T40_50T': 'total_weight_40_50', 'T50_60T': 'total_weight_50_60',
                 'T60_70T': 'total_weight_60_70', 'T70_80T': 'total_weight_70_80', 'T80_90T': 'total_weight_80_90',
                 'T90_100T': 'total_weight_90_100', 'T100T': 'total_weight_100'}, inplace=True)

    wide_table_21 = pd.DataFrame(wide_table_21,
                                 columns=['out_station', 'pass_num', 'truck_num',
                                          'overrun_num', 'over_rate', 'truck_over_rate', 'no_car_num', 'overrun_0',
                                          'overrun_0_10', 'overrun_10_20', 'overrun_20_30', 'overrun_30_40',
                                          'overrun_40_50', 'overrun_50_60', 'overrun_60_70',
                                          'overrun_70_80', 'overrun_80_90', 'overrun_90_100', 'overrun_100',
                                          'total_weight_0_5', 'total_weight_5_10', 'total_weight_10_20',
                                          'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50',
                                          'total_weight_50_60', 'total_weight_60_70', 'total_weight_70_80',
                                          'total_weight_80_90',
                                          'total_weight_90_100', 'total_weight_100'])

    ##基础表合并
    # wide_table = pd.concat([wide_table_21, wide_table_31, wide_table_41, wide_table_71])
    wide_table = pd.concat([wide_table_31, wide_table_71, wide_table_21])
    # wide_table.to_excel('C:/Users/Administrator/Desktop/wide_tableqian.xlsx')
    wide_table = pd.merge(U_station_area, wide_table, left_on='station_code', right_on='out_station', how='left')
    # wide_table.to_excel('C:/Users/Administrator/Desktop/wide_table111.xlsx')

    # stationcode1 = t_station['out_station']
    wide_table['refresh'] = 0
    wide_table['direction'] = 0
    wide_table['statistics_date'] = starttime
    starttime1 = starttime.strftime("%Y%m%d")
    wide_table = wide_table.fillna(value=0)
    wide_table['id'] = wide_table['station_code'].astype('string') + starttime1
    wide_table['id'] = wide_table['id'].astype('object')
    from datetime import datetime

    insert_time = datetime.now()
    wide_table['insert_time'] = insert_time
    online_duration = datetime.now().strftime("%H%M%S")
    wide_table['online_duration'] = online_duration
    wide_table = wide_table[(wide_table.pass_num > 0)]
    wide_table.rename(
        columns={'statistics_date': 'statistic_date'}, inplace=True)
    wide_table = pd.DataFrame(wide_table,
                              columns=['id', 'city_code', 'city_name', 'county_code', 'county_name', 'station_code',
                                       'station_name', 'direction', 'statistic_date', 'pass_num', 'truck_num',
                                       'overrun_num', 'over_rate', 'truck_over_rate', 'no_car_num', 'overrun_0',
                                       'overrun_0_10', 'overrun_10_20', 'overrun_20_30', 'overrun_30_40',
                                       'overrun_40_50', 'overrun_50_60', 'overrun_60_70',
                                       'overrun_70_80', 'overrun_80_90', 'overrun_90_100', 'overrun_100',
                                       'total_weight_0_5', 'total_weight_5_10', 'total_weight_10_20',
                                       'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50',
                                       'total_weight_50_60', 'total_weight_60_70', 'total_weight_70_80',
                                       'total_weight_80_90',
                                       'total_weight_90_100', 'total_weight_100', 'station_status', 'station_type',
                                       'is_check_station',
                                       'refresh', 'online_duration', 'insert_time'])


    '''数据库删除'''
    sql={
    "tableName": "t_bas_basic_data_pass",
    "where": " statistic_date= '{}'".format(starttime),
    "columns": "*"
     }
    delete_data(sql)

    '''数据插入'''
    df=wide_table.reset_index(drop=True)
    # df=df.drop(['update_time','create_time'],axis=1)
    df['statistic_date']=df['statistic_date'].astype('str')
    df['insert_time']=df['insert_time'].astype('str')
    column=df.columns.to_list()
    num=len(df['id'])
    h = 0
    while h < num:
        valve = df.loc[h].to_list()
        column_str = str(column).replace('[', "").replace(']', "").replace("'", "")
        valve_str = str(valve).replace('[', "").replace(']', "")
        sql = {
            "tableName": " t_bas_basic_data_pass  ",
            "where": "{}".format(valve_str),
            "columns": "{}".format(column_str)
        }
        insert_data(sql)
        h += 1


    del t_bas_pass_data_31
    del t_bas_pass_data_71
    del t_bas_pass_data_21
    from datetime import datetime

    js = datetime.now()
    sjc = js - ks
    print('运行耗时', sjc)

    print(wide_table)
    return wide_table

def delete_data(sql):
    # if index == 3:
    #     print (sql['where'])
    #     return
    data = {
        "type": "delete",
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

def chushihua():
    c = 2
    j = 1
    while c <= 14:
        from datetime import datetime
        ks = datetime.now()
        print('初始化开始时间', ks)
        data_sours(c,j)
        c += 1
        j += 1

def t_bas_pass_statistics_data(k,j):
    import pandas as pd
    from datetime import datetime
    import calendar

    today = datetime.now()

    from datetime import datetime

    ks = datetime.now()
    print('运行开始时间', ks)

    import datetime
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

    starttime = this_month_start - datetime.timedelta(days=k)
    print('starttime', starttime)
    endtime = this_month_end - datetime.timedelta(days=j)
    print('endtime', endtime)

    ##原始表获取
    t_bas_basic_data_pass = {
        "tableName": "t_bas_basic_data_pass  ",
        "where": "  statistic_date >= '{}' AND statistic_date < '{}' and station_status=0 and is_check_station =1  and station_type = 31  ".format(
        starttime, endtime),
        "columns": "*"
    }
    t_bas_basic_data_pass = get_data(t_bas_basic_data_pass)
    print(t_bas_basic_data_pass)


    t_bas_basic_data_pass71 = {
        "tableName": "t_bas_basic_data_pass  ",
        "where": "  statistic_date >= '{}' AND statistic_date < '{}' and station_status=0   and station_type = 71  ".format(
        starttime, endtime),
        "columns": "*"
    }
    t_bas_basic_data_pass71 = get_data(t_bas_basic_data_pass71)


    t_bas_basic_data_pass21 = {
        "tableName": "t_bas_basic_data_pass  ",
        "where": "  statistic_date >= '{}' AND statistic_date < '{}' and station_status=0   and station_type = 21  ".format(
        starttime, endtime),
        "columns": "*"
    }
    t_bas_basic_data_pass21 = get_data(t_bas_basic_data_pass21)

    # t_bas_basic_data_pass71.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\t_bas_basic_data_pass71.xlsx')

    # 区县汇总=t_bas_basic_data_pass.groupby(['地市','区县']).agg({'货车数': ['sum'],'超限数': ['sum'],'剔除10%超限数（不包含临界点）': ['sum'],'剔除20%超限数（不包含临界点）': ['sum']})

    ###非现场表
    ##县级

    # county_sum31= t_bas_basic_data_pass[(t_bas_basic_data_pass.station_type == 31)]
    county_sum31 = t_bas_basic_data_pass.loc[(t_bas_basic_data_pass.station_type == 31)]
    county_sum31 = county_sum31.groupby(['city_code', 'city_name', 'county_code', 'county_name']).sum().reset_index()
    county_sum31 = pd.DataFrame(county_sum31)

    # county_sum31.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\county_sum311230.xlsx')
    county_sum31['hundred_king_num'] = county_sum31['total_weight_100']
    county_sum31['overrun100_num'] = county_sum31['overrun_100']
    county_sum31['overrun_rate'] = (county_sum31['overrun_num'] / county_sum31['truck_num'] * 100).round(2)
    county_sum31['overrun020_count'] = county_sum31['overrun_0_10'] + county_sum31['overrun_10_20']
    county_sum31['overrun020_rate'] = (county_sum31['overrun020_count'] / county_sum31['truck_num'] * 100).round(2)
    county_sum31['overrun2050_count'] = county_sum31['overrun_20_30'] + county_sum31['overrun_30_40'] + county_sum31[
        'overrun_40_50']
    county_sum31['overrun2050_rate'] = (county_sum31['overrun2050_count'] / county_sum31['truck_num'] * 100).round(2)
    county_sum31['overrun50100_count'] = county_sum31['overrun_50_60'] + county_sum31['overrun_60_70'] + county_sum31[
        'overrun_70_80'] + county_sum31['overrun_80_90'] + county_sum31['overrun_90_100']
    county_sum31['overrun50100_rate'] = (county_sum31['overrun50100_count'] / county_sum31['truck_num'] * 100).round(2)
    county_sum31['overrun100_count'] = county_sum31['overrun_100']
    county_sum31['overrun100_rate'] = (county_sum31['overrun100_count'] / county_sum31['truck_num'] * 100).round(2)
    county_sum31['station_type'] = 31
    county_sum31['update_time'] = today
    county_sum31['create_time'] = today
    county_sum31['statistics_date'] = this_month
    this_month2 = datetime.datetime(now.year, now.month, 1).strftime("%Y%m")
    county_sum31['id'] = this_month2 + county_sum31['county_code'].astype('str') + county_sum31['station_type'].astype(
        'str')
    county_sum31 = pd.DataFrame(county_sum31,
                                columns=['id', 'statistics_date', 'city_code', 'city_name', 'county_code',
                                         'county_name',
                                         'pass_num', 'truck_num',
                                         'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count',
                                         'overrun020_rate',
                                         'overrun2050_count', 'overrun2050_rate', 'overrun50100_count',
                                         'overrun50100_rate',
                                         'overrun100_count', 'overrun100_rate', 'station_type',
                                         'update_time', 'create_time'])
    ##市级
    county_sum地市 = county_sum31.groupby(['city_code', 'city_name']).sum().reset_index()
    county_sum地市['overrun_rate'] = (county_sum地市['overrun_num'] / county_sum地市['truck_num'] * 100).round(2)
    county_sum地市['overrun020_rate'] = (county_sum地市['overrun020_count'] / county_sum地市['truck_num'] * 100).round(2)
    county_sum地市['overrun2050_rate'] = (county_sum地市['overrun2050_count'] / county_sum地市['truck_num'] * 100).round(2)
    county_sum地市['overrun50100_rate'] = (county_sum地市['overrun50100_count'] / county_sum地市['truck_num'] * 100).round(2)
    county_sum地市['overrun100rate'] = (county_sum地市['overrun100_count'] / county_sum地市['truck_num'] * 100).round(2)
    county_sum地市['station_type'] = 31
    county_sum地市['update_time'] = today
    county_sum地市['create_time'] = today
    county_sum地市['statistics_date'] = this_month
    county_sum地市['id'] = this_month2 + county_sum地市['city_code'].astype('str') + county_sum地市['station_type'].astype(
        'str')

    ###省级
    county_sum省 = county_sum地市
    county_sum省['area_code'] = '330000'
    county_sum省['area_name'] = '浙江'
    county_sum省 = county_sum省.groupby(['area_code', 'area_name']).sum().reset_index()
    county_sum省.drop(['overrun100rate'], axis=1, inplace=True)
    county_sum省['overrun_rate'] = (county_sum省['overrun_num'] / county_sum省['truck_num'] * 100).round(2)
    county_sum省['overrun020_rate'] = (county_sum省['overrun020_count'] / county_sum省['truck_num'] * 100).round(2)
    county_sum省['overrun2050_rate'] = (county_sum省['overrun2050_count'] / county_sum省['truck_num'] * 100).round(2)
    county_sum省['overrun50100_rate'] = (county_sum省['overrun50100_count'] / county_sum省['truck_num'] * 100).round(2)
    county_sum省['overrun100_rate'] = (county_sum省['overrun100_count'] / county_sum省['truck_num'] * 100).round(2)
    county_sum省['station_type'] = 31
    county_sum省['update_time'] = today
    county_sum省['create_time'] = today
    county_sum省['statistics_date'] = this_month
    county_sum省['id'] = this_month2 + county_sum省['area_code'].astype('str') + county_sum省['station_type'].astype('str')
    # county_sum省.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\county_sum省.xlsx')

    ##合并

    county_sum31 = pd.DataFrame(county_sum31,
                                columns=['id', 'statistics_date', 'county_code', 'county_name',
                                         'pass_num', 'truck_num',
                                         'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count',
                                         'overrun020_rate',
                                         'overrun2050_count', 'overrun2050_rate', 'overrun50100_count',
                                         'overrun50100_rate',
                                         'overrun100_count', 'overrun100_rate', 'station_type',
                                         'update_time', 'create_time'])
    county_sum地市 = pd.DataFrame(county_sum地市,
                                columns=['id', 'statistics_date', 'city_code', 'city_name',
                                         'pass_num', 'truck_num',
                                         'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count',
                                         'overrun020_rate',
                                         'overrun2050_count', 'overrun2050_rate', 'overrun50100_count',
                                         'overrun50100_rate',
                                         'overrun100_count', 'overrun100_rate', 'station_type',
                                         'update_time', 'create_time'])
    county_sum31.rename(
        columns={'county_code': 'area_code', 'county_name': 'area_name'}, inplace=True)
    county_sum地市.rename(
        columns={'city_code': 'area_code', 'city_name': 'area_name'}, inplace=True)

    wide_table31 = pd.concat([county_sum31, county_sum地市, county_sum省])
    # wide_table31.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\wide_table31.xlsx')

    ###货运源头表
    ##县级
    county_sum71 = t_bas_basic_data_pass71
    county_sum71 = county_sum71.groupby(['city_code', 'city_name', 'county_code', 'county_name']).sum().reset_index()
    county_sum71 = pd.DataFrame(county_sum71)
    # county_sum71.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\county_sum71区县汇总.xlsx')
    county_sum71['hundred_king_num'] = county_sum71['total_weight_100']
    county_sum71['overrun100_num'] = county_sum71['overrun_100']
    county_sum71['overrun_rate'] = (county_sum71['overrun_num'] / county_sum71['truck_num'] * 100).round(2)
    county_sum71['overrun020_count'] = county_sum71['overrun_0_10'] + county_sum71['overrun_10_20']
    county_sum71['overrun020_rate'] = (county_sum71['overrun020_count'] / county_sum71['truck_num'] * 100).round(2)
    county_sum71['overrun2050_count'] = county_sum71['overrun_20_30'] + county_sum71['overrun_30_40'] + county_sum71[
        'overrun_40_50']
    county_sum71['overrun2050_rate'] = (county_sum71['overrun2050_count'] / county_sum71['truck_num'] * 100).round(2)
    county_sum71['overrun50100_count'] = county_sum71['overrun_50_60'] + county_sum71['overrun_60_70'] + county_sum71[
        'overrun_70_80'] + county_sum71['overrun_80_90'] + county_sum71['overrun_90_100']
    county_sum71['overrun50100_rate'] = (county_sum71['overrun50100_count'] / county_sum71['truck_num'] * 100).round(2)
    county_sum71['overrun100_count'] = county_sum71['overrun_100']
    county_sum71['overrun100_rate'] = (county_sum71['overrun100_count'] / county_sum71['truck_num'] * 100).round(2)
    county_sum71['station_type'] = 71
    county_sum71['update_time'] = today
    county_sum71['create_time'] = today
    county_sum71['statistics_date'] = this_month
    this_month2 = datetime.datetime(now.year, now.month, 1).strftime("%Y%m")
    county_sum71['id'] = this_month2 + county_sum71['county_code'].astype('str') + county_sum71['station_type'].astype(
        'str')
    county_sum71 = pd.DataFrame(county_sum71,
                                columns=['id', 'statistics_date', 'city_code', 'city_name', 'county_code',
                                         'county_name',
                                         'pass_num', 'truck_num',
                                         'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count',
                                         'overrun020_rate',
                                         'overrun2050_count', 'overrun2050_rate', 'overrun50100_count',
                                         'overrun50100_rate',
                                         'overrun100_count', 'overrun100_rate', 'station_type',
                                         'update_time', 'create_time'])
    ##市级
    county_sum地市71 = county_sum71.groupby(['city_code', 'city_name']).sum().reset_index()
    county_sum地市71['overrun_rate'] = (county_sum地市71['overrun_num'] / county_sum地市71['truck_num'] * 100).round(2)
    county_sum地市71['overrun020_rate'] = (county_sum地市71['overrun020_count'] / county_sum地市71['truck_num'] * 100).round(
        2)
    county_sum地市71['overrun2050_rate'] = (
                county_sum地市71['overrun2050_count'] / county_sum地市71['truck_num'] * 100).round(2)
    county_sum地市71['overrun50100_rate'] = (
                county_sum地市71['overrun50100_count'] / county_sum地市71['truck_num'] * 100).round(2)
    county_sum地市71['overrun100rate'] = (county_sum地市71['overrun100_count'] / county_sum地市71['truck_num'] * 100).round(2)
    county_sum地市71['station_type'] = 71
    county_sum地市71['update_time'] = today
    county_sum地市71['create_time'] = today
    county_sum地市71['statistics_date'] = this_month
    county_sum地市71['id'] = this_month2 + county_sum地市71['city_code'].astype('str') + county_sum地市71[
        'station_type'].astype('str')

    ###省级
    county_sum省71 = county_sum地市71
    county_sum省71['area_code'] = '330000'
    county_sum省71['area_name'] = '浙江'
    county_sum省71 = county_sum省71.groupby(['area_code', 'area_name']).sum().reset_index()
    county_sum省71.drop(['overrun100rate'], axis=1, inplace=True)
    county_sum省71['overrun_rate'] = (county_sum省71['overrun_num'] / county_sum省71['truck_num'] * 100).round(2)
    county_sum省71['overrun020_rate'] = (county_sum省71['overrun020_count'] / county_sum省71['truck_num'] * 100).round(2)
    county_sum省71['overrun2050_rate'] = (county_sum省71['overrun2050_count'] / county_sum省71['truck_num'] * 100).round(2)
    county_sum省71['overrun50100_rate'] = (county_sum省71['overrun50100_count'] / county_sum省71['truck_num'] * 100).round(
        2)
    county_sum省71['overrun100_rate'] = (county_sum省71['overrun100_count'] / county_sum省71['truck_num'] * 100).round(2)
    county_sum省71['station_type'] = 71
    county_sum省71['update_time'] = today
    county_sum省71['create_time'] = today
    county_sum省71['statistics_date'] = this_month
    county_sum省71['id'] = this_month2 + county_sum省71['area_code'].astype('str') + county_sum省71['station_type'].astype(
        'str')
    # county_sum省71.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\county_sum省71.xlsx')

    ##合并

    county_sum71 = pd.DataFrame(county_sum71,
                                columns=['id', 'statistics_date', 'county_code', 'county_name',
                                         'pass_num', 'truck_num',
                                         'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count',
                                         'overrun020_rate',
                                         'overrun2050_count', 'overrun2050_rate', 'overrun50100_count',
                                         'overrun50100_rate',
                                         'overrun100_count', 'overrun100_rate', 'station_type',
                                         'update_time', 'create_time'])
    county_sum地市71 = pd.DataFrame(county_sum地市71,
                                  columns=['id', 'statistics_date', 'city_code', 'city_name',
                                           'pass_num', 'truck_num',
                                           'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count',
                                           'overrun020_rate',
                                           'overrun2050_count', 'overrun2050_rate', 'overrun50100_count',
                                           'overrun50100_rate',
                                           'overrun100_count', 'overrun100_rate', 'station_type',
                                           'update_time', 'create_time'])
    county_sum71.rename(
        columns={'county_code': 'area_code', 'county_name': 'area_name'}, inplace=True)
    county_sum地市71.rename(
        columns={'city_code': 'area_code', 'city_name': 'area_name'}, inplace=True)

    wide_table71 = pd.concat([county_sum71, county_sum地市71, county_sum省71])

    ###现场
    ##县级

    # county_sum21= t_bas_basic_data_pass[(t_bas_basic_data_pass.station_type == 21)]
    county_sum21 = t_bas_basic_data_pass21.loc[(t_bas_basic_data_pass21.station_type == 21)]
    print(county_sum21)
    county_sum21 = county_sum21.groupby(
        ['city_code', 'city_name', 'county_code', 'county_name']).sum().reset_index()
    county_sum21 = pd.DataFrame(county_sum21)

    # county_sum21.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\county_sum211230.xlsx')
    county_sum21['hundred_king_num'] = county_sum21['total_weight_100']
    county_sum21['overrun100_num'] = county_sum21['overrun_100']
    county_sum21['overrun_rate'] = (county_sum21['overrun_num'] / county_sum21['truck_num'] * 100).round(2)
    county_sum21['overrun020_count'] = county_sum21['overrun_0_10'] + county_sum21['overrun_10_20']
    county_sum21['overrun020_rate'] = (
            county_sum21['overrun020_count'] / county_sum21['truck_num'] * 100).round(2)
    county_sum21['overrun2050_count'] = county_sum21['overrun_20_30'] + county_sum21['overrun_30_40'] + \
                                        county_sum21['overrun_40_50']
    county_sum21['overrun2050_rate'] = (
            county_sum21['overrun2050_count'] / county_sum21['truck_num'] * 100).round(2)
    county_sum21['overrun50100_count'] = county_sum21['overrun_50_60'] + county_sum21['overrun_60_70'] + \
                                         county_sum21['overrun_70_80'] + county_sum21['overrun_80_90'] + \
                                         county_sum21['overrun_90_100']
    county_sum21['overrun50100_rate'] = (
            county_sum21['overrun50100_count'] / county_sum21['truck_num'] * 100).round(2)
    county_sum21['overrun100_count'] = county_sum21['overrun_100']
    county_sum21['overrun100_rate'] = (
            county_sum21['overrun100_count'] / county_sum21['truck_num'] * 100).round(2)
    county_sum21['station_type'] = 21
    county_sum21['update_time'] = today
    county_sum21['create_time'] = today
    county_sum21['statistics_date'] = this_month
    this_month2 = datetime.datetime(now.year, now.month, 1).strftime("%Y%m")
    county_sum21['id'] = this_month2 + county_sum21['county_code'].astype('str') + county_sum21[
        'station_type'].astype('str')
    county_sum21 = pd.DataFrame(county_sum21,
                                columns=['id', 'statistics_date', 'city_code', 'city_name', 'county_code',
                                         'county_name',
                                         'pass_num', 'truck_num',
                                         'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count',
                                         'overrun020_rate',
                                         'overrun2050_count', 'overrun2050_rate', 'overrun50100_count',
                                         'overrun50100_rate',
                                         'overrun100_count', 'overrun100_rate', 'station_type',
                                         'update_time', 'create_time'])
    ##市级
    county_sum地市 = county_sum21.groupby(['city_code', 'city_name']).sum().reset_index()
    county_sum地市['overrun_rate'] = (county_sum地市['overrun_num'] / county_sum地市['truck_num'] * 100).round(2)
    county_sum地市['overrun020_rate'] = (
            county_sum地市['overrun020_count'] / county_sum地市['truck_num'] * 100).round(2)
    county_sum地市['overrun2050_rate'] = (
            county_sum地市['overrun2050_count'] / county_sum地市['truck_num'] * 100).round(2)
    county_sum地市['overrun50100_rate'] = (
            county_sum地市['overrun50100_count'] / county_sum地市['truck_num'] * 100).round(2)
    county_sum地市['overrun100rate'] = (county_sum地市['overrun100_count'] / county_sum地市['truck_num'] * 100).round(
        2)
    county_sum地市['station_type'] = 21
    county_sum地市['update_time'] = today
    county_sum地市['create_time'] = today
    county_sum地市['statistics_date'] = this_month
    county_sum地市['id'] = this_month2 + county_sum地市['city_code'].astype('str') + county_sum地市[
        'station_type'].astype('str')

    ###省级
    county_sum省 = county_sum地市
    county_sum省['area_code'] = '330000'
    county_sum省['area_name'] = '浙江'
    county_sum省 = county_sum省.groupby(['area_code', 'area_name']).sum().reset_index()
    county_sum省.drop(['overrun100rate'], axis=1, inplace=True)
    county_sum省['overrun_rate'] = (county_sum省['overrun_num'] / county_sum省['truck_num'] * 100).round(2)
    county_sum省['overrun020_rate'] = (county_sum省['overrun020_count'] / county_sum省['truck_num'] * 100).round(2)
    county_sum省['overrun2050_rate'] = (county_sum省['overrun2050_count'] / county_sum省['truck_num'] * 100).round(
        2)
    county_sum省['overrun50100_rate'] = (
            county_sum省['overrun50100_count'] / county_sum省['truck_num'] * 100).round(2)
    county_sum省['overrun100_rate'] = (county_sum省['overrun100_count'] / county_sum省['truck_num'] * 100).round(2)
    county_sum省['station_type'] = 21
    county_sum省['update_time'] = today
    county_sum省['create_time'] = today
    county_sum省['statistics_date'] = this_month
    county_sum省['id'] = this_month2 + county_sum省['area_code'].astype('str') + county_sum省[
        'station_type'].astype('str')
    # county_sum省.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\county_sum省.xlsx')

    ##合并

    county_sum21 = pd.DataFrame(county_sum21,
                                columns=['id', 'statistics_date', 'county_code', 'county_name',
                                         'pass_num', 'truck_num',
                                         'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count',
                                         'overrun020_rate',
                                         'overrun2050_count', 'overrun2050_rate', 'overrun50100_count',
                                         'overrun50100_rate',
                                         'overrun100_count', 'overrun100_rate', 'station_type',
                                         'update_time', 'create_time'])
    county_sum地市 = pd.DataFrame(county_sum地市,
                                columns=['id', 'statistics_date', 'city_code', 'city_name',
                                         'pass_num', 'truck_num',
                                         'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count',
                                         'overrun020_rate',
                                         'overrun2050_count', 'overrun2050_rate', 'overrun50100_count',
                                         'overrun50100_rate',
                                         'overrun100_count', 'overrun100_rate', 'station_type',
                                         'update_time', 'create_time'])
    county_sum21.rename(
        columns={'county_code': 'area_code', 'county_name': 'area_name'}, inplace=True)
    county_sum地市.rename(
        columns={'city_code': 'area_code', 'city_name': 'area_name'}, inplace=True)

    wide_table21 = pd.concat([county_sum21, county_sum地市, county_sum省])
    # wide_table21.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\wide_table21.xlsx')

    wide_table = pd.concat([wide_table31, wide_table71, wide_table21])

    wide_table = wide_table.fillna(value=0)
    wide_table = wide_table[(wide_table.id != 0)]
    wide_table = wide_table[(wide_table.id != '0')]
    wide_table['overrun100_num'] = wide_table['overrun100_count']
    wide_table['device_good_rate'] = 0.00
    wide_table['off_site_punish_rate'] = 0.00
    wide_table['off_site_qualified_rate'] = 0.00
    wide_table = pd.DataFrame(wide_table,
                              columns=['id', 'statistics_date', 'area_code', 'area_name',
                                       'pass_num', 'truck_num',
                                       'overrun_num', 'hundred_king_num', 'overrun100_num', 'overrun_rate',
                                       'device_good_rate', 'off_site_punish_rate', 'off_site_qualified_rate',
                                       'overrun020_count', 'overrun020_rate',
                                       'overrun2050_count', 'overrun2050_rate', 'overrun50100_count',
                                       'overrun50100_rate',
                                       'overrun100_count', 'overrun100_rate', 'station_type',
                                       'update_time', 'create_time'])
    # print(wide_table.info())
    #
    wide_table = wide_table[(wide_table.area_code != '0')
    ]
    wide_table.drop_duplicates('id', keep='last', inplace=True)
    # wide_table.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\12月\wide_table.xlsx')


    '''数据库删除'''
    sql={
    "tableName": "t_bas_pass_statistics_data",
    "where": " statistics_date= '{}'".format(this_month),
    "columns": "*"
     }
    delete_data(sql)

    '''数据插入'''
    df=wide_table.reset_index(drop=True)
    # df=df.drop(['update_time','create_time'],axis=1)
    df['statistics_date']=df['statistics_date'].astype('str')
    df['update_time']=df['update_time'].astype('str')
    df['create_time'] = df['create_time'].astype('str')
    column=df.columns.to_list()
    num=len(df['id'])
    h = 0
    while h < num:
        valve = df.loc[h].to_list()
        column_str = str(column).replace('[', "").replace(']', "").replace("'", "")
        valve_str = str(valve).replace('[', "").replace(']', "")
        sql = {
            "tableName": " t_bas_pass_statistics_data  ",
            "where": "{}".format(valve_str),
            "columns": "{}".format(column_str)
        }
        insert_data(sql)
        h += 1

    del t_bas_basic_data_pass
    del t_bas_basic_data_pass71
    del t_bas_basic_data_pass21
    from datetime import datetime

    js = datetime.now()
    sjc = js - ks
    print('运行耗时', sjc)

def ShouYe():
    from datetime import datetime
    ks = datetime.now()
    print('首页开始时间', ks)
    data_sours(0,-1)
    js = datetime.now()
    sjc = js - ks
    print('首页运行耗时', sjc)
    from threading import Timer
    import datetime
    """定时1天"""
    now_time = datetime.datetime.now()
    next_time = now_time + datetime.timedelta(days=+1)
    next_year = next_time.date().year
    next_month = next_time.date().month
    next_day = next_time.date().day
    next_time2 = datetime.datetime.strptime(
        str(next_year) + "-" + str(next_month) + "-" + str(next_day) + " " + "12:00:00", "%Y-%m-%d %H:%M:%S")
    timer_start_time2 = (next_time2 - now_time).total_seconds()
    print('下次首页运行耗时', next_time2)
    t2 = Timer(timer_start_time2, ShouYe)  # 此处使用递归调用实现
    t2.start()

def statistics_data():
    from datetime import datetime
    ks = datetime.now()
    print('结果表开始时间', ks)
    t_bas_pass_statistics_data(0,-1)
    js = datetime.now()
    sjc = js - ks
    print('结果表运行耗时', sjc)
    from threading import Timer
    import datetime
    """定时1天"""
    now_time = datetime.datetime.now()
    next_time = now_time + datetime.timedelta(days=+1)
    next_year = next_time.date().year
    next_month = next_time.date().month
    next_day = next_time.date().day
    next_time2 = datetime.datetime.strptime(
        str(next_year) + "-" + str(next_month) + "-" + str(next_day) + " " + "12:30:00", "%Y-%m-%d %H:%M:%S")
    timer_start_time2 = (next_time2 - now_time).total_seconds()
    print('下次结果表运行耗时', next_time2)
    t2 = Timer(timer_start_time2, ShouYe)  # 此处使用递归调用实现
    t2.start()

def t_bas_deal_case_statistics_data():
    from datetime import datetime
    day = datetime.now().date()  # 获取当前系统时间
    today = datetime.now()
    from datetime import datetime
    ks = datetime.now()
    print('运行开始时间', ks)
    import datetime
    starttime = day - datetime.timedelta(days=0)
    import datetime
    today = datetime.datetime.today()
    year = today.year
    month=today.month
    print(month)

    from dateutil.relativedelta import relativedelta
    now = datetime.datetime.now()
    start_time = datetime.datetime(now.year, now.month, 1).date()
    # start_time= ('{}' + '-01-01').format(year)
    print('starttime', start_time)
    end_time = start_time + relativedelta(months=1)
    print('endtime', end_time)
    入库明细 = {
        "tableName": "t_bas_over_data_collection_31  a  LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
        "where": "a.valid_time >='{} 00:00:00' and a.valid_time <'{} 00:00:00'  ".format(
            start_time, end_time),
        "columns": "a.area_county as area_code,a.car_no ,a.total_weight,a.overrun_rate,a.status ,"
                   "a.law_judgment ,c.punish_money ,a.xh_count ,a.dx_count ,a.record_code"
    }
    data2=get_data(入库明细)

    data2['overrun_rate'] = data2['overrun_rate'].astype('float')
    data2['total_weight'] = data2['total_weight'].astype('float')
    data2['punish_money'] = data2['punish_money'].astype('float')
    data2['law_judgment'] = data2['law_judgment'].astype('int')
    data2['xh_count'] = data2['xh_count'].astype('int')
    data2['dx_count'] = data2['dx_count'].astype('int')
    first_trial_num = data2.groupby(['area_code'])['record_code'].count().reset_index(name='first_trial_num')
    first_trial_pass_num = data2[(data2['status'] != 15)].groupby(['area_code'])['record_code'].count().reset_index(name='first_trial_pass_num')
    check_num = data2[(data2['status'] != 15)].groupby(['area_code'])['record_code'].count().reset_index(
        name='check_num')
    check_pass_num = data2[(data2['status'] != 15)&(data2['status'] != 0)].groupby(['area_code'])['record_code'].count().reset_index(
        name='check_pass_num')
    illegal_judgment_num = data2[(data2['law_judgment'] == 1)].groupby(['area_code'])['record_code'].count().reset_index(
        name='illegal_judgment_num')
    letter_inform_num = data2[(data2['xh_count'] > 0)].groupby(['area_code'])['record_code'].count().reset_index(
        name='letter_inform_num')
    sms_inform_num = data2[(data2['dx_count'] > 0)].groupby(['area_code'])['record_code'].count().reset_index(
        name='sms_inform_num')
    case_num = data2[(data2['status'] == 13)&(data2['status'] == 6)].groupby(['area_code'])['record_code'].count().reset_index(
        name='case_num')
    overrun100_case_num = data2[(data2['status'] == 13)&(data2['status'] == 6)&(data2['overrun_rate'] >= 100)&(data2['total_weight'] < 100)].groupby(['area_code'])['record_code'].count().reset_index(
        name='overrun100_case_num')
    hundred_king_case_num = data2[(data2['status'] == 13)&(data2['status'] == 6)&(data2['total_weight'] >= 100)].groupby(['area_code'])['record_code'].count().reset_index(
        name='hundred_king_case_num')
    case_over_num = data2[(data2['status'] == 13)].groupby(['area_code'])['record_code'].count().reset_index(
        name='case_over_num')
    overrun100_case_over_num = data2[(data2['status'] == 13)&(data2['overrun_rate'] >= 100)&(data2['total_weight'] < 100)].groupby(['area_code'])['record_code'].count().reset_index(
        name='overrun100_case_over_num')
    hundred_king_case_over_num = data2[(data2['status'] == 13)&(data2['total_weight'] >= 100)].groupby(['area_code'])['record_code'].count().reset_index(
        name='hundred_king_case_over_num')
    punish_money = data2.groupby(['area_code'])['punish_money'].sum().reset_index(
        name='punish_money')
    t_bas_deal_case_statistics_data = pd.merge(first_trial_num, first_trial_pass_num, on=['area_code'], how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, check_num, on=['area_code'], how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, check_pass_num, on=['area_code'], how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, illegal_judgment_num, on=['area_code'], how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, letter_inform_num, on=['area_code'], how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, sms_inform_num, on=['area_code'], how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, case_num, on=['area_code'],
                                               how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, overrun100_case_num, on=['area_code'],
                                               how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, hundred_king_case_num, on=['area_code'],
                                               how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, case_over_num, on=['area_code'],
                                               how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, overrun100_case_over_num, on=['area_code'],
                                               how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, hundred_king_case_over_num, on=['area_code'],
                                               how='left')
    t_bas_deal_case_statistics_data = pd.merge(t_bas_deal_case_statistics_data, punish_money, on=['area_code'],
                                               how='left')
    t_bas_deal_case_statistics_data=t_bas_deal_case_statistics_data.fillna(0)
    t_bas_deal_case_statistics_data['statistics_date']=starttime.strftime("%Y-%m")
    t_bas_deal_case_statistics_data['first_trial_pass_rate']= t_bas_deal_case_statistics_data.apply(lambda x: x['first_trial_pass_num'] / x['first_trial_num']*100, axis=1).round(2)
    t_bas_deal_case_statistics_data['check_pass_rate'] = t_bas_deal_case_statistics_data.apply(
        lambda x: x['check_pass_num'] / x['check_num'] * 100, axis=1).round(2)
    t_bas_deal_case_statistics_data['inform_num'] = t_bas_deal_case_statistics_data.apply(
        lambda x: x['letter_inform_num']+x['sms_inform_num'], axis=1).round(0)
    t_bas_deal_case_statistics_data['avg_illegal_judgment_num'] = t_bas_deal_case_statistics_data.apply(
        lambda x: x['illegal_judgment_num'] / month, axis=1).round(2)

    try:
        from sqlalchemy import create_engine

        user = "root"
        password = "zcits123456"
        host = "192.168.1.229"
        db = "db_manage_overruns"

        pwd = parse.quote_plus(password)

        engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:3306/{db}?charset=utf8")
        # result_table
        # 要写入的数据表，这样写的话要提前在数据库建好表
        # t_bas_though.to_sql(name='t_bas_car_through', con=engine, if_exists='append', index=False)
        # result_table
        # 要写入的数据表，这样写的话要提前在数据库建好表
        t_bas_deal_case_statistics_data.to_sql(name='t_bas_deal_case_statistics_data', con=engine, if_exists='append', index=False)
    except Exception as e:
        print("mysql插入失败", e)
    # data2 = pd.DataFrame(data2,
    #                        columns=['地市', '区县', '车籍地', '车牌号', '所属运输企业名称','联系电话','检测时间', '入库时间', '检测站点', '总重', '轴数','超限率', '案件状态', '外省抄告', '结案时间', '处罚金额','record_code'])

    # with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\t_bas_deal_case_statistics_data.xlsx') as writer1:
    #     # data1.to_excel(writer1, sheet_name='sql_80吨以上交通现场', index=True)
    #     t_bas_deal_case_statistics_data.to_excel(writer1, sheet_name='双百入库明细', index=True)




if __name__ == "__main__":
     # ShouYe()
     # chushihua()
     statistics_data()
     # t_bas_deal_case_statistics_data()


