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

def 数字路政通报数据写入():
    import pandas as pd
    from datetime import datetime
    import calendar

    today = datetime.now()

    from datetime import datetime
    this_month='2023-08'
    wide_table = pd.read_excel(r'C:\Users\stayhungary\Desktop\数字路政.xlsx', sheet_name='t_bas_pass_statistics_data')
    wide_table.drop_duplicates('id', keep='last', inplace=True)
    # wide_table.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\12月\wide_table.xlsx')


    '''数据库删除'''
    sql={
    "tableName": "t_bas_pass_statistics_data",
    "where": " statistics_date= '2023-10'",
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
        print(sql)
        h += 1

def t_bas_basic_data_report():
    import pandas as pd
    from datetime import datetime
    import calendar

    today = datetime.now()

    from datetime import datetime
    this_month='2023-08'
    wide_table = pd.read_excel(r'C:\Users\stayhungary\Desktop\t_bas_basic_data_report.xlsx', sheet_name='Sheet1')
    wide_table.drop_duplicates('id', keep='last', inplace=True)
    # wide_table.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\12月\wide_table.xlsx')


    '''数据库删除'''
    sql={
    "tableName": "t_bas_basic_data_report",
    "where": " statistic_date = '2023-09'",
    "columns": "*"
     }
    delete_data(sql)

    '''数据插入'''
    df=wide_table.reset_index(drop=True)
    # df=df.drop(['update_time','create_time'],axis=1)
    df['statistic_date']=df['statistic_date'].astype('str')
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
            "tableName": " t_bas_basic_data_report  ",
            "where": "{}".format(valve_str),
            "columns": "{}".format(column_str)
        }
        insert_data(sql)
        print(sql)
        h += 1


def t_sys_access_quarter():
    import pandas as pd
    from datetime import datetime
    import calendar

    today = datetime.now()

    from datetime import datetime
    this_month='2023-08'
    wide_table = pd.read_excel(r'C:\Users\stayhungary\Desktop\t_sys_access_quarter.xls', sheet_name='t_sys_access_quarter')
    wide_table.drop_duplicates('id', keep='last', inplace=True)
    # wide_table.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\12月\wide_table.xlsx')


    # '''数据库删除'''
    sql={
    "tableName": "t_sys_access_quarter",
    "where": " id ='46668208802852326'",
    "columns": "*"
     }
    delete_data(sql)

    # '''数据库删除'''
    sql={
    "tableName": "t_sys_access_quarter",
    "where": " statistic_date ='2023-07'",
    "columns": "*"
     }
    delete_data(sql)
    '''数据插入'''
    df=wide_table.reset_index(drop=True)
    # df=df.drop(['update_time','create_time'],axis=1)
    df['access_time']=df['access_time'].astype('str')
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
            "tableName": " t_sys_access_quarter ",
            "where": "{}".format(valve_str),
            "columns": "{}".format(column_str)
        }
        insert_data(sql)
        print(sql)
        h += 1

def t_sys_access():
    import pandas as pd
    from datetime import datetime
    import calendar

    today = datetime.now()

    from datetime import datetime
    this_month='2023-08'
    wide_table = pd.read_excel(r'C:\Users\stayhungary\Desktop\t_sys_access.xlsx', sheet_name='t_sys_access')
    wide_table.drop_duplicates('id', keep='last', inplace=True)
    # wide_table.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\12月\wide_table.xlsx')
    wide_table=wide_table.fillna(0)

    # '''数据库删除'''
    sql={
    "tableName": "t_sys_access",
    "where": " statistic_date ='2023-09'",
    "columns": "*"
     }
    delete_data(sql)

    '''数据插入'''
    df=wide_table.reset_index(drop=True)
    # df=df.drop(['update_time','create_time'],axis=1)
    df['access_time']=df['access_time'].astype('str')
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
            "tableName": " t_sys_access ",
            "where": "{}".format(valve_str),
            "columns": "{}".format(column_str)
        }
        insert_data(sql)
        print(sql)
        h += 1

def t_bas_station_statistics_data():
    import pandas as pd
    from datetime import datetime
    import calendar

    today = datetime.now()

    from datetime import datetime
    this_month='2023-08'
    wide_table = pd.read_excel(r'C:\Users\stayhungary\Desktop\数字路政.xlsx', sheet_name='t_bas_station_statistics_data')
    wide_table.drop_duplicates('id', keep='last', inplace=True)
    # wide_table.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\12月\wide_table.xlsx')


    '''数据库删除'''
    sql={
    "tableName": "t_bas_station_statistics_data",
    "where": " statistics_date>= '2023-09'",
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
            "tableName": " t_bas_station_statistics_data  ",
            "where": "{}".format(valve_str),
            "columns": "{}".format(column_str)
        }
        insert_data(sql)
        print(sql)
        h += 1


if __name__ == "__main__":
     # ShouYe()
     # statistics_data()
     # t_sys_access_quarter()
     数字路政通报数据写入()
     # t_bas_basic_data_report()
     # t_bas_station_statistics_data()

