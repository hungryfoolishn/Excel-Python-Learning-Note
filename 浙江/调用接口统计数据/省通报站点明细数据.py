# coding: utf-8


import time
import pandas as pd
import requests
import base64
import datetime
import pymysql
import json
import csv


# ##超限率以及完好率
# df_数据汇总 = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\月报\202304\4数据汇总表0504.xlsx")
# # df_数据汇总.columns = df_数据汇总.iloc[0]
# # df_数据汇总 = df_数据汇总.iloc[3:].reset_index(drop=True)
# df_报修点位统计 = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\月报\202304\报修点位统计表.xls")
# df_报修点位统计.columns = df_报修点位统计.iloc[0]
# df_报修点位统计 = df_报修点位统计.iloc[1:].reset_index(drop=True)
""" 引入原始表 """
q案件 = '2023-01-01'
s案件 = '2023-02-01'
q合规率 = '2023-01-01'
s合规率 = '2023-02-01'
start_time = '2023-10-01'
end_time = '2023-10-11'
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


def data_station():
    from datetime import datetime
    from urllib import parse

    day = datetime.now().date()  # 获取当前系统时间
    import datetime

    now = day - datetime.timedelta(days=0)
    print('starttime', now)
    from datetime import datetime

    starttime = start_time
    endtime = end_time
    starttime1 = datetime.strptime(starttime, '%Y-%m-%d')
    endtime1 = datetime.strptime(endtime, '%Y-%m-%d')

    理应在线天数 = (endtime1 - starttime1).days

    sql_t_code_area = {
        "tableName": "   t_code_area    ",
        "where": "  is_deleted =0 and province_code= '330000' ",
        "columns": "county_code ,city_code,city ,county"
    }
    t_code_area = get_data(sql_t_code_area)

    ##总站点
    total_station = {
        "tableName": "   t_sys_station     ",
        "where": "   station_type=31 and is_deleted= 0 and station_status in (0,3)",
        "columns": "station_name,station_code,station_status,station_type,area_county"
    }
    total_station = get_data(total_station)

    # ##报修站点
    # maintain_station = {
    #     "tableName": " t_sys_station_maintain a inner join t_sys_station b on a.station_code=b.station_code and b.station_status in (0,3) and a.insert_time <'{}'     ".format(
    #     endtime),
    #     "where": "   end_time>='{}'   or end_time is null".format(starttime),
    #     "columns": "b.station_name as '报修站点名称',a.station_code,reason as '备注',a.area_county "
    # }
    # maintain_station = get_data(maintain_station)
    # print(maintain_station)
    # df_报修点位统计 = pd.merge(maintain_station, t_code_area, how='left', left_on=['area_county'], right_on=['county_code'])
    # df_报修点位统计 = df_报修点位统计.drop_duplicates(['station_code'])
    # df_报修点位统计 = df_报修点位统计.sort_values('county_code', ascending=True, ignore_index=True)
    #
    # df_报修点位统计.rename(
    #     columns={'city': '地市', 'county': '区县'}, inplace=True)
    # df_报修点位统计 = pd.DataFrame(df_报修点位统计,
    #                          columns=['地市', '区县', '报修站点名称', '备注'])

    # ##在用站点
    # online_station = total_station[~total_station.loc[:, 'station_code'].isin(maintain_station['station_code'])]

    ##在用站点明细数据
    pass_truck_num = {
        "tableName": "t_bas_basic_data_pass a LEFT JOIN t_sys_station b on a.station_code=b.station_code   ",
        "where": " a.statistic_date  >='{}' and a.statistic_date  <='{}'and county_name ='鄞州' and a.station_type =31".format(starttime, endtime),
        "columns": "city_name,county_code,county_name,a.station_code,a.station_name,statistic_date,truck_num,overrun_num,no_car_num,overrun_0_10,overrun_10_20"
    }
    pass_truck_num = get_data(pass_truck_num)

    # pass_truck_num = pass_truck_num[pass_truck_num.loc[:, 'station_code'].isin(online_station['station_code'])]
    df = pass_truck_num.groupby(
        ['city_name', 'county_code', 'county_name', 'station_code', 'station_name']).sum().reset_index()

    pass_truck_num = pass_truck_num[(0 < pass_truck_num['truck_num'])]
    pass_truck_num['statistic_date'] = pd.to_datetime(pass_truck_num['statistic_date'])
    pass_truck_num['取日'] = pass_truck_num['statistic_date'].apply(lambda x: x.strftime('%d'))
    实际在线天数 = pass_truck_num.groupby(['station_code'])['取日'].nunique().reset_index(name='实际在线天数')
    df = pd.merge(df, 实际在线天数, on=['station_code'], how='left')
    df['理应在线天数'] = 理应在线天数
    df['在线率'] = (df['实际在线天数'] / df['理应在线天数'] * 100).round(2)
    df['百吨王数'] = 0
    df['超限100%数'] = 0
    # city_name,county_code,county_name,a.station_code,a.station_name,statistic_date,truck_num,
    # overrun_num,no_car_num,overrun_0_10,overrun_10_20
    df['超限率(%)'] = (df['overrun_num'] / df['truck_num'] * 100).round(2)
    df['超限10%除外数'] = df['overrun_num'] - df['overrun_0_10']
    df['超限10%除外超限率(%)'] = (df['超限10%除外数'] / df['truck_num'] * 100).round(2)
    df['超限20%除外数'] = df['overrun_num'] - df['overrun_0_10'] - df['overrun_10_20']
    df['超限20%除外超限率(%)'] = (df['超限20%除外数'] / df['truck_num'] * 100).round(2)
    df['最后接收时间'] = now
    df = df.fillna(value=0)

    df.rename(
        columns={'station_name': '站点名称',
                 'city_name': '地市', 'county_name': '区县', 'truck_num': '货车数',
                 'overrun_num': '超限数'}, inplace=True)

    df = pd.DataFrame(df, columns=['站点名称', '地市', '区县', '理应在线天数', '实际在线天数', '在线率', '货车数', '超限数', '百吨王数', '超限100%数',
                                   '超限10%除外超限率(%)', '超限20%除外数', '超限20%除外超限率(%)', '超限率(%)', '最后接收时间', 'county_code',
                                   'station_code'])
    df_数据汇总 = df.sort_values('county_code', ascending=True, ignore_index=True)
    return df_数据汇总



if __name__ == "__main__":
    df_数据汇总= data_station()

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\宁波鄞州10月截止10号2.xlsx') as writer1:
        # df_报修点位统计.to_excel(writer1, sheet_name='报修', index=False)
        df_数据汇总.to_excel(writer1, sheet_name='在线')
