# coding: utf-8


import pandas as pd
import numpy as np
import time
import requests
import base64
import json


file_name = r"G:\智诚\2023日常给出数据\省通报\202309\0927test.xlsx"
q案件 = '2023-01-01'
s案件 = '2023-09-30'
start_time = '2023-10-01'
end_time = '2023-10-20'




# 获取数据
def get_df_from_db(sql):
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

def data_source():
        df_数据汇总 = pd.read_excel(file_name, sheet_name='on_line')
        df_数据汇总.columns = df_数据汇总.iloc[0]
        df_数据汇总 = df_数据汇总.iloc[1:].reset_index(drop=True)
        df_数据汇总.loc[df_数据汇总['货车数'] == 0, '货车数'] = 1
        df_数据汇总['超限20%除外数'] = df_数据汇总['超限20%除外数'].astype('int')
        df_数据汇总['货车数'] = df_数据汇总['货车数'].astype('int')
        df_数据汇总['超限20%除外超限率(%)'] = (df_数据汇总['超限20%除外数'] / df_数据汇总['货车数'] * 100).round(2)
        df_数据汇总.loc[df_数据汇总['货车数'] == 1, '货车数'] = 0

        df_报修点位统计 = pd.read_excel(file_name, sheet_name='off_line')
        df_报修点位统计.columns = df_报修点位统计.iloc[0]
        df_报修点位统计 = df_报修点位统计.iloc[1:].reset_index(drop=True)
        df_接入数 = pd.read_excel(file_name,sheet_name='接入数')
        return df_数据汇总, df_报修点位统计,df_接入数


def 总重80吨以上明细():
    ##合规率
    t_sys_station = {
        "tableName": "t_sys_station ",
        "where": " station_status=0 and station_type =31 and is_deleted= 0 ",
        "columns": "station_name,station_code,station_status,station_type,area_county  "
    }
    t_sys_station = get_df_from_db(t_sys_station)
    df_数据汇总, df_报修点位统计,df_接入数 = data_source()

    U_汇总_站点表 = pd.merge(df_数据汇总, t_sys_station, left_on='站点名称', right_on='station_name', how='left')
    station_code = U_汇总_站点表['station_code']

    sql = {
        "tableName": "t_bas_over_data_31 a left join t_bas_over_data_collection_31 b on a.record_code=b.record_code left join t_code_area c on a.area_county=c.county_code ",
        "where": "a.out_station_time between '{} 00:00:00' and '{} 23:59:59' and a.total_weight>84 and a.area_county=330212  AND a.is_unusual = 0  and a.allow is null ".format(start_time, end_time),
        "columns": "c.city 地市,c.county 区县, a.area_county,a.out_station,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号码,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,b.status 案件状态,a.site_name 站点名称,a.is_collect 证据满足,b.law_judgment 判定需处罚,b.make_copy  外省抄告,a.link_man 所属运输企业名称,a.phone 联系电话, b.vehicle_county 车籍地,a.record_code 流水号"
    }
    t_bas_over_data_31_80 = get_df_from_db(sql)
    df_big = t_bas_over_data_31_80[t_bas_over_data_31_80.loc[:, 'out_station'].isin(station_code)]

    df_big = pd.DataFrame(df_big,
                          columns=["地市", "区县", "站点名称", "检测时间", "车牌号码", "总重", "限重",
                                   "超重", "轴数", "超限率", "证据满足", "所属运输企业名称", "联系电话", "车籍地", "流水号", "入库时间", "案件状态", "判定需处罚",
                                   "外省抄告", "area_county"])
    总重80吨以上明细 = df_big.sort_values(by=['area_county'],
                                   ascending=True).reset_index(drop=True)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\总重80吨以上明细22.xlsx') as writer1:
        总重80吨以上明细.to_excel(writer1, sheet_name='明细', index=False)

def Compliance_rate():
    df_big = pd.read_excel(r'C:\Users\stayhungary\Desktop\总重80吨以上明细22.xlsx', sheet_name='明细')
    sql = {
        "tableName": "t_bas_over_data_collection_31 ",
        "where": "out_station_time between '{} 00:00:00' and  '{} 00:00:00' and total_weight>80 and law_judgment=1".format(start_time, end_time),
        "columns": "area_county,out_station_time,valid_time,status,total_weight,record_code 流水号"
    }
    t_bas_over_data_collection_31 = get_df_from_db(sql)

    总重80吨以上数 = df_big.groupby(['area_county'])['流水号'].count().reset_index(name='本月超限80吨以上')
    df_big['总重'] = df_big['总重'].astype('float')
    总重90吨以上数 = df_big[df_big['总重'] > 90].groupby(['area_county'])['流水号'].count().reset_index(name='本月超限90吨以上')
    总重80_90以上 = pd.merge(总重80吨以上数, 总重90吨以上数, on=['area_county'], how='left')

    U_过车_站点表 = df_big.copy()
    U_过车_站点表 = U_过车_站点表[(U_过车_站点表['证据满足'] == 1)]

    月超限80以上且满足处罚条件总数 = U_过车_站点表.groupby(['area_county'])['流水号'].count().reset_index(name='80吨以上且满足')

    月超限80以上审核通过总数 = t_bas_over_data_collection_31.groupby(['area_county'])['流水号'].count().reset_index(
        name='80吨以上且审核通过')

    总重80吨以上相关 = pd.merge(总重80_90以上, 月超限80以上且满足处罚条件总数, on=['area_county'], how='left')
    总重80吨以上相关['area_county'] = 总重80吨以上相关['area_county'].astype('string')
    总重80吨以上相关 = pd.merge(总重80吨以上相关, 月超限80以上审核通过总数, on=['area_county'], how='outer')

    总重80吨以上相关 = 总重80吨以上相关.fillna(0, inplace=False)
    总重80吨以上相关.rename(columns={'area_county': '区县编码'}, inplace=True)
    return 总重80吨以上相关,df_big

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
    import datetime
    endtime1=endtime1+ datetime.timedelta(days=1)
    理应在线天数 = (endtime1 - starttime1).days
    print(理应在线天数)

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
    pass_truck_num = get_df_from_db(pass_truck_num)

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

def Key_freight_sources():
    """ 引入原始表 """
    t_code_area = {
        "tableName": "t_code_area ",
        "where": "province_code = '330000'",
        "columns": "city,county,city_code,county_code"
    }
    t_code_area = get_df_from_db(t_code_area)
    # sql = "SELECT * from t_bas_source_company where area_province = '330000' and is_statictis =1  and is_deleted=0 "
    # t_bas_source_company=get_df_from_db(sql)
    # sql = "SELECT * FROM t_bas_source_company_equipment WHERE is_deleted = 0 OR is_deleted IS NULL"
    # t_bas_source_company_equipment=get_df_from_db(sql)
    站点总数 = pd.read_excel(r"G:\智诚\2023日常给出数据\货运源头\重点货运源头445家明细0309.xlsx")
    数据站点总数 = 站点总数.groupby(['city', 'city_code', 'county', 'county_code'])['id'].count().reset_index(name='数据站点总数')

    数据站点总数['county_code'] = 数据站点总数['county_code'].astype('string')

    t_sys_station = {
        "tableName": "t_sys_station ",
        "where": "station_type = 71 AND is_deleted = 0 AND station_status != 1  and station_code IS NOT null",
        "columns": "station_name,station_code,station_status,station_type,area_county  "
    }
    t_sys_station = get_df_from_db(t_sys_station)
    # sql = "SELECT * FROM t_sys_station WHERE station_type = 71 AND is_deleted = 0 AND station_status != 1  and station_code IS NOT null "
    # t_sys_station = get_df_from_db(sql)

    t_bas_pass_data_71 = {
        "tableName": "t_bas_pass_data_71 ",
        "where": " out_station_time between '{} 00:00:00' and '{} 00:00:00'  and is_truck =1 and insert_time <='{} 00:00:00'".format( start_time, end_time, end_time),
        "columns": "area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate,axis,site_name"
    }
    t_bas_pass_data_71 = get_df_from_db(t_bas_pass_data_71)
    t_bas_pass_data_71['total_weight'] = t_bas_pass_data_71['total_weight'].astype('float')
    t_bas_pass_data_71['overrun_rate'] = t_bas_pass_data_71['overrun_rate'].astype('float')
    t_bas_pass_data_71['overrun'] = t_bas_pass_data_71['overrun'].astype('float')
    t_bas_pass_data_71['out_station_time'] = pd.to_datetime(t_bas_pass_data_71['out_station_time'])


    """拼接表"""
    U_源头_区域表 = pd.merge(t_bas_pass_data_71, t_code_area, left_on='area_county', right_on='county_code', how='left')

    企业_源头_站点表 = pd.merge(t_sys_station, t_code_area, left_on='area_county', right_on='county_code', how='left')

    station_code = 企业_源头_站点表['station_code']
    U_源头_区域表 = U_源头_区域表[U_源头_区域表.loc[:, 'out_station'].isin(station_code)]
    # q = input("请输入储存路径(C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总)：")
    # with pd.ExcelWriter('{}/U_源头_区域表.xlsx'.format(q))as writer1:
    #      U_源头_区域表.to_excel(writer1, sheet_name='sheet1', index=True)

    """超限数"""
    超限20_50 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 20) & (U_源头_区域表['overrun_rate'] <= 50)].groupby(
        ['city', 'county', 'county_code'])['car_no'].count().reset_index(name='20-50%数')
    超限50_100 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 50) & (U_源头_区域表['overrun_rate'] <= 100)].groupby(
        ['city', 'county', 'county_code'])['car_no'].count().reset_index(name='50-100%数')
    超限100 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 100) & (U_源头_区域表['overrun_rate'] <= 450)].groupby(
        ['city', 'county', 'county_code'])['car_no'].count().reset_index(name='100%以上数')

    """设备上线率"""

    U_源头_区域表['取日'] = U_源头_区域表['out_station_time'].apply(lambda x: x.strftime('%d'))
    在线天数大于20天 = U_源头_区域表.groupby(['city', 'county_code', 'county', 'out_station', 'site_name'])['取日'].nunique().reset_index(
        name='在线天数')
    货运量大于2万吨 = U_源头_区域表.groupby(['city', 'county_code', 'county', 'out_station', 'site_name'])['total_weight'].sum().reset_index(
        name='货运总重')
    过车数大于410辆 = U_源头_区域表.groupby(['city', 'county_code', 'county', 'out_station', 'site_name'])['city'].count().reset_index(
        name='货车数')
    站点完好数 = pd.merge(在线天数大于20天, 货运量大于2万吨, on=['city', 'county_code', 'county', 'out_station', 'site_name'], how='left')
    站点完好数 = pd.merge(站点完好数, 过车数大于410辆, on=['city', 'county_code', 'county', 'out_station', 'site_name'], how='left')
    站点完好数['货运总重'] = pd.to_numeric(站点完好数['货运总重'], errors='coerce')

    站点完好数区县 = 站点完好数.groupby(['city', 'county', 'county_code'])['货车数', '货运总重'].sum()
    在线站点数 = 站点完好数[(站点完好数['在线天数'] > 20) | (站点完好数['货运总重'] > 20000) | (站点完好数['货车数'] > 410)].groupby(
        ['city', 'county', 'county_code'])['out_station'].count().reset_index(name='在线站点数')

    """聚合"""

    货运源头监控数据 = pd.merge(数据站点总数, 超限20_50, on=['city', 'county', 'county_code'], how='left')
    货运源头监控数据 = pd.merge(货运源头监控数据, 超限50_100, on=['city', 'county', 'county_code'], how='left')
    货运源头监控数据 = pd.merge(货运源头监控数据, 超限100, on=['city', 'county', 'county_code'], how='left')
    货运源头监控数据 = pd.merge(货运源头监控数据, 在线站点数, on=['city', 'county', 'county_code'], how='left')
    货运源头监控数据 = pd.merge(货运源头监控数据, 站点完好数区县, on=['city', 'county', 'county_code'], how='left')
    货运源头监控数据 = 货运源头监控数据.fillna(0, inplace=False)
    货运源头监控数据['源头单位平均过车数（辆次）'] = 货运源头监控数据.apply(lambda x: x['货车数'] / (x['数据站点总数'] + 0.0000001), axis=1).round(0)
    货运源头监控数据['设备上线率（%）'] = 货运源头监控数据.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据['20-50%占比'] = 货运源头监控数据.apply(lambda x: x['20-50%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据['50-100%占比'] = 货运源头监控数据.apply(lambda x: x['50-100%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据['100%以上占比'] = 货运源头监控数据.apply(lambda x: x['100%以上数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据['设备上线率（%）'] = 货运源头监控数据.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据 = 货运源头监控数据.fillna(0, inplace=False)
    货运源头监控数据['设备上线率（%）'] = 货运源头监控数据['设备上线率（%）'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据['20-50%占比'] = 货运源头监控数据['20-50%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据['50-100%占比'] = 货运源头监控数据['50-100%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据['100%以上占比'] = 货运源头监控数据['100%以上占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据.rename(columns={'city': '地市', 'county': '区县'}, inplace=True)
    货运源头监控数据 = pd.DataFrame(货运源头监控数据,
                            columns=['地市', '区县', '数据站点总数', '货车数', '源头单位平均过车数（辆次）', '在线站点数', '设备上线率（%）', '20-50%数',
                                     '50-100%数', '100%以上数', '20-50%占比', '50-100%占比', '100%以上占比', 'city_code',
                                     'county_code'])
    货运源头监控数据 = 货运源头监控数据.sort_values('county_code', ascending=True)

    货运源头监控数据地市 = 货运源头监控数据.groupby(['地市', 'city_code']).sum().reset_index()
    货运源头监控数据地市 = 货运源头监控数据地市.fillna(0, inplace=False)
    货运源头监控数据地市['源头单位平均过车数（辆次）'] = 货运源头监控数据地市.apply(lambda x: x['货车数'] / (x['数据站点总数'] + 0.0000001), axis=1).round(0)
    货运源头监控数据地市['设备上线率（%）'] = 货运源头监控数据地市.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据地市['20-50%占比'] = 货运源头监控数据地市.apply(lambda x: x['20-50%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据地市['50-100%占比'] = 货运源头监控数据地市.apply(lambda x: x['50-100%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据地市['100%以上占比'] = 货运源头监控数据地市.apply(lambda x: x['100%以上数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据地市['设备上线率（%）'] = 货运源头监控数据地市.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据地市 = 货运源头监控数据地市.fillna(0, inplace=False)
    货运源头监控数据地市['设备上线率（%）'] = 货运源头监控数据地市['设备上线率（%）'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据地市['20-50%占比'] = 货运源头监控数据地市['20-50%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据地市['50-100%占比'] = 货运源头监控数据地市['50-100%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据地市['100%以上占比'] = 货运源头监控数据地市['100%以上占比'].apply(lambda x: format(x, '.2%'))

    货运源头监控数据地市 = pd.DataFrame(货运源头监控数据地市,
                              columns=['地市', '数据站点总数', '货车数', '源头单位平均过车数（辆次）', '在线站点数', '设备上线率（%）', '20-50%数',
                                       '50-100%数', '100%以上数', '20-50%占比', '50-100%占比', '100%以上占比', 'city_code'])
    货运源头监控数据地市 = 货运源头监控数据地市.sort_values('city_code', ascending=True)
    货运源头监控数据省 = 货运源头监控数据地市
    货运源头监控数据省['省'] = '浙江省'
    货运源头监控数据省 = 货运源头监控数据省.groupby(['省']).sum().reset_index()
    货运源头监控数据省 = 货运源头监控数据省.fillna(0, inplace=False)
    货运源头监控数据省['源头单位平均过车数（辆次）'] = 货运源头监控数据省.apply(lambda x: x['货车数'] / (x['数据站点总数'] + 0.0000001), axis=1).round(0)
    货运源头监控数据省['设备上线率（%）'] = 货运源头监控数据省.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据省['20-50%占比'] = 货运源头监控数据省.apply(lambda x: x['20-50%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据省['50-100%占比'] = 货运源头监控数据省.apply(lambda x: x['50-100%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据省['100%以上占比'] = 货运源头监控数据省.apply(lambda x: x['100%以上数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据省['设备上线率（%）'] = 货运源头监控数据省.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据省 = 货运源头监控数据省.fillna(0, inplace=False)
    货运源头监控数据省['设备上线率（%）'] = 货运源头监控数据省['设备上线率（%）'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据省['20-50%占比'] = 货运源头监控数据省['20-50%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据省['50-100%占比'] = 货运源头监控数据省['50-100%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据省['100%以上占比'] = 货运源头监控数据省['100%以上占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据省.rename(columns={'省': '地市'}, inplace=True)
    货运源头监控数据省 = pd.DataFrame(货运源头监控数据省, columns=['地市', '数据站点总数', '货车数', '源头单位平均过车数（辆次）', '在线站点数', '设备上线率（%）', '20-50%数',
                                                 '50-100%数', '100%以上数', '20-50%占比', '50-100%占比', '100%以上占比'])
    货运源头监控数据省市 = pd.concat([货运源头监控数据地市, 货运源头监控数据省])

    # with pd.ExcelWriter(r'C:\Users\liu.wenjie\Desktop\全部源头.xlsx') as writer1:
    #     货运源头监控数据.to_excel(writer1, sheet_name='区县', index=False)
    #     货运源头监控数据地市.to_excel(writer1, sheet_name='地市', index=False)
    #     货运源头监控数据省市.to_excel(writer1, sheet_name='省', index=False)
    #     站点完好数.to_excel(writer1, sheet_name='站点数据明细', index=False)
    货运源头监控数据 = pd.DataFrame(货运源头监控数据,
                            columns=['后面是源头数据', '数据站点总数', '货车数', '源头单位平均过车数（辆次）', '在线站点数', '设备上线率（%）', '20-50%数',
                                     '50-100%数', '100%以上数', '20-50%占比', '50-100%占比', '100%以上占比','county_code'])
    货运源头监控数据.rename(columns={'货车数': '源头货车数'}, inplace=True)
    货运源头监控数据.rename(columns={'county_code': '区县编码'}, inplace=True)
    货运源头监控数据['区县编码'] = 货运源头监控数据['区县编码'].astype('string')
    return  货运源头监控数据,货运源头监控数据省市,站点完好数




if __name__ == "__main__":
    总重80吨以上明细()
    总重80吨以上相关,df_big=Compliance_rate()
    货运源头监控数据, 货运源头监控数据省市, 站点完好数=Key_freight_sources()
    df_数据汇总 = data_station()
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\宁波鄞州10月截止20号v1.0.xlsx') as writer1:
        # df_报修点位统计.to_excel(writer1, sheet_name='报修', index=False)
        df_数据汇总.to_excel(writer1, sheet_name='在线')
        总重80吨以上相关.to_excel(writer1, sheet_name='总重80吨以上相关汇总', index=False)
        df_big.to_excel(writer1, sheet_name='总重80吨以上相关明细', index=True)
        站点完好数.to_excel(writer1, sheet_name='重点货运源头', index=True)

