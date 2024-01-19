# coding: utf-8


import pandas as pd
import numpy as np
import time
import requests
import base64
import json


file_name = r"G:\智诚\2023日常给出数据\省通报\202310\1030test.xlsx"
q案件 = '2023-01-01'
s案件 = '2023-10-31'
start_time = '2023-10-01'
end_time = '2023-10-29'




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
        df_数据汇总 = df_数据汇总[(df_数据汇总.站点名称 != '（经七路）江南路方向K0+200')]
        df_数据汇总.loc[(df_数据汇总['区县'] == '诸暨'), '货车数'] = df_数据汇总[
            '货车数'].map(lambda x: float(x) * 1.4).round(0)
        df_数据汇总.loc[(df_数据汇总['区县'] == '诸暨'), '超限20%除外数'] = df_数据汇总[
            '超限20%除外数'].map(lambda x: float(x) * 0.4).round(0)
        df_数据汇总.loc[(df_数据汇总['区县'] == '临安'), '超限20%除外数'] = df_数据汇总[
            '超限20%除外数'].map(lambda x: float(x) * 0.39).round(0)
        # df_数据汇总.loc[(df_数据汇总['区县'] == '富阳'), '货车数'] = df_数据汇总[
        #     '货车数'].map(lambda x: float(x) * 1.45).round(0)
        # df_数据汇总.loc[(df_数据汇总['区县'] == '富阳'), '超限20%除外数'] = df_数据汇总[
        #     '超限20%除外数'].map(lambda x: float(x) * 0.55).round(0)
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
        "where": "a.out_station_time between '{} 00:00:00' and '{} 23:59:59' and a.total_weight>84   AND a.is_unusual = 0  and a.allow is null ".format(start_time, end_time),
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
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\总重80吨以上明细.xlsx') as writer1:
        总重80吨以上明细.to_excel(writer1, sheet_name='明细', index=False)

def 超限100明细():
    t_sys_station = {
        "tableName": "t_sys_station ",
        "where": " station_status=0 and station_type =31 and is_deleted= 0 ",
        "columns": "station_name,station_code,station_status,station_type,area_county  "
    }
    t_sys_station = get_df_from_db(t_sys_station)
    df_数据汇总, df_报修点位统计, df_接入数 = data_source()

    U_汇总_站点表 = pd.merge(df_数据汇总, t_sys_station, left_on='站点名称', right_on='station_name', how='left')
    station_code = U_汇总_站点表['station_code']

    sql = {
        "tableName": "t_bas_over_data_31 a left join t_bas_over_data_collection_31 b on a.record_code=b.record_code left join t_code_area c on a.area_county=c.county_code ",
        "where": "a.out_station_time between '{} 00:00:00' and '{} 23:59:59' and a.overrun_rate>=105 and a.total_weight<100   AND a.is_unusual = 0  and a.allow is null ".format(start_time, end_time),
        "columns": "c.city 地市,c.county 区县,a.area_city 地市编码, a.area_county 区县编码,a.out_station,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号码,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,b.status 状态,a.site_name 站点名称,a.is_collect 是否是需采集的数据,a.is_unusual 异常数据,b.make_copy  外省抄告,a.photo1,a.photo2,a.photo3,a.vedio,a.record_code 流水号"
    }
    df_100 = get_df_from_db(sql)

    U_过车_站点表 = df_100[df_100.loc[:, 'out_station'].isin(station_code)]

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
    U_过车_站点表['总重'] = U_过车_站点表['总重'].astype('float')
    U_过车_站点表['超重'] = U_过车_站点表['超重'].astype('float')
    U_过车_站点表['limit_weight'] = U_过车_站点表['总重'] - U_过车_站点表['超重']
    U_过车_站点表['limit_weight'] = U_过车_站点表['limit_weight'].astype('float')
    U_过车_站点表.loc[U_过车_站点表['limit_weight'] < 0, 'limit_weight'] = 0.0001
    for item in i.items():
        key = item[0]
        value = item[1]

        U_过车_站点表.loc[((U_过车_站点表['地市编码'] == key) | (U_过车_站点表['区县编码'] == key)) & (
                U_过车_站点表['总重'] < 100), 'vehicle_brand'] = U_过车_站点表['limit_weight'].map(
            lambda x: float(x) * value).round(4)
    U_过车_站点表['limit_weight'] = U_过车_站点表['limit_weight'].astype('float')
    U_过车_站点表['总重'] = U_过车_站点表['总重'].astype('float')
    U_过车_站点表['vehicle_brand'] = U_过车_站点表['vehicle_brand'].astype('float')

    U_过车_站点表['超限率100'] = U_过车_站点表.apply(
        lambda x: (x['总重'] - x['vehicle_brand']) - x['vehicle_brand'],
        axis=1).round(2)
    U_过车_站点表 = U_过车_站点表[(U_过车_站点表['超限率100'] >= 0)
    ]
    U_过车_站点表 = U_过车_站点表.fillna(0)
    U_过车_站点表 = pd.DataFrame(U_过车_站点表,
                            columns=["地市", "区县", "车牌号码", "检测时间", "轴数", "总重",
                                     "超重",
                                     "超限率", "站点名称", "状态", "是否是需采集的数据", "异常数据",
                                     "流水号",
                                     "photo1", "photo2", "photo3", "vedio", '区县编码'])

    超限100数 = U_过车_站点表.groupby(["地市", "区县"])['流水号'].count().reset_index(name='超限100')

    U_过车_站点表 = U_过车_站点表.sort_values(by=['区县编码', '地市', '区县', '检测时间'],
                                    ascending=True).reset_index(drop=True)
    U_过车_站点表 = U_过车_站点表.drop_duplicates(subset=['流水号'])
    U_过车_站点表['状态'] = U_过车_站点表['状态'].astype('int')
    U_过车_站点表['是否是需采集的数据'] = U_过车_站点表['是否是需采集的数据'].astype('int')
    U_过车_站点表.loc[U_过车_站点表['状态'] == 0, '状态'] = '未采集'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 1, '状态'] = '待初审'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 2, '状态'] = '待审核'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 3, '状态'] = '待判定'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 4, '状态'] = '已告知'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 5, '状态'] = '免处理'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 6, '状态'] = '已立案'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 12, '状态'] = '待告知'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 13, '状态'] = '已结案'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 9, '状态'] = '判定不处理'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 15, '状态'] = '初审不通过'
    U_过车_站点表.loc[U_过车_站点表['是否是需采集的数据'] == 1, '是否是需采集的数据'] = '满足'
    U_过车_站点表.loc[U_过车_站点表['是否是需采集的数据'] == 0, '是否是需采集的数据'] = '不满足'

    """车牌处理"""
    U_过车_站点表['车牌号码'].fillna('无牌', inplace=True)
    U_过车_站点表['字节数'] = U_过车_站点表['车牌号码'].str.len()
    U_过车_站点表.loc[U_过车_站点表['字节数'] <= 5, '车牌号码'] = '无牌'
    U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['是否是需采集的数据'] == '满足')), 'photo1'] = ''
    U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['是否是需采集的数据'] == '满足')), 'photo2'] = ''
    U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['是否是需采集的数据'] == '满足')), 'photo3'] = ''
    U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['是否是需采集的数据'] == '满足')), 'vedio'] = ''

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\超限100明细.xlsx') as writer1:
        U_过车_站点表.to_excel(writer1, sheet_name='超限100明细', index=False)

def 百吨王明细():
    t_sys_station = {
        "tableName": "t_sys_station ",
        "where": " station_status=0 and station_type =31 and is_deleted= 0 ",
        "columns": "station_name,station_code,station_status,station_type,area_county  "
    }
    t_sys_station = get_df_from_db(t_sys_station)
    df_数据汇总, df_报修点位统计, df_接入数 = data_source()

    U_汇总_站点表 = pd.merge(df_数据汇总, t_sys_station, left_on='站点名称', right_on='station_name', how='left')
    station_code = U_汇总_站点表['station_code']

    sql = {
        "tableName": "t_bas_over_data_31 a left join t_bas_over_data_collection_31 b on a.record_code=b.record_code left join t_code_area c on a.area_county=c.county_code ",
        "where": "a.out_station_time between '{} 00:00:00' and '{} 23:59:59'  and a.total_weight >=100   AND a.is_unusual = 0  and a.allow is null ".format(start_time, end_time),
        "columns": "c.city 地市,c.county 区县,a.area_city 地市编码, a.area_county 区县编码,a.out_station,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号码,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,b.status 状态,a.site_name 站点名称,a.is_collect 是否是需采集的数据,a.is_unusual 异常数据,b.make_copy  外省抄告,a.photo1,a.photo2,a.photo3,a.vedio,a.record_code 流水号"
    }
    df_100t = get_df_from_db(sql)
    print(df_100t)
    df_100t = df_100t[df_100t.loc[:, 'out_station'].isin(station_code)]
    df_100t=df_100t.fillna(0)
    U_过车_站点表 = pd.DataFrame(df_100t,
                            columns=["地市", "区县", "车牌号码", "检测时间", "轴数", "总重",
                                     "超重",
                                     "超限率", "站点名称", "状态", "是否是需采集的数据", "异常数据",
                                     "流水号",
                                     "photo1", "photo2", "photo3", "vedio", '区县编码'])
    U_过车_站点表 = U_过车_站点表.sort_values(by=['区县编码', '地市', '区县', '检测时间'],
                                    ascending=True).reset_index(drop=True)
    U_过车_站点表['状态'] = U_过车_站点表['状态'].astype('int')
    U_过车_站点表['是否是需采集的数据'] = U_过车_站点表['是否是需采集的数据'].astype('int')
    U_过车_站点表.loc[U_过车_站点表['状态'] == 0, '状态'] = '未采集'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 1, '状态'] = '待初审'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 2, '状态'] = '待审核'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 3, '状态'] = '待判定'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 4, '状态'] = '已告知'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 5, '状态'] = '免处理'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 6, '状态'] = '已立案'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 12, '状态'] = '待告知'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 13, '状态'] = '已结案'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 9, '状态'] = '判定不处理'
    U_过车_站点表.loc[U_过车_站点表['状态'] == 15, '状态'] = '初审不通过'
    U_过车_站点表.loc[U_过车_站点表['是否是需采集的数据'] == 1, '是否是需采集的数据'] = '满足'
    U_过车_站点表.loc[U_过车_站点表['是否是需采集的数据'] == 0, '是否是需采集的数据'] = '不满足'

    U_过车_站点表['区县编码'] = U_过车_站点表['区县编码'].astype('string')
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\百吨王明细.xlsx') as writer1:
        U_过车_站点表.to_excel(writer1, sheet_name='百吨王明细', index=False)

def Compliance_rate():
    df_big = pd.read_excel(r'C:\Users\stayhungary\Desktop\总重80吨以上明细.xlsx', sheet_name='明细')
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

def over_100_num():
    df_数据汇总, df_报修点位统计, df_接入数 = data_source()

    U_过车_站点表=pd.read_excel(r'C:\Users\stayhungary\Desktop\超限100明细.xlsx', sheet_name='超限100明细')


    超限100数 = U_过车_站点表.groupby(["地市", "区县"])['流水号'].count().reset_index(name='超限100')
    满足证据条件数 = U_过车_站点表[((U_过车_站点表['车牌号码'] == '无牌') & (U_过车_站点表['是否是需采集的数据'] == '满足')&(U_过车_站点表['异常数据'] == '是'))].groupby(["地市", "区县"])[
        '流水号'].count().reset_index(name='超限100%遮挡车牌数量（辆）')
    超限100数 = pd.merge(超限100数, 满足证据条件数, on=["地市", "区县"], how='left')



    """地市货车数及超限100%数"""
    df_数据汇总2 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                        & (df_数据汇总.货车数 > 500))]
    货车数 = df_数据汇总2.groupby(['地市'])['货车数'].sum()
    货车数 = 货车数.to_frame()
    货车数 = 货车数.reset_index()
    超限100 = U_过车_站点表.groupby(['地市'])['流水号'].count()
    超限100 = 超限100.to_frame()
    超限100 = 超限100.reset_index()
    附件2 = pd.merge(货车数, 超限100, left_on='地市', right_on='地市', how='left', )
    附件2.rename(columns={'流水号': '超限100%数'}, inplace=True)
    附件2 = 附件2.fillna(0, inplace=False)
    附件2['超限100%数占货车数比例'] = 附件2.apply(lambda x: x['超限100%数'] / x['货车数'], axis=1)
    附件2['排名（占比由高到低）'] = 附件2['超限100%数占货车数比例'].rank(ascending=False, method='first')
    附件2['超限100%数占货车数比例'] = 附件2['超限100%数占货车数比例'].apply(lambda x: format(x, '.3%'))
    未识别到车牌数 = U_过车_站点表[U_过车_站点表['车牌号码'] == '无牌'].groupby(['地市'])['车牌号码'].count().reset_index(name='未识别到车牌')
    满足证据条件数 = U_过车_站点表[((U_过车_站点表['车牌号码'] == '无牌') & (U_过车_站点表['是否是需采集的数据'] == '满足'))].groupby(['地市'])[
        '车牌号码'].count().reset_index(name='满足证据条件')
    满足证据条件且故意遮挡车牌 = U_过车_站点表[((U_过车_站点表['车牌号码'] == '无牌') & (U_过车_站点表['是否是需采集的数据'] == '满足')&(U_过车_站点表['异常数据'] == '是'))].groupby(['地市'])[
            '车牌号码'].count().reset_index(name='满足证据条件且故意遮挡车牌')
    附件2 = pd.merge(df_接入数, 附件2, on='地市', how='left')
    附件2 = pd.merge(附件2, 未识别到车牌数, left_on='地市', right_on='地市', how='left')
    附件2 = pd.merge(附件2, 满足证据条件数, left_on='地市', right_on='地市', how='left')
    附件2 = pd.merge(附件2, 满足证据条件且故意遮挡车牌, left_on='地市', right_on='地市', how='left')
    附件2 = pd.DataFrame(附件2, columns=['地市', '货车数', '超限100%数', '超限100%数占货车数比例', '排名（占比由高到低）', '未识别到车牌', '满足证据条件',
                                     '满足证据条件且故意遮挡车牌'])
    附件2 = 附件2.fillna(0, inplace=False)
    附件2 = 附件2.drop(index=(附件2.loc[(附件2['地市'] == '义乌')].index))

    return 超限100数, 附件2, U_过车_站点表

def total_weight_100_num():

    U_过车_站点表=pd.read_excel(r'C:\Users\stayhungary\Desktop\百吨王明细.xlsx', sheet_name='百吨王明细')
    百吨王数 = U_过车_站点表.groupby(['区县编码'])['流水号'].count().reset_index(name='百吨王数')
    百吨王遮挡车牌数量= U_过车_站点表[(U_过车_站点表['异常数据'] == '车牌附近安装大灯，货物遮盖，无法识别装载物，无法确认是否超限')].groupby(["区县编码"])[
        '流水号'].count().reset_index(name='百吨王遮挡车牌数量（辆）')
    百吨王数 = pd.merge(百吨王数, 百吨王遮挡车牌数量, on=["区县编码"], how='left')
    百吨王数['区县编码'] = 百吨王数['区县编码'].astype('string')
    return  百吨王数,U_过车_站点表

def case_statistic():
    sql_area = {
        "tableName": "t_code_area  ",
        "where": "  is_deleted = 0 and province_code = '330000' ",
        "columns": "city,city_code,county,county_code as area_county"
    }
    t_code_area = get_df_from_db(sql_area)
    sql_非现入库数 = {
        "tableName": "t_bas_over_data_collection_31  ",
        "where": " law_judgment = 1  AND valid_time between '{} 00:00:00'  AND '{} 23:59:59'  and status !=5 GROUP BY area_county  ".format(q案件,s案件),
        "columns": "area_county, count( 1 )  入库数路政, sum(IF( car_no LIKE '%浙%', 1, 0 ))  本省入库数  "
    }

    sql_非现入库数=get_df_from_db(sql_非现入库数)
    sql_非现入库数 = pd.DataFrame(sql_非现入库数,columns=['area_county', '入库数路政', '本省入库数'])

    sql_交通现场查处数 = {
        "tableName": " t_bas_over_data_collection_sign c",
        "where": " c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time between '{} 00:00:00'  and '{} 23:59:59'  GROUP BY c.area_county  ".format(q案件,s案件),
        "columns": "c.area_county  ,count( DISTINCT ( record_id ) ) AS 现场处罚路政 "
    }
    sql_交通现场查处数=get_df_from_db(sql_交通现场查处数)
    sql_交通现场查处数 = pd.DataFrame(sql_交通现场查处数,columns=['area_county', '现场处罚路政'])
    sql_交通现场查处数.loc[sql_交通现场查处数['area_county'] == '331083', 'area_county'] = '331021'
    sql_交通现场查处数.loc[sql_交通现场查处数['area_county'] == '330284', 'area_county'] = '330204'

    sql_非现处罚数处罚 = {
        "tableName": "t_case_sign_result  ",
        "where": " record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND close_case_time between '{} 00:00:00' AND  '{} 23:59:59' AND area_province = '330000' GROUP BY  dept_county".format(q案件,s案件),
        "columns": "dept_county as area_county,count(DISTINCT ( CASE_NUM )) AS 非现场处罚路政处罚,  count(DISTINCT IF( car_no LIKE '%浙%', CASE_NUM, NULL )) AS 非现场处罚本省路政处罚 "
    }
    sql_非现处罚数处罚=get_df_from_db(sql_非现处罚数处罚)
    sql_非现处罚数处罚 = pd.DataFrame(sql_非现处罚数处罚,columns=['area_county', '非现场处罚路政处罚', '非现场处罚本省路政处罚'])
    sql_非现处罚数处罚.loc[sql_非现处罚数处罚['area_county'] == '331083', 'area_county'] = '331021'
    sql_非现处罚数处罚.loc[sql_非现处罚数处罚['area_county'] == '330284', 'area_county'] = '330204'

    sql_非现当年处罚数处罚 = {
        "tableName": "t_case_sign_result c LEFT JOIN t_bas_over_data_collection_31 b ON c.record_code = b.record_code  ",
        "where": " record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND close_case_time between '{} 00:00:00' AND  '{} 23:59:59' AND c.area_province = '330000' and  b.valid_time between '{} 00:00:00' and  '{} 23:59:59' GROUP BY  c.dept_county".format(q案件,s案件,q案件,s案件),
        "columns": "c.dept_county as  area_county,count(DISTINCT ( c.CASE_NUM )) AS 非现场处罚路政处罚当年, count(DISTINCT IF( c.car_no LIKE '%浙%', c.CASE_NUM, NULL )) AS 非现场处罚本省路政处罚当年  "
    }
    sql_非现当年处罚数处罚=get_df_from_db(sql_非现当年处罚数处罚)
    sql_非现当年处罚数处罚 = pd.DataFrame(sql_非现当年处罚数处罚,columns=['area_county', '非现场处罚路政处罚当年', '非现场处罚本省路政处罚当年'])
    sql_非现当年处罚数处罚.loc[sql_非现当年处罚数处罚['area_county'] == '331083', 'area_county'] = '331021'
    sql_非现当年处罚数处罚.loc[sql_非现当年处罚数处罚['area_county'] == '330284', 'area_county'] = '330204'

    sql_交警现场 = {
        "tableName": "   t_bas_police_road_site   ",
        "where": "   punish_time between '{} 00:00:00' AND  '{} 23:59:59'  and case_status=2  GROUP BY area_county  ".format(q案件,s案件),
        "columns": "area_county,count(DISTINCT case_number) as 交警现场查处数"
    }
    sql_交警现场=get_df_from_db(sql_交警现场)
    sql_交警现场 = pd.DataFrame(sql_交警现场,columns=['area_county', '交警现场查处数'])



    sql_外省抄告数 = {
        "tableName": "t_bas_over_data_collection_31  ",
        "where": " law_judgment = 1  AND valid_time between '{} 00:00:00'  AND '{} 23:59:59'  and status !=5 GROUP BY area_county  ".format(q案件,s案件),
        "columns": "area_county ,sum(IF( make_copy=1 and car_no not LIKE '%浙%', 1, 0 )) 外省抄告 "
    }
    sql_外省抄告数 =get_df_from_db(sql_外省抄告数)
    sql_外省抄告数 = pd.DataFrame(sql_外省抄告数,columns=['area_county', '外省抄告'])



    sql_非现处罚数案发= {
        "tableName": "t_case_sign_result ",
        "where": " record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND close_case_time between '{} 00:00:00' AND  '{} 23:59:59' AND area_province = '330000' GROUP BY  area_county".format(q案件,s案件),
        "columns": "area_county,count(DISTINCT ( CASE_NUM )) AS 非现场处罚路政案发,  count(DISTINCT IF( car_no LIKE '%浙%', CASE_NUM, NULL )) AS 非现场处罚本省路政案发 "
    }
    sql_非现处罚数案发=get_df_from_db(sql_非现处罚数案发)
    sql_非现处罚数案发 = pd.DataFrame(sql_非现处罚数案发,columns=['area_county', '非现场处罚路政案发', '非现场处罚本省路政案发'])


    sql_非现当年处罚数案发 = {
        "tableName": "t_case_sign_result c LEFT JOIN t_bas_over_data_collection_31 b ON c.record_code = b.record_code  ",
        "where": " record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND close_case_time between '{} 00:00:00' AND  '{} 23:59:59' AND c.area_province = '330000' and  b.valid_time between '{} 00:00:00' and  '{} 23:59:59' GROUP BY  c.area_county".format(q案件,s案件,q案件,s案件),
        "columns": "c.area_county,count(DISTINCT ( c.CASE_NUM )) AS 非现场处罚路政案发当年, count(DISTINCT IF( c.car_no LIKE '%浙%', c.CASE_NUM, NULL )) AS 非现场处罚本省路政案发当年  "
    }
    sql_非现当年处罚数案发=get_df_from_db(sql_非现当年处罚数案发)
    sql_非现当年处罚数案发 = pd.DataFrame(sql_非现当年处罚数案发,columns=['area_county', '非现场处罚路政案发当年', '非现场处罚本省路政案发当年'])

    case=pd.merge(t_code_area, sql_非现入库数, on='area_county', how='outer')
    case=pd.merge(case, sql_交通现场查处数, on='area_county', how='outer')
    case = pd.merge(case, sql_非现处罚数处罚, on='area_county', how='outer')
    case = pd.merge(case, sql_非现当年处罚数处罚, on='area_county', how='outer')
    case = pd.merge(case, sql_交警现场, on='area_county', how='outer')
    case = pd.merge(case, sql_外省抄告数, on='area_county', how='outer')
    case = pd.merge(case, sql_非现处罚数案发, on='area_county', how='outer')
    case = pd.merge(case, sql_非现当年处罚数案发, on='area_county', how='outer')
    case = case.fillna(0)
    case['本省入库数'] = case['本省入库数'].astype('int')
    case['外省抄告'] = case['外省抄告'].astype('int')
    case.rename(columns={'area_county': '区县编码'}, inplace=True)
    case['区县编码'] = case['区县编码'].astype('string')
    return case

def data_station():
        df_数据汇总, df_报修点位统计, df_接入数=data_source()


        T_义乌_数据为0 = df_数据汇总[(df_数据汇总.区县 == '义乌')
                            & (df_数据汇总.实际在线天数 == 0)
                            ]
        T_义乌_数据为0数 = T_义乌_数据为0.区县.count()
        T_义乌_汇总 = df_数据汇总[(df_数据汇总.区县 == '义乌')
                          & (df_数据汇总.实际在线天数 < 10)
                          & (df_数据汇总.实际在线天数 > 0)]
        T_义乌_10数 = T_义乌_汇总.区县.count()
        T_义乌_汇总 = df_数据汇总[(df_数据汇总.区县 == '义乌')
                          & (df_数据汇总.实际在线天数 >= 10)
                          & (df_数据汇总.货车数 < 500)
                          ]
        T_义乌_500数 = T_义乌_汇总.区县.count()
        T_义乌_10_500数 = T_义乌_10数 + T_义乌_500数
        T_义乌_报修 = df_报修点位统计[df_报修点位统计.区县 == '义乌']
        T_义乌_报修数 = T_义乌_报修.区县.count()
        T_义乌_在线 = df_数据汇总[df_数据汇总.区县 == '义乌']
        T_义乌_在线数 = T_义乌_在线.区县.count()
        df_数据汇总1 = df_数据汇总[(df_数据汇总.区县 != '义乌')
        ]
        在用数 = df_数据汇总1.groupby(['地市'])['站点名称'].count()
        df_报修点位统计1 = df_报修点位统计[df_报修点位统计.区县 != '义乌']
        报修数 = df_报修点位统计1.groupby(['地市'])['报修站点名称'].count()
        实际站点数 = pd.merge(在用数, 报修数, on='地市', how='outer')
        实际站点数 = 实际站点数.fillna(0, inplace=False)
        实际站点数['实际站点数'] = 实际站点数.apply(lambda x: x[0] + x[1], axis=1)
        实际站点数.实际站点数 = 实际站点数.实际站点数.astype(int)
        实际站点数.报修站点名称 = 实际站点数.报修站点名称.astype(int)
        实际站点数.rename(columns={'站点名称': '在用数', '报修站点名称': '实际报修数'}, inplace=True)
        T_10筛选 = df_数据汇总1[(df_数据汇总1.实际在线天数 < 10)
                          & (df_数据汇总1.实际在线天数 > 0)
                          ]
        T_10筛选 = T_10筛选.groupby([T_10筛选.地市]).count()
        T_10筛选 = T_10筛选.loc[:, ['站点名称']]
        T_10筛选.columns = ['站点名称']
        T_500筛选 = df_数据汇总1[((df_数据汇总1.实际在线天数 >= 10)
                            & (df_数据汇总1.货车数 < 500))]
        T_500筛选 = T_500筛选.groupby([T_500筛选.地市]).count()
        T_500筛选 = T_500筛选.loc[:, ['站点名称']]
        T_500筛选.columns = ['站点名称']
        T_500筛选.rename(columns={'站点名称': '500辆次'}, inplace=True)
        T_筛选 = pd.merge(T_10筛选, T_500筛选, how='outer', on='地市')
        T_筛选 = T_筛选.fillna(value=0)
        T_筛选['在线天数＜10天或货车数＜500辆次'] = T_筛选['站点名称'] + T_筛选['500辆次']
        T_10或500 = T_筛选['在线天数＜10天或货车数＜500辆次']
        t = df_数据汇总1[(df_数据汇总1.实际在线天数 == 0)]
        T_数据为0数 = t.groupby([t['地市']]).count()
        T_数据为0数 = T_数据为0数.loc[:, ['实际在线天数']]
        T_数据为0数.columns = ['实际在线天数']
        T_数据为0数.rename(columns={'实际在线天数': '数据为0'}, inplace=True)
        站点设备完好率 = pd.merge(df_接入数, 实际站点数, on='地市', how='left')
        站点设备完好率.loc[11, '在用数'] = T_义乌_在线数
        站点设备完好率.loc[11, '实际报修数'] = T_义乌_报修数
        站点设备完好率.loc[11, '实际站点数'] = T_义乌_报修数 + T_义乌_在线数
        站点设备完好率 = 站点设备完好率.fillna(0, inplace=False)
        站点设备完好率['报修数'] = 站点设备完好率.apply(lambda x: x['接入数（修正后）'] - x['在用数'], axis=1)
        站点设备完好率.loc[站点设备完好率['报修数'] < 0, '报修数'] = 0
        站点设备完好率 = pd.merge(站点设备完好率, T_数据为0数, on='地市', how='left')
        站点设备完好率 = pd.merge(站点设备完好率, T_10或500, on='地市', how='left')
        站点设备完好率.loc[11, '数据为0'] = T_义乌_数据为0数
        站点设备完好率.loc[11, '在线天数＜10天或货车数＜500辆次'] = T_义乌_10_500数
        站点设备完好率 = 站点设备完好率.fillna(0, inplace=False)

        站点设备完好率['实际完好数'] = 站点设备完好率['在用数'] - 站点设备完好率.数据为0 - 站点设备完好率['在线天数＜10天或货车数＜500辆次']
        站点设备完好率['修正完好数'] = 站点设备完好率.apply(lambda x: min(x['接入数（修正后）'], x['实际完好数']), axis=1)
        站点设备完好率['实际完好率'] = 站点设备完好率.apply(lambda x: x['实际完好数'] / x['实际站点数'], axis=1).round(4)
        站点设备完好率['实际完好率'] = 站点设备完好率['实际完好率'].apply(lambda x: format(x, '.2%'))
        站点设备完好率['调整后完好率'] = 站点设备完好率.apply(lambda x: x['修正完好数'] / x['接入数（修正后）'], axis=1).round(4)
        站点设备完好率['调整后完好率'] = 站点设备完好率['调整后完好率'].apply(lambda x: format(x, '.2%'))
        站点设备完好率 = pd.DataFrame(站点设备完好率,
                               columns=["地市", "接入数（修正后）", "实际站点数", "在用数", "实际报修数", "报修数", "数据为0", "在线天数＜10天或货车数＜500辆次",
                                        "实际完好数", "实际完好率", "修正完好数", "调整后完好率"])

        df_数据汇总, df_报修点位统计, df_接入数=data_source()
        """做sheet1"""

        df_数据汇总 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                           & (df_数据汇总.货车数 > 500))]
        T_义乌 = df_数据汇总[(df_数据汇总['区县'] == '义乌')]
        T_义乌_货车数 = T_义乌.groupby(['区县'])['货车数'].sum()
        T_义乌_超限数 = T_义乌.groupby(['区县'])['超限20%除外数'].sum()
        df_数据汇总1 = df_数据汇总[(df_数据汇总.区县 != '义乌')
        ]
        货车数 = df_数据汇总1.groupby(['地市'])['货车数'].sum()
        超限数 = df_数据汇总1.groupby(['地市'])['超限20%除外数'].sum()
        df_sheet1 = pd.merge(货车数, 超限数, how='left', on='地市')
        df_sheet1 = pd.merge(df_接入数, df_sheet1, how='left', on='地市')
        df_sheet1.loc[11, '货车数'] = T_义乌_货车数[0]
        df_sheet1.loc[11, '超限20%除外数'] = T_义乌_超限数[0]
        df_sheet1['超限率'] = df_sheet1.apply(lambda x: x['超限20%除外数'] / x['货车数'], axis=1).round(4)
        df_sheet1['超限率'] = df_sheet1['超限率'].apply(lambda x: format(x, '.2%'))
        df_sheet1['超限率排名'] = df_sheet1['超限率'].rank(ascending=False, method='first')
        df_sheet1后 = pd.DataFrame(站点设备完好率, columns=["地市", "实际站点数", "实际完好率", "调整后完好率"])
        df_sheet1 = pd.merge(df_sheet1, df_sheet1后, how='left', on='地市')
        df_sheet1 = pd.DataFrame(df_sheet1,
                                 columns=["地市", "货车数", "超限20%除外数", "超限率", "超限率排名", "实际站点数", "接入数（修正后）", "实际完好率",
                                          "调整后完好率"])
        df_sheet1.rename(columns={'超限20%除外数': '超限数'}, inplace=True)

        """在线天数大于等于10天货车数大于500的站点数据"""

        T_20天与500筛选 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                               & (df_数据汇总.货车数 > 500))]
        T_20天与500筛选 = pd.DataFrame(T_20天与500筛选,
                                   columns=["站点名称", "地市", "区县", "理应在线天数", "实际在线天数", "在线率", "货车数", "超限20%除外数",
                                            "超限20%除外超限率(%)"])
        T_20天与500筛选.rename(columns={'理应在线天数': '应在线天数', '超限20%除外数': '超限数', '超限20%除外超限率(%)': '超限率'}, inplace=True)
        T_20天与500筛选1 = T_20天与500筛选.sort_values(by="超限率", ascending=False, ignore_index=True)

        '''区县超限率排名'''
        T_20天与500筛选 = pd.DataFrame(T_20天与500筛选1, columns=["地市", "区县", "货车数", "超限数"])
        区县超限率排序 = T_20天与500筛选.groupby([T_20天与500筛选['地市'], T_20天与500筛选['区县']]).sum()
        区县超限率排序['超限率'] = 区县超限率排序.apply(lambda x: x['超限数'] / x['货车数'], axis=1).round(4)
        区县超限率排序 = 区县超限率排序.sort_values('超限率', ascending=False)
        区县超限率排序['超限率'] = 区县超限率排序['超限率'].apply(lambda x: format(x, '.2%'))
        区县超限率排序 = 区县超限率排序.reset_index()
        df_数据汇总, df_报修点位统计, df_接入数=data_source()
        return df_数据汇总, df_报修点位统计, df_sheet1, 站点设备完好率, 区县超限率排序, T_20天与500筛选1

def overrun_site_rate():
    """附件1"""
    df_数据汇总, df_报修点位统计, df_接入数 =data_source()
    df_数据汇总2 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                        & (df_数据汇总.货车数 > 500))]
    货车数 = df_数据汇总2.groupby(['地市', '区县'])['货车数'].sum().reset_index()
    超限数 = df_数据汇总2.groupby(['地市', '区县'])['超限20%除外数'].sum().reset_index()
    df_sheet1 = pd.merge(货车数, 超限数, how='left', on=['地市', '区县'])
    df_sheet1.loc[df_sheet1['货车数'] == 0, '货车数'] = 1
    df_sheet1['超限率'] = df_sheet1.apply(lambda x: x['超限20%除外数'] / x['货车数'], axis=1).round(4)
    df_sheet1 = df_sheet1.fillna(0, inplace=False)
    df_sheet1['超限率'] = df_sheet1['超限率'].apply(lambda x: format(x, '.2%'))
    df_sheet1.loc[df_sheet1['货车数'] == 1, '货车数'] = 0
    df_sheet1.rename(columns={'超限20%除外数': '超限数'}, inplace=True)

    """附件7"""

    df_数据汇总, df_报修点位统计, df_接入数=data_source()
    # df_数据汇总.columns = df_数据汇总.iloc[2]
    # df_数据汇总 = df_数据汇总.iloc[3:].reset_index(drop=True)
    在用数 = df_数据汇总.groupby(['地市', '区县'])['站点名称'].count()
    报修数 = df_报修点位统计.groupby(['地市', '区县'])['报修站点名称'].count()
    实际站点数 = pd.merge(在用数, 报修数, on=['地市', '区县'], how='outer')
    实际站点数 = 实际站点数.fillna(0, inplace=False)
    实际站点数['站点数'] = 实际站点数.apply(lambda x: x[0] + x[1], axis=1)
    实际站点数.站点数 = 实际站点数.站点数.astype(int)
    实际站点数.报修站点名称 = 实际站点数.报修站点名称.astype(int)
    实际站点数.rename(columns={'站点名称': '在用数', '报修站点名称': '报修数'}, inplace=True)

    T_10筛选 = df_数据汇总[(df_数据汇总.实际在线天数 < 10)
                     & (df_数据汇总.实际在线天数 > 0)
                     ]
    T_10筛选 = T_10筛选.groupby(['地市', '区县']).count()
    T_10筛选 = T_10筛选.loc[:, ['站点名称']]
    T_10筛选.columns = ['站点名称']
    T_500筛选 = df_数据汇总[(df_数据汇总.实际在线天数 >= 10) & (df_数据汇总.货车数 < 500)]
    T_500筛选 = T_500筛选.groupby(['地市', '区县']).count()
    T_500筛选 = T_500筛选.loc[:, ['站点名称']]
    T_500筛选.columns = ['站点名称']
    T_500筛选.rename(columns={'站点名称': '500辆次'}, inplace=True)
    T_筛选 = pd.merge(T_10筛选, T_500筛选, how='outer', on=['地市', '区县'])
    T_筛选 = T_筛选.fillna(value=0)
    T_筛选['在线天数＜10天或货车数＜500辆次'] = T_筛选['站点名称'] + T_筛选['500辆次']
    T_10或500 = T_筛选['在线天数＜10天或货车数＜500辆次']
    t = df_数据汇总[(df_数据汇总.实际在线天数 == 0)]
    T_数据为0数 = t.groupby(['地市', '区县']).count()

    T_数据为0数 = T_数据为0数.loc[:, ['实际在线天数']]
    T_数据为0数.columns = ['实际在线天数']
    T_数据为0数.rename(columns={'实际在线天数': '数据为0'}, inplace=True)
    附件7 = pd.merge(实际站点数, T_数据为0数, how='left', on=['地市', '区县'])
    附件7 = pd.merge(附件7, T_10或500, how='left', on=['地市', '区县'])
    附件7 = 附件7.fillna(0, inplace=False)
    附件7['异常数'] = 附件7.apply(lambda x: x['报修数'] + x['数据为0'] + x['在线天数＜10天或货车数＜500辆次'], axis=1)
    附件7['异常数'] = 附件7['异常数'].astype('float')
    附件7['站点数'] = 附件7['站点数'].astype('float')
    附件7['设备完好率'] = (附件7['站点数'] - 附件7['报修数']) / 附件7['站点数']
    附件7['数据完好率'] = (附件7['站点数'] - 附件7['异常数']) / 附件7['站点数']
    附件7 = 附件7.fillna(0, inplace=False)
    附件7['数据完好率'] = 附件7['数据完好率'].apply(lambda x: format(x, '.2%'))
    附件7['设备完好率'] = 附件7['设备完好率'].apply(lambda x: format(x, '.2%'))
    附件7 = pd.DataFrame(附件7, columns=["站点数", "报修数",'设备完好率', "异常数", "数据完好率"]).reset_index()
    附件7 = pd.merge(df_sheet1, 附件7, how='outer', on=['地市', '区县'])
    return 附件7
    # q = input("请输入存储路径(C:/Users/Administrator/Desktop/输出报表/其他市月报表)：")

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
    超限100明细()
    百吨王明细()
    df_区县编码 = pd.read_excel(file_name, sheet_name='code_area')
    df_区县编码['区县编码'] = df_区县编码['区县编码'].astype('string')
    df_区县编码['地市编码'] = df_区县编码['地市编码'].astype('string')
    df_数据汇总, df_报修点位统计, df_接入数 = data_source()
    df_案件=case_statistic()
    df_源头, 货运源头监控数据省市, df_源头站点明细=Key_freight_sources()
    df_数据汇总, df_报修点位统计,df_sheet1,站点设备完好率,区县超限率排序,T_20天与500筛选1 = data_station()
    超限100数,超限100汇总,超限100明细=over_100_num()
    总重80_90以上,总重80吨以上明细 =Compliance_rate()
    百吨王数, 百吨王明细 = total_weight_100_num()
    附件7 = overrun_site_rate()
    附件7 = pd.merge(df_区县编码, 附件7, on=['地市', '区县'], how='outer')
    附件7 = pd.merge(附件7, 超限100数, on=['地市','区县'],how='left')
    附件7 = pd.merge(附件7, 总重80_90以上, on=['区县编码'], how='left')
    附件7 = pd.merge(附件7, 百吨王数, on=['区县编码'], how='left')
    附件7 = pd.merge(附件7, df_案件, on=['区县编码'], how='outer')
    附件7 = pd.merge(附件7, df_源头, on=['区县编码'], how='outer')
    附件7['超限100%数货车数/万辆'] = 附件7.apply(lambda x: x['超限100'] / (x['货车数'] + 0.0000001)*10000, axis=1).round(4)
    附件7['源头货车数'] = pd.to_numeric(附件7['源头货车数'], errors='coerce')
    附件7['数据站点总数'] = pd.to_numeric(附件7['数据站点总数'], errors='coerce')
    附件7['在线站点数'] = pd.to_numeric(附件7['在线站点数'], errors='coerce')
    附件7['20-50%数'] = pd.to_numeric(附件7['20-50%数'], errors='coerce')
    附件7['50-100%数'] = pd.to_numeric(附件7['50-100%数'], errors='coerce')
    附件7['100%以上数'] = pd.to_numeric(附件7['100%以上数'], errors='coerce')
    附件7['源头单位平均过车数（辆次）'] = 附件7.apply(lambda x: x['源头货车数'] / (x['数据站点总数'] + 0.0000001), axis=1).round(0)
    附件7['设备上线率（%）'] = 附件7.apply(lambda x: x['在线站点数'] / (x['数据站点总数']+ 0.0000001), axis=1)
    附件7['20-50%占比'] = 附件7.apply(lambda x: x['20-50%数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7['50-100%占比'] = 附件7.apply(lambda x: x['50-100%数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7['100%以上占比'] = 附件7.apply(lambda x: x['100%以上数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7 = 附件7[(附件7.区县编码 != '330621')]
    附件7 = 附件7[(附件7.区县编码 != '330155')]
    附件7 = 附件7.fillna(0, inplace=False)
    附件7['设备上线率（%）'] = 附件7['设备上线率（%）'].apply(lambda x: format(x, '.2%'))
    附件7['20-50%占比'] = 附件7['20-50%占比'].apply(lambda x: format(x, '.2%'))
    附件7['50-100%占比'] = 附件7['50-100%占比'].apply(lambda x: format(x, '.2%'))
    附件7['100%以上占比'] = 附件7['100%以上占比'].apply(lambda x: format(x, '.2%'))
    附件7['80吨以上总数'] =附件7['本月超限80吨以上']
    附件7.rename(columns={'入库数路政': '入库数(路政)','现场处罚路政': '现场处罚(路政)','非现场处罚路政处罚': '非现场处罚(路政)'
                        ,'非现场处罚本省路政处罚': '非现场处罚本省(路政)','非现场处罚路政处罚当年': '非现场处罚(路政)当年','非现场处罚本省路政处罚当年': '非现场处罚本省(路政)当年'
                        }, inplace=True)
    附件7['入库数(路政)'] = 附件7['入库数(路政)'].astype('int')
    附件7['本省入库数'] = 附件7['本省入库数'].astype('int')
    附件7['外省入库数'] = 附件7['入库数(路政)']-附件7['本省入库数']
    附件7['非现场处罚(路政)'] = 附件7['非现场处罚(路政)'].astype('int')
    附件7['非现场处罚本省(路政)']= 附件7['非现场处罚本省(路政)'].astype('int')
    附件7['非现场处罚(路政)当年'] = 附件7['非现场处罚(路政)当年'].astype('int')
    附件7['非现场处罚本省(路政)当年'] = 附件7['非现场处罚本省(路政)当年'].astype('int')
    附件7['交警现场查处数'] = 附件7['交警现场查处数'].astype('int')
    附件7['外省抄告'] = 附件7['外省抄告'].astype('int')
    附件7['现场处罚(路政)'] = 附件7['现场处罚(路政)'].astype('int')
    附件7['非现场处罚外省(路政)'] = 附件7['非现场处罚(路政)'] - 附件7['非现场处罚本省(路政)']
    附件7['统计月份']='2023-09'
    附件7 = pd.DataFrame(附件7, columns=['统计月份', '地市编码','地市','区县编码','区县','货车数','超限数','超限率','超限100','超限100%数货车数/万辆',
                                     '本月超限80吨以上','本月超限90吨以上','百吨王数','超限100%遮挡车牌数量（辆）', '百吨王遮挡车牌数量（辆）', '站点数',
                                     '报修数','设备完好率','异常数','数据完好率','入库数(路政)','本省入库数','外省入库数','现场处罚(路政)','非现场处罚(路政)','非现场处罚本省(路政)','非现场处罚(路政)当年',
                                     '非现场处罚本省(路政)当年','非现场处罚外省(路政)','交警现场查处数','交警非现场处罚数','交警非现查处数本省','需处罚数/非现入库数（总计）','需处罚数/非现入库数（本省）',
                                     '需处罚数/非现入库数（外省）','非现处罚数（总计）','非现处罚数（本省）','非现处罚数（外省）','年处罚数','外省抄告','非现场处罚率（本省）','非现场处罚率（外省）',
                                     '处罚率(含抄告）','80吨以上总数','80吨以上且满足','80吨以上且审核通过', '合规率','后面是源头数据','数据站点总数','源头货车数','源头单位平均过车数（辆次）',
                                     '在线站点数','设备上线率（%）','20-50%数','50-100%数','100%以上数','20-50%占比','50-100%占比','100%以上占比','非现场处罚路政案发','非现场处罚本省路政案发','非现场处罚路政案发当年','非现场处罚本省路政案发当年'])
    附件7 = 附件7.sort_values(by=['区县编码'],
                                    ascending=True).reset_index(drop=True)
    附件7 = 附件7.fillna(0, inplace=False)
    附件7地市 = 附件7.groupby(['地市编码','地市']).sum().reset_index()
    附件7地市.loc[附件7地市['货车数'] == 0, '货车数'] = 1
    附件7地市['超限率'] = 附件7地市.apply(lambda x: x['超限数'] / x['货车数'], axis=1).round(4)
    附件7地市 = 附件7地市.fillna(0, inplace=False)
    附件7地市['超限率'] = 附件7地市['超限率'].apply(lambda x: format(x, '.2%'))
    附件7地市.loc[附件7地市['货车数'] == 1, '货车数'] = 0
    附件7地市['设备完好率'] = (附件7地市['站点数'] - 附件7地市['报修数']) / 附件7地市['站点数']
    附件7地市['数据完好率'] = (附件7地市['站点数'] - 附件7地市['异常数']) / 附件7地市['站点数']
    附件7地市 = 附件7地市.fillna(0, inplace=False)
    附件7地市['数据完好率'] = 附件7地市['数据完好率'].apply(lambda x: format(x, '.2%'))
    附件7地市['设备完好率'] = 附件7地市['设备完好率'].apply(lambda x: format(x, '.2%'))
    附件7地市['超限100%数货车数/万辆'] = 附件7地市.apply(lambda x: x['超限100'] / (x['货车数'] + 0.0000001)*10000, axis=1).round(4)
    附件7地市['源头货车数'] = pd.to_numeric(附件7地市['源头货车数'], errors='coerce')
    附件7地市['数据站点总数'] = pd.to_numeric(附件7地市['数据站点总数'], errors='coerce')
    附件7地市['在线站点数'] = pd.to_numeric(附件7地市['在线站点数'], errors='coerce')
    附件7地市['20-50%数'] = pd.to_numeric(附件7地市['20-50%数'], errors='coerce')
    附件7地市['50-100%数'] = pd.to_numeric(附件7地市['50-100%数'], errors='coerce')
    附件7地市['100%以上数'] = pd.to_numeric(附件7地市['100%以上数'], errors='coerce')
    附件7地市['源头单位平均过车数（辆次）'] = 附件7地市.apply(lambda x: x['源头货车数'] / (x['数据站点总数'] + 0.0000001), axis=1).round(0)
    附件7地市['设备上线率（%）'] = 附件7地市.apply(lambda x: x['在线站点数'] / (x['数据站点总数']+ 0.0000001), axis=1)
    附件7地市['20-50%占比'] = 附件7地市.apply(lambda x: x['20-50%数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7地市['50-100%占比'] = 附件7地市.apply(lambda x: x['50-100%数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7地市['100%以上占比'] = 附件7地市.apply(lambda x: x['100%以上数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7地市 = 附件7地市.fillna(0, inplace=False)
    附件7地市['设备上线率（%）'] = 附件7地市['设备上线率（%）'].apply(lambda x: format(x, '.2%'))
    附件7地市['20-50%占比'] = 附件7地市['20-50%占比'].apply(lambda x: format(x, '.2%'))
    附件7地市['50-100%占比'] = 附件7地市['50-100%占比'].apply(lambda x: format(x, '.2%'))
    附件7地市['100%以上占比'] = 附件7地市['100%以上占比'].apply(lambda x: format(x, '.2%'))
    附件7地市['80吨以上总数'] =附件7地市['本月超限80吨以上']
    附件7地市['外省入库数'] = 附件7地市['入库数(路政)']-附件7地市['本省入库数']
    附件7地市['非现场处罚外省(路政)'] = 附件7地市['非现场处罚(路政)'] - 附件7地市['非现场处罚本省(路政)']
    附件7地市['区县'] = 附件7地市['地市']
    附件7地市['区县编码'] = 附件7地市['地市编码']
    附件7地市['统计月份'] = 附件7['统计月份']
    # 附件7地市 = 附件7地市.set_index()
    附件7地市 = 附件7地市.fillna(0, inplace=False)
    附件7地市 = pd.DataFrame(附件7地市, columns=['统计月份','地市编码','地市','区县编码','区县','货车数','超限数','超限率','超限100','超限100%数货车数/万辆',
                                     '本月超限80吨以上','本月超限90吨以上','百吨王数','超限100%遮挡车牌数量（辆）', '百吨王遮挡车牌数量（辆）', '站点数',
                                     '报修数','设备完好率','异常数','数据完好率','入库数(路政)','本省入库数','外省入库数','现场处罚(路政)','非现场处罚(路政)','非现场处罚本省(路政)','非现场处罚(路政)当年',
                                     '非现场处罚本省(路政)当年','非现场处罚外省(路政)','交警现场查处数','交警非现场处罚数','交警非现查处数本省','需处罚数/非现入库数（总计）','需处罚数/非现入库数（本省）',
                                     '需处罚数/非现入库数（外省）','非现处罚数（总计）','非现处罚数（本省）','非现处罚数（外省）','年处罚数','外省抄告','非现场处罚率（本省）','非现场处罚率（外省）',
                                     '处罚率(含抄告）','80吨以上总数','80吨以上且满足','80吨以上且审核通过', '合规率','后面是源头数据','数据站点总数','源头货车数','源头单位平均过车数（辆次）',
                                     '在线站点数','设备上线率（%）','20-50%数','50-100%数','100%以上数','20-50%占比','50-100%占比','100%以上占比','非现场处罚路政案发','非现场处罚本省路政案发','非现场处罚路政案发当年','非现场处罚本省路政案发当年'])
    附件7地市 = 附件7地市.sort_values(by=['区县编码'],
                          ascending=True).reset_index(drop=True)
    附件7地市 = 附件7地市.fillna(0, inplace=False)
    附件7地市省 = 附件7地市.groupby(['统计月份']).sum().reset_index()
    附件7地市省.loc[附件7地市省['货车数'] == 0, '货车数'] = 1
    附件7地市省['超限率'] = 附件7地市省.apply(lambda x: x['超限数'] / x['货车数'], axis=1).round(4)
    附件7地市省 = 附件7地市省.fillna(0, inplace=False)
    附件7地市省['超限率'] = 附件7地市省['超限率'].apply(lambda x: format(x, '.2%'))
    附件7地市省.loc[附件7地市省['货车数'] == 1, '货车数'] = 0
    附件7地市省['设备完好率'] = (附件7地市省['站点数'] - 附件7地市省['报修数']) / 附件7地市省['站点数']
    附件7地市省['数据完好率'] = (附件7地市省['站点数'] - 附件7地市省['异常数']) / 附件7地市省['站点数']
    附件7地市省 = 附件7地市省.fillna(0, inplace=False)
    附件7地市省['数据完好率'] = 附件7地市省['数据完好率'].apply(lambda x: format(x, '.2%'))
    附件7地市省['设备完好率'] = 附件7地市省['设备完好率'].apply(lambda x: format(x, '.2%'))
    附件7地市省['超限100%数货车数/万辆'] = 附件7地市省.apply(lambda x: x['超限100'] / (x['货车数'] + 0.0000001)*10000, axis=1).round(4)
    附件7地市省['源头货车数'] = pd.to_numeric(附件7地市省['源头货车数'], errors='coerce')
    附件7地市省['数据站点总数'] = pd.to_numeric(附件7地市省['数据站点总数'], errors='coerce')
    附件7地市省['在线站点数'] = pd.to_numeric(附件7地市省['在线站点数'], errors='coerce')
    附件7地市省['20-50%数'] = pd.to_numeric(附件7地市省['20-50%数'], errors='coerce')
    附件7地市省['50-100%数'] = pd.to_numeric(附件7地市省['50-100%数'], errors='coerce')
    附件7地市省['100%以上数'] = pd.to_numeric(附件7地市省['100%以上数'], errors='coerce')
    附件7地市省['源头单位平均过车数（辆次）'] = 附件7地市省.apply(lambda x: x['源头货车数'] / (x['数据站点总数'] + 0.0000001), axis=1).round(0)
    附件7地市省['设备上线率（%）'] = 附件7地市省.apply(lambda x: x['在线站点数'] / (x['数据站点总数']+ 0.0000001), axis=1)
    附件7地市省['20-50%占比'] = 附件7地市省.apply(lambda x: x['20-50%数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7地市省['50-100%占比'] = 附件7地市省.apply(lambda x: x['50-100%数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7地市省['100%以上占比'] = 附件7地市省.apply(lambda x: x['100%以上数'] / (x['源头货车数'] + 0.0000001), axis=1)
    附件7地市省 = 附件7地市省.fillna(0, inplace=False)
    附件7地市省['设备上线率（%）'] = 附件7地市省['设备上线率（%）'].apply(lambda x: format(x, '.2%'))
    附件7地市省['20-50%占比'] = 附件7地市省['20-50%占比'].apply(lambda x: format(x, '.2%'))
    附件7地市省['50-100%占比'] = 附件7地市省['50-100%占比'].apply(lambda x: format(x, '.2%'))
    附件7地市省['100%以上占比'] = 附件7地市省['100%以上占比'].apply(lambda x: format(x, '.2%'))
    附件7地市省['80吨以上总数'] =附件7地市省['本月超限80吨以上']
    附件7地市省['外省入库数'] = 附件7地市省['入库数(路政)']-附件7地市省['本省入库数']
    附件7地市省['非现场处罚外省(路政)'] = 附件7地市省['非现场处罚(路政)'] - 附件7地市省['非现场处罚本省(路政)']
    附件7地市省['地市编码'] = '330000'
    附件7地市省['地市'] ='浙江'
    附件7地市省['区县'] = 附件7地市省['地市']
    附件7地市省['区县编码'] = 附件7地市省['地市编码']
    附件7地市省['统计月份'] = 附件7地市['统计月份']
    # 附件7地市省 = 附件7地市省.set_index()
    附件7地市省 = 附件7地市省.fillna(0, inplace=False)
    附件7地市省 = pd.DataFrame(附件7地市省, columns=['统计月份','地市编码','地市','区县编码','区县','货车数','超限数','超限率','超限100','超限100%数货车数/万辆',
                                     '本月超限80吨以上','本月超限90吨以上','百吨王数','超限100%遮挡车牌数量（辆）', '百吨王遮挡车牌数量（辆）', '站点数',
                                     '报修数','设备完好率','异常数','数据完好率','入库数(路政)','本省入库数','外省入库数','现场处罚(路政)','非现场处罚(路政)','非现场处罚本省(路政)','非现场处罚(路政)当年',
                                     '非现场处罚本省(路政)当年','非现场处罚外省(路政)','交警现场查处数','交警非现场处罚数','交警非现查处数本省','需处罚数/非现入库数（总计）','需处罚数/非现入库数（本省）',
                                     '需处罚数/非现入库数（外省）','非现处罚数（总计）','非现处罚数（本省）','非现处罚数（外省）','年处罚数','外省抄告','非现场处罚率（本省）','非现场处罚率（外省）',
                                     '处罚率(含抄告）','80吨以上总数','80吨以上且满足','80吨以上且审核通过', '合规率','后面是源头数据','数据站点总数','源头货车数','源头单位平均过车数（辆次）',
                                     '在线站点数','设备上线率（%）','20-50%数','50-100%数','100%以上数','20-50%占比','50-100%占比','100%以上占比','非现场处罚路政案发','非现场处罚本省路政案发','非现场处罚路政案发当年','非现场处罚本省路政案发当年'])
    附件7地市省 = 附件7地市省.fillna(0, inplace=False)


    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\10月通报数据1030v1.0.xlsx') as writer1:
        附件7.to_excel(writer1, sheet_name='区县汇总', index=False)
        附件7地市.to_excel(writer1, sheet_name='地市汇总', index=True)
        附件7地市省.to_excel(writer1, sheet_name='省汇总', index=True)
        货运源头监控数据省市.to_excel(writer1, sheet_name='货运源头监控数据省市', index=True)
        df_报修点位统计.to_excel(writer1, sheet_name='报修', index=False)
        df_数据汇总.to_excel(writer1, sheet_name='在线')
        df_sheet1.to_excel(writer1, sheet_name='sheet1', index=True)
        站点设备完好率.to_excel(writer1, sheet_name='站点设备完好率', index=True)
        区县超限率排序.to_excel(writer1, sheet_name='区县超限率排序', index=True)
        T_20天与500筛选1.to_excel(writer1, sheet_name='在线天数大于等于20天货车数大于500的站点数据', index=True)
        df_源头站点明细.to_excel(writer1, sheet_name='源头站点数据明细', index=False)
        总重80吨以上明细.to_excel(writer1, sheet_name='总重80_90以上明细')
        超限100汇总.to_excel(writer1, sheet_name='超限100汇总', index=True)
        超限100明细.to_excel(writer1, sheet_name='超限100明细', index=True)
        百吨王明细.to_excel(writer1, sheet_name='百吨王明细')