# coding: utf-8


import time
import pandas as pd
import requests
import base64
import datetime
import pymysql
import json
import csv


start_time='2022-09-30'
end_time='2023-09-30'


部级黑名单交通现场= {
    "tableName": "t_case_sign_result c left  join  t_bas_over_data_collection_sign d  ON c.record_code = d.record_code ",
    "where": "c.record_type = 99 and c.insert_type = 5 AND c.close_case_time between '{} 00:00:00'  AND '{} 23:59:59' AND c.car_no like '%浙A%'  ".format(start_time,end_time),
    "columns": "c.dept_county 处罚地,c.area_county 案发地,c.area_city 案发地市,c.record_code,c.car_no  as 车牌号,c.car_qua_code 道路运输证号,c.driver_name 驾驶员姓名,c.driver_id_card 身份证号,c.CASE_NUM as 行政处罚决定书文号,DATE_FORMAT(d.out_station_time,'%Y-%m-%d') 违法时间,DATE_FORMAT(c.close_case_time,'%Y-%m-%d') 处罚决定日期, d.punish_money 罚款金额,c.org_name  执法机构名称"
}

部级黑名单交警现场= {
    "tableName": "t_bas_police_road_site c  ",
    "where": "c.punish_time between '{} 00:00:00' AND '{} 00:00:00' AND c.car_number like '%浙A%'".format(start_time,end_time),
    "columns": "c.area_county 处罚地,c.area_county 案发地,c.area_city 案发地市,c.id as record_code,c.car_number as 车牌号,c.server_licence 道路运输证号,c.driver_name 驾驶员姓名,c.driver_licence 身份证号, c.traffic_police_punish_number 行政处罚决定书文号,DATE_FORMAT(c.transfer_time,'%Y-%m-%d')  违法时间,DATE_FORMAT(c.punish_time,'%Y-%m-%d') 处罚决定日期,c.punish_money 罚款金额,c.punish_dept 执法机构名称"
}

部级黑名单交通非现 = {
    "tableName": "t_case_sign_result c left  join  t_bas_over_data_collection_sign d  ON c.record_code = d.record_code ",
    "where": "c.record_type = 31 and c.insert_type = 1 AND c.data_source = 1 AND c.case_type = 1  AND c.close_case_time between '{} 00:00:00'  AND '{} 23:59:59' AND c.car_no like '%浙A%'  ".format(start_time,end_time),
    "columns": "d.total_weight as 总重,d.limit_weight as 限重, d.overrun_rate as 超限率,c.dept_county 处罚地,c.area_county 案发地,c.area_city 案发地市,c.record_code,c.car_no  as 车牌号,c.car_qua_code 道路运输证号,c.driver_name 驾驶员姓名,c.driver_id_card 身份证号,c.CASE_NUM as 行政处罚决定书文号,DATE_FORMAT(d.out_station_time,'%Y-%m-%d') 违法时间,DATE_FORMAT(c.close_case_time,'%Y-%m-%d') 处罚决定日期, d.punish_money 罚款金额,c.org_name  执法机构名称"
}
t_bas_car_information= {
    "tableName": "t_bas_car_information ",
    "where": " car_number like '%浙A%' and county is not null ",
    "columns": "county ,car_number"
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
    file_name = r"G:\智诚\2023日常给出数据\省通报\202309\0927test.xlsx"
    t_bas_car_information=get_data(t_bas_car_information)
    运管车辆信息 = pd.read_excel(r'G:\智诚\2023日常给出数据\部级黑名单\运管车辆信息杭州.xlsx')
    t_bas_car_information=pd.concat([t_bas_car_information,运管车辆信息])
    t_bas_car_information = t_bas_car_information.drop_duplicates(subset=['county','car_number'])
    t_bas_car_information.rename(columns={'county': '车籍地','car_number': '车牌号'}, inplace=True)
    部级黑名单交通现场 = get_data(部级黑名单交通现场)
    部级黑名单交警现场 = get_data(部级黑名单交警现场)
    部级黑名单交通非现 = get_data(部级黑名单交通非现)
    部级黑名单现场=pd.concat([部级黑名单交通现场,部级黑名单交警现场])
    部级黑名单现场 = pd.merge(部级黑名单现场, t_bas_car_information, on=['车牌号'], how='left')
    部级黑名单交通非现 = pd.merge(部级黑名单交通非现, t_bas_car_information, on=['车牌号'], how='left')

    部级黑名单非现=部级黑名单交通非现
    全部3次以上车辆现场= 部级黑名单现场.groupby(['车籍地','车牌号'])[
        '车牌号'].count().reset_index(name='全部3次以上车辆现场')
    全部3次以上车辆现场=全部3次以上车辆现场[(全部3次以上车辆现场['全部3次以上车辆现场'] >=3)]
    全部3次以上车辆现场=全部3次以上车辆现场['车牌号']
    部级黑名单现场 = 部级黑名单现场[部级黑名单现场.loc[:, '车牌号'].isin(全部3次以上车辆现场)]
    部级黑名单现场 = 部级黑名单现场.sort_values(by=['车牌号','处罚决定日期'],
                                    ascending=True).reset_index(drop=True)

    全部3次以上车辆非现= 部级黑名单非现.groupby(['车籍地','车牌号'])[
        '车牌号'].count().reset_index(name='全部3次以上车辆非现')
    全部3次以上车辆非现=全部3次以上车辆非现[(全部3次以上车辆非现['全部3次以上车辆非现'] >=3)]
    全部3次以上车辆非现=全部3次以上车辆非现['车牌号']
    部级黑名单非现 = 部级黑名单非现[部级黑名单非现.loc[:, '车牌号'].isin(全部3次以上车辆非现)]
    部级黑名单非现 = 部级黑名单非现.sort_values(by=['车牌号','处罚决定日期'],
                                    ascending=True).reset_index(drop=True)
    df_区县编码 = pd.read_excel(file_name, sheet_name='code_area')
    df_区县编码['区县编码'] = df_区县编码['区县编码'].astype('string')
    df_区县编码['地市编码'] = df_区县编码['地市编码'].astype('string')
    部级黑名单现场['车籍地'] = 部级黑名单现场['车籍地'] .fillna(0)
    部级黑名单非现['车籍地'] = 部级黑名单非现['车籍地'] .fillna(0)
    部级黑名单现场['车籍地'] = 部级黑名单现场['车籍地'].astype('int')
    部级黑名单非现['车籍地'] = 部级黑名单非现['车籍地'].astype('int')
    部级黑名单现场['车籍地'] = 部级黑名单现场['车籍地'].astype('string')
    部级黑名单非现['车籍地'] = 部级黑名单非现['车籍地'].astype('string')
    部级黑名单现场.loc[部级黑名单现场['车籍地'] == '330184', '车籍地'] = '330110'
    部级黑名单现场.loc[部级黑名单现场['车籍地'] == '330155', '车籍地'] = '330114'
    部级黑名单非现.loc[部级黑名单非现['车籍地'] == '330184', '车籍地'] = '330110'
    部级黑名单非现.loc[部级黑名单非现['车籍地'] == '330155', '车籍地'] = '330114'
    部级黑名单现场=pd.merge(部级黑名单现场,df_区县编码, left_on=['车籍地'] ,right_on=['区县编码'], how='left')
    部级黑名单非现=pd.merge(部级黑名单非现,df_区县编码, left_on=['车籍地'] ,right_on=['区县编码'], how='left')


    全部3次以上车辆现场 = 部级黑名单现场.groupby(['车籍地', '区县'])['车牌号'].nunique().reset_index(name='全部3次以上车辆现场')
    全部3次以上车辆非现 = 部级黑名单非现.groupby(['车籍地', '区县'])['车牌号'].nunique().reset_index(name='全部3次以上车辆非现')

    部级黑名单非现['超限率'] = 部级黑名单非现['超限率'] .astype('float')
    部级黑名单非现['总重'] = 部级黑名单非现['总重'].astype('float')
    车辆非现超限20_50 = 部级黑名单非现[(部级黑名单非现['超限率'] >= 20) & (部级黑名单非现['超限率'] < 50)].groupby(['车籍地', '区县'])[
        '车牌号'].nunique().reset_index(name='车辆非现超限20_50')
    车辆非现超限50_100 = 部级黑名单非现[(部级黑名单非现['超限率'] >= 50) & (部级黑名单非现['超限率'] < 100)].groupby(['车籍地', '区县'])[
        '车牌号'].nunique().reset_index(name='车辆非现超限50_100')
    车辆非现超限100 = 部级黑名单非现[(部级黑名单非现['超限率'] >= 100)].groupby(['车籍地', '区县'])['车牌号'].nunique().reset_index(name='车辆非现超限100')
    车辆非现总重80以下 = 部级黑名单非现[(部级黑名单非现['总重'] <= 80)].groupby(['车籍地', '区县'])['车牌号'].nunique().reset_index(name='车辆非现总重80以下')
    车辆非现总重80以上 = 部级黑名单非现[(部级黑名单非现['总重'] > 80)].groupby(['车籍地', '区县'])['车牌号'].nunique().reset_index(name='车辆非现总重80以上')

    部级黑名单汇总车辆=pd.merge(全部3次以上车辆现场,全部3次以上车辆非现, on=['车籍地','区县'], how='outer')
    部级黑名单汇总车辆=pd.merge(部级黑名单汇总车辆,车辆非现超限20_50, on=['车籍地','区县'], how='outer')
    部级黑名单汇总车辆=pd.merge(部级黑名单汇总车辆,车辆非现超限50_100, on=['车籍地','区县'], how='outer')
    部级黑名单汇总车辆=pd.merge(部级黑名单汇总车辆,车辆非现超限100, on=['车籍地','区县'], how='outer')
    部级黑名单汇总车辆=pd.merge(部级黑名单汇总车辆,车辆非现总重80以下, on=['车籍地','区县'], how='outer')
    部级黑名单汇总车辆=pd.merge(部级黑名单汇总车辆,车辆非现总重80以上, on=['车籍地','区县'], how='outer')

    部级黑名单汇总车辆=部级黑名单汇总车辆.fillna(0)


    ##市级
    部级黑名单现场['案发地市'] = 部级黑名单现场['案发地市'].astype('int')
    市部级黑名单现场=部级黑名单现场[(部级黑名单现场['案发地市'] == 330100)]
    # 市部级黑名单非现=部级黑名单非现[(部级黑名单非现['处罚地'] < 330200)]
    市部级黑名单非现=部级黑名单非现[部级黑名单非现['行政处罚决定书文号'].str.contains('杭', case=False)]
    全部3次以上车辆现场 = 市部级黑名单现场.groupby(['车籍地', '区县'])['车牌号'].nunique().reset_index(name='全部3次以上车辆现场')
    全部3次以上车辆非现 = 市部级黑名单非现.groupby(['车籍地', '区县'])['车牌号'].nunique().reset_index(name='全部3次以上车辆非现')
    车辆非现超限20_50 = 市部级黑名单非现[(市部级黑名单非现['超限率'] >= 20) & (市部级黑名单非现['超限率'] < 50)].groupby(['车籍地', '区县'])[
        '车牌号'].nunique().reset_index(name='车辆非现超限20_50')
    车辆非现超限50_100 = 市部级黑名单非现[(市部级黑名单非现['超限率'] >= 50) & (市部级黑名单非现['超限率'] < 100)].groupby(['车籍地', '区县'])[
        '车牌号'].nunique().reset_index(name='车辆非现超限50_100')
    车辆非现超限100 = 市部级黑名单非现[(市部级黑名单非现['超限率'] >= 100)].groupby(['车籍地', '区县'])['车牌号'].nunique().reset_index(name='车辆非现超限100')
    车辆非现总重80以下 = 市部级黑名单非现[(市部级黑名单非现['总重'] <= 80)].groupby(['车籍地', '区县'])['车牌号'].nunique().reset_index(name='车辆非现总重80以下')
    车辆非现总重80以上 = 市部级黑名单非现[(市部级黑名单非现['总重'] > 80)].groupby(['车籍地', '区县'])['车牌号'].nunique().reset_index(name='车辆非现总重80以上')
    市部级黑名单汇总车辆 = pd.merge(全部3次以上车辆现场, 全部3次以上车辆非现, on=['车籍地', '区县'], how='outer')
    市部级黑名单汇总车辆 = pd.merge(市部级黑名单汇总车辆, 车辆非现超限20_50, on=['车籍地', '区县'], how='outer')
    市部级黑名单汇总车辆 = pd.merge(市部级黑名单汇总车辆, 车辆非现超限50_100, on=['车籍地', '区县'], how='outer')
    市部级黑名单汇总车辆 = pd.merge(市部级黑名单汇总车辆, 车辆非现超限100, on=['车籍地', '区县'], how='outer')
    市部级黑名单汇总车辆 = pd.merge(市部级黑名单汇总车辆, 车辆非现总重80以下, on=['车籍地', '区县'], how='outer')
    市部级黑名单汇总车辆 = pd.merge(市部级黑名单汇总车辆, 车辆非现总重80以上, on=['车籍地', '区县'], how='outer')
    市部级黑名单汇总车辆 = 市部级黑名单汇总车辆.fillna(0)

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\杭州部级黑名单现场2022.9.30-2023.9.30.xlsx') as writer1:
        市部级黑名单汇总车辆.to_excel(writer1, sheet_name='市部级黑名单汇总车辆', index=True)
        部级黑名单汇总车辆.to_excel(writer1, sheet_name='省部级黑名单汇总车辆', index=True)
        部级黑名单现场.to_excel(writer1, sheet_name='部级黑名单现场车辆', index=True)
        部级黑名单非现.to_excel(writer1, sheet_name='部级黑名单非现车辆', index=True)



