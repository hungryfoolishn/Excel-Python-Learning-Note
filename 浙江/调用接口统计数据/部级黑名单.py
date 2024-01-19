# coding: utf-8


import time
import pandas as pd
import requests
import base64
import datetime
import pymysql
import json
import csv


start_time='2023-01-01'
end_time='2023-09-31'


部级黑名单交通现场= {
    "tableName": "t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  left  join  t_bas_over_data_collection_sign d  ON c.record_code = d.record_code  ",
    "where": "c.record_type = 99 and c.insert_type = 5 AND c.close_case_time between '{} 00:00:00'  AND '{} 23:59:59' AND c.area_province = 330000  ".format(start_time,end_time),
    "columns": "c.record_code,c.car_no  as 车牌号,c.car_qua_code 道路运输证号,c.driver_name 驾驶员姓名,c.driver_id_card 身份证号,c.CASE_NUM as 行政处罚决定书文号,DATE_FORMAT(d.out_station_time,'%Y-%m-%d') 违法时间,DATE_FORMAT(c.close_case_time,'%Y-%m-%d') 处罚决定日期, d.punish_money 罚款金额,c.org_name  执法机构名称"
}

部级黑名单交警现场= {
    "tableName": "t_bas_police_road_site ",
    "where": "punish_time between '{} 00:00:00' AND '{} 00:00:00' ".format(start_time,end_time),
    "columns": "id as record_code,car_number as 车牌号,server_licence 道路运输证号,driver_name 驾驶员姓名,driver_licence 身份证号, traffic_police_punish_number 行政处罚决定书文号,DATE_FORMAT(transfer_time,'%Y-%m-%d')  违法时间,DATE_FORMAT(punish_time,'%Y-%m-%d') 处罚决定日期,punish_money 罚款金额,punish_dept 执法机构名称"
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



def 格式处理(data1):
    data1['字节数'] = data1['身份证号'].str.len()
    data1['字节数2'] = data1['行政处罚决定书文号'].str.len()
    data1 = data1.drop_duplicates(subset=['record_code'])
    data1 = data1.drop_duplicates(subset=['行政处罚决定书文号'])
    data1 = data1[(data1['罚款金额'] >0)]
    data1 = data1[(data1['字节数'] ==18)]
    data1['处罚决定日期'] = pd.to_datetime(data1['处罚决定日期'],format='%Y-%m-%d')
    本季度前3次以上车牌= data1[(data1['处罚决定日期'] <= '2023-07-01 00:00:00')].groupby(["车牌号"])[
        'record_code'].count().reset_index(name='本季度前3次以上车牌')
    本季度前3次以上车牌=本季度前3次以上车牌[(本季度前3次以上车牌['本季度前3次以上车牌'] >=3)]
    本季度前3次以上车牌=本季度前3次以上车牌['车牌号']

    全部3次以上车牌= data1[(data1['处罚决定日期'] >= '2023-01-01 00:00:00')].groupby(["车牌号"])[
        'record_code'].count().reset_index(name='全部3次以上车牌')
    全部3次以上车牌=全部3次以上车牌[(全部3次以上车牌['全部3次以上车牌'] >=3)]
    全部3次以上车牌=全部3次以上车牌['车牌号']
    print(全部3次以上车牌)
    新增车牌 = data1[data1.loc[:, '车牌号'].isin(全部3次以上车牌)]
    print(新增车牌)
    新增车牌 = 新增车牌[~新增车牌.loc[:, '车牌号'].isin(本季度前3次以上车牌)]

    新增车牌 = 新增车牌.sort_values(by=['车牌号','处罚决定日期'],
                                    ascending=True).reset_index(drop=True)
    return 新增车牌

if __name__ == "__main__":
    data1=get_data(部级黑名单交警现场)
    data1=格式处理(data1)
    data1 = pd.read_excel(r'C:\Users\stayhungary\Desktop\部级黑名单交警现场.xlsx')
    交警现场=格式处理(data1)
    data1 = pd.read_excel(r'C:\Users\stayhungary\Desktop\部级黑名单.xlsx')
    交通现场=格式处理(data1)
    部级黑名单车辆=pd.concat([交通现场,交警现场])
    全部3次以上人员= 部级黑名单车辆.groupby(['身份证号',"驾驶员姓名"])[
        '身份证号'].count().reset_index(name='全部3次以上人员')
    全部3次以上人员=全部3次以上人员[(全部3次以上人员['全部3次以上人员'] >=3)]
    全部3次以上人员=全部3次以上人员['身份证号']
    部级黑名单人员 = 部级黑名单车辆[部级黑名单车辆.loc[:, '身份证号'].isin(全部3次以上人员)]
    部级黑名单人员 = 部级黑名单人员.sort_values(by=['身份证号','处罚决定日期'],
                                    ascending=True).reset_index(drop=True)

    部级黑名单人员['处罚决定日期'] = 部级黑名单人员['处罚决定日期'].map(lambda x: x.strftime('%Y-%m-%d'))
    部级黑名单车辆['处罚决定日期'] = 部级黑名单车辆['处罚决定日期'].map(lambda x: x.strftime('%Y-%m-%d'))

    部级黑名单车辆 = pd.DataFrame(部级黑名单车辆, columns=['序号', '车牌号','道路运输证号','违章次数','驾驶员姓名','身份证号','违法时间','行政处罚决定书文号','处罚决定日期','罚款金额','执法机构名称'])
    部级黑名单人员 = pd.DataFrame(部级黑名单人员, columns=['序号', '驾驶员姓名','身份证号','违章次数','车牌号','道路运输证号','违法时间','行政处罚决定书文号','处罚决定日期','罚款金额','执法机构名称'])
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\部级黑名单全部v2.0.xlsx') as writer1:
        部级黑名单车辆.to_excel(writer1, sheet_name='车辆', index=True)
        部级黑名单人员.to_excel(writer1, sheet_name='人员', index=True)



