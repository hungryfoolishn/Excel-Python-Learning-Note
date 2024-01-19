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
end_time='2023-09-30'


部级黑名单交通现场= {
    "tableName": "t_case_sign_result c left  join  t_bas_over_data_collection_sign d  ON c.record_code = d.record_code  left join t_bas_car_information t on c.car_no =t.car_number",
    "where": "c.record_type = 99 and c.insert_type = 5 AND c.close_case_time between '{} 00:00:00'  AND '{} 23:59:59' AND c.car_no like '%浙A%'  ".format(start_time,end_time),
    "columns": "t.county as 车籍地,c.dept_county 处罚地,c.area_county 案发地,c.area_city 案发地市,c.record_code,c.car_no  as 车牌号,c.car_qua_code 道路运输证号,c.driver_name 驾驶员姓名,c.driver_id_card 身份证号,c.CASE_NUM as 行政处罚决定书文号,DATE_FORMAT(d.out_station_time,'%Y-%m-%d') 违法时间,DATE_FORMAT(c.close_case_time,'%Y-%m-%d') 处罚决定日期, d.punish_money 罚款金额,c.org_name  执法机构名称"
}


部级黑名单交警现场= {
    "tableName": "t_bas_police_road_site c left join t_bas_car_information t on c.car_number =t.car_number ",
    "where": "c.punish_time between '{} 00:00:00' AND '{} 00:00:00' AND c.car_number like '%浙A%'".format(start_time,end_time),
    "columns": "t.county as 车籍地,c.area_county 处罚地,c.area_county 案发地,c.area_city 案发地市,c.id as record_code,c.car_number as 车牌号,c.server_licence 道路运输证号,c.driver_name 驾驶员姓名,c.driver_licence 身份证号, c.traffic_police_punish_number 行政处罚决定书文号,DATE_FORMAT(c.transfer_time,'%Y-%m-%d')  违法时间,DATE_FORMAT(c.punish_time,'%Y-%m-%d') 处罚决定日期,c.punish_money 罚款金额,c.punish_dept 执法机构名称"
}

部级黑名单交通非现 = {
    "tableName": "t_case_sign_result c left  join  t_bas_over_data_collection_sign d  ON c.record_code = d.record_code  left join t_bas_car_information t on c.car_no =t.car_number",
    "where": "c.record_type = 31 and c.insert_type = 1 AND c.data_source = 1 AND c.case_type = 1  AND c.close_case_time between '{} 00:00:00'  AND '{} 23:59:59' AND c.car_no like '%浙A%'  ".format(start_time,end_time),
    "columns": "d.total_weight as 总重,d.limit_weight as 限重, d.overrun_rate as 超限率,t.county as 车籍地,c.dept_county 处罚地,c.area_county 案发地,c.area_city 案发地市,c.record_code,c.car_no  as 车牌号,c.car_qua_code 道路运输证号,c.driver_name 驾驶员姓名,c.driver_id_card 身份证号,c.CASE_NUM as 行政处罚决定书文号,DATE_FORMAT(d.out_station_time,'%Y-%m-%d') 违法时间,DATE_FORMAT(c.close_case_time,'%Y-%m-%d') 处罚决定日期, d.punish_money 罚款金额,c.org_name  执法机构名称"
}


t_bas_transport_car ={
    "tableName":"t_bas_transport_car  ",
    "where":"car_no  like '%浙A%' and is_deleted=0 ",
    "columns":"transport_company_id,car_no"
}

t_bas_transport_company ={
    "tableName":"t_bas_transport_company  ",
    "where":" city=330100 and is_deleted=0 ",
    "columns":"id,name,city,district"
}
# 获取数据
def get_df_from_db(sql):
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


# 获取数据
def get_data_car(sql):
    data = {
    "carNumber": "浙D15737"
    }
    url = 'https://lwjc.jtyst.zj.gov.cn:7443/zc-interface/trafficManagmentData/queryYGCar'
    headers = {'content-type': 'application/json'}
    res = requests.post(url, json=data, headers=headers)
    res = json.loads(res.text)
    data = res['data']
    data=pd.DataFrame(data)
    return  data


if __name__ == "__main__":
    file_name=r'G:\智诚\2023日常给出数据\其他任务\杭州部级黑名单车辆2022.9.30-2023.9.30.xlsx'
    部级黑名单非现车辆 = pd.read_excel('{}'.format(file_name),sheet_name='部级黑名单非现车辆')
    部级黑名单现场车辆 = pd.read_excel('{}'.format(file_name),sheet_name='部级黑名单现场车辆')
    t_bas_transport_company_car = pd.read_excel(r'C:\Users\stayhungary\Desktop\t_bas_transport_car.xlsx',sheet_name='t_bas_transport_company_car')
    省部级黑名单非现车辆_car=部级黑名单非现车辆['车牌号']
    省部级黑名单非现车辆_car = pd.DataFrame(省部级黑名单非现车辆_car)
    省部级黑名单现场车辆_car=部级黑名单现场车辆['车牌号']
    省部级黑名单现场车辆_car = pd.DataFrame(省部级黑名单现场车辆_car)
    省All_car_no=pd.concat([省部级黑名单现场车辆_car,省部级黑名单非现车辆_car])
    省All_car_no = 省All_car_no.drop_duplicates(subset=['车牌号'])
    省All_car_no['是否处罚']='是'
    省t_bas_transport_company_car=pd.merge(t_bas_transport_company_car, 省All_car_no, left_on=['car_no'],right_on=['车牌号'], how='left')
    企业车辆数 = 省t_bas_transport_company_car.groupby(['district', 'name'])[
        'id'].count().reset_index(name='区县企业车辆数')
    区县企业车辆处罚数 = 省t_bas_transport_company_car.groupby(['district', 'name'])[
        '车牌号'].count().reset_index(name='区县企业车辆处罚数')
    省企业处罚占比=pd.merge(企业车辆数, 区县企业车辆处罚数, on=['district', 'name'], how='left')

    省企业处罚占比=省企业处罚占比[(省企业处罚占比['区县企业车辆处罚数']>0)]
    省企业处罚占比['省企业处罚占比超过10%']=省企业处罚占比['区县企业车辆处罚数']/省企业处罚占比['区县企业车辆数']
    省企业处罚占比=省企业处罚占比[(省企业处罚占比['省企业处罚占比超过10%']>=0.1)]
    省企业处罚占比['省企业处罚占比超过10%']= 省企业处罚占比['省企业处罚占比超过10%'].apply(lambda x: format(x, '.2%'))
    file_name = r"G:\智诚\2023日常给出数据\省通报\202309\0927test.xlsx"
    df_区县编码 = pd.read_excel(file_name, sheet_name='code_area')
    df_区县编码['区县编码'] = df_区县编码['区县编码'].astype('string')
    df_区县编码['地市编码'] = df_区县编码['地市编码'].astype('string')
    省企业处罚占比['district'] = 省企业处罚占比['district'] .astype('string')
    省企业处罚占比.loc[省企业处罚占比['district'] == '330184', 'district'] = '330110'
    省企业处罚占比.loc[省企业处罚占比['district'] == '330155', 'district'] = '330114'
    省企业处罚占比=pd.merge(省企业处罚占比,df_区县编码, left_on=['district'] ,right_on=['区县编码'], how='left')
    省企业处罚区县汇总 = 省企业处罚占比.groupby(['district', '区县'])[
        'name'].count().reset_index(name='企业占比超限10%数')


    ##市级
    部级黑名单现场车辆['案发地市'] = 部级黑名单现场车辆['案发地市'].astype('int')
    市部级黑名单现场车辆=部级黑名单现场车辆[(部级黑名单现场车辆['案发地市'] == 330100)]
    市部级黑名单非现车辆=部级黑名单非现车辆[部级黑名单非现车辆['行政处罚决定书文号'].str.contains('杭', case=False)]
    市部级黑名单非现车辆_car=市部级黑名单非现车辆['车牌号']
    市部级黑名单非现车辆_car = pd.DataFrame(市部级黑名单非现车辆_car)
    市部级黑名单现场车辆_car=市部级黑名单现场车辆['车牌号']
    市部级黑名单现场车辆_car = pd.DataFrame(市部级黑名单现场车辆_car)
    市All_car_no=pd.concat([市部级黑名单现场车辆_car,市部级黑名单非现车辆_car])
    市All_car_no = 市All_car_no.drop_duplicates(subset=['车牌号'])
    市All_car_no['是否处罚']='是'
    市t_bas_transport_company_car=pd.merge(t_bas_transport_company_car, 市All_car_no, left_on=['car_no'],right_on=['车牌号'], how='left')
    企业车辆数 = 市t_bas_transport_company_car.groupby(['district', 'name'])[
        'id'].count().reset_index(name='区县企业车辆数')
    区县企业车辆处罚数 = 市t_bas_transport_company_car.groupby(['district', 'name'])[
        '车牌号'].count().reset_index(name='区县企业车辆处罚数')
    市企业处罚占比=pd.merge(企业车辆数, 区县企业车辆处罚数, on=['district', 'name'], how='left')
    市企业处罚占比=市企业处罚占比[(市企业处罚占比['区县企业车辆处罚数']>0)]
    市企业处罚占比['市企业处罚占比超过10%']=市企业处罚占比['区县企业车辆处罚数']/市企业处罚占比['区县企业车辆数']
    市企业处罚占比=市企业处罚占比[(市企业处罚占比['市企业处罚占比超过10%']>=0.1)]
    市企业处罚占比['市企业处罚占比超过10%']= 市企业处罚占比['市企业处罚占比超过10%'].apply(lambda x: format(x, '.2%'))
    file_name = r"G:\智诚\2023日常给出数据\省通报\202309\0927test.xlsx"
    df_区县编码 = pd.read_excel(file_name, sheet_name='code_area')
    df_区县编码['区县编码'] = df_区县编码['区县编码'].astype('string')
    df_区县编码['地市编码'] = df_区县编码['地市编码'].astype('string')
    市企业处罚占比['district'] = 市企业处罚占比['district'] .astype('string')
    市企业处罚占比.loc[市企业处罚占比['district'] == '330184', 'district'] = '330110'
    市企业处罚占比.loc[市企业处罚占比['district'] == '330155', 'district'] = '330114'
    市企业处罚占比=pd.merge(市企业处罚占比,df_区县编码, left_on=['district'] ,right_on=['区县编码'], how='left')
    市企业处罚区县汇总 = 市企业处罚占比.groupby(['district', '区县'])[
        'name'].count().reset_index(name='企业占比超限10%数')

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\杭州一超四罚2022.9.30-2023.9.30企业占比.xlsx') as writer1:
        市企业处罚区县汇总.to_excel(writer1, sheet_name='市企业处罚区县汇总', index=True)
        省企业处罚区县汇总.to_excel(writer1, sheet_name='省企业处罚区县汇总', index=True)
        市企业处罚占比.to_excel(writer1, sheet_name='市企业处罚占比', index=True)
        省企业处罚占比.to_excel(writer1, sheet_name='省企业处罚占比', index=True)
        省t_bas_transport_company_car.to_excel(writer1, sheet_name='省t_bas_transport_company_car', index=True)





