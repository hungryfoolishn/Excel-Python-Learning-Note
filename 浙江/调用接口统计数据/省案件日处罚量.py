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
start_time='2023-09-20'
end_time='2023-09-20'


##--交警现场
sql_t_code_area ={
    "tableName":"   t_code_area    ",
    "where":"  is_deleted =0 and province_code= '330000' ",
    "columns":"county_code as area_county,city_code,city as 地市,county as 区县"
}


##--交通现场查处数

sql_交通现场查处数={
    "tableName":" t_case_sign_result c LEFT JOIN t_bas_over_data_collection_sign d on d.record_code=c.record_code",
    "where":" c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time >= '{} 00:00:00' AND c.close_case_time <= '{} 23:59:59'  GROUP BY c.area_county  ".format(start_time,end_time),
    "columns":"c.area_county as area_county ,count( DISTINCT ( CASE_NUM ) ) AS 交通现场查处数 "
}


##--交通现场查处数明细

sql_交通非现查处数={
    "tableName":"t_case_sign_result a  ",
    "where":" record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND close_case_time between '{} 00:00:00' AND  '{} 23:59:59' AND area_province = '330000' GROUP BY  dept_county".format(start_time,end_time),
    "columns":"dept_county as area_county,count(DISTINCT ( CASE_NUM )) AS 交通非现查处数,  count(DISTINCT IF( car_no LIKE '%浙%', CASE_NUM, NULL )) AS 交通非现查处数本省 "
}


##--交警现场
sql_交警现场 ={
    "tableName":"   t_bas_police_road_site a    ",
    "where":"   a.punish_time between '{} 00:00:00' AND  '{} 23:59:59'  and case_status=2  GROUP BY area_county  ".format(start_time,end_time),
    "columns":"area_county,count(DISTINCT case_number) as 交警现场查处数"
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
    t_code_area=get_data(sql_t_code_area)

    交通现场查处数 = get_data(sql_交通现场查处数)
    交通现场查处数 = pd.DataFrame(交通现场查处数,
                           columns=['area_county', '交通现场查处数'])

    交通非现查处数 = get_data(sql_交通非现查处数)
    交警现场 = get_data(sql_交警现场)
    交警现场 = pd.DataFrame(交警现场,
                           columns=['area_county', '交警现场查处数'])
    U_all = pd.merge(t_code_area, 交通非现查处数, on=['area_county'], how='left')
    U_all = pd.merge(U_all, 交通现场查处数, on=['area_county'], how='left')
    U_all = pd.merge(U_all, 交警现场, on=['area_county'], how='left')
    U_all = pd.DataFrame(U_all,
                           columns=['地市', '区县', '交通非现查处数', '交通非现查处数本省', '交通现场查处数', '交警现场查处数', 'city_code', 'area_county'])
    U_all=U_all.fillna(0)
    U_all = U_all.sort_values(by=['area_county'],
                                    ascending=True).reset_index(drop=True)
    U_all地市 = U_all.groupby(['city_code', '地市']).sum().reset_index()
    U_all地市 = pd.DataFrame(U_all地市,
                           columns=['地市', '交通非现查处数', '交通非现查处数本省', '交通现场查处数', '交警现场查处数'])
    U_all省=U_all地市.copy()
    U_all省['省']='浙江'
    U_all省 = U_all省.groupby(['省']).sum().reset_index()
    U_all省 = pd.DataFrame(U_all省,
                           columns=['省', '交通非现查处数', '交通非现查处数本省', '交通现场查处数', '交警现场查处数'])

    U_all省.rename(
        columns={'省': '地市'}, inplace=True)
    U_all省=pd.concat([U_all地市,U_all省])
    print(U_all省)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\每日全省案件处罚数据.xlsx') as writer1:
        U_all省.to_excel(writer1, sheet_name='地市汇总', index=True)
        U_all地市.to_excel(writer1, sheet_name='地市汇总', index=True)
        U_all.to_excel(writer1, sheet_name='每日全省案件处罚数据', index=True)
        交通现场查处数.to_excel(writer1, sheet_name='交通现场查处数', index=True)
        交警现场.to_excel(writer1, sheet_name='交警现场', index=True)
