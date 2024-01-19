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

start_time='2023-01-01'
end_time='2023-08-31'


##--交通现场查处数明细


sql_非现处罚数={
    "tableName":"t_case_sign_result a left join t_code_area b on a.dept_county =b.county_code ",
    "where":" record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND close_case_time between '2023-01-01 00:00:00' AND  '2023-08-31 23:59:59' AND area_province = '330000' GROUP BY  dept_county",
    "columns":"b.city_code,dept_county countyCode,count(DISTINCT ( CASE_NUM )) AS offSiteNum,  count(DISTINCT IF( car_no LIKE '%浙%', CASE_NUM, NULL )) AS offSiteNumP "
}

sql_非现入库数={
    "tableName":"t_bas_over_data_collection_31  ",
    "where":" law_judgment = 1  AND valid_time between '2023-01-01 00:00:00'  AND '2023-01-31 23:59:59'  and status !=5 GROUP BY area_county  ",
    "columns":"area_city,area_county countyCode, count( 1 ) inboundCase, sum(IF( car_no LIKE '%浙%', 1, 0 )) inboundCaseP  "
}


sql_非现当年处罚数={
    "tableName":"t_case_sign_result c LEFT JOIN t_bas_over_data_collection_31 b ON c.record_code = b.record_code  ",
    "where":" record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND close_case_time between '2023-01-01 00:00:00' AND  '2023-08-31 23:59:59' AND c.area_province = '330000' and  b.valid_time between '2023-01-01 00:00:00' and  '2023-08-31 23:59:59' GROUP BY  dept_county",
    "columns":"c.dept_county countyCode,count(DISTINCT ( c.CASE_NUM )) AS offSiteYearNum, count(DISTINCT IF( c.car_no LIKE '%浙%', c.CASE_NUM, NULL )) AS offSiteYearNumP  "
}

sql_外省抄告数={
    "tableName":"t_bas_over_data_collection_makecopy   ",
    "where":" insert_time >= '2023-01-01 00:00:00' AND insert_time <= '2023-08-31 23:59:59'  and valid_time >= '2023-01-01 00:00:00' GROUP BY area_county ",
    "columns":"area_city,area_county, count(1) makecopyOtherP    "
}

sql_外省抄告数2={
    "tableName":"t_bas_over_data_collection_31  ",
    "where":" law_judgment = 1  AND valid_time between '2023-01-01 00:00:00'  AND '2023-08-31 23:59:59'  and status !=5 GROUP BY area_county  ",
    "columns":"area_city,area_county countyCode, count( 1 ) inboundCase, sum(IF( car_no LIKE '%浙%', 1, 0 )) inboundCaseP ,sum(IF( make_copy=1 and car_no not LIKE '%浙%', 1, 0 )) makecopyOther "
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
    # data1=get_data(sql_非现入库数)
    # data2=get_data(sql_非现处罚数)
    # data3=get_data(sql_非现当年处罚数)
    # df_数据汇总 = pd.merge(data1, data2, on=['countyCode'],
    #                    how='outer')
    # df_数据汇总 = pd.merge(df_数据汇总, data3, on=['countyCode'],
    #                     how='outer')
    # with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\0906pass4.0.xlsx') as writer1:
    #     data1.to_excel(writer1, sheet_name='sql_非现入库数', index=True)
    #     data2.to_excel(writer1, sheet_name='sql_非现处罚数', index=True)
    #     data3.to_excel(writer1, sheet_name='sql_非现当年处罚数', index=True)
    #     df_数据汇总.to_excel(writer1, sheet_name='df_数据汇总', index=True)
    data1=get_data(sql_非现入库数)
    # data2=get_data(sql_外省抄告数2)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\0911pass3.0.xlsx') as writer1:
        data1.to_excel(writer1, sheet_name='sql_外省抄告数', index=True)
        # data2.to_excel(writer1, sheet_name='sql_非现入库数', index=True)

