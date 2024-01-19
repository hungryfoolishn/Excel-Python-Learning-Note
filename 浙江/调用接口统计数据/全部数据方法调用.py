# coding: utf-8


import time
import pandas as pd
import requests
import base64
import datetime
import pymysql
import json
import csv





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
def get_data_car():
    data = {
    "carNumber": "川K69948"
    }
    url = 'https://lwjc.jtyst.zj.gov.cn:7443/zc-interface/trafficManagmentData/queryYGCar'
    headers = {'content-type': 'application/json'}
    res = requests.post(url, json=data, headers=headers)
    res = json.loads(res.text)
    data = res['data']
    data=pd.DataFrame(data)
    print(data)
    return  data

def 稽查布控数据():
        ##预警数
        预警数 = {
            "tableName": "t_bas_fence_control_warning    ",
            "where": " warning_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' ".format(start_time, end_time),
            "columns": "COUNT(*)  as 预警数"
        }
        # 预警数
        稽查布控数_出动人次1 = {
            "tableName": "t_bas_fence_control_process     ",
            "where": " issue_time BETWEEN '{} 00:00:00' AND '{} 23:59:59'  ".format(start_time, end_time),
            "columns": "COUNT(*) as 稽查布控数_出动人次1   "
        }

        # 预警数
        稽查布控数_出动人次2 = {
            "tableName": "t_bas_fence_control_result      ",
            "where": " intercept_time BETWEEN '{} 00:00:00' AND '{} 23:59:59'  ".format(start_time, end_time),
            "columns": "COUNT(*) as 稽查布控数_出动人次2  "
        }


        布控车辆数1 = {
            "tableName": "t_bas_fence_control_process     ",
            "where": " issue_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' ".format(start_time, end_time),
            "columns": "COUNT(distinct warning_id) as 布控车辆数1  "
        }


        布控车辆数2 = {
            "tableName": "t_bas_fence_control_result ",
            "where": " intercept_time BETWEEN '{} 00:00:00' AND '{} 23:59:59'  ".format(start_time, end_time),
            "columns": "COUNT(distinct car_no) as 布控车辆数2  "
        }

        # 处罚车辆数
        处罚车辆数 = {
            "tableName": "t_bas_fence_control_result a, t_bas_over_data_collection_sign b ",
            "where": "a.car_no = b.car_no AND a.car_no_color = b.car_no_color AND a.intercept_time  < b.insert_time AND a.intercept_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' AND a.county_code = b.area_county ".format(
                start_time, pun_time),
            "columns": "COUNT(*) as 处罚车辆数  "
        }
        data1=get_df_from_db(预警数)
        data2=get_df_from_db(稽查布控数_出动人次1)
        data3=get_df_from_db(稽查布控数_出动人次2)
        data4=get_df_from_db(布控车辆数1)
        data5=get_df_from_db(布控车辆数2)
        data6=get_df_from_db(处罚车辆数)
        data=pd.concat([data1,data2,data3,data4,data5,data6],axis=1)
        data['稽查布控数']=data['稽查布控数_出动人次1']+data['稽查布控数_出动人次2']
        data['布控车辆数'] = data['布控车辆数1'] + data['布控车辆数2']
        return data


def 稽查布控区县数据():
    ##预警数
    预警数 = {
        "tableName": "t_bas_fence_control_process a left join t_bas_fence_control_warning b on a.warning_id =b.id    ",
        "where": " warning_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' and b.county_code =330603 ".format(start_time, end_time),
        "columns": "*"
    }

    ##预警数
    预警数2 = {
        "tableName": "t_bas_fence_control_warning    ",
        "where": " warning_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' and county_code =330603".format(start_time, end_time),
        "columns": "*"
    }
    # # 预警数
    # 稽查布控数_出动人次1 = {
    #     "tableName": "t_bas_fence_control_process     ",
    #     "where": " issue_time BETWEEN '{} 00:00:00' AND '{} 23:59:59'  ".format(start_time, end_time),
    #     "columns": "COUNT(*) as 稽查布控数_出动人次1   "
    # }
    #
    # # 预警数
    # 稽查布控数_出动人次2 = {
    #     "tableName": "t_bas_fence_control_result      ",
    #     "where": " intercept_time BETWEEN '{} 00:00:00' AND '{} 23:59:59'  ".format(start_time, end_time),
    #     "columns": "COUNT(*) as 稽查布控数_出动人次2  "
    # }
    #
    # # 预警数
    # 布控车辆数1 = {
    #     "tableName": "t_bas_fence_control_process     ",
    #     "where": " issue_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' ".format(start_time, end_time),
    #     "columns": "COUNT(distinct warning_id) as 布控车辆数1  "
    # }
    #
    # 预警数
    布控车辆数2 = {
        "tableName": "t_bas_fence_control_result ",
        "where": " intercept_time BETWEEN '{} 00:00:00' AND '{} 23:59:59'".format(start_time, end_time),
        "columns": "* "
    }
    #
    # # 处罚车辆数
    # 处罚车辆数 = {
    #     "tableName": "t_bas_fence_control_result a, t_bas_over_data_collection_sign b ",
    #     "where": "a.car_no = b.car_no AND a.car_no_color = b.car_no_color AND a.intercept_time  < b.insert_time AND a.intercept_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' AND a.county_code = b.area_county ".format(
    #         start_time, pun_time),
    #     "columns": "COUNT(*) as 处罚车辆数  "
    # }
    data1 = get_df_from_db(预警数)
    data2 = get_df_from_db(预警数2)
    # data2 = get_df_from_db(稽查布控数_出动人次1)
    # data3 = get_df_from_db(稽查布控数_出动人次2)
    # data4 = get_df_from_db(布控车辆数1)
    data5 = get_df_from_db(布控车辆数2)
    # data6 = get_df_from_db(处罚车辆数)
    # data = pd.concat([data1, data2, data3, data4, data5, data6], axis=1)
    # data['稽查布控数'] = data['稽查布控数_出动人次1'] + data['稽查布控数_出动人次2']
    # data['布控车辆数'] = data['布控车辆数1'] + data['布控车辆数2']
    return data1,data5,data2


def case_statistic():
    sql_area = {
        "tableName": "t_code_area  ",
        "where": "  is_deleted = 0 and province_code = '330000' ",
        "columns": "city,city_code,county,county_code as area_county"
    }
    t_code_area = get_df_from_db(sql_area)
    sql_非现入库数 = {
        "tableName": "t_bas_over_data_collection_31  ",
        "where": " law_judgment = 1  AND valid_time between '{} 00:00:00'  AND '{} 23:59:59'  and status !=5 GROUP BY area_county  ".format(start_time,end_time),
        "columns": "area_county, count( 1 )  入库数路政, sum(IF( car_no LIKE '%浙%', 1, 0 ))  本省入库数  "
    }

    sql_非现入库数=get_df_from_db(sql_非现入库数)
    sql_非现入库数 = pd.DataFrame(sql_非现入库数,columns=['area_county', '入库数路政', '本省入库数'])

    sql_交通现场查处数 = {
        "tableName": " t_bas_over_data_collection_sign c",
        "where": " c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time between '{} 00:00:00'  and '{} 23:59:59'  GROUP BY c.area_county  ".format(start_time,end_time),
        "columns": "c.area_county  ,count( DISTINCT ( record_id ) ) AS 现场处罚路政 "
    }
    sql_交通现场查处数=get_df_from_db(sql_交通现场查处数)
    sql_交通现场查处数 = pd.DataFrame(sql_交通现场查处数,columns=['area_county', '现场处罚路政'])
    sql_交通现场查处数.loc[sql_交通现场查处数['area_county'] == '331083', 'area_county'] = '331021'
    sql_交通现场查处数.loc[sql_交通现场查处数['area_county'] == '330284', 'area_county'] = '330204'

    sql_非现处罚数处罚 = {
        "tableName": "t_case_sign_result  ",
        "where": " record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND close_case_time between '{} 00:00:00' AND  '{} 23:59:59' AND area_province = '330000' GROUP BY  dept_county".format(start_time,end_time),
        "columns": "dept_county as area_county,count(DISTINCT ( CASE_NUM )) AS 非现场处罚路政处罚,  count(DISTINCT IF( car_no LIKE '%浙%', CASE_NUM, NULL )) AS 非现场处罚本省路政处罚 "
    }
    sql_非现处罚数处罚=get_df_from_db(sql_非现处罚数处罚)
    sql_非现处罚数处罚 = pd.DataFrame(sql_非现处罚数处罚,columns=['area_county', '非现场处罚路政处罚', '非现场处罚本省路政处罚'])
    sql_非现处罚数处罚.loc[sql_非现处罚数处罚['area_county'] == '331083', 'area_county'] = '331021'
    sql_非现处罚数处罚.loc[sql_非现处罚数处罚['area_county'] == '330284', 'area_county'] = '330204'

    sql_非现当年处罚数处罚 = {
        "tableName": "t_case_sign_result c LEFT JOIN t_bas_over_data_collection_31 b ON c.record_code = b.record_code  ",
        "where": " record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND close_case_time between '{} 00:00:00' AND  '{} 23:59:59' AND c.area_province = '330000' and  b.valid_time between '{} 00:00:00' and  '{} 23:59:59' GROUP BY  c.dept_county".format(start_time,end_time,start_time,end_time),
        "columns": "c.dept_county as  area_county,count(DISTINCT ( c.CASE_NUM )) AS 非现场处罚路政处罚当年, count(DISTINCT IF( c.car_no LIKE '%浙%', c.CASE_NUM, NULL )) AS 非现场处罚本省路政处罚当年  "
    }
    sql_非现当年处罚数处罚=get_df_from_db(sql_非现当年处罚数处罚)
    sql_非现当年处罚数处罚 = pd.DataFrame(sql_非现当年处罚数处罚,columns=['area_county', '非现场处罚路政处罚当年', '非现场处罚本省路政处罚当年'])
    sql_非现当年处罚数处罚.loc[sql_非现当年处罚数处罚['area_county'] == '331083', 'area_county'] = '331021'
    sql_非现当年处罚数处罚.loc[sql_非现当年处罚数处罚['area_county'] == '330284', 'area_county'] = '330204'

    sql_交警现场 = {
        "tableName": "   t_bas_police_road_site   ",
        "where": "   punish_time between '{} 00:00:00' AND  '{} 23:59:59'  and case_status=2  GROUP BY area_county  ".format(start_time,end_time),
        "columns": "area_county,count(DISTINCT case_number) as 交警现场查处数"
    }
    sql_交警现场=get_df_from_db(sql_交警现场)
    sql_交警现场 = pd.DataFrame(sql_交警现场,columns=['area_county', '交警现场查处数'])



    sql_外省抄告数 = {
        "tableName": "t_bas_over_data_collection_31  ",
        "where": " law_judgment = 1  AND valid_time between '{} 00:00:00'  AND '{} 23:59:59'  and status !=5 GROUP BY area_county  ".format(start_time,end_time),
        "columns": "area_county ,sum(IF( make_copy=1 and car_no not LIKE '%浙%', 1, 0 )) 外省抄告 "
    }
    sql_外省抄告数 =get_df_from_db(sql_外省抄告数)
    sql_外省抄告数 = pd.DataFrame(sql_外省抄告数,columns=['area_county', '外省抄告'])



    sql_非现处罚数案发= {
        "tableName": "t_case_sign_result ",
        "where": " record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND close_case_time between '{} 00:00:00' AND  '{} 23:59:59' AND area_province = '330000' GROUP BY  area_county".format(start_time,end_time),
        "columns": "area_county,count(DISTINCT ( CASE_NUM )) AS 非现场处罚路政案发,  count(DISTINCT IF( car_no LIKE '%浙%', CASE_NUM, NULL )) AS 非现场处罚本省路政案发 "
    }
    sql_非现处罚数案发=get_df_from_db(sql_非现处罚数案发)
    sql_非现处罚数案发 = pd.DataFrame(sql_非现处罚数案发,columns=['area_county', '非现场处罚路政案发', '非现场处罚本省路政案发'])


    sql_非现当年处罚数案发 = {
        "tableName": "t_case_sign_result c LEFT JOIN t_bas_over_data_collection_31 b ON c.record_code = b.record_code  ",
        "where": " record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND close_case_time between '{} 00:00:00' AND  '{} 23:59:59' AND c.area_province = '330000' and  b.valid_time between '{} 00:00:00' and  '{} 23:59:59' GROUP BY  c.area_county".format(start_time,end_time,start_time,end_time),
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

def 高速明细数据(city_code):
    ##杭州高速数据分析
    sql_杭州高速数据分析 = {
        "tableName": "t_bas_over_data_41 a  left join t_sys_station s on a.out_station = s.station_code  ",
        "where": " (a.total_weight -a.limit_weight*1.05)>0 and a.allow is null and a.direction = 0 and a.out_station_time between '{} 00:00:00' and '{} 23:59:59' and a.area_city = {} ".format(start_time, end_time,city_code),
        "columns": " a.record_code as 流水号,s.road_name as 线路名称,a.out_station as 站点编码,s.station_name as 站点名称, a.car_no as 车牌号码,a.out_station_time as 检测时间,a.total_weight as 总重,a.axis as 轴数, a.limit_weight as 限重,a.overrun as 超限,a.overrun_rate as 超限率,a.link_man as 联系人, a.phone as 联系电话,a.car_holder_addr as 联系地址"
    }



    data1 = get_df_from_db(sql_杭州高速数据分析)


    with pd.ExcelWriter(r'F:\Learn\工作\数据明细\杭州高速10月数据分析.xlsx') as writer1:
        data1.to_excel(writer1, sheet_name='高速', index=False)

def 数字路政():
        ##预警数
        t_bas_pass_statistics_data = {
            "tableName": "t_bas_pass_statistics_data    ",
            "where": " statistics_date >='2023-01' ",
            "columns": "*"
        }
        # 预警数
        t_bas_station_statistics_data = {
            "tableName": "t_bas_station_statistics_data     ",
            "where": " statistics_date >='2023-01' ",
            "columns": "*"
        }


        t_bas_pass_statistics_data = get_df_from_db(t_bas_pass_statistics_data)
        t_bas_station_statistics_data = get_df_from_db(t_bas_station_statistics_data)
        return t_bas_pass_statistics_data,t_bas_station_statistics_data


def 台州双70数据():
    ##台州70数
    sql_台州70明细吨 = {
        "tableName": "t_bas_over_data_31 a  left join t_code_area b on a.area_county = b.county_code",
        "where": "  out_station_time between '{} 00:00' and '{} 23:59:59' and allow is  null and city ='台州' and is_unusual=0 and a.total_weight between 70 and 300".format(start_time, end_time),
        "columns": "*"
    }

    ##台州70数
    sql_台州70明细率 = {
        "tableName": "t_bas_over_data_31 a  left join t_code_area b on a.area_county = b.county_code",
        "where": "  out_station_time between '{} 00:00' and '{} 23:59:59' and allow is  null and city ='台州' and is_unusual=0 and a.overrun_rate between 70 and 1500 ".format(start_time, end_time),
        "columns": "* "
    }



    data1 = get_df_from_db(sql_台州70明细吨)
    data2 = get_df_from_db(sql_台州70明细率)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\台州双70数据截止{}.xlsx'.format(end_time)) as writer1:
        data1.to_excel(writer1, sheet_name='sql_台州70明细吨', index=True)
        data2.to_excel(writer1, sheet_name='sql_台州70明细率', index=True)

def 双百处罚明细(city):
    双百入库明细 = {
        "tableName": "t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
        "where": "  a.law_judgment=1 and b.city='{}' and a.valid_time >='{} 00:00:00' and a.valid_time <'{} 23:59:59'  and a.overrun_rate>=100 and a.status !=5   ".format(
            city,start_time, end_time),
        "columns": "b.city 地市,b.county  区县,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
                   "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
    }

    ##80吨以上非现查处数明细
    双百处罚明细 = {
        "tableName": "t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
        "where": "  a.law_judgment=1 and b.city='{}' and c.close_case_time >='{} 00:00:00' and c.close_case_time <'{} 23:59:59'  and a.overrun_rate>=100  ".format(
           city, start_time, end_time),
        "columns": "b.city 地市,b.county  区县,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
                   "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
    }

    data2=get_df_from_db(双百入库明细)
    data2 = pd.DataFrame(data2,
                           columns=['地市', '区县', '车籍地', '车牌号', '所属运输企业名称','联系电话','检测时间', '入库时间', '检测站点', '总重', '轴数','超限率', '案件状态', '外省抄告', '结案时间', '处罚金额','record_code'])
    # data3=get_data(sql_交警现场)
    data3 = get_df_from_db(双百处罚明细)
    data3 = pd.DataFrame(data3,
                         columns=['地市', '区县', '车籍地', '车牌号', '所属运输企业名称', '联系电话', '检测时间', '入库时间', '检测站点', '总重', '轴数',
                                  '超限率', '案件状态', '外省抄告', '结案时间', '处罚金额', 'record_code'])

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\{}截止20230731双百入库处罚明细2.xlsx'.format(city)) as writer1:
        # data1.to_excel(writer1, sheet_name='sql_80吨以上交通现场', index=True)
        data2.to_excel(writer1, sheet_name='双百入库明细', index=True)
        data3.to_excel(writer1, sheet_name='双百处罚明细', index=True)
        # data3.to_excel(writer1, sheet_name='sql_80吨以上交警现场', index=True)

def 单月80吨以上入库明细(city):
    sql_80吨以上非现查处数明细 = {
        "tableName": "t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
        "where": "  a.law_judgment=1 and b.city='{}' and a.valid_time >='{} 00:00:00' and a.valid_time <'{} 23:59:59'  and a.total_weight>80 and a.status !=5   ".format(
            city,start_time, end_time),
        "columns": "b.city 地市,b.county  区县,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
                   "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
    }
    data2=get_df_from_db(sql_80吨以上非现查处数明细)
    data2 = pd.DataFrame(data2,
                           columns=['地市', '区县', '车籍地', '车牌号', '所属运输企业名称','联系电话','检测时间', '入库时间', '检测站点', '总重', '轴数','超限率', '案件状态', '外省抄告', '结案时间', '处罚金额','record_code'])
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\{}截止{}入库明细80吨以上.xlsx'.format(city,end_time)) as writer1:
        data2.to_excel(writer1, sheet_name='80吨以上', index=True)


def 通报数据汇总(sheet_name):
    df_数据汇总 = pd.read_excel(r'G:\智诚\2023日常给出数据\汇总数据\省通报全部月汇总.xlsx', sheet_name='{}'.format(sheet_name))

    df_数据汇总地市 = df_数据汇总.groupby(['地市编码', '地市', '区县编码', '区县']).sum().reset_index()
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\{}.xlsx'.format(sheet_name)) as writer1:
        df_数据汇总地市.to_excel(writer1, sheet_name='地市', index=True)

def 站点明细数据(city):

    sql_station = {
        "tableName": "t_sys_station a left join t_code_area b on a.area_county =b.county_code  ",
        "where": "  a.is_deleted = 0 and a.station_type in (31)   and b.city='{}' ".format(city),
        "columns": "b.county 区县,a.station_name 站点名称,a.station_status 站点状态,a.lanes_count"
    }


    data1 = get_df_from_db(sql_station)

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\{}站点明细数据.xlsx'.format(city)) as writer1:
        data1.to_excel(writer1, sheet_name='区县全部', index=True)



def 重点数据处罚数():
    ##80吨以上非现查处数
    sql_80吨以上非现查处数 = {
        "tableName": "t_case_sign_result c  left join t_code_area  b on c.dept_county=b.county_code LEFT JOIN  t_bas_over_data_collection_31  a  on a.record_code =c.record_code  ",
        "where": "  a.law_judgment=1 and c.close_case_time>='{} 00:00:00' and c.close_case_time<'{} 23:59:59' and total_weight >80  GROUP by b.city ,c.dept_county,b.county ".format(
            start_time, end_time),
        "columns": "c.dept_county as area_county,count(DISTINCT case_num) 交通非现80吨以上查处数 "
    }

    ##80吨以上非现查处数
    sql_超限100非现查处数 = {
        "tableName": "t_case_sign_result c  left join t_code_area  b on c.dept_county=b.county_code LEFT JOIN  t_bas_over_data_collection_31  a  on a.record_code =c.record_code  ",
        "where": "  a.law_judgment=1 and c.close_case_time>='{} 00:00:00' and c.close_case_time<'{} 23:59:59' and  total_weight <=80  and overrun_rate>=100 GROUP by b.city , c.dept_county,b.county ".format(
            start_time, end_time),
        "columns": "c.dept_county as area_county,count(DISTINCT case_num) as 交通非现超限100查处数 "
    }

    ##--交通现场查处数

    sql_80吨以上交通现场查处数 = {
        "tableName": " t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  LEFT JOIN t_bas_over_data_collection_sign d on d.record_code=c.record_code",
        "where": " c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time >= '{} 00:00:00' AND c.close_case_time <= '{} 23:59:59' and a.city='绍兴'and total_weight >80 GROUP BY a.city ,c.area_county,a.county  ".format(
            start_time, end_time),
        "columns": " c.area_county, count( DISTINCT ( CASE_NUM ) ) AS 交通现场80吨以上查处数 "
    }

    sql_超限100交通现场查处数 = {
        "tableName": " t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  LEFT JOIN t_bas_over_data_collection_sign d on d.record_code=c.record_code",
        "where": " c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time >= '{} 00:00:00' AND c.close_case_time <= '{} 23:59:59'   and  total_weight <=80  and overrun_rate>=100 GROUP BY a.city ,c.area_county,a.county   ".format(
            start_time, end_time),
        "columns": "c.area_county,  count( DISTINCT ( CASE_NUM ) ) AS 交通现场超限100查处数 "
    }

    ##--交警现场
    sql_80吨以上交警现场 = {
        "tableName": "   t_bas_police_road_site a LEFT JOIN t_code_area b ON a.area_county = b.county_code   ",
        "where": "   a.punish_time >= '{} 00:00:00' AND a.punish_time < '{} 23:59:59'  and case_status=2 and total_weight>80000 GROUP BY  b.city ,a.area_county,b.county ".format(
            start_time, end_time),
        "columns": "a.area_county, count(DISTINCT case_number) as 交警现场80吨以上查处数 "
    }

    sql_超限100交警现场 = {
        "tableName": "   t_bas_police_road_site a LEFT JOIN t_code_area b ON a.area_county = b.county_code   ",
        "where": "   a.punish_time >= '{} 00:00:00' AND a.punish_time < '{} 23:59:59'  and case_status=2 and total_weight<=80000 and overrun_rate>=100 GROUP BY b.city ,a.area_county,b.county ".format(
            start_time, end_time),
        "columns": "a.area_county,count(DISTINCT case_number) as 交警现场超限100查处数 "
    }
    data1=get_df_from_db(sql_80吨以上非现查处数)
    data2=get_df_from_db(sql_超限100非现查处数)
    data3=get_df_from_db(sql_80吨以上交通现场查处数)
    data3 = pd.DataFrame(data3, columns=['area_county', '交通现场80吨以上查处数'])
    data4=get_df_from_db(sql_超限100交通现场查处数)
    data4 = pd.DataFrame(data4, columns=['area_county', '交通现场超限100查处数'])
    data1.loc[data1['area_county'] == '331083', 'area_county'] = '331021'
    data1.loc[data1['area_county'] == '330284', 'area_county'] = '330201'
    data2.loc[data2['area_county'] == '331083', 'area_county'] = '331021'
    data2.loc[data2['area_county'] == '330284', 'area_county'] = '330201'
    data3.loc[data3['area_county'] == '331083', 'area_county'] = '331021'
    data3.loc[data3['area_county'] == '330284', 'area_county'] = '330201'
    data4.loc[data4['area_county'] == '331083', 'area_county'] = '331021'
    data4.loc[data4['area_county'] == '330284', 'area_county'] = '330201'
    data5=get_df_from_db(sql_80吨以上交警现场)
    data6=get_df_from_db(sql_超限100交警现场)
    data6 = pd.DataFrame(data6, columns=['area_county', '交警现场超限100查处数'])
    data=pd.merge(data1,data2,how='outer',on=['area_county'])
    data=pd.merge(data,data3,how='outer',on=['area_county'])
    data=pd.merge(data,data4,how='outer',on=['area_county'])
    data=pd.merge(data,data5,how='outer',on=['area_county'])
    data=pd.merge(data,data6,how='outer',on=['area_county'])
    sql_area = {
        "tableName": "t_code_area  ",
        "where": "  is_deleted = 0 and province_code = '330000' ",
        "columns": "city 地市,city_code,county 区县,county_code as area_county"
    }
    t_code_area = get_df_from_db(sql_area)
    data = pd.merge(data, t_code_area, how='left', on=['area_county'])
    data = pd.DataFrame(data,
                         columns=[ '地市','city_code', '区县','area_county', '交通非现80吨以上查处数', '交通非现超限100查处数', '交通现场80吨以上查处数', '交通现场超限100查处数', '交警现场80吨以上查处数', '交警现场超限100查处数'])
    data=data.fillna(0)
    data = data.sort_values(by=['area_county'],
                                    ascending=True).reset_index(drop=True)
    data地市=data.groupby(['city_code','地市']).sum().reset_index()

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\重点处罚数据\浙江省{}_{}重点超限数据查处数.xlsx'.format(start_time,end_time)) as writer1:
        data.to_excel(writer1, sheet_name='区县全部', index=True)
        data地市.to_excel(writer1, sheet_name='地市', index=True)


def case_statistic():
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
        "columns": "a.area_county as area_code,a.car_no ,a.total_weight,a.overrun_rate,a.status ,a.law_judgment ,c.punish_money ,a.xh_count ,a.dx_count a.record_code"
    }
    data2=get_df_from_db(入库明细)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\data2.xlsx') as writer1:
        # data1.to_excel(writer1, sheet_name='sql_80吨以上交通现场', index=True)
        data2.to_excel(writer1, sheet_name='双百入库明细', index=True)

def 试点县站点明细数据():
    在线 = pd.read_excel(r'G:\智诚\2023日常给出数据\省通报\202309\9月通报数据1012v4.0.xlsx', sheet_name='在线')
    报修 = pd.read_excel(r'G:\智诚\2023日常给出数据\省通报\202309\9月通报数据1012v4.0.xlsx', sheet_name='报修')
    总重80_90以上明细 = pd.read_excel(r'G:\智诚\2023日常给出数据\省通报\202309\9月通报数据1012v4.0.xlsx', sheet_name='总重80_90以上明细')
    超限100明细 = pd.read_excel(r'G:\智诚\2023日常给出数据\省通报\202309\9月通报数据1012v4.0.xlsx', sheet_name='超限100明细')
    源头站点数据明细 = pd.read_excel(r'G:\智诚\2023日常给出数据\省通报\202309\9月通报数据1012v4.0.xlsx', sheet_name='源头站点数据明细')
    试点县=['萧山','鄞州','泰顺','平湖','长兴','柯桥','兰溪','开化','新城','温岭','松阳']
    试点县在线 = 在线[在线['区县'].isin(试点县)]
    试点县报修= 报修[报修['区县'].isin(试点县)]
    试点县80=总重80_90以上明细[总重80_90以上明细['区县'].isin(试点县)]
    试点县超限100明细 = 超限100明细[超限100明细['区县'].isin(试点县)]
    试点县源头站点数据明细 = 源头站点数据明细[源头站点数据明细['county'].isin(试点县)]

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\试点县9月明细数据.xlsx') as writer1:
        试点县在线.to_excel(writer1, sheet_name='非现在线', index=True)
        试点县报修.to_excel(writer1, sheet_name='非现报修', index=True)
        试点县80.to_excel(writer1, sheet_name='80吨以上', index=True)
        试点县超限100明细.to_excel(writer1, sheet_name='超限100明细', index=True)
        试点县源头站点数据明细.to_excel(writer1, sheet_name='源头站点数据明细', index=True)

def 绍兴遮牌():
    sql_绍兴遮牌 = {
        "tableName": "  t_bas_over_data_31 a LEFT JOIN t_code_area b ON a.area_county = b.county_code",
        "where": "a.out_station_time between '{} 00:00:00' and '{} 23:59:59' and ( a.total_weight - a.limit_weight * 1.2 )> 0  and a.is_unusual = 0  and a.allow is null and b.city='绍兴' GROUP BY a.area_county,b.county".format(
            start_time, end_time),
        "columns": "b.city,b.county as 区县,count(1) as 超限总数 ,sum(IF( a.car_no like '%牌%', 1, 0 )) as 无牌数 "
    }


    data1 = get_df_from_db(sql_绍兴遮牌)

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\绍兴遮牌.xlsx') as writer1:
        data1.to_excel(writer1, sheet_name='区县全部', index=True)

def 总重80吨以上明细():
    ##合规率
    t_sys_station = {
        "tableName": "t_sys_station ",
        "where": " station_status=0 and station_type =31 and is_deleted= 0 ",
        "columns": "station_name,station_code,station_status,station_type,area_county  "
    }
    t_sys_station = get_df_from_db(t_sys_station)


    sql = {
        "tableName": "t_bas_over_data_31 a left join t_bas_over_data_collection_31 b on a.record_code=b.record_code left join t_code_area c on a.area_county=c.county_code ",
        "where": "a.out_station_time between '{} 00:00:00' and '{} 23:59:59' and a.total_weight>84   AND a.is_unusual = 0  and a.allow is null ".format(start_time, end_time),
        "columns": "c.city 地市,c.county 区县, a.area_county,a.out_station,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号码,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,b.status 案件状态,a.site_name 站点名称,a.is_collect 证据满足,b.law_judgment 判定需处罚,b.make_copy  外省抄告,a.link_man 所属运输企业名称,a.phone 联系电话, b.vehicle_county 车籍地,a.record_code 流水号"
    }
    t_bas_over_data_31_80 = get_df_from_db(sql)


    df_big = pd.DataFrame(t_bas_over_data_31_80,
                          columns=["地市", "区县", "站点名称", "检测时间", "车牌号码", "总重", "限重",
                                   "超重", "轴数", "超限率", "证据满足", "所属运输企业名称", "联系电话", "车籍地", "流水号", "入库时间", "案件状态", "判定需处罚",
                                   "外省抄告", "area_county"])
    总重80吨以上明细 = df_big.sort_values(by=['area_county'],
                                   ascending=True).reset_index(drop=True)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\截止1024总重80吨以上明细.xlsx') as writer1:
        总重80吨以上明细.to_excel(writer1, sheet_name='明细', index=False)

def 稽查布控数据明细():
        ##预警数
        预警数 = {
            "tableName": "t_bas_fence_control_process a left join t_bas_fence_control_warning b on a.warning_id =b.id    ",
            "where": " warning_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' ".format(start_time,end_time),
            "columns": "*"
        }
        # 预警数
        稽查布控数_出动人次1 = {
            "tableName": "t_bas_fence_control_process     ",
            "where": " issue_time BETWEEN '{} 00:00:00' AND '{} 23:59:59'  ".format(start_time, end_time),
            "columns": "*  "
        }

        # 预警数
        稽查布控数_出动人次2 = {
            "tableName": "t_bas_fence_control_result      ",
            "where": " intercept_time BETWEEN '{} 00:00:00' AND '{} 23:59:59'  ".format(start_time, end_time),
            "columns": "* "
        }
        # 处罚车辆数
        处罚车辆数 = {
            "tableName": "t_bas_fence_control_result a left join  t_bas_over_data_collection_sign b on a.car_no = b.car_no left join t_case_sign_result c on b.record_code = c.record_code",
            "where": " a.intercept_time  < b.insert_time  AND a.car_no_color =b.car_no_color  AND a.intercept_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' AND a.county_code = b.area_county ".format(start_time, end_time),
            "columns": "*"
        }

        data3 = get_df_from_db(预警数)
        data1 = get_df_from_db(稽查布控数_出动人次1)
        data2 = get_df_from_db(稽查布控数_出动人次2)
        data6 = get_df_from_db(处罚车辆数)


        with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\稽查布控数据明细3.xlsx') as writer1:
          data1.to_excel(writer1, sheet_name='稽查布控1', index=False)
          data2.to_excel(writer1, sheet_name='稽查布控2', index=False)
          data3.to_excel(writer1, sheet_name='data3', index=False)
          data6.to_excel(writer1, sheet_name='处罚车辆', index=False)

def 车籍地单月80吨以上入库明细(city):
    sql_80吨以上非现查处数明细 = {
        "tableName": "t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
        "where": "  a.law_judgment=1 and b.city='{}' and a.valid_time >='{} 00:00:00' and a.valid_time <'{} 23:59:59'   and a.status !=5   ".format(
            city,start_time, end_time),
        "columns": "b.city 地市,b.county  车籍地,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
                   "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.area_county 案发地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
    }
    data2=get_df_from_db(sql_80吨以上非现查处数明细)
    data2 = pd.DataFrame(data2,
                           columns=['地市','车籍地','案发地', '车牌号', '所属运输企业名称','联系电话','检测时间', '入库时间', '检测站点', '总重', '轴数','超限率', '案件状态', '外省抄告', '结案时间', '处罚金额','record_code'])
    with pd.ExcelWriter(r'F:\Learn\工作\临时表\{}截止{}判定需处罚明细.xlsx'.format(city,end_time)) as writer1:
        data2.to_excel(writer1, sheet_name='判定需处罚明细', index=True)



def 全省站点明细数据():

    sql_station = {
        "tableName": "t_sys_station a left join t_code_area b on a.area_county =b.county_code  ",
        "where": "  a.is_deleted = 0 and a.station_type in (31)   ",
        "columns": " b.city 地市,b.county 区县,a.station_name 站点名称,a.station_status 站点状态,a.lanes_count"
    }


    data1 = get_df_from_db(sql_station)
    print(data1)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\全省站点明细数据.xlsx') as writer1:
        data1.to_excel(writer1, sheet_name='区县全部', index=True)

def Compliance_rate():
    sql = {
        "tableName": "t_bas_over_data_collection_31 ",
        "where": "out_station_time between '{} 00:00:00' and  '{} 00:00:00' and total_weight>80 and law_judgment=1 and area_county =330601 ".format(start_time, end_time),
        "columns": "*"
    }
    t_bas_over_data_collection_31 = get_df_from_db(sql)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\Compliance_rate.xlsx') as writer1:
        t_bas_over_data_collection_31.to_excel(writer1, sheet_name='区县全部', index=True)


def 桐庐总重80吨以上明细():
    ##合规率
    t_sys_station = {
        "tableName": "t_sys_station ",
        "where": " station_status=0 and station_type =31 and is_deleted= 0 ",
        "columns": "station_name,station_code,station_status,station_type,area_county  "
    }
    t_sys_station = get_df_from_db(t_sys_station)


    sql = {
        "tableName": "t_bas_over_data_31 a left join t_bas_over_data_collection_31 b on a.record_code=b.record_code left join t_code_area c on a.area_county=c.county_code ",
        "where": "a.out_station_time between '{} 00:00:00' and '{} 23:59:59' and a.total_weight>80   AND a.is_unusual = 0  and a.allow is null and a.site_name in () ".format(start_time, end_time),
        "columns": "c.city 地市,c.county 区县, a.area_county,a.out_station,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号码,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,b.status 案件状态,a.site_name 站点名称,a.is_collect 证据满足,b.law_judgment 判定需处罚,b.make_copy  外省抄告,a.link_man 所属运输企业名称,a.phone 联系电话, b.vehicle_county 车籍地,a.record_code 流水号"
    }
    t_bas_over_data_31_80 = get_df_from_db(sql)


    df_big = pd.DataFrame(t_bas_over_data_31_80,
                          columns=["地市", "区县", "站点名称", "检测时间", "车牌号码", "总重", "限重",
                                   "超重", "轴数", "超限率", "证据满足", "所属运输企业名称", "联系电话", "车籍地", "流水号", "入库时间", "案件状态", "判定需处罚",
                                   "外省抄告", "area_county"])
    总重80吨以上明细 = df_big.sort_values(by=['area_county'],
                                   ascending=True).reset_index(drop=True)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\截止1027总重80吨以上明细v2.0.xlsx') as writer1:
        总重80吨以上明细.to_excel(writer1, sheet_name='明细', index=False)


def 桐庐源头总重80吨以上明细():
    ##合规率
    t_sys_station = {
        "tableName": "t_sys_station ",
        "where": " station_status=0 and station_type =31 and is_deleted= 0 ",
        "columns": "station_name,station_code,station_status,station_type,area_county  "
    }
    t_sys_station = get_df_from_db(t_sys_station)


    sql = {
        "tableName": "t_bas_pass_data_71  ",
        "where": "car_no in ('川K69948','浙A3Y010','浙A78008','辽D22359','沪CJ3699','浙A5W833','浙A5W833','云A5W833','云A5W833','蒙DL6705','浙A5W833','云ADT123','浙A5W833','浙A5W833','皖S1B276') ",
        "columns": "*"
    }
    t_bas_over_data_31_80 = get_df_from_db(sql)


    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\t_bas_over_data_31_80.0.xlsx') as writer1:
        t_bas_over_data_31_80.to_excel(writer1, sheet_name='明细', index=False)


def 湖州月报():
    ##合规率
    t_sys_station = {
        "tableName": "t_bas_over_data_collection_31 a  left join t_code_area b on a.area_county = b.county_code ",
        "where": " out_station_time between '{} 00:00:00' and '{} 23:59:59'  and b.city ='湖州' GROUP BY  b.county".format(start_time,end_time),
        "columns": " b.county,sum(case when a.total_weight>=100 and a.status !=15 then 1 else 0 end) as 初审百吨王,sum(case when a.overrun_rate>=100 and a.total_weight<100 and a.status !=15 then 1 else 0 end) as 初审超限100数,sum(case when a.overrun_rate<100  and a.status !=15 then 1 else 0 end) as 初审其他超限数,sum(case when  a.status =15 then 1 else 0 end) as 初审不通过数,sum(case when a.total_weight>=100 and a.status in (4,5,6,12,13) then 1 else 0 end) as 二审百吨王,sum(case when a.overrun_rate>=100 and a.total_weight<100 and a.status in (4,5,6,12,13) then 1 else 0 end) as 二审超限100数,sum(case when a.overrun_rate<100  and a.status in (4,5,6,12,13) then 1 else 0 end) as 二审其他超限数,sum(case when  a.status =9 then 1 else 0 end) as 二审不通过数,sum(case when  a.dx_count >0 then 1 else 0 end) as 短信数,sum(case when  a.gs_count >0 then 1 else 0 end) as 公示数  "
    }
    t_sys_station = get_df_from_db(t_sys_station)




    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\湖州交通月报.0.xlsx') as writer1:
        t_sys_station.to_excel(writer1, sheet_name='明细', index=False)


def 案件查处明细():

    非现查处数明细 = {
        "tableName": "t_bas_over_data_collection_31  a left join t_code_area  b on a.vehicle_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
        "where": "  b.city='舟山' and c.close_case_time between '{} 00:00:00' and '{} 23:59:59'  ".format(
            start_time, end_time),
        "columns": "b.city 地市,b.county  区县,a.area_county 案发地,c.dept_county 处罚地,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
    }
    data2=get_df_from_db(非现查处数明细)
    data2 = pd.DataFrame(data2,
                           columns=['地市', '区县', '车籍地', '案发地','处罚地','车牌号', '所属运输企业名称','联系电话','检测时间', '入库时间', '检测站点', '总重', '轴数','超限率', '案件状态', '外省抄告', '结案时间', '处罚金额','record_code'])
    # data3=get_data(sql_交警现场)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\杭州截止20231030案件查处明细.xlsx') as writer1:
        # data1.to_excel(writer1, sheet_name='sql_80吨以上交通现场', index=True)
        data2.to_excel(writer1, sheet_name='明细', index=True)
        # data3.to_excel(writer1, sheet_name='sql_80吨以上交警现场', index=True)

def 案件未处罚明细():

    非现查处数明细 = {
        "tableName": "t_bas_over_data_collection_31  a left join t_code_area  b on a.vehicle_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
        "where": "  b.city='舟山' and a.valid_time between '{} 00:00:00' and '{} 23:59:59' and a.status in (4,6,12)  ".format(
            start_time, end_time),
        "columns": "b.city 地市,b.county  区县,a.area_county 案发地,c.dept_county 处罚地,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
    }
    data2=get_df_from_db(非现查处数明细)
    data2 = pd.DataFrame(data2,
                           columns=['地市', '区县', '车籍地', '案发地','处罚地','车牌号', '所属运输企业名称','联系电话','检测时间', '入库时间', '检测站点', '总重', '轴数','超限率', '案件状态', '外省抄告', '结案时间', '处罚金额','record_code'])
    # data3=get_data(sql_交警现场)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\舟山截止20231030案件查处明细.xlsx') as writer1:
        # data1.to_excel(writer1, sheet_name='sql_80吨以上交通现场', index=True)
        data2.to_excel(writer1, sheet_name='明细', index=True)
        # data3.to_excel(writer1, sheet_name='sql_80吨以上交警现场', index=True)

def t_bas_basic_data_pass():

    sql = {
        "tableName": " t_bas_basic_data_pass ",
        "where": "statistic_date='2023-10-29' ",
        "columns": " * "
    }
    t_bas_over_data_31_80 = get_df_from_db(sql)

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\t_bas_over_data_31_80.xlsx') as writer1:
        t_bas_over_data_31_80.to_excel(writer1, sheet_name='明细', index=False)


def 杭州1128():
    ##合规率
    t_sys_station = {
        "tableName": "t_bas_over_data_31 a  left join t_code_area b on a.area_county = b.county_code left join  ",
        "where": " car_no = '皖CE2862' ",
        "columns": "* "
    }
    t_sys_station = get_df_from_db(t_sys_station)
    with pd.ExcelWriter(r'F:\Learn\工作\临时表\杭州截止20231030案件查处明细.xlsx') as writer1:
        t_sys_station.to_excel(writer1, sheet_name='sql_80吨以上交通现场', index=True)

def 稽查布控区县数据():
    ##预警数
    # 预警数 = {
    #     "tableName": "t_bas_fence_control_process a left join t_bas_fence_control_warning b on a.warning_id =b.id    ",
    #     "where": " warning_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' and b.city_code =330100 ".format(start_time, end_time),
    #     "columns": "*"
    # }
    #
    # ##预警数
    # 预警数2 = {
    #     "tableName": "t_bas_fence_control_warning    ",
    #     "where": " warning_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' and city_code =330100".format(start_time, end_time),
    #     "columns": "*"
    # }
    # # 预警数
    # 稽查布控数_出动人次1 = {
    #     "tableName": "t_bas_fence_control_process     ",
    #     "where": " issue_time BETWEEN '{} 00:00:00' AND '{} 23:59:59'  ".format(start_time, end_time),
    #     "columns": "COUNT(*) as 稽查布控数_出动人次1   "
    # }
    #
    # # 预警数
    # 稽查布控数_出动人次2 = {
    #     "tableName": "t_bas_fence_control_result      ",
    #     "where": " intercept_time BETWEEN '{} 00:00:00' AND '{} 23:59:59'  ".format(start_time, end_time),
    #     "columns": "COUNT(*) as 稽查布控数_出动人次2  "
    # }
    #
    # # 预警数
    布控车辆数1 = {
        "tableName": "t_bas_fence_control_process a left join t_bas_fence_control_warning b on a.warning_id =b.id ",
        "where": " issue_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' and b.city_code =330100 ".format(start_time, end_time),
        "columns": "COUNT(distinct warning_id) as 布控车辆数1  "
    }

    # 预警数
    布控车辆数2 = {
        "tableName": "t_bas_fence_control_result a left join t_bas_fence_control_warning b on a.warning_id =b.id ",
        "where": " intercept_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' and b.city_code =330100".format(start_time, end_time),
        "columns": "* "
    }
    #
    # 处罚车辆数
    处罚车辆数 = {
        "tableName": "t_bas_fence_control_result a, t_bas_over_data_collection_sign b ",
        "where": "a.car_no = b.car_no AND a.car_no_color = b.car_no_color AND a.intercept_time  < b.insert_time AND a.intercept_time BETWEEN '{} 00:00:00' AND '{} 23:59:59' AND a.county_code = b.area_county and b.area_city =330100".format(
            start_time, pun_time),
        "columns": "COUNT(*) as 处罚车辆数  "
    }
    data1 = get_df_from_db(布控车辆数1)
    data2 = get_df_from_db(处罚车辆数)
    # data2 = get_df_from_db(稽查布控数_出动人次1)
    # data3 = get_df_from_db(稽查布控数_出动人次2)
    # data4 = get_df_from_db(布控车辆数1)
    data5 = get_df_from_db(布控车辆数2)
    # data6 = get_df_from_db(处罚车辆数)
    # data = pd.concat([data1, data2, data3, data4, data5, data6], axis=1)
    # data['稽查布控数'] = data['稽查布控数_出动人次1'] + data['稽查布控数_出动人次2']
    # data['布控车辆数'] = data['布控车辆数1'] + data['布控车辆数2']
    with pd.ExcelWriter(r'F:\Learn\工作\临时表\杭州截止20231030案件查处明细2.xlsx') as writer1:
        data1.to_excel(writer1, sheet_name='data1', index=True)
        data5.to_excel(writer1, sheet_name='data5', index=True)
        data2.to_excel(writer1, sheet_name='data2', index=True)
    return data1,data5,data2

def maintain_station():
    ##报修站点
    maintain_station = {
        "tableName": " t_sys_station_maintain a inner join t_sys_station b on a.station_code=b.station_code    "
            ,
        "where": "   b.station_code='3308223101' ",
        "columns": "* "
    }
    maintain_station = get_df_from_db(maintain_station)
    with pd.ExcelWriter(r'F:\Learn\工作\临时表\maintain_station.xlsx') as writer1:
        maintain_station.to_excel(writer1, sheet_name='判定需处罚明细', index=True)



def maintain_station():
    ##报修站点
    maintain_station = {
        "tableName": "t_sys_station b    "
            ,
        "where": "   b.station_name='招宝山大桥北侧连接线兴中路K1+400' ",
        "columns": "* "
    }
    maintain_station = get_df_from_db(maintain_station)
    with pd.ExcelWriter(r'F:\Learn\工作\临时表\maintain_station.xlsx') as writer1:
        maintain_station.to_excel(writer1, sheet_name='判定需处罚明细', index=True)
if __name__ == "__main__":
    # 稽查布控数据明细()
    # for start_time,end_time in ['2023-01-01','2023-01-31'],['2023-02-01','2023-02-28'],['2023-03-01','2023-03-31'],['2023-04-01','2023-04-30'],\
    #                            ['2023-05-01','2023-05-31'],['2023-06-01','2023-06-30'],['2023-07-01','2023-07-31'],['2023-08-01','2023-08-31'],\
    #                            ['2023-09-01','2023-09-30'],['2023-01-01','2023-09-30']:
    #  print(start_time,end_time)
    #  重点数据处罚数()
    start_time = '2023-10-01'
    end_time = '2023-10-31'
    pun_time = '2023-10-16'
    # 桐庐源头总重80吨以上明细()
    # 全省站点明细数据()
    maintain_station()








