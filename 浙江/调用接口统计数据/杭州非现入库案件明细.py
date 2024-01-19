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
end_time='2023-09-27'


##--交通现场查处数明细

sql_交通现场查处数明细={
    "tableName":" t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  LEFT JOIN t_bas_over_data_collection_sign d on d.record_code=c.record_code ",
    "where":"  a.record_type = 99 AND a.insert_type = 5 AND a.close_case_time >= '{} 00:00:00' AND a.close_case_time <= '{} 23:59:59' AND a.area_city = '330100'  ".format(start_time,end_time),
    "columns":"*"
}


##--交通现场查处数

sql_交通现场查处数={
    "tableName":" t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  LEFT JOIN t_bas_over_data_collection_sign d on d.record_code=c.record_code",
    "where":" c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time >= '{} 00:00:00' AND c.close_case_time <= '{} 23:59:59' AND c.area_city = 330100 and total_weight >80 GROUP BY c.area_city,c.area_county  ".format(start_time,end_time),
    "columns":"a.city,c.area_city,a.county, c.area_county,count( DISTINCT ( CASE_NUM ) ) AS 交通现场查处数 ,sum(d.punish_money) 交通现场处罚金额"
}


##--交警现场明细
sql_交警现场明细 ={
    "tableName":"  t_bas_police_road_site c left join t_code_area as b on c.area_county=b.county_code   ",
    "where":"   c.punish_time >= '{} 00:00:00' AND c.punish_time < '{} 23:59:59' and area_city = '330100' ".format(start_time,end_time),
    "columns":"*"
}

##--交警现场
sql_交警现场 ={
    "tableName":"   t_bas_police_road_site a LEFT JOIN t_code_area b ON a.area_county = b.county_code   ",
    "where":"   a.punish_time >= '{} 00:00:00' AND a.punish_time < '{} 23:59:59' and a.area_city = '330100' and case_status=2 and total_weight>80000 GROUP BY area_county  ".format(start_time,end_time),
    "columns":"area_county,count(DISTINCT case_number) as 交警现场查处数, sum(a.punish_money) 交警现场处罚金额"
}


##80吨以上非现查处数明细
sql_80吨以上非现查处数明细 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"  a.law_judgment=1 and b.city='杭州' and a.valid_time >='{} 00:00:00' and a.valid_time <'{} 23:59:59' and a.status in (4,6,12) and a.car_no like '%浙%'   ".format(start_time,end_time),
    "columns":"b.city 地市,b.county  区县,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
              "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
}

##80吨以上非现查处数
sql_80吨以上非现查处数 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"  a.law_judgment=1 and b.city='杭州' and c.close_case_time>='{} 00:00:00' and c.close_case_time<'{} 23:59:59' and total_weight >80 GROUP by b.city ,b.county ".format(start_time,end_time),
    "columns":" b.city as 地市,b.county as 区县,count(DISTINCT case_num) 交通非现查处数,SUM(c.punish_money) 交通非现处罚金额 "
}

##80吨以上非现查处数明细
sql_80吨以上非现查处数明细富阳 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"  a.law_judgment=1  and a.out_station_time <'2023-01-01 00:00:00'   and a.status in (4,6,12)  and a.car_no in ('所有人变更车辆号牌','浙A63D66','浙A12J38','浙A2M991','浙AF70B3','浙ABM71','浙A03F71','浙A1X038','浙A20C06','浙A1Q512','浙A9Y293','浙A8P917','浙A78C13','浙A37B16','浙A97C12','浙A97C12','浙A83C92','浙A8U831','浙A69K76','浙A71C79','浙A53F01','浙A72F00','浙A01A75','浙A23A61','浙A61C38','浙A65B50','浙A6X293','浙A29H87','浙A85E62','浙A8K793','浙A50D75','浙A3U456','浙A8Y520','浙A98G32','浙A39A90','浙A16H67','浙A91F66','浙A57B32','浙A2T177','浙A69C78','浙A69C78','浙A95J96','浙A9Z882','浙A2W901','浙A1R958','浙A99A06','浙A7X281','浙A76H80','浙A71F91','浙A6X526','浙A0W512','浙A18A20','浙A5Z969','浙A1Q519','浙A77C21','浙A22B35','浙A99K70','浙A6T795','浙A6T913','浙A9W116','浙A95F38','浙A3Y886','浙A3Y886','浙A3Y296','浙A3Y296','浙A69B38','浙A0Y523','浙A80F15','浙A7Z515','浙A92F65','浙A3L160','浙A88A88','浙A9S399','浙A5M127','浙A23A81','浙A79H65','浙A1Z338','浙A5S839','浙A5S839','浙A3Y575','浙A31A52','浙A5X910','浙A01B09','浙A00B00','浙A72B05','浙A1Q512','浙A86K97','浙A71C79','浙A86J70','浙A21K81','浙A55K12','浙A28B11','浙A99A36','浙A5T310','浙A09D92','浙A3U059','浙A63B98','浙A8P155','浙A36K92','浙A31B39','浙A23H32','浙A23H32','浙A83C58','浙A51D09','浙A22C15','浙A3Y119','浙A92C59','浙A00A96','浙A62D03','浙A9Z695','浙A50D75','浙A50A62','浙A73B69','浙A72B23','浙A91A98','浙A82B28','浙A81J98','浙A7P916','浙A83C81','浙A21C11','浙A1S855','浙A1V563','浙A77G28','浙ABM71','浙A81J98','浙A93D33','浙A20B75','浙A4U048','浙A7Q096','浙A2U308','浙A7V928','浙A16B22','浙A3U418','浙A77E73','浙A86D38','浙A70C91','浙A6V226','浙A65B75','浙A58B63','浙A28B76','浙EA277B','浙E969S3','浙E970A8','浙EB668D','浙EC679P','浙EA736Q','浙E919R5','浙EF535S','浙E696E7','浙EA879M','浙E039U5','浙EC667W','浙E357Y1','浙EH628N','浙EJ552B','浙EA060D','浙EA151B','浙EC762M','浙ED996G','浙EG379X','浙E507J5','浙E855Z9','浙EG311J','浙EB677N','浙DF0819','浙E156C5','浙EB878D','浙E515X0','浙E702Q9','浙E706D6','浙E397Q9','浙EK762N','浙DV3112','浙DX0914','浙E891Z8','浙E171Q8','浙EC788U','浙E653W8','浙EC986J','浙EH887E','浙D22855','浙D28535','浙DR3137','浙KK9590','浙H26615','浙E710R7')   ",
    "columns":"b.city 地市,b.county  区县,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
              "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
}



##80吨以上非现查处数明细
双百入库处罚明细 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"  a.law_judgment=1 and b.city='杭州' and a.valid_time >='{} 00:00:00' and a.valid_time <'{} 23:59:59'  and a.overrun_rate>=100   ".format(start_time,end_time),
    "columns":"b.city 地市,b.county  区县,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
              "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
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
    # data1=get_data(sql_交通现场查处数)
    data2=get_data(双百入库处罚明细)
    data2 = pd.DataFrame(data2,
                           columns=['地市', '区县', '车籍地', '车牌号', '所属运输企业名称','联系电话','检测时间', '入库时间', '检测站点', '总重', '轴数','超限率', '案件状态', '外省抄告', '结案时间', '处罚金额','record_code'])
    # data3=get_data(sql_交警现场)
    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\杭州截止20230927双百入库处罚明细2.xlsx') as writer1:
        # data1.to_excel(writer1, sheet_name='sql_80吨以上交通现场', index=True)
        data2.to_excel(writer1, sheet_name='双百入库处罚明细', index=True)
        # data3.to_excel(writer1, sheet_name='sql_80吨以上交警现场', index=True)


