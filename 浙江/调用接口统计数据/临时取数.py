# coding: utf-8


import time
import pandas as pd
import requests
import base64
import datetime
import pymysql
import json
import csv


start_time='2023-08-01'
end_time='2023-09-01'

sql_台州数据超限100数据明细={
    "tableName":"t_bas_over_data_31 ",
    "where":" record_code in ('3310043102_202308131223191240','3310043101_202308141557044404','3310043102_202308271746199352','12345_1692493188725000101','331022__1691707339414_9023624','331022__1691970675972_9064285','331022__1692004854690_9073292','331022__1692309348967_9116152','331022__1692403313520_9131117','331022__1692573337387_9160088','331022__1692691151393_9181829','331022__1692693323061_9182511','331022__1692695813847_9183118','331022__1692916421138_9213558','331022__1693005869466_9228271','331022__1693283736493_9276456','331023_202308091047181417395','331023_202308181503531441624','331023_202308200455581445162','331081_1691648873564000103','3310813107_1692141044932000101','3310813107_1692184684255000101','3310813107_1692184693057000101','3310813107_1692184816891000101','3310813107_1692185393437000101','3310813107_1692185594032000101','3310813107_1692185713585000101','3310813107_1692185742038000101','3310813107_1692186006455000101','3310813107_1692186249536000101','3310813107_1692186356462000101','3310813107_1692186378475000101','3310813107_1692186384502000101','3310813107_1692186399715000101','3310813107_1692186499401000101','3310813107_1692186505018000101','3310813107_1692186905172000101','3310813107_1692187428088000101','3310813107_1692187960209000101','3310813107_1692188072584000101','3310813107_1692188191618000101','3310813107_1692188214436000101','3310813107_1692188459843000101','3310813107_1692188860276000101','3310813107_1692189275877000101','3310813107_1692189501246000101','3310813107_1692189641263000101','3310813107_1692190402324000101','3310813107_1692190404814000101','3310813107_1692190543118000101','3310813107_1692190815688000101','3310813107_1692190918950000101','3310813107_1692191052121000101','3310813107_1692191176522000101','3310813107_1692191310744000101','3310813107_1692191573503000101','3310813107_1692191577930000101','3310813107_1692191591547000101','3310813107_1692192113496000101','3310813107_1692192218807000101','3310813107_1692192486099000101','3310813107_1692192612831000101','3310813107_1692192757692000101','3310813107_1692192874361000101','3310813107_1692193421877000101','3310813107_1692194176940000101','3310813107_1692194765351000101','3310813107_1692195551543000101','3310813107_1692196319596000101','3310813107_1692197345278000101','3310813107_1692197484403000101','3310813107_1692197487677000101','3310813107_1692198019975000101','3310813107_1692198110021000101','3310813107_1692200077818000101','3310813107_1692200460420000101','3310813107_1692200613347000101','3310813107_1692200720565000101','3310813107_1692201240323000101','3310813107_1692201870411000101','3310813107_1692201985745000101','3310813107_1692202392376000101','3310813107_1692202496189000101','3310813107_1692202507403000101','3310813107_1692202648723000101','3310813107_1692202878966000101','3310813107_1692203002274000101','3310813107_1692204071534000101','3310813107_1692204704858000101','3310813107_1692205212754000101','3310813107_1692205911702000101','3310813107_1692206021369000101','3310813107_1692206260912000101','3310813107_1692207286116000101','3310813107_1692207590862000101','3310813107_1692207693718000101','3310813107_1692207836201000101','3310813107_1692207976077000101','3310813107_1692209393759000101','3310813107_1692211481564000101','3310813107_1692211942380000101','3310813107_1692213175372000101','3310813107_1692213959362000101','3310813107_1692214481823000101','3310813107_1692221644615000101','3310813107_1692221750996000101','3310813107_1692221758219000101','3310813107_1692221775887000101','3310813107_1692222652004000101','3310813107_1692224168663000101','3310813107_1692224936035000101','3310813107_1692225452534000101','3310813107_1692225596474000101','3310813107_1692225745428000101','3310813107_1692226636292000101','3310813107_1692226890398000101','3310813107_1692227291791000101','3310813107_1692227937219000101','3310813107_1692228278455000101','3310813107_1692228423300000101','3310813107_1692228452709000101','3310813107_1692228898975000101','3310813107_1692228973053000101','3310813107_1692229089695000101','3310813107_1692229236242000101','3310813107_1692229324336000101','3310813107_1692229617470000101','3310813107_1692229723589000101','3310813107_1692229765978000101','3310813107_1692229771590000101','3310813107_1692230015538000101','3310813107_1692230108205000101','3310813107_1692230288791000101','3310813107_1692230400569000101','3310813107_1692230604765000101','3310813107_1692230942141000101','3310813107_1692231323595000201','3310813107_1692231428125000101','3310813107_1692231570349000101')  ",
    "columns":"*"
}

##--交通现场查处数明细

sql_交通现场查处数明细={
    "tableName":" t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  LEFT JOIN t_bas_over_data_collection_sign d on d.record_code=c.record_code ",
    "where":"  a.record_type = 99 AND a.insert_type = 5 AND a.close_case_time >= '{} 00:00:00' AND a.close_case_time <= '{} 23:59:59'   ".format(start_time,end_time),
    "columns":"*"
}


##--交通现场查处数

sql_交通现场查处数={
    "tableName":" t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  LEFT JOIN t_bas_over_data_collection_sign d on d.record_code=c.record_code",
    "where":" c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time >= '{} 00:00:00' AND c.close_case_time <= '{} 23:59:59' AND c.area_city = 330100 GROUP BY c.area_city,c.area_county  ".format(start_time,end_time),
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
    "where":"   a.punish_time >= '{} 00:00:00' AND a.punish_time < '{} 23:59:59' and a.area_city = '330100' and case_status=2  GROUP BY area_county  ".format(start_time,end_time),
    "columns":"area_county,count(DISTINCT case_number) as 交警现场查处数, sum(a.punish_money) 交警现场处罚金额"
}


##80吨以上非现查处数明细
sql_80吨以上非现查处数明细 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"  a.law_judgment=1 and b.city='杭州' and c.close_case_time>='{} 00:00:00' and c.close_case_time<'{} 23:59:59'  ".format(start_time,end_time),
    "columns":"b.city 地市,b.county  区县,a.out_station_time  检测时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
              "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time"
}

##80吨以上非现查处数
sql_80吨以上非现查处数 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"  a.law_judgment=1 and b.city='杭州' and c.close_case_time>='{} 00:00:00' and c.close_case_time<'{} 23:59:59'  GROUP by b.city ,b.county ".format(start_time,end_time),
    "columns":" b.city as 地市,b.county as 区县,count(DISTINCT case_num) 交通非现查处数,SUM(c.punish_money) 交通非现处罚金额 "
}

##杭州高速数据分析
sql_杭州高速数据分析 ={
    "tableName":"t_bas_over_data_41 a  left join t_sys_station s on a.out_station = s.station_code  ",
    "where":" (a.total_weight -a.limit_weight*1.05)>0 and a.allow is null and a.direction = 0 and a.out_station_time between '2023-01-01 00:00:00' and '2023-09-22 00:00:00' and a.area_city = 330100 ",
    "columns":" a.record_code as 流水号,s.road_name as 线路名称,a.out_station as 站点编码,s.station_name as 站点名称, a.car_no as 车牌号码,a.out_station_time as 检测时间,a.total_weight as 总重,a.axis as 轴数, a.limit_weight as 限重,a.overrun as 超限,a.overrun_rate as 超限率,a.link_man as 联系人, a.phone as 联系电话,a.car_holder_addr as 联系地址"
}

##杭州高速数据分析
sql_t_bas_fence_control_station ={
    "tableName":" t_bas_fence_control_station ",
    "where":" id is not null ",
    "columns":" *"
}

##杭州高速数据分析
t_bas_fence_control_result ={
    "tableName":" t_bas_fence_control_result ",
    "where":" id is not null ",
    "columns":" *"
}


##台州70数
sql_台州70数 ={
    "tableName": "t_bas_over_data_31 a  left join t_code_area b on a.area_county = b.county_code",
    "where": "  out_station_time between '2023-08-01 00:00' and '2023-08-31 23:59:59' and allow is  null and city ='台州' and is_unusual=0 group by area_county",
    "columns": "county, sum(IF( a.total_weight between 70 and 300 , 1, 0 )) 总重70吨以上数,sum(IF( a.overrun_rate between 70 and 1500 , 1, 0 ))  超限70以上数 "
}

##绍兴遮牌
sql_绍兴遮牌 ={
    "tableName": "  t_bas_over_data_31 a LEFT JOIN t_code_area b ON a.area_county = b.county_code",
    "where": "a.out_station_time >= '{} 00:00:00' and a.out_station_time < '{} 00:00:00' and ( a.total_weight - a.limit_weight * 1.2 )> 0  and a.is_unusual = 0  and a.allow is null and b.city='绍兴' GROUP BY a.area_county,b.county".format(start_time,end_time),
    "columns": "b.city,b.county as 区县,count(1) as 超限总数 ,sum(IF( a.car_no like '%牌%', 1, 0 )) as 无牌数 "
}


##绍兴遮牌
sql_绍兴遮牌2 ={
    "tableName": "t_bas_pass_data_31",
    "where": "out_station='330122_S208K19+900' and out_station_time between '2023-09-17 09:00:00' and '2023-09-17 21:00:00'",
    "columns": "*"
}

##台州70数
sql_台州70明细吨 ={
    "tableName": "t_bas_over_data_31 a  left join t_code_area b on a.area_county = b.county_code",
    "where": "  out_station_time between '2023-08-01 00:00' and '2023-08-31 23:59:59' and allow is  null and city ='台州' and is_unusual=0 and a.total_weight between 70 and 300",
    "columns": "*"
}

##台州70数
sql_台州70明细率 ={
    "tableName": "t_bas_over_data_31 a  left join t_code_area b on a.area_county = b.county_code",
    "where": "  out_station_time between '2023-08-01 00:00' and '2023-08-31 23:59:59' and allow is  null and city ='台州' and is_unusual=0 and a.overrun_rate between 70 and 1500 ",
    "columns": "* "
}

##杭州高速数据分析
sql_杭州高速数据分析 ={
    "tableName":"t_bas_over_data_41 a  left join t_sys_station s on a.out_station = s.station_code  ",
    "where":" (a.total_weight -a.limit_weight*1.05)>0 and a.allow is null and a.direction = 0 and a.out_station_time between '2023-01-01 00:00:00' and '2023-09-21 00:00:00' and a.area_city = 330900 ",
    "columns":"count(1)"
}

##杭州高速数据分析
sql_杭州高速数据分析2 ={
    "tableName":"t_bas_pass_data_41 a  left join t_sys_station s on a.out_station = s.station_code  ",
    "where":"  a.allow is null and a.direction = 0 and a.out_station_time between '2023-01-01 00:00:00' and '2023-09-21 00:00:00' and a.area_city = 330900 and total_weight >2.5 ",
    "columns":"count(1)"
}


##80吨以上非现查处数
sql_80吨以上非现查处数 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":" a.record_code in ('3301833107_1681736534426000101','3301833107_1681736534426000101') ",
    "columns":" b.city as 地市,b.county as 区县,count(DISTINCT case_num) 交通非现查处数,SUM(c.punish_money) 交通非现处罚金额 "
}

##80吨以上非现查处数明细
sql_80吨以上非现查处数明细13 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"  a.record_code in ('3301833107_1681736534426000101','3301273103_20230731002211000001') ",
    "columns":"b.city 地市,b.county  区县,a.out_station_time  检测时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
              "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time"
}
t_bas_over_data_collection_31 = {
        "tableName": "t_bas_over_data_31 a left join t_bas_over_data_collection_31 b on a.record_code=b.record_code left join t_code_area c on a.area_county=c.county_code ",
        "where": "a.out_station_time >= '{} 00:00:00' and  a.out_station_time <'{} 00:00:00' and a.total_weight>84   AND a.is_unusual = 0  and a.allow is null ",
        "columns": "c.city 地市,c.county 区县, a.area_county,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号码,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,b.status 案件状态,a.site_name 站点名称,a.is_collect 证据满足,b.law_judgment 判定需处罚,b.make_copy  外省抄告,a.link_man 所属运输企业名称,a.phone 联系电话, b.vehicle_county 车籍地,a.record_code 流水号"
    }


##杭州高速数据分析
sql_杭州高速数据分析2 ={
    "tableName":"t_bas_car_information   ",
    "where":"  car_number in ('浙A63D66','浙A12J38','浙A2M991','浙AF70B3','浙ABM71','浙A03F71','浙A1X038','浙A20C06','浙A1Q512','浙A9Y293','浙A8P917','浙A78C13','浙A37B16','浙A97C12','浙A83C92','浙A8U831','浙A69K76','浙A71C79','浙A53F01','浙A72F00','浙A01A75','浙A23A61','浙A61C38','浙A65B50','浙A6X293','浙A29H87','浙A85E62','浙A8K793','浙A50D75','浙A3U456','浙A8Y520','浙A98G32','浙A39A90','浙A16H67','浙A91F66','浙A57B32','浙A2T177','浙A69C78','浙A95J96','浙A9Z882','浙A2W901','浙A1R958','浙A99A06','浙A7X281','浙A76H80','浙A71F91','浙A6X526','浙A0W512','浙A18A20','浙A5Z969','浙A1Q519','浙A77C21','浙A22B35','浙A99K70','浙A6T795','浙A6T913','浙A9W116','浙A95F38','浙A3Y886','浙A3Y296','浙A69B38','浙A0Y523','浙A80F15','浙A7Z515','浙A92F65','浙A3L160','浙A88A88','浙A9S399','浙A5M127','浙A23A81','浙A79H65','浙A1Z338','浙A5S839','浙A3Y575','浙A31A52','浙A5X910','浙A01B09','浙A00B00','浙A72B05','浙A86K97','浙A86J70','浙A21K81','浙A55K12','浙A28B11','浙A99A36','浙A5T310','浙A09D92','浙A3U059','浙A63B98','浙A8P155','浙A36K92','浙A31B39','浙A23H32','浙A83C58','浙A51D09','浙A22C15','浙A3Y119','浙A92C59','浙A00A96','浙A62D03','浙A9Z695','浙A50A62','浙A73B69','浙A72B23','浙A91A98','浙A82B28','浙A81J98','浙A7P916','浙A83C81','浙A21C11','浙A1S855','浙A1V563','浙A77G28','浙A93D33','浙A20B75','浙A4U048','浙A7Q096','浙A2U308','浙A7V928','浙A16B22','浙A3U418','浙A77E73','浙A86D38','浙A70C91','浙A6V226','浙A65B75','浙A58B63','浙A28B76','浙EA277B','浙E969S3','浙E970A8','浙EB668D','浙EC679P','浙EA736Q','浙E919R5','浙EF535S','浙E696E7','浙EA879M','浙E039U5','浙EC667W','浙E357Y1','浙EH628N','浙EJ552B','浙EA060D','浙EA151B','浙EC762M','浙ED996G','浙EG379X','浙E507J5','浙E855Z9','浙EG311J','浙EB677N','浙DF0819','浙E156C5','浙EB878D','浙E515X0','浙E702Q9','浙E706D6','浙E397Q9','浙EK762N','浙DV3112','浙DX0914','浙E891Z8','浙E171Q8','浙EC788U','浙E653W8','浙EC986J','浙EH887E','浙D22855','浙D28535','浙DR3137','浙KK9590','浙H26615','浙E710R7','浙A00F35','浙A08D87','浙A09D10','浙A0M692','浙A0S916','浙A0T396','浙A0T511','浙A0T625','浙A0W151','浙A0Y153','浙A11C52','浙A11D67','浙A17A19','浙A17B67','浙A19F33','浙A1T001','浙A1X187','浙A1X273','浙A1Y281','浙A27B25','浙A2S873','浙A2T269','浙A2Y352','浙A35C69','浙A35D68','浙A39A25','浙A3V987','浙A3X886','浙A3Y753','浙A3Z211','浙A3Z338','浙A5V296','浙A5V680','浙A5V719','浙A5V733','浙A60E55','浙A65C96','浙A66C81','浙A67A36','浙A6S678','浙A6U869','浙A6Y808','浙A6Z839','浙A6Z860','浙A72B61','浙A76H20','浙A79D79','浙A7K663','浙A7M665','浙A7S988','浙A7X671','浙A81D57','浙A85D25','浙A86F23','浙A89A87','浙A8W396','浙A8Z098','浙A8Z261','浙A93E33','浙A96072','浙A97E07','浙A9U763','浙A9Z153','浙A9Z399','浙AL0668','浙B0D779','浙D12447','浙D16809','浙DB2570','浙DB5175','浙DD4043','浙DF2932','浙DQ2561','浙DR5060','浙G27192','浙G28922','浙G55011','浙G56892','浙GL1635','浙H15533','浙H17675','浙H26963','浙H29036','浙HA0520','浙HA1902','浙HA6529','浙HB6001','浙HB607','浙HB6128','浙HC9439','浙HD1881','浙HD6788','浙HD9296','浙HL977','浙J19771','浙KK1952','浙KK7565') ",
    "columns":"car_number,license_color"
}

##杭州高速数据分析
sql_杭州高速数据分析3 ={
    "tableName":"t_bas_over_data_collection_31   ",
    "where":"  car_no in ('浙A2T177','浙A2T269','浙A5M127','浙A6T795','浙AF70B3') ",
              "columns":"*"
}

##杭州车牌颜色核对
sql_杭州高速数据分析2 ={
    "tableName":"t_bas_over_data_collection_31   ",
    "where":"  car_no in ('浙A00F35','浙A08D87','浙A09D10','浙A0M692','浙A0S916','浙A0T396','浙A0T511','浙A0T625','浙A0W151','浙A0Y153','浙A11C52','浙A11D67','浙A17A19','浙A17B67','浙A19F33','浙A1T001','浙A1X187','浙A1X273','浙A1Y281','浙A27B25','浙A2S873','浙A2T269','浙A2Y352','浙A35C69','浙A35D68','浙A39A25','浙A3V987','浙A3X886','浙A3Y753','浙A3Z211','浙A3Z338','浙A5V296','浙A5V680','浙A5V719','浙A5V733','浙A60E55','浙A65C96','浙A66C81','浙A67A36','浙A6S678','浙A6U869','浙A6Y808','浙A6Z839','浙A6Z860','浙A72B61','浙A76H20','浙A79D79','浙A7K663','浙A7M665','浙A7S988','浙A7X671','浙A81D57','浙A85D25','浙A86F23','浙A89A87','浙A8W396','浙A8Z098','浙A8Z261','浙A93E33','浙A96072','浙A97E07','浙A9U763','浙A9Z153','浙A9Z399','浙AL0668','浙B0D779','浙D12447','浙D16809','浙DB2570','浙DB5175','浙DD4043','浙DF2932','浙DQ2561','浙DR5060','浙G27192','浙G28922','浙G55011','浙G56892','浙GL1635','浙H15533','浙H17675','浙H26963','浙H29036','浙HA0520','浙HA1902','浙HA6529','浙HB6001','浙HB607','浙HB6128','浙HC9439','浙HD1881','浙HD6788','浙HD9296','浙HL977','浙J19771','浙KK1952','浙KK7565','浙A63D66','浙A12J38','浙A2M991','浙AF70B3','浙ABM71','浙A03F71','浙A1X038','浙A20C06','浙A1Q512','浙A9Y293','浙A8P917','浙A78C13','浙A37B16','浙A97C12','浙A97C12','浙A83C92','浙A8U831','浙A69K76','浙A71C79','浙A53F01','浙A72F00','浙A01A75','浙A23A61','浙A61C38','浙A65B50','浙A6X293','浙A29H87','浙A85E62','浙A8K793','浙A50D75','浙A3U456','浙A8Y520','浙A98G32','浙A39A90','浙A16H67','浙A91F66','浙A57B32','浙A2T177','浙A69C78','浙A69C78','浙A95J96','浙A9Z882','浙A2W901','浙A1R958','浙A99A06','浙A7X281','浙A76H80','浙A71F91','浙A6X526','浙A0W512','浙A18A20','浙A5Z969','浙A1Q519','浙A77C21','浙A22B35','浙A99K70','浙A6T795','浙A6T913','浙A9W116','浙A95F38','浙A3Y886','浙A3Y886','浙A3Y296','浙A3Y296','浙A69B38','浙A0Y523','浙A80F15','浙A7Z515','浙A92F65','浙A3L160','浙A88A88','浙A9S399','浙A5M127','浙A23A81','浙A79H65','浙A1Z338','浙A5S839','浙A5S839','浙A3Y575','浙A31A52','浙A5X910','浙A01B09','浙A00B00','浙A72B05','浙A1Q512','浙A86K97','浙A71C79','浙A86J70','浙A21K81','浙A55K12','浙A28B11','浙A99A36','浙A5T310','浙A09D92','浙A3U059','浙A63B98','浙A8P155','浙A36K92','浙A31B39','浙A23H32','浙A23H32','浙A83C58','浙A51D09','浙A22C15','浙A3Y119','浙A92C59','浙A00A96','浙A62D03','浙A9Z695','浙A50D75','浙A50A62','浙A73B69','浙A72B23','浙A91A98','浙A82B28','浙A81J98','浙A7P916','浙A83C81','浙A21C11','浙A1S855','浙A1V563','浙A77G28','浙ABM71','浙A81J98','浙A93D33','浙A20B75','浙A4U048','浙A7Q096','浙A2U308','浙A7V928','浙A16B22','浙A3U418','浙A77E73','浙A86D38','浙A70C91','浙A6V226','浙A65B75','浙A58B63','浙A28B76','浙EA277B','浙E969S3','浙E970A8','浙EB668D','浙EC679P','浙EA736Q','浙E919R5','浙EF535S','浙E696E7','浙EA879M','浙E039U5','浙EC667W','浙E357Y1','浙EH628N','浙EJ552B','浙EA060D','浙EA151B','浙EC762M','浙ED996G','浙EG379X','浙E507J5','浙E855Z9','浙EG311J','浙EB677N','浙DF0819','浙E156C5','浙EB878D','浙E515X0','浙E702Q9','浙E706D6','浙E397Q9','浙EK762N','浙DV3112','浙DX0914','浙E891Z8','浙E171Q8','浙EC788U','浙E653W8','浙EC986J','浙EH887E','浙D22855','浙D28535','浙DR3137','浙KK9590','浙H26615','浙E710R7') ",
    "columns":"car_no,car_no_color"
}

##预警数
预警数 ={
    "tableName":"t_bas_fence_control_warning    ",
    "where":" warning_time BETWEEN '2023-01-01 00:00:00' AND '2023-08-31 23:59:59' ",
    "columns":"COUNT(*)  as 预警数"
}
#预警数
稽查布控数_出动人次1 ={
    "tableName":"t_bas_fence_control_process     ",
    "where":" issue_time BETWEEN '2023-01-01 00:00:00' AND '2023-08-31 23:59:59'  ",
    "columns":"COUNT(*) as 稽查布控数_出动人次1   "
}

#预警数
稽查布控数_出动人次2 ={
    "tableName":"t_bas_fence_control_result      ",
    "where":" intercept_time BETWEEN '2023-01-01 00:00:00' AND '2023-08-31 23:59:59'  ",
    "columns":"COUNT(*) as 稽查布控数_出动人次2  "
}

#预警数
布控车辆数1 ={
    "tableName":"t_bas_fence_control_process     ",
    "where":" issue_time BETWEEN '2023-01-01 00:00:00' AND '2023-08-31 23:59:59' ",
    "columns":"COUNT(distinct warning_id) as 布控车辆数1  "
}

#预警数
布控车辆数2={
    "tableName":"t_bas_fence_control_result ",
    "where":" intercept_time BETWEEN '2023-01-01 00:00:00' AND '2023-08-31 23:59:59'  ",
    "columns":"COUNT(distinct car_no) as 布控车辆数2  "
}


#处罚车辆数
处罚车辆数={
    "tableName":"t_bas_fence_control_result a, t_bas_over_data_collection_sign b ",
    "where":"a.car_no = b.car_no AND a.car_no_color = b.car_no_color AND a.intercept_time  < b.insert_time AND a.intercept_time BETWEEN '2023-01-01 00:00:00' AND '2023-09-31 23:59:59' AND a.county_code = b.area_county ",
    "columns":"COUNT(*) as 处罚车辆数  "
}

t_sys_station = {
    "tableName": "t_sys_station a  left join t_code_area b on a.area_county =b.county_code ",
    "where": "station_type = 71 AND a.is_deleted = 0  and area_city=330100" ,
    "columns": "county,station_name,station_code,station_status,station_type,area_county  "
}

sql_非现入库数 = {
    "tableName": "t_bas_over_data_collection_31  ",
    "where": " law_judgment = 1  AND valid_time between '2023-01-01 00:00:00'  AND '2023-08-31 23:59:59'  and status =4 and vehicle_county ='331021'  ",
    "columns": "count( 1 )  入库数路政  "
}


部级黑名单= {
    "tableName": "t_case_sign_result c LEFT JOIN t_code_area a ON c.area_county = a.county_code  left  join  t_bas_over_data_collection_sign d  ON c.record_code = d.record_code  ",
    "where": "c.record_type = 99 and c.insert_type = 5 AND c.close_case_time between '2023-01-01 00:00:00'  AND '2023-10-01 23:59:59' AND c.area_province = 330000  ",
    "columns": "c.record_code,c.car_no  as 车牌号,c.car_qua_code 道路运输证号,c.driver_name 驾驶员姓名,c.driver_id_card 身份证号,c.CASE_NUM as 行政处罚决定书文号,DATE_FORMAT(d.out_station_time,'%Y-%m-%d') 违法时间,DATE_FORMAT(c.jueding_time,'%Y-%m-%d') 处罚决定日期, d.punish_money 罚款金额,c.org_name  执法机构名称"
}

sql_交通现场查处数 = {
    "tableName": " t_bas_over_data_collection_sign c",
    "where": " c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time between '2023-01-01 00:00:00'  and '2023-09-31 23:59:59'  GROUP BY c.area_county  ",
    "columns": "c.area_county  ,count( DISTINCT ( CASE_NUM ) ) AS 现场处罚路政 "
}

sql_交通现场查处数 = {
    "tableName": " t_bas_over_data_collection_sign c",
    "where": " c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time between '2023-01-01 00:00:00' and '2023-09-31 23:59:59'  ",
    "columns": "* "
}

sql_交通现场查处数 = {
    "tableName": " t_bas_over_data_collection_sign c",
    "where": " c.record_type = 99 AND c.insert_type = 5  AND c.close_case_time between '2023-01-01 00:00:00' and '2023-09-31 23:59:59'    GROUP BY c.area_county  ",
    "columns": "c.area_county  ,count( DISTINCT ( record_id ) ) AS 现场处罚路政 "
}

##杭州高速数据分析
sql_舟山高速数据分析 ={
    "tableName":"t_bas_over_data_41 a  left join t_sys_station s on a.out_station = s.station_code  ",
    "where":" (a.total_weight -a.limit_weight*1.05)>0 and a.allow is null and a.direction = 0 and a.out_station_time between '2023-01-01 00:00:00' and '2023-09-22 00:00:00' and a.area_city = 330900 ",
    "columns":" a.record_code as 流水号,s.road_name as 线路名称,a.out_station as 站点编码,s.station_name as 站点名称, a.car_no as 车牌号码,a.out_station_time as 检测时间,a.total_weight as 总重,a.axis as 轴数, a.limit_weight as 限重,a.overrun as 超限,a.overrun_rate as 超限率,a.link_man as 联系人, a.phone as 联系电话,a.car_holder_addr as 联系地址"
}

# sql_外省抄告数 = {
#     "tableName": "t_bas_over_data_collection_31  ",
#     "where": " law_judgment = 1  AND valid_time between '2023-01-1 00:00:00'  AND '2023-08-31 23:59:59'  and status !=5 GROUP BY area_county  ",
#     "columns": "area_county ,sum(IF( make_copy=1 and car_no not LIKE '%浙%', 1, 0 )) 外省抄告 "
# }

sql_外省抄告数 = {
    "tableName": "t_bas_over_data_31  ",
    "where": " is_collect = 0  and is_unusual =0 AND out_station_time between '2023-09-01 00:00:00'  AND '2023-09-30 23:59:59' and area_county ='330185'",
    "columns": "record_code "
}

##80吨以上非现查处数明细
sql_80吨以上非现查处数明细 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"c.close_case_time >='2023-07-01 00:00:00' and c.close_case_time <'2023-10-10 23:59:59' and a.car_no like '%浙A%'   ",
    "columns":"b.city 地市,b.county  区县,a.area_county 区县编码,c.dept_county 处罚地,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
              "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
}




杭州部级黑名单企业占比 ={
    "tableName":"t_bas_share_punishment_transport a left join t_code_area b on a.area_county = b.county_code ",
    "where":"area_city ='330100' and  illegal_car_proportion >='10' and decide_time >='2023-01-01' and decide_time  <'2023-10-11' GROUP BY a.area_county",
    "columns":"a.area_county,b.county,count(a.id) 企业数"
}

杭州部级黑名单车辆数 ={
    "tableName":"t_bas_share_punishment_car a left join t_code_area b on a.area_county = b.county_code ",
    "where":"area_city ='330100' and  illegal_times >=3 and decide_time >='2023-01-01' and decide_time  <'2023-10-11' GROUP BY a.area_county",
    "columns":"b.county,count(a.id) 车辆数"
}

杭州部级黑名单车辆信息 ={
    "tableName":"t_bas_car_information  ",
    "where":"car_number ='浙A7X232'",
    "columns":"*"
}


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
运管车辆信息={
    "carNumber": "浙D15737"
}


sql_80吨以上非现查处数明细 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"c.valid_time >='2023-07-01 00:00:00' and c.valid_time <'2023-09-30 23:59:59' and a.area_city =330300  group by b.county,a.status",
    "columns":"b.city 地市,b.county  区县,a.area_county 区县编码,c.dept_county 处罚地,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
              "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
}

温州入库案件 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"a.valid_time >='2023-07-01 00:00:00' and a.valid_time <='2023-09-30 23:59:59' and a.area_city =330300  group by b.city, b.county,a.status",
    "columns":"b.city,b.county  区县,a.status 案件状态 , count(1)"
}
温州入库案件检测 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"a.out_station_time >='2023-07-01 00:00:00' and a.out_station_time <='2023-09-30 23:59:59' and a.area_city =330300  group by b.city, b.county,a.status",
    "columns":"b.city,b.county  区县,a.status 案件状态 , count(1)"
}
温州入库案件结案 ={
    "tableName":"t_case_sign_result c left join t_code_area  b on c.dept_county=b.county_code ",
    "where":"record_type = 31 AND insert_type = 1 AND data_source = 1 AND case_type = 1 AND c.close_case_time >='2023-07-01 00:00:00' and c.close_case_time <='2023-09-30 23:59:59' and b.city ='温州'  group by b.city,b.county,c.case_status",
    "columns":"b.city,b.county  区县,c.case_status 案件状态 , count(DISTINCT ( CASE_NUM )) AS 非现场处罚路政处罚"
}


现场超限数 ={
    "tableName":"t_bas_pass_data_41 ",
    "where":" out_station_time between '2023-01-01 00:00:00' and '2023-09-30 23:59:59'  and total_weight between 2.5 and 200 group by area_province",
    "columns":"area_province,count(1) as 货车数,sum(IF( overrun_rate between 0.05 and 500, 1, 0 ))  超限数 "
}

sql_station = {
    "tableName": "t_sys_station  ",
    "where": "  is_deleted = 0 and station_status in (0,3)  and station_type =41  group by area_province",
    "columns": "area_province ,count(1) as  现场站点数"
}

sql_station = {
    "tableName": "t_cockpit_business_statistics_data  ",
    "where": "  area_code = 330000 ",
    "columns": "*"
}

sql_外省抄告数 = {
    "tableName": "t_bas_over_data_collection_31  ",
    "where": " law_judgment = 1  AND valid_time between '2023-01-01 00:00:00'  AND '2023-09-30 23:59:59'  and status !=5   ",
    "columns": "area_county ,sum(IF( make_copy=1 and car_no not LIKE '%浙%', 1, 0 )) 外省抄告 "
}

sql_外省抄告数 ={
    "tableName":"t_bas_over_data_collection_31  a left join t_code_area  b on a.area_county=b.county_code LEFT JOIN t_case_sign_result c  on a.record_code =c.record_code  ",
    "where":"  a.law_judgment=1 and b.county='安吉' AND a.valid_time between '2023-01-01 00:00:00'  AND '2023-09-30 23:59:59'  and a.status !=5    ",
    "columns":"b.city 地市,b.county  区县,a.out_station_time  检测时间,a.valid_time 入库时间,a.car_no  车牌号,a.total_weight  总重,a.limit_weight 限重,a.overrun  超重,a.overrun_rate  超限率,a.axis  轴数,a.status 案件状态,a.site_name 检测站点,"
              "a.law_judgment  判定需处罚,a.make_copy  外省抄告,c.punish_money  处罚金额, a.link_man 所属运输企业名称,a.phone 联系电话, a.vehicle_county 车籍地,c.case_status 已结案,a.record_code,c.close_case_time 结案时间"
}


杭州部级黑名单企业占比 ={
    "tableName":"t_bas_transport_company a left join t_code_area b on a.area_county = b.county_code ",
    "where":"area_city ='330100' and  illegal_car_proportion >='10' and decide_time >='2023-01-01' and decide_time  <'2023-10-11' GROUP BY a.area_county",
    "columns":"a.area_county,b.county,count(a.id) 企业数"
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

高速超限率 ={
    "tableName":"t_bas_pass_data_41 ",
    "where":" out_station_time between '2023-01-01 00:00:00' and '2023-09-30 23:59:59'  and area_city =330200 and total_weight between 2.5 and 200 group by area_province",
    "columns":"area_province,count(1) as 货车数,sum(IF( overrun_rate between 0.05 and 500, 1, 0 ))  超限数 "
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
    高速超限率=get_df_from_db(高速超限率)

    with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\高速超限率.xlsx') as writer1:
        高速超限率.to_excel(writer1, sheet_name='高速超限率', index=True)





