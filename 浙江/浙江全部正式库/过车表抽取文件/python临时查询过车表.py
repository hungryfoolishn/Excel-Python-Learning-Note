import pandas as pd
import pymysql
from sqlalchemy import create_engine
import time
import schedule
from datetime import datetime
from datetime import timedelta



def get_data_from_sql(sql, cs_list):
    global db
    db = pymysql.connect(host='192.168.2.119',user='zjzhzcuser',passwd='F4dus0ee',
            port=3306,charset='utf8',database='db_manage_overruns')
    # db = pymysql.connect(host="192.168.1.229",port=3306,
    #                      user='root', password='zcits123456',
    #                      database='db_manage_overruns')

    cursor = db.cursor()
    cursor.execute(sql, cs_list)
    data = cursor.fetchall()
    '''空列表与非空列表均可以'''
    data=pd.DataFrame(columns=[i[0] for i in cursor.description],data=list(data))
    return data

tt='2022-08-18'
sql_606="select out_station,count(*) as 'car_no',sum(case when is_truck=1 then 1 else 0 end) as 'is_truck'\
            from t_bas_pass_data_31 \
            where date(out_station_time)=%s \
            group by out_station"
sql_808="select area_province,area_city,area_county,out_station,site_name,count(*) as 'car_no',sum(case when is_truck=1 then 1 else 0 end) as 'is_truck'\
            from t_bas_pass_data_31 \
            where date(out_station_time)=%s \
            group by area_province,area_city,area_county,out_station,site_name"
now_time = datetime.now()
jj=now_time.strftime('%Y-%m-%d-%H-%M')
print(jj)
start_11=time.time()
t0=get_data_from_sql(sql_606, [tt])
start_22=time.time()
print('运行时间'+str(start_22-start_11))
print(t0.head())