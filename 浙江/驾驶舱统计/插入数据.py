
# -*- coding:utf-8 -*-

import pymysql
import pandas as pd
from datetime import datetime
from urllib import parse




wide_table=pd.read_excel(r'G:\智诚\2023日常给出数据\紧急事件\t_bas_pass_statistics_data_截止三月通报驾驶舱（站点数据）.xlsx')

print(wide_table)

# db = pymysql.connect(
#    host='192.168.1.229',
#    user='root',
#    passwd='zcits123456',
#    port=3306,
#    charset='utf8',
#    database='db_manage_overruns'
# )

try:
    from sqlalchemy import create_engine

    user = "root"
    password = "zcits123456"
    host = "192.168.1.229"
    db = "db_manage_overruns"

    pwd = parse.quote_plus(password)

    engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:3306/{db}?charset=utf8")
    # result_table
    # 要写入的数据表，这样写的话要提前在数据库建好表
    # t_bas_though.to_sql(name='t_bas_car_through', con=engine, if_exists='append', index=False)
    # result_table
    # 要写入的数据表，这样写的话要提前在数据库建好表
    wide_table.to_sql(name='t_bas_pass_statistics_data', con=engine, if_exists='append', index=False)
except Exception as e:
    print("mysql插入失败")
