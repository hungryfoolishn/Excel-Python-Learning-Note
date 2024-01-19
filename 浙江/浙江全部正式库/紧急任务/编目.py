import pymysql
import pandas as pd
from urllib import parse
全部区县超限率 = pd.read_excel(r"C:\Users\Administrator\Desktop\8月全部区县超限率.xlsx")
print(全部区县超限率)

try:
    from sqlalchemy import create_engine

    user = "zjzhzcuser"
    password = "F4dus0ee"
    host = "192.168.2.39"
    db = "db_manage_overruns"



    pwd = parse.quote_plus(password)

    engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:3306/{db}?charset=utf8")
    #result_table
    #要写入的数据表，这样写的话要提前在数据库建好表
    #t_bas_though.to_sql(name='t_bas_car_through', con=engine, if_exists='append', index=False)
    #result_table
    #要写入的数据表，这样写的话要提前在数据库建好表
    全部区县超限率.to_sql(name='t_bas_bianmu_overrate', con=engine, if_exists='append', index=False)
except Exception as e:
            print("mysql插入失败")