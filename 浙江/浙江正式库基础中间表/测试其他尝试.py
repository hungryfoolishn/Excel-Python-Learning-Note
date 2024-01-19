#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 读取本地 excel 数据集到 Dataframe 中, 调用数据库工具类 DBUtils 中的insert_data 追加写入数据到数据库中

# 读取数据集
import pandas as pd
import  pymysql
from urllib import parse
class DBUtils:
    """
    数据库工具类
    """

    """:param
    db:     数据库连接:  db = pymysql.connect(host='192.168.1.1', user='root', password='1234', port=3306, db='database_name')
    cursor: 数据库游标:  cursor = db.cursor()
    data:   需写入数据:  Dataframe
    table:  写入表名    
    """

    def __init__(self, db, cursor, data, table):
        self.db = db
        self.cursor = cursor
        self.data = data
        self.table = table

    # 按主键去重追加更新
    def insert_data(self):
        keys = ', '.join('`' + self.data.keys() + '`')
        values = ', '.join(['%s'] * len(self.data.columns))
        # 根据表的唯一主键去重追加更新
        sql = 'INSERT INTO {table}({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE'.format(table=self.table,
                                                                                             keys=keys,
                                                                                             values=values)
        update = ','.join(["`{key}` = %s".format(key=key) for key in self.data])
        sql += update

        for i in range(len(self.data)):
            try:
                self.cursor.execute(sql, tuple(self.data.loc[i]) * 2)
                print('正在写入第%d条数据' % (i + 1))
                self.db.commit()
            except Exception as e:
                print("数据写入失败,原因为:" + e)
                self.db.rollback()

        self.cursor.close()
        self.db.close()
        print('数据已全部写入完成!')


wide_table= pd.read_excel("C:/Users/stayhungary/Desktop/wide_table.xlsx")
print(wide_table)
wide_table['statistic_date']=wide_table['statistic_date'].astype('object')
print(wide_table.info())
wide_table.fillna("", inplace=True) # 替换NaN,否则数据写入时会报错,也可替换成其他
# conn = pymysql.connect(
#     host='192.168.1.229',
#     user='root',
#     passwd='zcits123456',
#     port=3306,
#     charset='utf8',
#     database='db_manage_overruns'
# )
# 连接数据库,定义变量
db = pymysql.connect(host='192.168.1.229', user='root', password='zcits123456', port=3306, db='db_manage_overruns')
cursor = db.cursor()
table = "t_bas_basic_data_passtest" # 写入表名

# 写入数据
DBUtils.insert_data(DBUtils(db, cursor, wide_table, table))

# try:
#     from sqlalchemy import create_engine
#
#     user = "root"
#     password = "zcits123456"
#     host = "192.168.1.229"
#     db = "db_manage_overruns"
#
#
#
#     pwd = parse.quote_plus(password)
#
#     engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:3306/{db}?charset=utf8")
#     #result_table
#     #要写入的数据表，这样写的话要提前在数据库建好表
#     #t_bas_though.to_sql(name='t_bas_car_through', con=engine, if_exists='append', index=False)
#     #result_table
#     #要写入的数据表，这样写的话要提前在数据库建好表
#     wide_table.to_sql(name='t_bas_basic_data_passtest', con=engine, if_exists='append', index=False)
# except Exception as e:
#             print("mysql插入失败")
