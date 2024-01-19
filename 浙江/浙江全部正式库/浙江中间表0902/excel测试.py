
# -*- coding:utf-8 -*-
# 读取本地 excel 数据集到 Dataframe 中, 调用数据库工具类 DBUtils 中的insert_data 追加写入数据到数据库中


import pymysql
import pandas as pd
from datetime import datetime
from urllib import parse
# 读取数据集
import pandas as pd
import  pymysql
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
        print(sql)
        update = ','.join(["`{key}` = %s".format(key=key) for key in self.data])
        print(update)
        sql += update

        for i in range(len(self.data)):
            try:
                print(tuple(self.data.loc[i]) * 2)
                self.cursor.execute(sql, tuple(self.data.loc[i]) * 2)
                print('正在写入第%d条数据' % (i + 1))
                self.db.commit()
            except Exception as e:
                print("数据写入失败,原因为:" + e)
                self.db.rollback()

        self.cursor.close()
        self.db.close()
        print('数据已全部写入完成!')


db = pymysql.connect(
host='192.168.2.119',
user='zjzhzcuser',
passwd='F4dus0ee',
port=3306,
charset='utf8',
database='db_manage_overruns'
    )


def get_df_from_db(sql):
   cursor = db.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
   cursor.execute(sql)  # 执行SQL语句

   data = cursor.fetchall()

   # 下面为将获取的数据转化为dataframe格式
   columnDes = cursor.description  # 获取连接对象的描述信息
   columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
   df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df

   return df

sql = "select * from t_bas_basic_data_pass where statistic_date >='2022-09-11'"
wide_table = get_df_from_db(sql)
wide_table = wide_table.applymap(str)

print(wide_table)
print(wide_table.info())
wide_table.fillna("", inplace=True) # 替换NaN,否则数据写入时会报错,也可替换成其他
# 连接数据库,定义变量
db = pymysql.connect(host='192.168.2.39', user='zjzhzcuser', password='F4dus0ee', port=3306, db='db_manage_overruns')
cursor = db.cursor()
table = "t_bas_basic_data_passtest"  # 写入表名

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
