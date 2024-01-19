import pandas as pd
import  pymysql
from urllib import parse


wide_table= pd.read_excel("C:/Users/stayhungary/Desktop/wide_table.xlsx")
print(wide_table)

# conn = pymysql.connect(
#     host='192.168.1.229',
#     user='root',
#     passwd='zcits123456',
#     port=3306,
#     charset='utf8',
#     database='db_manage_overruns'
# )
# USER_TABLE_NAME = 't_bas_basic_data_passtest'
# # dft 是一个 dataframe 数据集
# wide_table.to_sql('temp', conn, if_exists='append', index=False) # 把新数据写入 temp 临时表
# connection = conn.cursor()
# # 替换数据的语句
# args1 = f""" REPLACE INTO "{USER_TABLE_NAME}"
#              SELECT * FROM "temp"
#          """
# connection.execute(args1)
# args2 = """ DROP Table If Exists "temp" """ # 把临时表删除
# connection.execute(args1)
# connection.close()
# conn.commit()

# config = {
#     'host': "192.168.1.229",
#     'port': 3306,
#     'database': "db_manage_overruns",
#     'user': "root",
#     'password': "zcits123456",
#     'charset':'utf8'
#
# }
# # 连接mysql
# def mysql_conn(config):
#     conn = pymysql.connect(**config)
#     return conn
# def insert_data(conn, df):
#     # 先创建cursor负责操作conn接口
#     cursor = conn.cursor()
#     # 先构造需要的或是和数据库相匹配的列
#     columns = list(df.columns)
#     # 可以删除不要的列或者数据库没有的列名
#     # columns.remove("列名")
#     # 重新构造df,用上面的columns,到这里你要保证你所有列都要准备往数据库写入了
#     new_df = df[columns].copy()
#
#     # 构造符合sql语句的列，因为sql语句是带有逗号分隔的,(这个对应上面的sql语句的(column1, column2, column3))
#     columns = ','.join(list(new_df.columns))
#
#     # 构造每个列对应的数据，对应于上面的((value1, value2, value3))
#     data_list = [tuple(i) for i in new_df.values]  # 每个元组都是一条数据，根据df行数生成多少元组数据
#
#     # 计算一行有多少value值需要用字符串占位
#     s_count = len(data_list[0]) * "%s,"
#
#     # 构造sql语句
#     insert_sql = "insert into " + "t_bas_basic_data_pass" + \
#         " (" + columns + ") values (" + s_count[:-1] + ") "
#
#     try:
#
#         cursor.executemany(insert_sql, data_list)
#         cursor.close()
#         conn.close()
#     except Exception as e:
#         print("mysql插入失败")
#         cursor.close()
#         conn.close()
# conn = mysql_conn(config)
#
# df=wide_table
# res = insert_data(conn, df)
# print(res)


# try:
#     from sqlalchemy import create_engine
#
#     user = "root"
#     password = "zcits123456"
#     host = "192.168.1.229"
#     db = "db_manage_overruns"
#
#
#     pwd = parse.quote_plus(password)
#
#     engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:3306/{db}?charset=utf8")
#     # result_table
#     # 要写入的数据表，这样写的话要提前在数据库建好表
#     # t_bas_though.to_sql(name='t_bas_car_through', con=engine, if_exists='append', index=False)
#     # result_table
#     # 要写入的数据表，这样写的话要提前在数据库建好表
#     wide_table.to_sql(name='t_bas_basic_data_passtest', con=engine, if_exists='append', index=False)
# except Exception as e:
#     print("mysql插入失败")

# import pymysql
# db1 = pymysql.connect(host='192.168.1.229',user='root', password='zcits123456', port=3306, db='db_manage_overruns',charset='utf8')
# cursor = db1.cursor()
# data = wide_table
# table = 't_bas_basic_data_passtest'
# keys = ', '.join(data.keys())
# values = ', '.join(['%s'] * len(data))
# sql = 'INSERT INTO {table}({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE'.format(table=table, keys=keys, values=values)
# update = ','.join([" {key} = (%s)".format(key=key) for key in data])
# sql += update
# try:
#    cursor.execute(sql, tuple(data.values()))
#    print('Successful')
#    db1.commit()
# except:
#    print('Failed')
#    db1.rollback()
# cursor.close()
# db1.close()
