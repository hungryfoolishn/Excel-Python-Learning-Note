import pymysql
import pandas as pd
import  time
from decimal import *
import numpy as np
import datetime as dt
import io

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
    """
    使用fetchall函数以元组形式返回所有查询结果并打印出来
    fetchone()返回第一行，fetchmany(n)返回前n行
    游标执行一次后则定位在当前操作行，下一次操作从当前操作行开始
    """
    data = cursor.fetchall()

    # 下面为将获取的数据转化为dataframe格式
    columnDes = cursor.description  # 获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # 获取列名
    df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # 得到的data为二维元组，逐行取出，转化为列表，再转化为df

    """
    使用完成之后需关闭游标和数据库连接，减少资源占用,cursor.close(),db.close()
    db.commit()若对数据库进行了修改，需进行提交之后再关闭
    """

    return df


""" 引入原始表 """
start = time.time()

cs = input("请输入省市名：")

sql = "SELECT * FROM t_sys_station where station_status=0 and station_type =31"
t_sys_station=get_df_from_db(sql)




station_code=t_sys_station['station_code']


sql = "SELECT * FROM t_code_area  "
t_code_area=get_df_from_db(sql)


sql = "SELECT * FROM t_bas_over_data_31 where out_station_time >= '2022-01-01 00:00:00' and  out_station_time <'2022-05-01 00:00:00' and overrun_rate>80  AND allow IS NULL AND is_unusual = 0  "
t_bas_over_data_31=get_df_from_db(sql)

sql = "SELECT * FROM t_bas_over_data_collection_31 where out_station_time >= '2022-01-01 00:00:00' and  out_station_time <'2022-05-01 00:00:00' and overrun_rate>80  and update_time <'2022-06-06 15:10:00'"
t_bas_over_data_collection_31=get_df_from_db(sql)
end = time.time()
time = end - start
print(time)