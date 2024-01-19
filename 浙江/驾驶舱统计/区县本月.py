# -*- coding:utf-8 -*-
import pymysql
import pandas as pd
from urllib import parse



def t_bas_basic_station():
    from datetime import datetime
    day = datetime.now().date()  # 获取当前系统时间

    import datetime
    next_time = day - datetime.timedelta(days=15)

    from datetime import datetime

    ks = datetime.now()
    print('站点开始时间', ks)

    db = pymysql.connect(
        host='10.22.83.142',
        user='admin',
        passwd='#zcits159357',
        port=9030,
        charset='utf8',
        database='db_manage_overruns_hunan'
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

    sqldb = pymysql.connect(
        host='10.22.83.147',
        user='root',
        passwd='ZhzcWSX123#@!',
        port=3306,
        charset='utf8',
        database='db_manage_overruns_hunan'
    )

    def get_df_from_db2(sql1):
        cursor = sqldb.cursor()  # 使用cursor()方法获取用于执行SQL语句的游标
        cursor.execute(sql1)  # 执行SQL语句
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

    ##原始表获取
    sql = "select id from  t_bas_basic_data_pass  where statistics_date >='2023-01-01' and station_state is null "
    t_bas_pass_data_21 = get_df_from_db(sql)
    print(t_bas_pass_data_21)


    for Id in t_bas_pass_data_21['id']:
        print(Id)

        db = pymysql.connect(host="10.22.83.142", port=9030, user='admin', password='#zcits159357',
                             database='db_manage_overruns_hunan',charset='utf8')
        # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
        #                  database='jingdong_ceshi')
        mycursor = db.cursor()
        sql = "delete from t_bas_basic_data_pass where id = '{}'".format(Id)
        mycursor.execute(sql)
        db.commit()
        db.close()



if __name__ == "__main__":

    t_bas_basic_station()

