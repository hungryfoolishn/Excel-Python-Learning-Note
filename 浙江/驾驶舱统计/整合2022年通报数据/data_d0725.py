# -*- coding:utf-8 -*-
import pymysql
import pandas as pd
from datetime import datetime
from urllib import parse

day = datetime.now().date()  # 获取当前系统时间

today = datetime.now()
# import time
#
#
# def sleeptime(hour, min, sec):
#     return 3600 * hour + 60 * min + sec
#
#
# print('暂停：', sleeptime(0, 0, 20), '秒')
# seconds = sleeptime(0, 0, 0)
# time.sleep(seconds)
from datetime import datetime

ks = datetime.now()
print('运行开始时间', ks)

import datetime

now = day - datetime.timedelta(days=0)
print('starttime', now)
import calendar
import datetime
now = datetime.datetime.now()
this_month_start = datetime.datetime(now.year, now.month, 1).date()
this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date().strftime("%Y-%m-%d")
this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m-%d")

starttime=this_month
endtime=this_month_end

# db = pymysql.connect(
#     host='172.19.116.150',
#     user='zjzhzcuser',
#     passwd='F4dus0ee',
#     port=11806,
#     charset='utf8',
#     database='db_manage_overruns'
# )


""" 引入原始表 """
def data_d():
    # db = pymysql.connect(host="192.168.1.229", port=3306, user='root', password='zcits123456', charset='utf8',
    #                      database='db_manage_overruns')
    db = pymysql.connect(host="192.168.2.39", port=3306, user='zjzhzcuser', password='F4dus0ee',charset='utf8',
                         database='db_manage_overruns')

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

    ##原始表获取
    # sql = "select  out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_21 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'".format(
    #     starttime, endtime)
    # t_bas_pass_data_21 = get_df_from_db(sql)

    sql = """ 
    SELECT record_code FROM t_bas_over_data_31 where is_unusual = 0
    and out_station_time >= '{} 00:00:00' and  out_station_time <'{} 00:00:00'
    and allow is null   and overrun_rate >= 100 and total_weight <100 
    and area_county   in (330122,330183,330681) 
    and car_no not like '%浙%'
    """.format(starttime, endtime)
    超限100浙牌 = get_df_from_db(sql)
    超限100浙牌 = pd.DataFrame(超限100浙牌)

    sql = """SELECT record_code FROM t_bas_over_data_31 where is_unusual = 0 
    and out_station_time >= '{} 00:00:00' and  out_station_time <'{} 00:00:00' 
    and allow is null    and total_weight >= 100
    and area_city =330100
    """.format(starttime, endtime)
    百吨王 = get_df_from_db(sql)
    百吨王 = pd.DataFrame(百吨王)

    sql = """SELECT a.record_code
    FROM t_bas_over_data_31 a LEFT JOIN
    t_bas_over_data_collection_31 b 
    on a.record_code = b.record_code
    where a.is_unusual = 0 
    and a.out_station_time >= '{} 00:00:00' and  a.out_station_time <'{} 00:00:00'
    and a.allow is null  and a.area_province=330000 and a.overrun_rate >=100
    and b.`status` =15
    """.format(starttime, endtime)
    超限100初审不通过全省 = get_df_from_db(sql)
    超限100初审不通过全省 = pd.DataFrame(超限100初审不通过全省)

    sql = """SELECT a.record_code
    FROM t_bas_over_data_31 a LEFT JOIN
    t_bas_over_data_collection_31 b 
    on a.record_code = b.record_code
    where a.is_unusual = 0 
    and a.out_station_time >= '{} 00:00:00' and  a.out_station_time <'{} 00:00:00'
    and a.allow is null   and a.area_county in (330122,330813,330681,330603,330683)  and a.overrun_rate <100
    and a.total_weight >80 
    and b.`status` not in (3,4,5,6,12,13)
    """.format(starttime, endtime)
    初审不通过80吨以上 = get_df_from_db(sql)
    初审不通过80吨以上 = pd.DataFrame(初审不通过80吨以上)

    wide_table31 = pd.concat([超限100浙牌, 百吨王, 超限100初审不通过全省, 初审不通过80吨以上])
    record_code更新 = wide_table31['record_code'].to_list()
    record_code更新 = tuple(record_code更新)
    # print(record_code更新)
    if record_code更新:
        # wide_table31.to_excel(r'C:\Users\liu.wenjie\Desktop\月度更新.xlsx')
        '''数据更新'''
        db = pymysql.connect(host="192.168.2.39", port=3306, user='zjzhzcuser', password='F4dus0ee',
                             database='db_manage_overruns')

        mycursor = db.cursor()
        sql = "UPDATE t_bas_over_data_31 set is_unusual = 2 where record_code in {}".format(record_code更新)
        mycursor.execute(sql)
        db.commit()
        db.close()
    else:
      print('无数据需更新')
    from threading import Timer
    t2 = Timer(25, data_d)  # 此处使用递归调用实现
    t2.start()


if __name__ == "__main__":

    data_d()