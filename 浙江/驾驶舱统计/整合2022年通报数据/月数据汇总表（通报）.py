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
starttime='2023-04-01'
endtime='2023-05-01'
理应在线天数=30
db = pymysql.connect(
    host='172.19.116.150',
    user='zjzhzcuser',
    passwd='F4dus0ee',
    port=11806,
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


sqldb = pymysql.connect(
    host='172.19.116.150',
    user='zjzhzcuser',
    passwd='F4dus0ee',
    port=11806,
    charset='utf8',
    database='db_manage_overruns'
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
# sql = "select  out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_21 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'".format(
#     starttime, endtime)
# t_bas_pass_data_21 = get_df_from_db(sql)
sql = """ 
SELECT city_name,county_code,county_name,a.station_code,a.station_name,statistic_date,truck_num,
overrun_num,no_car_num,overrun_0_10,overrun_10_20
FROM t_bas_basic_data_pass a
LEFT JOIN t_sys_station b on a.station_code=b.station_code
where a.statistic_date  >='{}'
and a.statistic_date  <'{}'
and a.station_type =31
and b.station_status = 0
""".format(
    starttime, endtime)
pass_truck_num = get_df_from_db(sql)
pass_truck_num = pd.DataFrame(pass_truck_num)
# pass_truck_num.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\pass_truck_num.xlsx')

df=pass_truck_num.groupby(['city_name','county_code','county_name','station_code','station_name']).sum().reset_index()
pass_truck_num = pass_truck_num[(0 <pass_truck_num['truck_num'])]
pass_truck_num['statistic_date'] = pd.to_datetime(pass_truck_num['statistic_date'])
pass_truck_num['取日'] = pass_truck_num['statistic_date'].apply(lambda x: x.strftime('%d'))
实际在线天数 = pass_truck_num.groupby(['station_code'])['取日'].nunique().reset_index( name='实际在线天数')
df = pd.merge(df, 实际在线天数, on=['station_code'], how='left')
df['理应在线天数'] = 理应在线天数
df['在线率'] = (df['实际在线天数']/df['理应在线天数']*100).round(2)
df['百吨王数'] = 0
df['超限100%数'] = 0
# city_name,county_code,county_name,a.station_code,a.station_name,statistic_date,truck_num,
# overrun_num,no_car_num,overrun_0_10,overrun_10_20
df['超限率(%)'] = (df['overrun_num']/df['truck_num']*100).round(2)
df['超限10%除外数'] = df['overrun_num']-df['overrun_0_10']
df['超限10%除外超限率(%)'] = (df['超限10%除外数']/df['truck_num']*100).round(2)
df['超限20%除外数'] = df['overrun_num']-df['overrun_0_10']-df['overrun_10_20']
df['超限20%除外超限率(%)'] = (df['超限20%除外数']/df['truck_num']*100).round(2)
df['最后接收时间'] = now
df = df.fillna(value=0)

df.rename(
    columns={'station_name': '站点名称',
             'city_name': '地市', 'county_name': '区县', 'truck_num': '货车数',
             'overrun_num': '超限数'}, inplace=True)

df = pd.DataFrame(df,columns=['站点名称', '地市', '区县','理应在线天数', '实际在线天数', '在线率', '货车数', '超限数','百吨王数', '超限100%数', '超限10%除外超限率(%)', '超限20%除外数','超限20%除外超限率(%)', '超限率(%)', '最后接收时间','county_code', 'station_code'])
df = df.sort_values('county_code', ascending=True)
df.to_excel(r'C:\Users\liu.wenjie\Desktop\导出数据\test0625.xlsx')