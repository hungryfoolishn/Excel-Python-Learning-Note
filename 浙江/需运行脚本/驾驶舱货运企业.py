# -*- coding:utf-8 -*-

from retrying import retry



@retry(stop_max_attempt_number=300, stop_max_delay=2000, wait_fixed=500)
def t_bas_basic_station():
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

    starttime = day - datetime.timedelta(days=0)
    print('starttime', starttime)
    endtime = day + datetime.timedelta(days=1)
    print('endtime', endtime)
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

    ##超限超载
    # sql = "select  out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_21 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'".format(
    #     starttime, endtime)
    # t_bas_pass_data_21 = get_df_from_db(sql)
    sql = "SELECT a.company_name,b.car_no_color,b.car_no FROM t_bas_track_company a LEFT JOIN t_bas_track_company_car b on a.id = b.company_id where a.is_deleted = 0 and b.is_deleted = 0 and a.area_county ='330122'"
    t_bas_track_company_car = get_df_from_db(sql)

    sql = "SELECT a.car_no ,a.out_station_time ,a.axis,a.total_weight , a.limit_weight ,a.overrun_rate,a.site_name ,31 as 'station_type', b.city ,b.county ,a.record_code FROM t_bas_over_data_31 a  LEFT JOIN t_code_area b ON a.area_county = b.county_code " \
          "where a.area_county ='330122' and a.out_station_time >= '{} 00:00:00' AND a.out_station_time < '{} 00:00:00'".format(starttime, endtime)
    t_bas_over_data_31 = get_df_from_db(sql)
    sql = "SELECT c.status ,c.record_code FROM t_bas_over_data_collection_31 c " \
          "where c.area_county ='330122' and c.out_station_time >= '{} 00:00:00' AND c.out_station_time < '{} 00:00:00'".format(starttime, endtime)
    t_bas_over_data_collection_31 = get_df_from_db(sql)
    sql = "SELECT a.car_no ,a.out_station_time ,a.axis,a.total_weight , a.limit_weight ,a.overrun_rate,a.site_name ,71 as 'station_type', b.city ,b.county ,a.record_code FROM t_bas_over_data_71 a  LEFT JOIN t_code_area b ON a.area_county = b.county_code " \
          "where a.area_county ='330122' and a.out_station_time >= '{} 00:00:00' AND a.out_station_time < '{} 00:00:00'".format(starttime, endtime)
    t_bas_over_data_71 = get_df_from_db(sql)
    sql = "SELECT c.status ,c.record_code FROM t_bas_over_data_collection_71 c " \
          "where c.area_county ='330122' and c.out_station_time >= '{} 00:00:00' AND c.out_station_time < '{} 00:00:00'".format(starttime, endtime)
    t_bas_over_data_collection_71 = get_df_from_db(sql)
    t_bas_over_data_31 = pd.merge(t_bas_over_data_31, t_bas_over_data_collection_31, on='record_code', how='left')
    t_bas_over_data_71 = pd.merge(t_bas_over_data_71, t_bas_over_data_collection_71, on='record_code', how='left')
    wide_table = pd.concat([t_bas_over_data_31, t_bas_over_data_71])
    wide_table = pd.merge(wide_table, t_bas_track_company_car, on='car_no', how='inner')
    t_bas_over_data = wide_table.fillna(value=0)
    # print(wide_table)
    t_bas_over_data.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\pass_truck_num.xlsx')

    ##非法改装等
    import datetime
    import calendar
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, 1, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

    starttime2 = this_month_start + datetime.timedelta(days=0)
    print('starttime', starttime2)
    endtime2 = this_month_end + datetime.timedelta(days=1)
    print('endtime', endtime2)
    sql = "SELECT a.car_number as car_no ,a.over_time as out_station_time,a.axis_count as axis,a.total_weight , a.limit_Weight as limit_weight ,a.reason,a.audit_result ,a.station_name , b.city ,b.county ,a.record_id FROM t_bas_police_road_offsite a LEFT JOIN t_code_area b ON a.area_county = b.county_code " \
          "WHERE  a.audit_time >= '{} 00:00:00' and a.audit_time < '{} 00:00:00' and audit_result=1 and a.area_county = '330122'".format(starttime2, endtime2)
    t_bas_police_road_offsite = get_df_from_db(sql)
    t_bas_police_road_offsite = pd.merge(t_bas_police_road_offsite, t_bas_track_company_car, on='car_no', how='inner')
    t_bas_police_road_offsite = t_bas_police_road_offsite.fillna(value=0)
    t_bas_police_road_offsite.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\t_bas_police_road_offsite.xlsx')

    ##非法改装等
    sql = "SELECT object_name as car_no,alarm_time,alarm_reason,station_name,paidan_time,department_name,deal_status,deal_opinions,deal_time,alarm_type FROM tl_mul_link_gov a  where alarm_time>='{} 00:00:00' and alarm_time<'{} 00:00:00' ".format(starttime, endtime)
    tl_mul_link_gov = get_df_from_db(sql)
    tl_mul_link_gov1 = pd.merge(tl_mul_link_gov, t_bas_track_company_car, on='car_no', how='inner')
    tl_mul_link_gov1 = tl_mul_link_gov1.fillna(value=0)
    tl_mul_link_gov1.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\pass_truck_num2.xlsx')


     ##写入数据库
    # '''数据库删除'''
    # db = pymysql.connect(host="172.19.116.150", port=11806, user='zjzhzcuser', password='F4dus0ee',
    #                      database='db_manage_overruns')
    # # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
    # #                  database='jingdong_ceshi')
    # mycursor = db.cursor()
    # sql = "DELETE FROM t_bas_company_pass_statistics_data WHERE statistics_date = '{}'".format(starttime2)
    # mycursor.execute(sql)
    # db.commit()
    # db.close()
    try:
        from sqlalchemy import create_engine

        user = "zjzhzcuser"
        password = "F4dus0ee"
        host = "172.19.116.150"
        db = "db_manage_overruns"

        pwd = parse.quote_plus(password)

        engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:11806/{db}?charset=utf8")
        # result_table
        # 要写入的数据表，这样写的话要提前在数据库建好表
        # t_bas_though.to_sql(name='t_bas_car_through', con=engine, if_exists='append', index=False)
        # result_table
        # 要写入的数据表，这样写的话要提前在数据库建好表
        t_bas_over_data.to_sql(name='t_statistic_over_data', con=engine, if_exists='append', index=False)
        t_bas_police_road_offsite.to_sql(name='t_statistic_police_road_offsite', con=engine, if_exists='append', index=False)
        tl_mul_link_gov1.to_sql(name='hy_statistic_link_gov', con=engine, if_exists='append', index=False)
    except Exception as e:
        print("mysql插入失败")


def demo1():
    try:
        t_bas_basic_station()
    except Exception as e:
        print(e)
        raise TimeoutError


if __name__ == '__main__':
    demo1()