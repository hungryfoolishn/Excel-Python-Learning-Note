c=11
j=10
while c>=0:
    import pymysql
    import pandas as pd
    from datetime import datetime
    from urllib import parse
    import calendar

    day = datetime.now().date()  # 获取当前系统时间

    today = datetime.now()
    import time


    def sleeptime(hour, min, sec):
        return 3600 * hour + 60 * min + sec


    print('暂停：', sleeptime(0, 0, 0), '秒')
    seconds = sleeptime(0, 0, 0)
    time.sleep(seconds)
    from datetime import datetime

    ks = datetime.now()
    print('运行开始时间', ks)

    import datetime
    from dateutil.relativedelta import relativedelta

    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

    starttime = this_month_start - relativedelta(months=c)
    print('starttime', starttime)
    endtime = this_month_start - relativedelta(months=j)
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


    """ 引入原始表 """

    ##原始表获取
    # sql = "select  out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_21 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'".format(
    #     starttime, endtime)
    # t_bas_pass_data_21 = get_df_from_db(sql)
    sql = "SELECT city,county,car_no,monitor_state,intercept_state FROM t_fence_alarm_vehicle_info WHERE alarm_time>= '{}' AND alarm_time < '{}' and deleted=0 ".format(
        starttime, endtime)
    t_fence_alarm_vehicle_info = get_df_from_db(sql)

    sql = "SELECT b.county,count(alarm_id) as pt_num " \
          "FROM t_monitor_alarm_vehicle_message a INNER JOIN t_fence_alarm_vehicle_info b on a.alarm_id = b.id " \
          "where a.send_time>='{}' " \
          "and a.send_time<'{}' and a.STATUS =1 GROUP BY b.county".format(starttime, endtime)
    t_monitor_alarm_vehicle_message = get_df_from_db(sql)

    sql = "SELECT city as city_name,county as county_name,county_code as county FROM t_code_area"
    t_code_area = get_df_from_db(sql)

    # t_bas_basic_data_pass71.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\t_bas_basic_data_pass71.xlsx')

    # 区县汇总=t_bas_basic_data_pass.groupby(['地市','区县']).agg({'货车数': ['sum'],'超限数': ['sum'],'剔除10%超限数（不包含临界点）': ['sum'],'剔除20%超限数（不包含临界点）': ['sum']})

    ##县级
    # t_fence_alarm_vehicle_info.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\12月\t_fence_alarm_vehicle_infowu.xlsx')
    t_fence_alarm_vehicle_info = t_fence_alarm_vehicle_info.drop_duplicates(subset=['car_no'])
    # t_fence_alarm_vehicle_info.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\12月\t_fence_alarm_vehicle_info.xlsx')

    yj_num = t_fence_alarm_vehicle_info.groupby(['city', 'county'])['car_no'].count().reset_index(name='yj_num')
    bk_num = t_fence_alarm_vehicle_info[(t_fence_alarm_vehicle_info.monitor_state != 0)]
    bk_num = bk_num.groupby(['city', 'county'])['car_no'].count().reset_index(name='bk_num')
    lj_num = t_fence_alarm_vehicle_info[(t_fence_alarm_vehicle_info.intercept_state == 2)]
    lj_num = lj_num.groupby(['city', 'county'])['car_no'].count().reset_index(name='lj_num')

    U_xj = pd.merge(yj_num, bk_num, on=['city', 'county'], how='left')
    U_xj = pd.merge(U_xj, lj_num, on=['city', 'county'], how='left')
    U_xj = pd.merge(U_xj, t_monitor_alarm_vehicle_message, on=['county'], how='left')
    U_xj = pd.merge(U_xj, t_code_area, on=['county'], how='left')
    U_xj['statistics_date'] = starttime.strftime("%Y-%m")
    U_xj = U_xj.fillna(value=0)

    ##市级
    U_ds = U_xj.groupby(['statistics_date', 'city', 'city_name']).sum().reset_index()

    ##省级
    U_sj = U_ds.groupby(['statistics_date']).sum().reset_index()
    U_sj['area_code'] = '330000'
    U_sj['area_name'] = '浙江'

    ##合并

    U_xj = pd.DataFrame(U_xj,
                        columns=['statistics_date',
                                 'county', 'county_name', 'yj_num', 'bk_num', 'lj_num', 'pt_num'])
    U_ds = pd.DataFrame(U_ds,
                        columns=['statistics_date',
                                 'city', 'city_name', 'yj_num', 'bk_num', 'lj_num', 'pt_num'])

    U_xj.rename(
        columns={'county': 'area_code', 'county_name': 'area_name'}, inplace=True)
    U_ds.rename(
        columns={'city': 'area_code', 'city_name': 'area_name'}, inplace=True)

    U_all = pd.concat([U_xj, U_ds, U_sj])
    starttime1 = starttime.strftime("%Y%m")
    U_all = U_all.fillna(0)
    U_all['id'] = U_all['area_code'].astype('string') + starttime1
    U_all['lj_rate'] = (U_all['lj_num'] / (U_all['bk_num'] + 0.00001) * 100).round(2)
    U_all['DX_num'] = 0

    U_all = U_all.applymap(str)

    '''数据库删除'''
    db = pymysql.connect(host="172.19.116.150", port=11806, user='zjzhzcuser', password='F4dus0ee',
                         database='db_manage_overruns')
    # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
    #                  database='jingdong_ceshi')
    mycursor = db.cursor()
    sql = "DELETE FROM t_bas_fence_alarm_vehicle_statistics_data WHERE statistics_date = '{}'".format(
        starttime.strftime("%Y-%m"))
    mycursor.execute(sql)
    db.commit()
    db.close()
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
        U_all.to_sql(name='t_bas_fence_alarm_vehicle_statistics_data', con=engine, if_exists='append', index=False)
    except Exception as e:
        print("mysql插入失败")

    # import pymysql
    #
    #
    # class DBUtils:
    #     """
    #     数据库工具类
    #     """
    #
    #     """:param
    #     db:     数据库连接:  db = pymysql.connect(host='192.168.1.1', user='root', password='1234', port=3306, db='database_name')
    #     cursor: 数据库游标:  cursor = db.cursor()
    #     data:   需写入数据:  Dataframe
    #     table:  写入表名
    #     """
    #
    #     def __init__(self, db, cursor, data, table):
    #         self.db = db
    #         self.cursor = cursor
    #         self.data = data
    #         self.table = table
    #
    #     # 按主键去重追加更新
    #     def insert_data(self):
    #         keys = ', '.join('`' + self.data.keys() + '`')
    #         values = ', '.join(['%s'] * len(self.data.columns))
    #         # 根据表的唯一主键去重追加更新
    #         sql = 'INSERT INTO {table}({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE'.format(table=self.table,
    #                                                                                              keys=keys,
    #                                                                                              values=values)
    #         update = ','.join(["`{key}` = %s".format(key=key) for key in self.data])
    #         sql += update
    #
    #         for i in range(len(self.data)):
    #             try:
    #                 self.cursor.execute(sql, tuple(self.data.loc[i]) * 2)
    #                 self.db.commit()
    #             except Exception as e:
    #                 print("数据写入失败,原因为:" + e)
    #                 self.db.rollback()
    #
    #         self.cursor.close()
    #         self.db.close()
    #         print('数据已全部写入完成!')
    #
    #
    # U_all.fillna("", inplace=True)  # 替换NaN,否则数据写入时会报错,也可替换成其他
    # # 连接数据库,定义变量
    # db = pymysql.connect(host='172.19.116.150', user='zjzhzcuser', password='F4dus0ee', port=11806,
    #                      db='db_manage_overruns')
    # cursor = db.cursor()
    # table = "t_bas_fence_alarm_vehicle_statistics_data"  # 写入表名
    #
    # # 写入数据
    # DBUtils.insert_data(DBUtils(db, cursor, U_all, table))
    from datetime import datetime
    js = datetime.now()
    sjc = js - ks
    print('运行耗时', sjc)
    c -= 1
    j -= 1