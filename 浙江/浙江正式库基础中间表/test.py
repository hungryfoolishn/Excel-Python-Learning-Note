i=int(0)
I=int(1)
while i!=I:

    import pymysql
    import pandas as pd
    from datetime import datetime
    from urllib import parse

    day = datetime.now().date()  # 获取当前系统时间

    today = datetime.now()
    import time


    def sleeptime(hour, min, sec):
        return 3600 * hour + 60 * min + sec


    print('暂停：', sleeptime(0, 0, 20), '秒')
    seconds = sleeptime(0, 0, 20)
    time.sleep(seconds)
    from datetime import datetime

    ks = datetime.now()
    print('运行开始时间', ks)

    import datetime

    starttime = day - datetime.timedelta(days=0)
    print('starttime', starttime)
    endtime = day + datetime.timedelta(days=1)
    print('endtime', endtime)
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


    sqldb = pymysql.connect(
        host='192.168.2.119',
        user='zjzhzcuser',
        passwd='F4dus0ee',
        port=3306,
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
    sql = "select  area_city,area_county,out_station,out_station_time,insert_time,car_no,total_weight,limit_weight from t_bas_pass_data_31 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00' ".format(
        starttime, endtime)
    t_bas_pass_data_31 = get_df_from_db(sql)

    # and area_county = 330781

    sql = "select  area_city,area_county,out_station,out_station_time,insert_time,car_no,total_weight,limit_weight from t_bas_pass_data_71 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'  ".format(
        starttime, endtime)
    t_bas_pass_data_71 = get_df_from_db(sql)

    # sql = "select out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_71 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00' ".format(
    #     starttime, endtime)
    # t_bas_pass_data_71 = get_df_from_db(sql)
    sql1 = "select  station_code,station_status,station_type,station_name, area_county from t_sys_station where is_deleted = 0  and station_status=0 and station_type in (31,71) "
    t_sys_station = get_df_from_db2(sql1)
    sql1 = "select city_code,county_code,city,county FROM t_code_area  "
    t_code_area = get_df_from_db2(sql1)
    # sql1 = "select car_no,province_code,city_code from t_bas_province_car"
    # t_bas_province_car = get_df_from_db2(sql1)
    import datetime

    # ##倒退N小时取站点编码
    # datetime3 = today - datetime.timedelta(minutes=1440)
    # sql = "select  out_station  from t_bas_pass_data_21 where insert_time >= '{} 00:00:00' ".format(
    #     datetime3)
    # t_station_21 = get_df_from_db(sql)
    # sql = "select  out_station  from t_bas_pass_data_31 where insert_time >= '{} 00:00:00'  ".format(
    #     datetime3)
    # t_station_31 = get_df_from_db(sql)
    # sql = "select  out_station from t_bas_pass_data_41 where insert_time >= '{} 00:00:00'  ".format(
    #     datetime3)
    # t_station_41 = get_df_from_db(sql)
    # sql = "select  out_station from t_bas_pass_data_71 where insert_time >= '{} 00:00:00'  ".format(
    #     datetime3)
    # t_station_71 = get_df_from_db(sql)
    # t_station = t_station_21.append([t_station_31, t_station_41, t_station_71])
    # # t_station.to_excel('C:/Users/Administrator/Desktop/t_station.xlsx')

    ##站点区域表连接
    U_station_area = pd.merge(t_sys_station, t_code_area, left_on='area_county', right_on='county_code', how='left')
    # U_station_area.to_excel('C:/Users/Administrator/Desktop/U_station_area.xlsx')

    U_station_area.rename(
        columns={'city': 'city_name', 'county': 'county_name'}, inplace=True)
    U_station_area = pd.DataFrame(U_station_area,
                                  columns=['city_code', 'city_name', 'county_code', 'county_name', 'station_code',
                                           'station_name', 'station_status', 'station_type'])

    ##各表car_no空值填充
    # U_pass21_area_station = t_bas_pass_data_21
    # U_pass21_area_station['car_no'] = U_pass21_area_station['car_no'].fillna(value=0)
    # # U_pass21_area_station['car_no'] = U_pass21_area_station['car_no'].apply(lambda car_no: car_no[:2])
    t_bas_pass_data_31['out_station_time'] = t_bas_pass_data_31['out_station_time'].apply(lambda x: x.strptime(x, '%Y-%m-%d'))
    t_bas_pass_data_31['insert_time'] = t_bas_pass_data_31['insert_time'].apply(lambda x: x.strptime(x, '%Y-%m-%d'))
    U_pass31_area_station = t_bas_pass_data_31
    U_pass31_area_station['car_no'] = U_pass31_area_station['car_no'].fillna(value=0)



    i = {
        "330100": 1.1,
        "330200": 1,
        "330300": 1,
        "330400": 1.1,
        "330500": 1,
        "330600": 1,
        "330700": 1,
        "330800": 1.1,
        "330900": 1.1,
        "331000": 1.1,
        "331100": 1.1,
        "330122": 1.2,
        "330183": 1.21,
        "330523": 1.03,
        "330603": 1.1,
        "330604": 1.2,
        "330624": 1.2,
        "330681": 1.1,
        "330703": 1.2,
        "330782": 1.1
    }
    for item in i.items():
        key = item[0]
        value = item[1]
        U_pass31_area_station.loc[
            ((U_pass31_area_station['area_county'] == key) | (U_pass31_area_station['area_city'] == key)) & (
                    U_pass31_area_station['total_weight'] < 100), 'limit_weight'] = U_pass31_area_station[
            'limit_weight'].map(
            lambda x: float(x) * value).round(0)
    U_pass31_area_station['limit_weight'] = U_pass31_area_station['limit_weight'].fillna(value=1)
    U_pass31_area_station.loc[U_pass31_area_station['limit_weight']  < 1, 'limit_weight'] = 18
    U_pass31_area_station['total_weight'] = U_pass31_area_station['total_weight'].astype('float')
    U_pass31_area_station['limit_weight'] = U_pass31_area_station['limit_weight'].astype('float')
    U_pass31_area_station['overrun_rate'] = U_pass31_area_station.apply(
        lambda x: (x['total_weight'] - x['limit_weight']) * 100/ x['limit_weight'], axis=1).round(4)

    # U_pass31_area_station['car_no'] = U_pass31_area_station['car_no'].apply(lambda car_no: car_no[:2])


    t_bas_pass_data_71['out_station_time'] = t_bas_pass_data_71['out_station_time'].apply(lambda x: x.strptime(x, '%Y-%m-%d'))
    t_bas_pass_data_71['insert_time'] = t_bas_pass_data_71['insert_time'].apply(lambda x: x.strptime(x, '%Y-%m-%d'))
    U_pass71_area_station = t_bas_pass_data_71
    U_pass71_area_station['car_no'] = U_pass71_area_station['car_no'].fillna(value=0)
    # # U_pass41_area_station['car_no'] = U_pass41_area_station['car_no'].apply(lambda car_no: car_no[:2])
    # U_pass71_area_station = t_bas_pass_data_71
    # U_pass71_area_station['car_no'] = U_pass71_area_station['car_no'].fillna(value=0)
    # # U_pass71_area_station['car_no'] = U_pass71_area_station['car_no'].apply(lambda car_no: car_no[:2])

    U_pass31_area_station = pd.DataFrame(U_pass31_area_station,
                                         columns=['out_station', 'out_station_time','insert_time', 'car_no', 'total_weight',
                                                  'limit_weight',
                                                  'overrun_rate'])

    U_pass71_area_station = pd.DataFrame(U_pass71_area_station,
                                         columns=['out_station', 'out_station_time', 'insert_time', 'car_no', 'total_weight',
                                                  'limit_weight',
                                                  'overrun_rate'])

    #31表
    #建立关键字段
    wide_table_31 = U_pass31_area_station.groupby(
        ['out_station', 'out_station_time', 'insert_time'])[
        'car_no'].count().reset_index(name='pass_num')
    wide_table_31 = pd.merge(wide_table_31, t_sys_station, left_on='out_station', right_on='station_code',
                             how='left')
    wide_table_31 = pd.DataFrame(wide_table_31)

    T0_5T = U_pass31_area_station[
        ((0 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 5))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T0_5T')

    U_pass31_area_station = U_pass31_area_station[(U_pass31_area_station.total_weight >= 2.5)]

    truck_num = U_pass31_area_station.groupby(['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(
        name='truck_num')
    wide_table_31 = pd.merge(wide_table_31, truck_num, on=['out_station','out_station_time', 'insert_time'], how='left')
    over_num = U_pass31_area_station[(U_pass31_area_station['overrun_rate'] > 0)].groupby(['out_station'])[
        'car_no'].count().reset_index(name='over_num')
    wide_table_31 = pd.merge(wide_table_31, over_num, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = wide_table_31.fillna(value=0)
    wide_table_31['over_rate'] = (wide_table_31['over_num'] / wide_table_31['pass_num'] * 100).round(2)
    wide_table_31['truck_over_rate'] = (wide_table_31['over_num'] / wide_table_31['truck_num'] * 100).round(2)
    wide_table_31['no_over'] = wide_table_31['truck_num'] - wide_table_31['over_num']
    no_car = \
        U_pass31_area_station[(U_pass31_area_station['car_no'].str.len() <= 5)].groupby(['out_station','out_station_time', 'insert_time'])[
            'car_no'].count().reset_index(name='no_car')
    wide_table_31 = pd.merge(wide_table_31, no_car, on=['out_station','out_station_time', 'insert_time'], how='left')

    ##超限程度分布
    C0_10X = U_pass31_area_station[
        ((0 < U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 10))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C0_10X')
    C10_20X = U_pass31_area_station[
        ((10 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 20))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C10_20X')
    C20_30X = U_pass31_area_station[
        ((20 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 30))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C20_30X')
    C30_40X = U_pass31_area_station[
        ((30 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 40))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C30_40X')
    C40_50X = U_pass31_area_station[
        ((40 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 50))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C40_50X')
    C50_60X = U_pass31_area_station[
        ((50 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 60))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C50_60X')
    C60_70X = U_pass31_area_station[
        ((60 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 70))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C60_70X')
    C70_80X = U_pass31_area_station[
        ((70 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 80))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C70_80X')
    C80_90X = U_pass31_area_station[
        ((80 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 90))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C80_90X')
    C90_100X = U_pass31_area_station[
        ((90 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 100))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C90_100X')
    C100X = U_pass31_area_station[(100 <= U_pass31_area_station['overrun_rate'])].groupby(['out_station','out_station_time', 'insert_time'])[
        'car_no'].count().reset_index(name='C100X')

    ##聚合超限程度
    wide_table_31 = pd.merge(wide_table_31, C0_10X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C10_20X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C20_30X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C30_40X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C40_50X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C50_60X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C60_70X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C70_80X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C80_90X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C90_100X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, C100X, on=['out_station','out_station_time', 'insert_time'], how='left')

    ##吨位分布
    T0_5T = T0_5T
    T5_10T = U_pass31_area_station[
        ((5 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 10))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T5_10T')
    T10_20T = U_pass31_area_station[
        ((10 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 20))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T10_20T')
    T20_30T = U_pass31_area_station[
        ((20 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 30))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T20_30T')
    T30_40T = U_pass31_area_station[
        ((30 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 40))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T30_40T')
    T40_50T = U_pass31_area_station[
        ((40 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 50))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T40_50T')
    T50_60T = U_pass31_area_station[
        ((50 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 60))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T50_60T')
    T60_70T = U_pass31_area_station[
        ((60 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 70))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T60_70T')
    T70_80T = U_pass31_area_station[
        ((70 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 80))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T70_80T')
    T80_90T = U_pass31_area_station[
        ((80 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 90))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T80_90T')
    T90_100T = U_pass31_area_station[
        ((90 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 100))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T90_100T')
    T100T = U_pass31_area_station[(100 <= U_pass31_area_station['total_weight'])].groupby(['out_station','out_station_time', 'insert_time'])[
        'car_no'].count().reset_index(name='T100T')

    ##聚合吨位分布
    wide_table_31 = pd.merge(wide_table_31, T0_5T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T5_10T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T10_20T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T20_30T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T30_40T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T40_50T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T50_60T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T60_70T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T70_80T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T80_90T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T90_100T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_31 = pd.merge(wide_table_31, T100T, on=['out_station','out_station_time', 'insert_time'], how='left')

    wide_table_31 = wide_table_31.fillna(value=0)

    wide_table_31.rename(
        columns={'over_num': 'overrun_num',
                 'out_station_time': 'statistics_date', 'no_over': 'overrun_0', 'no_car': 'no_car_num',
                 'C0_10X': 'overrun_0_10', 'C10_20X': 'overrun_10_20', 'C20_30X': 'overrun_20_30',
                 'C30_40X': 'overrun_30_40', 'C40_50X': 'overrun_40_50', 'C50_60X': 'overrun_50_60',
                 'C60_70X': 'overrun_60_70', 'C70_80X': 'overrun_70_80', 'C80_90X': 'overrun_80_90',
                 'C90_100X': 'overrun_90_100',
                 'C100X': 'overrun_100', 'T0_5T': 'total_weight_0_5', 'T5_10T': 'total_weight_5_10',
                 'T10_20T': 'total_weight_10_20', 'T20_30T': 'total_weight_20_30',
                 'T30_40T': 'total_weight_30_40', 'T40_50T': 'total_weight_40_50', 'T50_60T': 'total_weight_50_60',
                 'T60_70T': 'total_weight_60_70', 'T70_80T': 'total_weight_70_80', 'T80_90T': 'total_weight_80_90',
                 'T90_100T': 'total_weight_90_100', 'T100T': 'total_weight_100'}, inplace=True)

    wide_table_31 = pd.DataFrame(wide_table_31,
                                 columns=['out_station', 'statistics_date','insert_time','pass_num', 'truck_num',
                                          'overrun_num', 'over_rate', 'truck_over_rate', 'no_car_num', 'overrun_0',
                                          'overrun_0_10', 'overrun_10_20', 'overrun_20_30', 'overrun_30_40',
                                          'overrun_40_50', 'overrun_50_60', 'overrun_60_70',
                                          'overrun_70_80', 'overrun_80_90', 'overrun_90_100', 'overrun_100',
                                          'total_weight_0_5', 'total_weight_5_10', 'total_weight_10_20',
                                          'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50',
                                          'total_weight_50_60', 'total_weight_60_70', 'total_weight_70_80',
                                          'total_weight_80_90',
                                          'total_weight_90_100', 'total_weight_100'])

    # though_area_31.to_excel('C:/Users/Administrator/Desktop/though_area_31.xlsx')

    #71表
    #建立关键字段
    wide_table_71 = U_pass71_area_station.groupby(
        ['out_station', 'out_station_time', 'insert_time'])[
        'car_no'].count().reset_index(name='pass_num')
    wide_table_71 = pd.merge(wide_table_71, t_sys_station, left_on='out_station', right_on='station_code',
                             how='left')
    wide_table_71 = pd.DataFrame(wide_table_71)

    T0_5T = U_pass71_area_station[
        ((0 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 5))].groupby(
        ['out_station'])['car_no'].count().reset_index(name='T0_5T')

    U_pass71_area_station = U_pass71_area_station[(U_pass71_area_station.total_weight >= 2.5)]

    truck_num = U_pass71_area_station.groupby(['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(
        name='truck_num')
    wide_table_71 = pd.merge(wide_table_71, truck_num, on=['out_station','out_station_time', 'insert_time'], how='left')
    over_num = U_pass71_area_station[(U_pass71_area_station['overrun_rate'] > 0)].groupby(['out_station'])[
        'car_no'].count().reset_index(name='over_num')
    wide_table_71 = pd.merge(wide_table_71, over_num, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = wide_table_71.fillna(value=0)
    wide_table_71['over_rate'] = (wide_table_71['over_num'] / wide_table_71['pass_num'] * 100).round(2)
    wide_table_71['truck_over_rate'] = (wide_table_71['over_num'] / wide_table_71['truck_num'] * 100).round(2)
    wide_table_71['no_over'] = wide_table_71['truck_num'] - wide_table_71['over_num']
    no_car = \
        U_pass71_area_station[(U_pass71_area_station['car_no'].str.len() <= 5)].groupby(['out_station','out_station_time', 'insert_time'])[
            'car_no'].count().reset_index(name='no_car')
    wide_table_71 = pd.merge(wide_table_71, no_car, on=['out_station','out_station_time', 'insert_time'], how='left')

    ##超限程度分布
    C0_10X = U_pass71_area_station[
        ((0 < U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 10))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C0_10X')
    C10_20X = U_pass71_area_station[
        ((10 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 20))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C10_20X')
    C20_30X = U_pass71_area_station[
        ((20 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 30))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C20_30X')
    C30_40X = U_pass71_area_station[
        ((30 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 40))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C30_40X')
    C40_50X = U_pass71_area_station[
        ((40 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 50))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C40_50X')
    C50_60X = U_pass71_area_station[
        ((50 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 60))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C50_60X')
    C60_70X = U_pass71_area_station[
        ((60 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 70))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C60_70X')
    C70_80X = U_pass71_area_station[
        ((70 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 80))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C70_80X')
    C80_90X = U_pass71_area_station[
        ((80 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 90))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C80_90X')
    C90_100X = U_pass71_area_station[
        ((90 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 100))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='C90_100X')
    C100X = U_pass71_area_station[(100 <= U_pass71_area_station['overrun_rate'])].groupby(['out_station','out_station_time', 'insert_time'])[
        'car_no'].count().reset_index(name='C100X')

    ##聚合超限程度
    wide_table_71 = pd.merge(wide_table_71, C0_10X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C10_20X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C20_30X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C30_40X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C40_50X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C50_60X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C60_70X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C70_80X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C80_90X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C90_100X, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, C100X, on=['out_station','out_station_time', 'insert_time'], how='left')

    ##吨位分布
    T0_5T = T0_5T
    T5_10T = U_pass71_area_station[
        ((5 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 10))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T5_10T')
    T10_20T = U_pass71_area_station[
        ((10 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 20))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T10_20T')
    T20_30T = U_pass71_area_station[
        ((20 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 30))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T20_30T')
    T30_40T = U_pass71_area_station[
        ((30 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 40))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T30_40T')
    T40_50T = U_pass71_area_station[
        ((40 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 50))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T40_50T')
    T50_60T = U_pass71_area_station[
        ((50 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 60))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T50_60T')
    T60_70T = U_pass71_area_station[
        ((60 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 70))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T60_70T')
    T70_80T = U_pass71_area_station[
        ((70 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 80))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T70_80T')
    T80_90T = U_pass71_area_station[
        ((80 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 90))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T80_90T')
    T90_100T = U_pass71_area_station[
        ((90 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 100))].groupby(
        ['out_station','out_station_time', 'insert_time'])['car_no'].count().reset_index(name='T90_100T')
    T100T = U_pass71_area_station[(100 <= U_pass71_area_station['total_weight'])].groupby(['out_station','out_station_time', 'insert_time'])[
        'car_no'].count().reset_index(name='T100T')

    ##聚合吨位分布
    wide_table_71 = pd.merge(wide_table_71, T0_5T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T5_10T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T10_20T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T20_30T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T30_40T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T40_50T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T50_60T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T60_70T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T70_80T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T80_90T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T90_100T, on=['out_station','out_station_time', 'insert_time'], how='left')
    wide_table_71 = pd.merge(wide_table_71, T100T, on=['out_station','out_station_time', 'insert_time'], how='left')

    wide_table_71 = wide_table_71.fillna(value=0)

    wide_table_71.rename(
        columns={'over_num': 'overrun_num',
                 'out_station_time': 'statistics_date', 'no_over': 'overrun_0', 'no_car': 'no_car_num',
                 'C0_10X': 'overrun_0_10', 'C10_20X': 'overrun_10_20', 'C20_30X': 'overrun_20_30',
                 'C30_40X': 'overrun_30_40', 'C40_50X': 'overrun_40_50', 'C50_60X': 'overrun_50_60',
                 'C60_70X': 'overrun_60_70', 'C70_80X': 'overrun_70_80', 'C80_90X': 'overrun_80_90',
                 'C90_100X': 'overrun_90_100',
                 'C100X': 'overrun_100', 'T0_5T': 'total_weight_0_5', 'T5_10T': 'total_weight_5_10',
                 'T10_20T': 'total_weight_10_20', 'T20_30T': 'total_weight_20_30',
                 'T30_40T': 'total_weight_30_40', 'T40_50T': 'total_weight_40_50', 'T50_60T': 'total_weight_50_60',
                 'T60_70T': 'total_weight_60_70', 'T70_80T': 'total_weight_70_80', 'T80_90T': 'total_weight_80_90',
                 'T90_100T': 'total_weight_90_100', 'T100T': 'total_weight_100'}, inplace=True)

    wide_table_71 = pd.DataFrame(wide_table_71,
                                 columns=['out_station', 'statistics_date','insert_time','pass_num', 'truck_num',
                                          'overrun_num', 'over_rate', 'truck_over_rate', 'no_car_num', 'overrun_0',
                                          'overrun_0_10', 'overrun_10_20', 'overrun_20_30', 'overrun_30_40',
                                          'overrun_40_50', 'overrun_50_60', 'overrun_60_70',
                                          'overrun_70_80', 'overrun_80_90', 'overrun_90_100', 'overrun_100',
                                          'total_weight_0_5', 'total_weight_5_10', 'total_weight_10_20',
                                          'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50',
                                          'total_weight_50_60', 'total_weight_60_70', 'total_weight_70_80',
                                          'total_weight_80_90',
                                          'total_weight_90_100', 'total_weight_100'])

    # though_area_71.to_excel('C:/Users/Administrator/Desktop/though_area_71.xlsx')



    ##基础表合并
    # wide_table = pd.concat([wide_table_21, wide_table_31, wide_table_41, wide_table_71])
    wide_table =  pd.concat([wide_table_31,  wide_table_71])
    # wide_table.to_excel('C:/Users/Administrator/Desktop/wide_tableqian.xlsx')
    wide_table = pd.merge(U_station_area, wide_table, left_on='station_code', right_on='out_station', how='left')
    # wide_table.to_excel('C:/Users/Administrator/Desktop/wide_table111.xlsx')

    # stationcode1 = t_station['out_station']
    wide_table['station_status'] = 0
    # wide_table.loc[wide_table['station_type'] == 41, 'station_status'] = 1
    # wide_table.loc[wide_table.loc[:, 'station_code'].isin(stationcode1), 'station_status'] = 1
    wide_table.loc[wide_table['pass_num'] > 0, 'station_status'] = 1
    wide_table['refresh'] = 0
    wide_table['direction'] = 0
    starttime1 = starttime.strftime("%Y%m%d")
    wide_table = wide_table.fillna(value=0)
    wide_table['id'] = wide_table['station_code'].astype('string') + starttime1
    wide_table['id']=wide_table['id'].astype('object')
    from datetime import datetime

    insert_time = datetime.now()
    wide_table['insert_time'] = insert_time
    online_duration = datetime.now().strftime("%H%M%S")
    wide_table['online_duration'] = online_duration
    wide_table.rename(
        columns={'statistics_date': 'statistic_date'}, inplace=True)
    wide_table = pd.DataFrame(wide_table,
                              columns=['id', 'city_code', 'city_name', 'county_code', 'county_name', 'station_code',
                                       'station_name', 'direction', 'statistic_date', 'pass_num', 'truck_num',
                                       'overrun_num', 'over_rate', 'truck_over_rate', 'no_car_num', 'overrun_0',
                                       'overrun_0_10', 'overrun_10_20', 'overrun_20_30', 'overrun_30_40',
                                       'overrun_40_50', 'overrun_50_60', 'overrun_60_70',
                                       'overrun_70_80', 'overrun_80_90', 'overrun_90_100', 'overrun_100',
                                       'total_weight_0_5', 'total_weight_5_10', 'total_weight_10_20',
                                       'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50',
                                       'total_weight_50_60', 'total_weight_60_70', 'total_weight_70_80',
                                       'total_weight_80_90',
                                       'total_weight_90_100', 'total_weight_100', 'station_status', 'station_type',
                                       'refresh', 'online_duration', 'insert_time'])
    # print(wide_table.info())
    print(wide_table)

    wide_table.to_excel('C:/Users/Administrator/Desktop/wide_table111.xlsx')



    from datetime import datetime
    js = datetime.now()
    sjc = js - ks
    print('运行耗时', sjc)