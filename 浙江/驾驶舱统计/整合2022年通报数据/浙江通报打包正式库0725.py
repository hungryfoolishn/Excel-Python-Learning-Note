# -*- coding:utf-8 -*-

import pymysql
import pandas as pd
import numpy as np
import  time
from urllib import parse




def case_statistic():
    db = pymysql.connect(
        host='192.168.2.39',
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

    ##超限率以及完好率
    from datetime import datetime

    day = datetime.now().date()  # 获取当前系统时间
    today = datetime.now()
    ks = datetime.now()
    print('运行开始时间', ks)

    import calendar
    import datetime
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

    starttime2 = this_month_start + datetime.timedelta(days=0)

    endtime = this_month_end + datetime.timedelta(days=1)
    import datetime
    starttime3 = day - datetime.timedelta(days=0)
    starttime = day - datetime.timedelta(days=-1)
    print('starttime', starttime)
    print('starttime2', starttime2)
    starttime = starttime.strftime('%Y-%m-%d')
    starttime2 = starttime2.strftime('%Y-%m-%d')
    starttime3 = starttime3.strftime('%Y-%m-%d')
    import datetime

    today = datetime.datetime.today()
    year = today.year
    from dateutil.relativedelta import relativedelta
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    q = ('{}' + '-01-01').format(year)
    """ 引入原始表 """
    q案件 = q
    s案件 = starttime
    q合规率 = q
    s合规率 = starttime
    start_time = starttime2
    end_time = starttime
    end_time2 = starttime3
    ##非现处罚率
    # q = '2022-01-01'
    # s = '2022-10-01'
    # cs = '全部9月'
    start = time.time()
    sql = "SELECT * FROM t_code_area"
    t_code_area = get_df_from_db(sql)

    sql = "SELECT * FROM t_bas_over_data_collection_31 where valid_time >= '{} 00:00:00' and valid_time < '{} 00:00:00'".format(q案件, s案件)
    t_bas_over_data_collection_31 = get_df_from_db(sql)
    sql = "SELECT * FROM t_case_sign_result where close_case_time >= '{} 00:00:00' and close_case_time < '{} 00:00:00'".format(q案件, s案件)
    t_case_sign_result = get_df_from_db(sql)

    q = q案件.replace('-', '')
    s = s案件.replace('-', '')
    sql = "SELECT * FROM t_bas_over_data_collection_makecopy where insert_time >={} and insert_time <{}".format(q, s)
    t_bas_over_data_collection_makecopy = get_df_from_db(sql)

    sql = "SELECT * FROM t_sys_station "
    t_sys_station = get_df_from_db(sql)

    U_检测点_区域表 = pd.merge(t_bas_over_data_collection_31, t_code_area, left_on='area_county', right_on='county_code',
                         how='left')

    U_案件处罚_区域表 = pd.merge(t_case_sign_result, t_code_area, left_on='area_county', right_on='county_code', how='left')

    U_外省抄告_区域表 = pd.merge(t_bas_over_data_collection_makecopy, t_code_area, left_on='area_county',
                          right_on='county_code', how='left')

    T_入库查询 = U_检测点_区域表.loc[(U_检测点_区域表['law_judgment'] == "1")]
    T_入库查询本省 = U_检测点_区域表.loc[((U_检测点_区域表['law_judgment'] == "1") & (U_检测点_区域表['car_no'].str.contains('浙')))]

    T_现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 99)
                        & (U_案件处罚_区域表.insert_type == 5)

                        ]

    T_非现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 31)
                         & (U_案件处罚_区域表.insert_type == 1)
                         & (U_案件处罚_区域表.data_source == 1)
                         & (U_案件处罚_区域表.case_type == 1)

                         ]

    T_非现场处罚本省 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 31)
                           & (U_案件处罚_区域表.insert_type == 1)
                           & (U_案件处罚_区域表.data_source == 1)
                           & (U_案件处罚_区域表.case_type == 1)
                           & (U_案件处罚_区域表['car_no'].str.contains('浙'))

                           ]

    T_外省抄告 = U_外省抄告_区域表

    T_入库查询 = T_入库查询.sort_values(['area_county'], ascending=False).reset_index(drop=True)
    T_入库数 = T_入库查询.groupby([T_入库查询['area_county'], T_入库查询['county']]).count()
    T_入库数本省 = T_入库查询本省.groupby([T_入库查询本省['area_county'], T_入库查询本省['county']]).count()

    T_入库数本省 = T_入库数本省.loc[:, ['id_x']]
    T_入库数本省.rename(columns={'id_x': '入库数本省(系统)'}, inplace=True)
    T_入库数 = T_入库数.loc[:, ['id_x']]
    T_入库数.rename(columns={'id_x': '入库数(系统)'}, inplace=True)
    # print("T_入库数",T_入库数)
    # print("T_入库数本省",T_入库数本省)

    T_现场处罚 = T_现场处罚.drop_duplicates(['CASE_NUM'])
    T_现场处罚计数 = T_现场处罚.groupby([T_现场处罚['area_county']])['CASE_NUM'].count()
    T_现场处罚计数 = T_现场处罚计数.to_frame()
    T_现场处罚计数.rename(columns={'CASE_NUM': '现场处罚(系统)'}, inplace=True)
    # T_现场处罚计数.to_excel(r"C:\Users\liu.wenjie\Desktop\月报\9月\{}T_现场处罚计数.xlsx".format(cs))

    T_非现场处罚 = T_非现场处罚.drop_duplicates(['CASE_NUM'])
    T_非现场处罚计数 = T_非现场处罚.groupby([T_非现场处罚['area_county']])['CASE_NUM'].count()
    T_非现场处罚计数 = T_非现场处罚计数.to_frame()
    T_非现场处罚计数.rename(columns={'CASE_NUM': '非现场处罚(系统)'}, inplace=True)

    T_非现场处罚本省 = T_非现场处罚本省.drop_duplicates(['CASE_NUM'])
    T_非现场处罚计数本省 = T_非现场处罚本省.groupby([T_非现场处罚本省['area_county']])['CASE_NUM'].count()
    T_非现场处罚计数本省 = T_非现场处罚计数本省.to_frame()
    T_非现场处罚计数本省.rename(columns={'CASE_NUM': '非现场处罚本省(系统)'}, inplace=True)
    # print('T_非现场处罚计数本省',T_非现场处罚计数本省)
    # print('T_非现场处罚计数',T_非现场处罚计数)

    T_外省抄告 = T_外省抄告.groupby([T_外省抄告['area_county']]).count()
    T_外省抄告计数 = T_外省抄告.loc[:, ['id_x']]
    T_外省抄告计数.rename(columns={'id_x': '外省抄告(系统)'}, inplace=True)
    W_案件统计 = pd.merge(T_入库数, T_入库数本省, left_on='area_county', right_on='area_county', how='outer')
    W_案件统计 = pd.merge(W_案件统计, T_现场处罚计数, left_on='area_county', right_on='area_county', how='outer')
    W_案件统计 = pd.merge(W_案件统计, T_非现场处罚计数, left_on='area_county', right_on='area_county', how='outer')
    W_案件统计 = pd.merge(W_案件统计, T_非现场处罚计数本省, left_on='area_county', right_on='area_county', how='outer')
    W_案件统计 = pd.merge(W_案件统计, T_外省抄告计数, left_on='area_county', right_on='area_county', how='outer')
    W_案件统计[['外省抄告(系统)']] = W_案件统计[['外省抄告(系统)']].apply(np.int64)
    W_案件统计.loc[W_案件统计['外省抄告(系统)'] < 0, '外省抄告(系统)'] = 0
    W_案件统计 = W_案件统计.fillna(0, inplace=False)
    W_案件统计 = W_案件统计.reset_index()
    W_案件统计.rename(columns={'area_county': 'county_code'}, inplace=True)
    return W_案件统计

def case_statistic当年():
    db = pymysql.connect(
        host='192.168.2.39',
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

    ##超限率以及完好率
    from datetime import datetime

    day = datetime.now().date()  # 获取当前系统时间
    today = datetime.now()
    ks = datetime.now()
    print('运行开始时间', ks)

    import calendar
    import datetime
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

    starttime2 = this_month_start + datetime.timedelta(days=0)

    endtime = this_month_end + datetime.timedelta(days=1)
    import datetime
    starttime3 = day - datetime.timedelta(days=0)
    starttime = day - datetime.timedelta(days=-1)
    print('starttime', starttime)
    print('starttime2', starttime2)
    starttime = starttime.strftime('%Y-%m-%d')
    starttime2 = starttime2.strftime('%Y-%m-%d')
    starttime3 = starttime3.strftime('%Y-%m-%d')
    import datetime

    today = datetime.datetime.today()
    year = today.year
    from dateutil.relativedelta import relativedelta
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    q = ('{}' + '-01-01').format(year)
    """ 引入原始表 """
    q案件 = q
    s案件 = starttime
    q合规率 = q
    s合规率 = starttime
    start_time = starttime2
    end_time = starttime
    end_time2 = starttime3
    ##非现处罚率
    # q = '2022-01-01'
    # s = '2022-10-01'
    # cs = '全部9月'
    start = time.time()
    sql = "SELECT * FROM t_code_area"
    t_code_area = get_df_from_db(sql)

    sql = """SELECT
          area_county,
          count(DISTINCT case_number) as 交警现场查处数
          FROM
          t_bas_police_road_site a
          LEFT JOIN t_code_area b ON a.area_county = b.county_code
          WHERE 
          a.punish_time >= '{} 00:00:00'
          and a.punish_time < '{} 00:00:00'
          and a.create_time< '{} 00:00:00'
          and case_status=2
          GROUP BY area_county
          """.format(q案件, s案件,s案件)
    T_交警现场处罚 = get_df_from_db(sql)
    sql = "SELECT c.area_county, record_type,insert_type,data_source,case_type,c.car_no,c.CASE_NUM " \
          "FROM t_case_sign_result c LEFT JOIN   t_bas_over_data_collection_31  b ON c.record_code = b.record_code  " \
          "where close_case_time >= '{} 00:00:00' and close_case_time < '{} 00:00:00' and b.valid_time >= '{} 00:00:00' ".format(q案件, s案件,q案件)
    t_case_sign_result = get_df_from_db(sql)

    U_案件处罚_区域表 = pd.merge(t_case_sign_result, t_code_area, left_on='area_county', right_on='county_code', how='left')



    T_非现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 31)
                         & (U_案件处罚_区域表.insert_type == 1)
                         & (U_案件处罚_区域表.data_source == 1)
                         & (U_案件处罚_区域表.case_type == 1)

                         ]

    T_非现场处罚本省 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 31)
                           & (U_案件处罚_区域表.insert_type == 1)
                           & (U_案件处罚_区域表.data_source == 1)
                           & (U_案件处罚_区域表.case_type == 1)
                           & (U_案件处罚_区域表['car_no'].str.contains('浙'))

                           ]
    T_非现场处罚 = T_非现场处罚.drop_duplicates(['CASE_NUM'])
    T_非现场处罚计数 = T_非现场处罚.groupby([T_非现场处罚['area_county']])['CASE_NUM'].count()
    T_非现场处罚计数 = T_非现场处罚计数.to_frame()
    T_非现场处罚计数.rename(columns={'CASE_NUM': '非现场处罚(路政)当年'}, inplace=True)

    T_非现场处罚本省 = T_非现场处罚本省.drop_duplicates(['CASE_NUM'])
    T_非现场处罚计数本省 = T_非现场处罚本省.groupby([T_非现场处罚本省['area_county']])['CASE_NUM'].count()
    T_非现场处罚计数本省 = T_非现场处罚计数本省.to_frame()
    T_非现场处罚计数本省.rename(columns={'CASE_NUM': '非现场处罚本省(路政)当年'}, inplace=True)
    # print('T_非现场处罚计数本省',T_非现场处罚计数本省)
    # print('T_非现场处罚计数',T_非现场处罚计数)
    W_案件统计 = pd.merge(T_非现场处罚计数, T_非现场处罚计数本省, on='area_county', how='outer')
    W_案件统计 = pd.merge(W_案件统计, T_交警现场处罚, on='area_county', how='outer')
    W_案件统计 = W_案件统计.fillna(0, inplace=False)
    W_案件统计 = W_案件统计.reset_index()
    W_案件统计.rename(columns={'area_county': 'county_code'}, inplace=True)
    return W_案件统计



def Compliance_rate():
    db = pymysql.connect(
        host='192.168.2.39',
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

    ##超限率以及完好率
    from datetime import datetime

    day = datetime.now().date()  # 获取当前系统时间
    today = datetime.now()
    ks = datetime.now()
    print('运行开始时间', ks)

    import calendar
    import datetime
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

    starttime2 = this_month_start + datetime.timedelta(days=0)

    endtime = this_month_end + datetime.timedelta(days=1)
    import datetime
    starttime3 = day - datetime.timedelta(days=0)
    starttime = day - datetime.timedelta(days=-1)
    print('starttime', starttime)
    print('starttime2', starttime2)
    starttime = starttime.strftime('%Y-%m-%d')
    starttime2 = starttime2.strftime('%Y-%m-%d')
    starttime3 = starttime3.strftime('%Y-%m-%d')
    import datetime

    today = datetime.datetime.today()
    year = today.year
    from dateutil.relativedelta import relativedelta
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    q = ('{}' + '-01-01').format(year)
    """ 引入原始表 """
    q案件 = q
    s案件 = starttime
    q合规率 = q
    s合规率 = starttime
    start_time = starttime2
    end_time = starttime
    end_time2 = starttime3
    ##合规率
    sql = "SELECT * FROM t_sys_station where station_status !=1 and station_type =31"
    t_sys_station = get_df_from_db(sql)


    station_code = t_sys_station['station_code']

    sql = "SELECT * FROM t_code_area "
    t_code_area = get_df_from_db(sql)

    sql = "SELECT * FROM t_bas_over_data_31 where out_station_time >= '{} 00:00:00' and  out_station_time <'{} 00:00:00' and total_weight>80   AND is_unusual = 0  ".format(
        q合规率, s合规率)
    t_bas_over_data_31 = get_df_from_db(sql)

    sql = "SELECT * FROM t_bas_over_data_collection_31 where out_station_time >= '{} 00:00:00' and  out_station_time <'{} 00:00:00' and total_weight>80  ".format(
        q合规率, s合规率)
    t_bas_over_data_collection_31 = get_df_from_db(sql)

    U_过车_区域表 = pd.merge(t_bas_over_data_31, t_code_area, left_on='area_county', right_on='county_code', how='left')
    U_案件审核_区域表 = pd.merge(t_bas_over_data_collection_31, t_code_area, left_on='area_county', right_on='county_code',
                          how='left')
    U_案件审核_区域表 = U_案件审核_区域表[U_案件审核_区域表['out_station'].isin(station_code)]
    U_过车_站点表 = U_过车_区域表[U_过车_区域表.loc[:, 'out_station'].isin(station_code)]



    月超限80以上总数 = U_过车_站点表.groupby(['city_code', 'city', 'county_code', 'county']).count()

    月超限80以上总数 = 月超限80以上总数['id_x']

    U_过车_站点表 = U_过车_站点表[(U_过车_站点表['is_collect'] == 1)
    ]

    月超限80以上且满足处罚条件总数 = U_过车_站点表.groupby(['city_code', 'city', 'county_code', 'county']).count()

    月超限80以上且满足处罚条件总数 = 月超限80以上且满足处罚条件总数['id_x']

    snum = [3, 4, 5, 6, 9, 12, 13]

    U_过车_站点表 = U_案件审核_区域表[U_案件审核_区域表.loc[:, 'status_x'].isin(snum)]

    月超限80以上审核通过总数 = U_过车_站点表.groupby(['city_code', 'city', 'county_code', 'county']).count()

    月超限80以上审核通过总数 = 月超限80以上审核通过总数['id_x']

    超限80数 = pd.merge(月超限80以上总数, 月超限80以上且满足处罚条件总数, on=['city_code', 'city', 'county_code', 'county'], how='left')

    超限80数 = pd.merge(超限80数, 月超限80以上审核通过总数, on=['city_code', 'city', 'county_code', 'county'], how='left')

    超限80数.rename(columns={'id_x_x': '月超限80%以上总数', 'id_x_y': '月超限80%以上且满足处罚条件总数', 'id_x': '月超限80%以上审核通过总数'},
                 inplace=True)
    超限80数 = 超限80数.fillna(0, inplace=False)
    return 超限80数

def Key_freight_sources():
    db = pymysql.connect(
        host='192.168.2.39',
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

    ##超限率以及完好率
    from datetime import datetime

    day = datetime.now().date()  # 获取当前系统时间
    today = datetime.now()
    ks = datetime.now()
    print('运行开始时间', ks)

    import calendar
    import datetime
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

    starttime2 = this_month_start + datetime.timedelta(days=0)

    endtime = this_month_end + datetime.timedelta(days=1)
    import datetime
    starttime3 = day - datetime.timedelta(days=0)
    starttime = day - datetime.timedelta(days=-1)
    print('starttime', starttime)
    print('starttime2', starttime2)
    starttime = starttime.strftime('%Y-%m-%d')
    starttime2 = starttime2.strftime('%Y-%m-%d')
    starttime3 = starttime3.strftime('%Y-%m-%d')
    import datetime

    today = datetime.datetime.today()
    year = today.year
    from dateutil.relativedelta import relativedelta
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    q = ('{}' + '-01-01').format(year)
    """ 引入原始表 """
    q案件 = q
    s案件 = starttime
    q合规率 = q
    s合规率 = starttime
    start_time = starttime2
    end_time = starttime
    end_time2 = starttime3
    """ 引入原始表 """
    sql = "SELECT city,county,city_code,county_code  FROM t_code_area where province_code = '330000'"
    t_code_area = get_df_from_db(sql)
    # sql = "SELECT * from t_bas_source_company where area_province = '330000' and is_statictis =1  and is_deleted=0 "
    # t_bas_source_company=get_df_from_db(sql)
    # sql = "SELECT * FROM t_bas_source_company_equipment WHERE is_deleted = 0 OR is_deleted IS NULL"
    # t_bas_source_company_equipment=get_df_from_db(sql)
    sql = """ SELECT id ,area_county FROM t_sys_station where is_deleted =0 and station_code  
              in ( '3301227101',
	'3301837101',
	'3301827101',
	'3301057101',
	'3301837102',
	'3301837103',
	'3301827102',
	'3301557101',
	'3301067101',
	'3301557102',
	'3301097104',
	'3301097106',
	'3301137101',
	'3301837105',
	'3301857102',
	'3301827105',
	'3301277101',
	'3301107101',
	'3301227102',
	'3301827107',
	'3301227103',
	'3301227104',
	'3301227105',
	'3301227106',
	'3301227107',
	'3301227108',
	'3301227109',
	'3301227110',
	'3301227111',
	'3301097108',
	'3302067101',
	'3302037101',
	'3302067103',
	'3302067104',
	'3302067105',
	'3302067107',
	'3302067108',
	'3302067109',
	'3302067112',
	'3302067114',
	'3302067116',
	'3302267101',
	'3302067119',
	'3302067118',
	'3302067119',
	'3302067121',
	'3302067122',
	'3302067123',
	'3302067126',
	'3302067127',
	'3302067128',
	'3302067129',
	'3302067130',
	'3302257101',
	'3302827101',
	'3302827102',
	'3302827103',
	'3302837101',
	'3302837105',
	'3302837106',
	'3302267102',
	'3302267103',
	'3302267104',
	'3302267105',
	'3302267106',
	'3302267107',
	'3302267108',
	'3302257105',
	'3302257107',
	'3302257108',
	'3302257109',
	'3302257110',
	'3302257111',
	'3302127102',
	'3302817101',
	'3302817102',
	'3302817103',
	'3302817104',
	'3302257112',
	'3302127103',
	'3303817101',
	'3303817102',
	'3303267101',
	'3303287101',
	'3303027101',
	'3303027102',
	'3303027103',
	'3303027104',
	'3303027105',
	'3303027106',
	'3303047101',
	'3303047102',
	'3303047103',
	'3303037101',
	'3303037102',
	'3303037103',
	'3303037104',
	'3303037105',
	'3303037106',
	'3303037107',
	'3303037108',
	'3303037109',
	'3303037110',
	'3303227101',
	'3303227102',
	'3303227103',
	'3303827101',
	'3303827102',
	'3303827103',
	'3303827104',
	'3303827105',
	'3303827106',
	'3303827107',
	'3303827108',
	'3303827110',
	'3303827111',
	'3303827112',
	'3303827113',
	'3303827114',
	'3303827115',
	'3303827116',
	'3303827117',
	'3303827120',
	'3303827121',
	'3303817104',
	'3303817105',
	'3303817106',
	'3303817107',
	'3303817108',
	'3303817109',
	'3303817110',
	'3303817111',
	'3303817112',
	'3303817113',
	'3303817114',
	'3303817115',
	'3303817116',
	'3303817117',
	'3303247101',
	'3303247102',
	'3303267102',
	'3303277102',
	'330481_G524WRL15',
	'3304817101',
	'3304817102',
	'3304817103',
	'3304817104',
	'3304817105',
	'3304817106',
	'3304817107',
	'3304817108',
	'3304817109',
	'3304247101',
	'3304247102',
	'3304247103',
	'3304247104',
	'3304247106',
	'3304247107',
	'3304247108',
	'3304247109',
	'3304247110',
	'3304247111',
	'3304247112',
	'3304247113',
	'3304247114',
	'3304247115',
	'3304247116',
	'3304247117',
	'3304247118',
	'3304247119',
	'3304247120',
	'3304817110',
	'3304817111',
	'3304817112',
	'3304817113',
	'3304817114',
	'3304817115',
	'3304837101',
	'3304837102',
	'3304837103',
	'3304837104',
	'3304837105',
	'3304837106',
	'3304837107',
	'3304837108',
	'3304837109',
	'3304837110',
	'3304817116',
	'3304027101',
	'3304027102',
	'3304027103',
	'3304027104',
	'3304827101',
	'3304827102',
	'3304827103',
	'3304217101',
	'3304217102',
	'3304217103',
	'3304217104',
	'3304837111',
	'3304217106',
	'3304217107',
	'3304217108',
	'3304217110',
	'3304217111',
	'3304217112',
	'3304217113',
	'3304217114',
	'3304837112',
	'3304837113',
	'3304837114',
	'3304837116',
	'3304837117',
	'3304027105',
	'3304027106',
	'3304027107',
	'3304027108',
	'3304027109',
	'3304027110',
	'3304027111',
	'3304027112',
	'3304027113',
	'3304027114',
	'3304027115',
	'3304117101',
	'3304117102',
	'3304117103',
	'3304117104',
	'3304117106',
	'3304117107',
	'3304117108',
	'3304827104',
	'3304827105',
	'3304017101',
	'3304217117',
	'3304817126',
	'3304217105',
	'3304817118',
	'3305227101',
	'3305237101',
	'3305237102',
	'3305237103',
	'3305237104',
	'3305237105',
	'3305227102',
	'3305227103',
	'3305227104',
	'3305037101',
	'3305037102',
	'3305037103',
	'3305037104',
	'3305217101',
	'3305217102',
	'3305217103',
	'3305027101',
	'3305027102',
	'3305027104',
	'3305237106',
	'3305237107',
	'3305237108',
	'3305237109',
	'3305237110',
	'3305237111',
	'3305237113',
	'3305237114',
	'3305237115',
	'3305237116',
	'3305237117',
	'3305237118',
	'3305237119',
	'3305237120',
	'3305237121',
	'3305237122',
	'3305237123',
	'3305237124',
	'3305237125',
	'3305237126',
	'3305237127',
	'3305237128',
	'3305237129',
	'3305237130',
	'3305237131',
	'3305237132',
	'3305237133',
	'3305237134',
	'3305237135',
	'3305237136',
	'3305227105',
	'3305227106',
	'3306027101',
	'3306027102',
	'3306047101',
	'3306047102',
	'3306037101',
	'3306037102',
	'3306027103',
	'3306037103',
	'3306247101',
	'3306247102',
	'3306817101',
	'3306037104',
	'3306037105',
	'3306037106',
	'3306037107',
	'3306037108',
	'3306037109',
	'3306037110',
	'3306037111',
	'3306037112',
	'3306037113',
	'3306037114',
	'3306037115',
	'3306817102',
	'3306817103',
	'3306817104',
	'3306837101',
	'3306837102',
	'3306837103',
	'3306837104',
	'3306817105',
	'3306817106',
	'3306817107',
	'3306817108',
	'3306817109',
	'3306817110',
	'3306047103',
	'3306837105',
	'3306837106',
	'3306837107',
	'3306837108',
	'3307817101',
	'3307817102',
	'3307817103',
	'3307817104',
	'3307817105',
	'3307817106',
	'3307817107',
	'3307817109',
	'3307817110',
	'3307817111',
	'3307817112',
	'3307817113',
	'3307817114',
	'3307817115',
	'3307817116',
	'3307817117',
	'3307817118',
	'3307817119',
	'3307817120',
	'3307817121',
	'3307817122',
	'3307817123',
	'3307817124',
	'3307817125',
	'3307817126',
	'3307817127',
	'3307267101',
	'3307267102',
	'3307837101',
	'3307837102',
	'3307837103',
	'3307837104',
	'3307827101',
	'3307027101',
	'3307027102',
	'3307027103',
	'3307027104',
	'3307037101',
	'3307037102',
	'3307037103',
	'3307817134',
	'3307827102',
	'3307827103',
	'3307827104',
	'3307827105',
	'3307827107',
	'3307847101',
	'3307847102',
	'3307847103',
	'3307267106',
	'3307267108',
	'3307267109',
	'3307267110',
	'3307237101',
	'3307237102',
	'3307237103',
	'3307277101',
	'3307277102',
	'3307277103',
	'3307827109',
	'3308027101',
	'3308027103',
	'3308037101',
	'3308027105',
	'3308027106',
	'3308257101',
	'3308257102',
	'3308257103',
	'3308257104',
	'3308257105',
	'3308257106',
	'3308817101',
	'3308817102',
	'3308817103',
	'3308817105',
	'3308227102',
	'3308227103',
	'3308227104',
	'3308247101',
	'3308247102',
	'3308037102',
	'3308817108',
	'3309027101',
	'3309017101',
	'3309027102',
	'3309027103',
	'3309027104',
	'3309027105',
	'3309217101',
	'3309217102',
	'3309027106',
	'3309027107',
	'3309027108',
	'3309027112',
	'331004_G104TDX',
	'3310037101',
	'3310037102',
	'3310217101',
	'3310817101',
	'3310027101',
	'3310827101',
	'3310037103',
	'3310037104',
	'3310047102',
	'3310827103',
	'3310827104',
	'3310237101',
	'3311217102',
	'3311217103',
	'3311027101',
	'3311217104',
	'3311217105',
	'3311237101',
'3311227101',
'3311257102')
          """
    t_code_area2 = get_df_from_db(sql)
    站点总数 = pd.merge(t_code_area2, t_code_area, left_on='area_county', right_on='county_code', how='left')
    # with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\汇总0712v2.0.xlsx') as writer1:
    #     站点总数.to_excel(writer1, sheet_name='区县', index=True)
    数据站点总数 = 站点总数.groupby(['city', 'city_code', 'county', 'county_code'])['id'].count().reset_index(name='数据站点总数')

    数据站点总数['county_code'] = 数据站点总数['county_code'].astype('string')

    sql = "SELECT * FROM t_sys_station WHERE station_type = 71 AND is_deleted = 0 AND station_status != 1  and station_code IS NOT null "
    t_sys_station = get_df_from_db(sql)
    sql = "SELECT area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate,axis" \
          " FROM t_bas_pass_data_71  where out_station_time >='{} 00:00:00' and out_station_time <='{} 00:00:00'  and is_truck =1 and insert_time <='{} 00:00:00' ".format(
        start_time, end_time, end_time)
    t_bas_pass_data_71 = get_df_from_db(sql)

    """拼接表"""
    U_源头_区域表 = pd.merge(t_bas_pass_data_71, t_code_area, left_on='area_county', right_on='county_code', how='left')

    企业_源头_站点表 = pd.merge(t_sys_station, t_code_area, left_on='area_county', right_on='county_code', how='left')

    station_code = 企业_源头_站点表['station_code']
    U_源头_区域表 = U_源头_区域表[U_源头_区域表.loc[:, 'out_station'].isin(station_code)]
    # q = input("请输入储存路径(C:/Users/Administrator/Desktop/输出报表/月报表相关数据汇总)：")
    # with pd.ExcelWriter('{}/U_源头_区域表.xlsx'.format(q))as writer1:
    #      U_源头_区域表.to_excel(writer1, sheet_name='sheet1', index=True)

    """超限数"""
    超限20_50 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 20) & (U_源头_区域表['overrun_rate'] <= 50)].groupby(
        ['city', 'county', 'county_code'])['car_no'].count().reset_index(name='20-50%数')
    超限50_100 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 50) & (U_源头_区域表['overrun_rate'] <= 100)].groupby(
        ['city', 'county', 'county_code'])['car_no'].count().reset_index(name='50-100%数')
    超限100 = U_源头_区域表[(U_源头_区域表['overrun_rate'] > 100) & (U_源头_区域表['overrun_rate'] <= 450)].groupby(
        ['city', 'county', 'county_code'])['car_no'].count().reset_index(name='100%以上数')

    """设备上线率"""

    U_源头_区域表['取日'] = U_源头_区域表['out_station_time'].apply(lambda x: x.strftime('%d'))
    在线天数大于20天 = U_源头_区域表.groupby(['city', 'county_code', 'county', 'out_station'])['取日'].nunique().reset_index(
        name='在线天数')
    货运量大于2万吨 = U_源头_区域表.groupby(['city', 'county_code', 'county', 'out_station'])['total_weight'].sum().reset_index(
        name='货运总重')
    过车数大于410辆 = U_源头_区域表.groupby(['city', 'county_code', 'county', 'out_station'])['city'].count().reset_index(
        name='货车数')
    站点完好数 = pd.merge(在线天数大于20天, 货运量大于2万吨, on=['city', 'county_code', 'county', 'out_station'], how='left')
    站点完好数 = pd.merge(站点完好数, 过车数大于410辆, on=['city', 'county_code', 'county', 'out_station'], how='left')
    站点完好数['货运总重'] = pd.to_numeric(站点完好数['货运总重'], errors='coerce')

    站点完好数区县 = 站点完好数.groupby(['city', 'county', 'county_code'])['货车数', '货运总重'].sum()
    在线站点数 = 站点完好数[(站点完好数['在线天数'] > 20) | (站点完好数['货运总重'] > 20000) | (站点完好数['货车数'] > 410)].groupby(
        ['city', 'county', 'county_code'])['out_station'].count().reset_index(name='在线站点数')

    """聚合"""

    货运源头监控数据 = pd.merge(数据站点总数, 超限20_50, on=['city', 'county', 'county_code'], how='left')
    货运源头监控数据 = pd.merge(货运源头监控数据, 超限50_100, on=['city', 'county', 'county_code'], how='left')
    货运源头监控数据 = pd.merge(货运源头监控数据, 超限100, on=['city', 'county', 'county_code'], how='left')
    货运源头监控数据 = pd.merge(货运源头监控数据, 在线站点数, on=['city', 'county', 'county_code'], how='left')
    货运源头监控数据 = pd.merge(货运源头监控数据, 站点完好数区县, on=['city', 'county', 'county_code'], how='left')
    货运源头监控数据 = 货运源头监控数据.fillna(0, inplace=False)
    货运源头监控数据['源头单位平均过车数（辆次）'] = 货运源头监控数据.apply(lambda x: x['货车数'] / (x['数据站点总数'] + 0.0000001), axis=1).round(0)
    货运源头监控数据['设备上线率（%）'] = 货运源头监控数据.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据['20-50%占比'] = 货运源头监控数据.apply(lambda x: x['20-50%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据['50-100%占比'] = 货运源头监控数据.apply(lambda x: x['50-100%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据['100%以上占比'] = 货运源头监控数据.apply(lambda x: x['100%以上数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据['设备上线率（%）'] = 货运源头监控数据.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据 = 货运源头监控数据.fillna(0, inplace=False)
    货运源头监控数据['设备上线率（%）'] = 货运源头监控数据['设备上线率（%）'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据['20-50%占比'] = 货运源头监控数据['20-50%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据['50-100%占比'] = 货运源头监控数据['50-100%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据['100%以上占比'] = 货运源头监控数据['100%以上占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据.rename(columns={'city': '地市', 'county': '区县'}, inplace=True)
    货运源头监控数据 = pd.DataFrame(货运源头监控数据,
                            columns=['地市', '区县', '数据站点总数', '货车数', '源头单位平均过车数（辆次）', '在线站点数', '设备上线率（%）', '20-50%数',
                                     '50-100%数', '100%以上数', '20-50%占比', '50-100%占比', '100%以上占比', 'city_code',
                                     'county_code'])
    货运源头监控数据 = 货运源头监控数据.sort_values('county_code', ascending=True)

    货运源头监控数据地市 = 货运源头监控数据.groupby(['地市', 'city_code']).sum().reset_index()
    货运源头监控数据地市 = 货运源头监控数据地市.fillna(0, inplace=False)
    货运源头监控数据地市['源头单位平均过车数（辆次）'] = 货运源头监控数据地市.apply(lambda x: x['货车数'] / (x['数据站点总数'] + 0.0000001), axis=1).round(0)
    货运源头监控数据地市['设备上线率（%）'] = 货运源头监控数据地市.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据地市['20-50%占比'] = 货运源头监控数据地市.apply(lambda x: x['20-50%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据地市['50-100%占比'] = 货运源头监控数据地市.apply(lambda x: x['50-100%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据地市['100%以上占比'] = 货运源头监控数据地市.apply(lambda x: x['100%以上数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据地市['设备上线率（%）'] = 货运源头监控数据地市.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据地市 = 货运源头监控数据地市.fillna(0, inplace=False)
    货运源头监控数据地市['设备上线率（%）'] = 货运源头监控数据地市['设备上线率（%）'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据地市['20-50%占比'] = 货运源头监控数据地市['20-50%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据地市['50-100%占比'] = 货运源头监控数据地市['50-100%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据地市['100%以上占比'] = 货运源头监控数据地市['100%以上占比'].apply(lambda x: format(x, '.2%'))

    货运源头监控数据地市 = pd.DataFrame(货运源头监控数据地市,
                              columns=['地市', '数据站点总数', '货车数', '源头单位平均过车数（辆次）', '在线站点数', '设备上线率（%）', '20-50%数',
                                       '50-100%数', '100%以上数', '20-50%占比', '50-100%占比', '100%以上占比', 'city_code'])
    货运源头监控数据地市 = 货运源头监控数据地市.sort_values('city_code', ascending=True)
    货运源头监控数据省 = 货运源头监控数据地市
    货运源头监控数据省['省'] = '浙江省'
    货运源头监控数据省 = 货运源头监控数据省.groupby(['省']).sum().reset_index()
    货运源头监控数据省 = 货运源头监控数据省.fillna(0, inplace=False)
    货运源头监控数据省['源头单位平均过车数（辆次）'] = 货运源头监控数据省.apply(lambda x: x['货车数'] / (x['数据站点总数'] + 0.0000001), axis=1).round(0)
    货运源头监控数据省['设备上线率（%）'] = 货运源头监控数据省.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据省['20-50%占比'] = 货运源头监控数据省.apply(lambda x: x['20-50%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据省['50-100%占比'] = 货运源头监控数据省.apply(lambda x: x['50-100%数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据省['100%以上占比'] = 货运源头监控数据省.apply(lambda x: x['100%以上数'] / (x['货车数'] + 0.0000001), axis=1)
    货运源头监控数据省['设备上线率（%）'] = 货运源头监控数据省.apply(lambda x: x['在线站点数'] / (x['数据站点总数']), axis=1)
    货运源头监控数据省 = 货运源头监控数据省.fillna(0, inplace=False)
    货运源头监控数据省['设备上线率（%）'] = 货运源头监控数据省['设备上线率（%）'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据省['20-50%占比'] = 货运源头监控数据省['20-50%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据省['50-100%占比'] = 货运源头监控数据省['50-100%占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据省['100%以上占比'] = 货运源头监控数据省['100%以上占比'].apply(lambda x: format(x, '.2%'))
    货运源头监控数据省.rename(columns={'省': '地市'}, inplace=True)
    货运源头监控数据省 = pd.DataFrame(货运源头监控数据省, columns=['地市', '数据站点总数', '货车数', '源头单位平均过车数（辆次）', '在线站点数', '设备上线率（%）', '20-50%数',
                                                 '50-100%数', '100%以上数', '20-50%占比', '50-100%占比', '100%以上占比'])
    货运源头监控数据省市 = pd.concat([货运源头监控数据地市, 货运源头监控数据省])

    # with pd.ExcelWriter(r'C:\Users\liu.wenjie\Desktop\全部源头.xlsx') as writer1:
    #     货运源头监控数据.to_excel(writer1, sheet_name='区县', index=False)
    #     货运源头监控数据地市.to_excel(writer1, sheet_name='地市', index=False)
    #     货运源头监控数据省市.to_excel(writer1, sheet_name='省', index=False)
    #     站点完好数.to_excel(writer1, sheet_name='站点数据明细', index=False)
    货运源头监控数据 = pd.DataFrame(货运源头监控数据,
                            columns=['后面是源头数据', '数据站点总数', '货车数', '源头单位平均过车数（辆次）', '在线站点数', '设备上线率（%）', '20-50%数',
                                     '50-100%数', '100%以上数', '20-50%占比', '50-100%占比', '100%以上占比','county_code'])
    货运源头监控数据.rename(columns={'货车数': '源头货车数'}, inplace=True)
    return  货运源头监控数据,货运源头监控数据省市,站点完好数


def over_100_num():
    db = pymysql.connect(
        host='192.168.2.39',
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

    ##超限率以及完好率
    from datetime import datetime

    day = datetime.now().date()  # 获取当前系统时间
    today = datetime.now()
    ks = datetime.now()
    print('运行开始时间', ks)

    import calendar
    import datetime
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

    starttime2 = this_month_start + datetime.timedelta(days=0)

    endtime = this_month_end + datetime.timedelta(days=1)
    import datetime
    starttime3 = day - datetime.timedelta(days=0)
    starttime = day - datetime.timedelta(days=-1)
    print('starttime', starttime)
    print('starttime2', starttime2)
    starttime = starttime.strftime('%Y-%m-%d')
    starttime2 = starttime2.strftime('%Y-%m-%d')
    starttime3 = starttime3.strftime('%Y-%m-%d')
    import datetime

    today = datetime.datetime.today()
    year = today.year
    from dateutil.relativedelta import relativedelta
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    q = ('{}' + '-01-01').format(year)
    """ 引入原始表 """
    q案件 = q
    s案件 = starttime
    q合规率 = q
    s合规率 = starttime
    start_time = starttime2
    end_time = starttime
    end_time2 = starttime3

    import time
    '''输入变量'''

    cs = '浙江'
    q = start_time
    s = end_time

    """浙江超限100%明细数据 以及地市货车数及超限100%数"""
    if cs == '浙江':
        """ 引入原始表 """
        start = time.time()

        sql = "SELECT * FROM t_sys_station where station_status =0 and station_type =31"
        t_sys_station = get_df_from_db(sql)

        station_code = t_sys_station['station_code']
        sql = "SELECT * FROM t_code_area  "
        t_code_area = get_df_from_db(sql)
        sql = "SELECT * FROM t_bas_over_data_31 where is_unusual = 0 and out_station_time >= '{} 00:00:00' and  out_station_time <'{} 00:00:00' and allow is null   and overrun_rate>=100 and total_weight<100".format(
            q, s)
        t_bas_over_data_31 = get_df_from_db(sql)
        end = time.time()
        time = end - start


        """超限1000全省明细"""

        U_过车_区域表 = pd.merge(t_bas_over_data_31, t_code_area, left_on='area_county', right_on='county_code', how='left')
        U_过车_站点表 = pd.merge(U_过车_区域表, t_sys_station, left_on='out_station', right_on='station_code', how='left')
        U_过车_站点表 = U_过车_站点表[U_过车_站点表.loc[:, 'out_station'].isin(station_code)]

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
            "330122": 1.091,
            "330183": 1.1,
            "330329": 1.2,
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
            U_过车_站点表.loc[((U_过车_站点表['area_county_x'] == key) | (U_过车_站点表['area_city_x'] == key)) & (
                    U_过车_站点表['total_weight'] < 100), 'vehicle_brand'] = U_过车_站点表['limit_weight'].map(
                lambda x: float(x) * value).round(4)
        U_过车_站点表['limit_weight'] = U_过车_站点表['limit_weight'].astype('float')
        U_过车_站点表['total_weight'] = U_过车_站点表['total_weight'].astype('float')
        U_过车_站点表['vehicle_brand'] = U_过车_站点表['vehicle_brand'].astype('float')
        U_过车_站点表['超限率100%'] = U_过车_站点表.apply(
            lambda x: (x['total_weight'] - x['vehicle_brand']) * 100 / x['vehicle_brand'],
            axis=1).round(2)
        U_过车_站点表 = U_过车_站点表[(U_过车_站点表['超限率100%'] >= 100)
        ]
        U_过车_站点表 = pd.DataFrame(U_过车_站点表,
                                columns=["city", "county", "car_no", "out_station_time", "axis", "total_weight",
                                         "overrun",
                                         "overrun_rate", "site_name", "status_x", "is_collect", "is_unusual",
                                         "record_code",
                                         "photo1", "photo2", "photo3", "vedio", 'county_code'])


        超限100数 = U_过车_站点表.groupby(['county_code'])['record_code'].count().reset_index(name='超限100')

        U_过车_站点表 = U_过车_站点表.sort_values(by=['county_code', 'city', 'county', 'out_station_time'],
                                        ascending=True).reset_index(drop=True)
        U_过车_站点表 = U_过车_站点表.drop_duplicates(subset=['record_code'])
        U_过车_站点表.loc[U_过车_站点表['status_x'] == 1, 'status_x'] = '待初审'
        U_过车_站点表.loc[U_过车_站点表['status_x'] == 2, 'status_x'] = '待审核'
        U_过车_站点表.loc[U_过车_站点表['status_x'] == 9, 'status_x'] = '判定不处理'
        U_过车_站点表.loc[U_过车_站点表['status_x'] == 15, 'status_x'] = '初审不通过'
        U_过车_站点表.loc[U_过车_站点表['is_collect'] == 1, 'is_collect'] = '满足'
        U_过车_站点表.loc[U_过车_站点表['is_collect'] == 0, 'is_collect'] = '不满足'

        """车牌处理"""
        U_过车_站点表['car_no'].fillna('无牌', inplace=True)
        U_过车_站点表['字节数'] = U_过车_站点表['car_no'].str.len()
        U_过车_站点表.loc[U_过车_站点表['字节数'] <= 5, 'car_no'] = '无牌'
        U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['is_collect'] == '满足')), 'photo1'] = ''
        U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['is_collect'] == '满足')), 'photo2'] = ''
        U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['is_collect'] == '满足')), 'photo3'] = ''
        U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['is_collect'] == '满足')), 'vedio'] = ''

        return 超限100数,U_过车_站点表
def total_weight_80_90():
    db = pymysql.connect(
        host='192.168.2.39',
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

    ##超限率以及完好率
    from datetime import datetime

    day = datetime.now().date()  # 获取当前系统时间
    today = datetime.now()
    ks = datetime.now()
    print('运行开始时间', ks)

    import calendar
    import datetime
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

    starttime2 = this_month_start + datetime.timedelta(days=0)

    endtime = this_month_end + datetime.timedelta(days=1)
    import datetime
    starttime3 = day - datetime.timedelta(days=0)
    starttime = day - datetime.timedelta(days=-1)
    print('starttime', starttime)
    print('starttime2', starttime2)
    starttime = starttime.strftime('%Y-%m-%d')
    starttime2 = starttime2.strftime('%Y-%m-%d')
    starttime3 = starttime3.strftime('%Y-%m-%d')
    import datetime

    today = datetime.datetime.today()
    year = today.year
    from dateutil.relativedelta import relativedelta
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    q = ('{}' + '-01-01').format(year)
    """ 引入原始表 """
    q案件 = q
    s案件 = starttime
    q合规率 = q
    s合规率 = starttime
    start_time = starttime2
    end_time = starttime
    end_time2 = starttime3
    """ 引入原始表 """
    sql = "SELECT record_code,b.city as '地市',a.area_city as '地市编码',b.county as '区县',b.county_code ,a.site_name as '站点名称',a.out_station_time as '检测时间',a.car_no as '车牌号码',a.total_weight ,a.limit_weight as '限重',a.overrun as '超重',a.axis as '轴数',a.overrun_rate as '超限率' \
                FROM t_bas_over_data_31 a \
                LEFT JOIN t_code_area b ON b.county_code = a.area_county \
                left join t_sys_station c on c.station_code = a.out_station \
                WHERE a.total_weight >80 AND a.out_station_time >= '{} 00:00:00'  AND a.out_station_time <'{} 00:00:00'    and a.is_unusual= 0 and c.is_deleted = 0 and c.station_status = 0 and c.station_code IS NOT null ORDER BY a.area_city,a.area_county,a.site_name".format(start_time, end_time)
    df_big = get_df_from_db(sql)
    sql = "SELECT * FROM t_sys_station where station_status =0 and station_type =31"
    t_sys_station = get_df_from_db(sql)

    station_name = t_sys_station['station_name']
    df_big = df_big[df_big.loc[:, '站点名称'].isin(station_name)]
    总重80吨以上数 = df_big.groupby(['county_code'])['record_code'].count().reset_index(name='本月超限80吨以上')
    总重90吨以上数 = df_big[df_big['total_weight']>90].groupby(['county_code'])['record_code'].count().reset_index(name='本月超限90吨以上')
    总重80_90以上 = pd.merge(总重80吨以上数, 总重90吨以上数, on=['county_code'],how='left')
    return 总重80_90以上,df_big

def total_weight_100_num():
    db = pymysql.connect(
        host='192.168.2.39',
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

    ##超限率以及完好率
    from datetime import datetime

    day = datetime.now().date()  # 获取当前系统时间
    today = datetime.now()
    ks = datetime.now()
    print('运行开始时间', ks)

    import calendar
    import datetime
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

    starttime2 = this_month_start + datetime.timedelta(days=0)

    endtime = this_month_end + datetime.timedelta(days=1)
    import datetime
    starttime3 = day - datetime.timedelta(days=0)
    starttime = day - datetime.timedelta(days=-1)
    print('starttime', starttime)
    print('starttime2', starttime2)
    starttime = starttime.strftime('%Y-%m-%d')
    starttime2 = starttime2.strftime('%Y-%m-%d')
    starttime3 = starttime3.strftime('%Y-%m-%d')
    import datetime

    today = datetime.datetime.today()
    year = today.year
    from dateutil.relativedelta import relativedelta
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    q = ('{}' + '-01-01').format(year)
    """ 引入原始表 """
    q案件 = q
    s案件 = starttime
    q合规率 = q
    s合规率 = starttime
    start_time = starttime2
    end_time = starttime
    end_time2 = starttime3
    import time
    q = start_time
    s = end_time
    start = time.time()

    sql = "SELECT * FROM t_sys_station "
    t_sys_station = get_df_from_db(sql)
    sql = "SELECT * FROM t_code_area  "
    t_code_area = get_df_from_db(sql)

    sql = "SELECT * FROM t_bas_over_data_31 where is_unusual = 0 and out_station_time >= '{} 00:00:00' and  out_station_time <'{} 00:00:00' and allow is null   and area_province=330000 and total_weight >= 100 ".format(
        q, s)
    t_bas_over_data_31 = get_df_from_db(sql)

    U_过车_区域表 = pd.merge(t_bas_over_data_31, t_code_area, left_on='area_county', right_on='county_code', how='left')
    U_过车_站点表 = pd.merge(U_过车_区域表, t_sys_station, left_on='out_station', right_on='station_code', how='left')
    U_过车_站点表 = U_过车_站点表[(U_过车_站点表['station_status'] == 0)]

    end = time.time()
    time = end - start

    U_过车_站点表 = U_过车_站点表[(U_过车_站点表['province'] == '浙江')
    ]
    U_过车_站点表 = pd.DataFrame(U_过车_站点表,
                            columns=["city", "county", "car_no", "out_station_time", "axis", "total_weight", "overrun",
                                     "overrun_rate", "site_name", "status_x", "is_collect", "is_unusual", "record_code",
                                     "photo1", "photo2", "photo3", "vedio", 'county_code'])
    U_过车_站点表 = U_过车_站点表.sort_values(by=['county_code', 'city', 'county', 'out_station_time']).reset_index(drop=True)
    U_过车_站点表 = U_过车_站点表.drop_duplicates(subset=['record_code'])
    百吨王数 = U_过车_站点表.groupby(['county_code'])['record_code'].count().reset_index(name='百吨王数')
    return  百吨王数,U_过车_站点表

def writing_data():
    db = pymysql.connect(
        host='192.168.2.39',
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

    ##超限率以及完好率
    from datetime import datetime

    day = datetime.now().date()  # 获取当前系统时间
    today = datetime.now()
    ks = datetime.now()
    print('运行开始时间', ks)

    import calendar
    import datetime
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

    starttime2 = this_month_start + datetime.timedelta(days=0)

    endtime = this_month_end + datetime.timedelta(days=1)
    import datetime
    starttime3 = day - datetime.timedelta(days=0)
    starttime = day - datetime.timedelta(days=-1)
    print('starttime', starttime)
    print('starttime2', starttime2)
    starttime = starttime.strftime('%Y-%m-%d')
    starttime2 = starttime2.strftime('%Y-%m-%d')
    starttime3 = starttime3.strftime('%Y-%m-%d')
    import datetime

    today = datetime.datetime.today()
    year = today.year
    from dateutil.relativedelta import relativedelta
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    q = ('{}' + '-01-01').format(year)
    """ 引入原始表 """
    q案件 = q
    s案件 = starttime
    q合规率 = q
    s合规率 = starttime
    start_time = starttime2
    end_time = starttime
    end_time2 = starttime3
    from datetime import datetime
    W_案件统计 = case_statistic()
    超限80数 = Compliance_rate()
    W_案件统计当年 = case_statistic当年()
    货运源头监控数据区县, 货运源头监控数据省市, 站点完好数 = Key_freight_sources()
    超限100数, 超限100明细 = over_100_num()
    总重80_90以上, 总重80_90以上明细 = total_weight_80_90()
    百吨王数, 百吨王明细 = total_weight_100_num()
    end_time3 = datetime.strptime(end_time2, '%Y-%m-%d').date()
    sql = "SELECT city_code, city, county_code, county FROM t_code_area where province_code = 330000 "
    t_code_area = get_df_from_db(sql)
    U_all = pd.merge(t_code_area, W_案件统计, on=['county_code'], how='left')
    U_all = pd.merge(U_all, W_案件统计当年, on=['county_code'], how='left')
    U_all = pd.merge(U_all, 超限80数, on=['city_code', 'city', 'county_code', 'county'], how='left')
    U_all = pd.merge(U_all, 货运源头监控数据区县, on=['county_code'], how='left')
    U_all = pd.merge(U_all, 超限100数, on=['county_code'], how='left')
    U_all = pd.merge(U_all, 总重80_90以上, on=['county_code'], how='left')
    U_all = pd.merge(U_all, 百吨王数, on=['county_code'], how='left')

    U_all['statistic_date'] = end_time3
    U_all = U_all.fillna(value=0)
    总重80_90以上明细['statistic_date'] = end_time3
    超限100明细['statistic_date'] = end_time3
    百吨王明细['statistic_date'] = end_time3
    货运源头监控数据省市['statistic_date'] = end_time3
    站点完好数['statistic_date'] = end_time3

    try:
        from sqlalchemy import create_engine

        user = "zjzhzcuser"
        password = "F4dus0ee"
        host = "192.168.2.39"
        db = "db_manage_overruns"

        pwd = parse.quote_plus(password)

        engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:3306/{db}?charset=utf8")
        # result_table 要写入的数据表，这样写的话要提前在数据库建好表
        U_all.to_sql(name='U_all_report', con=engine, if_exists='append', index=False)
        总重80_90以上明细.to_sql(name='detail_80_90', con=engine, if_exists='append', index=False)
        超限100明细.to_sql(name='detail_over_100', con=engine, if_exists='append', index=False)
        百吨王明细.to_sql(name='detail_total_100', con=engine, if_exists='append', index=False)
        # result_table 要写入的数据表，这样写的话要提前在数据库建好表
        货运源头监控数据省市.to_sql(name='source_city', con=engine, if_exists='append', index=False)
        站点完好数.to_sql(name='source_station', con=engine, if_exists='append', index=False)

    except Exception as e:
        print("mysql插入失败")


    # '''数据库删除'''
    # db = pymysql.connect(host="172.19.116.150", port=11806, user='zjzhzcuser', password='F4dus0ee',
    #                      database='db_manage_overruns')
    # # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
    # #                  database='jingdong_ceshi')
    # mycursor = db.cursor()
    # sql1 = "delete from U_all_report where statistic_date < '{}'".format(end_time2)
    # sql2 = "delete from detail_80_90 where statistic_date < '{}'".format(end_time2)
    # sql3 = "delete from detail_over_100 where statistic_date < '{}'".format(end_time2)
    # sql4 = "delete from detail_total_100 where statistic_date < '{}'".format(end_time2)
    # sql5 = "delete from source_city where statistic_date < '{}'".format(end_time2)
    # sql6 = "delete from source_station where statistic_date < '{}'".format(end_time2)
    # mycursor.execute(sql1)
    # mycursor.execute(sql2)
    # mycursor.execute(sql3)
    # mycursor.execute(sql4)
    # mycursor.execute(sql5)
    # mycursor.execute(sql6)
    # db.commit()
    # db.close()
    from threading import Timer
    import datetime

    """定时1天"""
    now_time = datetime.datetime.now()

    # next_time = now_time + datetime.timedelta(days=+1)
    next_time = now_time + datetime.timedelta(days=+1)
    next_year = next_time.date().year
    next_month = next_time.date().month
    next_day = next_time.date().day
    #
    # get next day 3:00 time
    next_time2 = datetime.datetime.strptime(
        str(next_year) + "-" + str(next_month) + "-" + str(next_day) + " " + "6:00:00", "%Y-%m-%d %H:%M:%S")
    print('下次12点程序的运行时间', next_time2)
    timer_start_time2 = (next_time2 - now_time).total_seconds()
    t2 = Timer(timer_start_time2, writing_data)  # 此处使用递归调用实现
    t2.start()




if __name__ == "__main__":
 writing_data()





