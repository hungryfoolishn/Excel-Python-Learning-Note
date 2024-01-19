import pymysql
import numpy as np
import pandas as pd
from urllib import parse


def case_month_statistic():
        # import time
        # def sleeptime(hour, min, sec):
        #     return 3600 * hour + 60 * min + sec
        #
        # print('暂停：', sleeptime(18, 0, 0), '秒')
        # seconds = sleeptime(18, 0, 0)
        # time.sleep(seconds)
        from datetime import datetime
        day = datetime.now().date()  # 获取当前系统时间
        today = datetime.now()
        from datetime import datetime
        ks = datetime.now()
        print('运行开始时间', ks)
        import datetime
        starttime = day - datetime.timedelta(days=0)
        import datetime
        today = datetime.datetime.today()
        year = today.year

        from dateutil.relativedelta import relativedelta
        now = datetime.datetime.now()
        this_month_start = datetime.datetime(now.year, now.month, 1).date()
        q = ('{}' + '-01-01').format(year)
        print('starttime', q)
        endtime = this_month_start + relativedelta(months=1)
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

        s = endtime

        sql = "SELECT * FROM t_code_area"
        t_code_area = get_df_from_db(sql)

        sql = "SELECT * FROM t_bas_over_data_collection_31 where valid_time >= '{} 00:00:00' and valid_time < '{} 00:00:00'".format(
            q, s)
        t_bas_over_data_collection_31 = get_df_from_db(sql)
        sql = "SELECT * FROM t_case_sign_result where close_case_time >= '{} 00:00:00' and close_case_time < '{} 00:00:00'".format(
            q, s)
        t_case_sign_result = get_df_from_db(sql)

        sql = "SELECT * FROM t_bas_over_data_collection_makecopy where insert_time >='{} 00:00:00' and insert_time <'{} 00:00:00' ".format(
            q, s)
        t_bas_over_data_collection_makecopy = get_df_from_db(sql)

        sql = "SELECT * FROM t_sys_station "
        t_sys_station = get_df_from_db(sql)

        U_检测点_区域表 = pd.merge(t_bas_over_data_collection_31, t_code_area, left_on='area_county', right_on='county_code',
                             how='left')

        U_案件处罚_区域表 = pd.merge(t_case_sign_result, t_code_area, left_on='area_county', right_on='county_code',
                              how='left')

        U_外省抄告_区域表 = pd.merge(t_bas_over_data_collection_makecopy, t_code_area, left_on='area_county',
                              right_on='county_code', how='left')

        T_入库查询 = U_检测点_区域表.loc[(U_检测点_区域表['law_judgment'] == "1")
        ]

        ###地市
        T_现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 99)
                            & (U_案件处罚_区域表.insert_type == 5)
                            & (U_案件处罚_区域表.area_province == '330000')
                            ]

        T_非现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 31)
                             & (U_案件处罚_区域表.insert_type == 1)
                             & (U_案件处罚_区域表.data_source == 1)
                             & (U_案件处罚_区域表.case_type == 1)
                             & (U_案件处罚_区域表.area_province == '330000')
                             ]

        T_外省抄告 = U_外省抄告_区域表

        T_入库查询 = T_入库查询.sort_values(['area_county'], ascending=False).reset_index(drop=True)

        T_入库数 = T_入库查询.groupby(['area_city', 'city', 'area_county', 'county'])['id_x'].count().reset_index(
            name='入库数(系统)')

        T_现场处罚 = T_现场处罚.drop_duplicates(['CASE_NUM'])
        T_现场处罚计数 = T_现场处罚.groupby(['area_city', 'city', 'area_county', 'county'])['CASE_NUM'].count().reset_index(
            name='现场处罚(系统)')

        T_非现场处罚 = T_非现场处罚.drop_duplicates(['CASE_NUM'])
        T_非现场处罚计数 = T_非现场处罚.groupby(['area_city', 'city', 'area_county', 'county'])['CASE_NUM'].count().reset_index(
            name='非现场处罚(系统)')

        T_外省抄告计数 = T_外省抄告.groupby(['area_city', 'city', 'area_county', 'county'])['id_x'].count().reset_index(
            name='外省抄告(系统)')

        W_案件统计 = pd.merge(T_入库数, T_现场处罚计数, on=['area_city', 'city', 'area_county', 'county'], how='outer')
        W_案件统计 = pd.merge(W_案件统计, T_非现场处罚计数, on=['area_city', 'city', 'area_county', 'county'], how='outer')
        W_案件统计 = pd.merge(W_案件统计, T_外省抄告计数, on=['area_city', 'city', 'area_county', 'county'], how='outer')
        W_案件统计[['外省抄告(系统)']] = W_案件统计[['外省抄告(系统)']].apply(np.int64)
        W_案件统计.loc[W_案件统计['外省抄告(系统)'] < 0, '外省抄告(系统)'] = 0
        W_案件统计 = W_案件统计.fillna(0, inplace=False)
        W_案件统计['案件数'] = W_案件统计['非现场处罚(系统)'] + W_案件统计['现场处罚(系统)']
        W_案件统计['非现场处罚率(不含抄告)'] = (W_案件统计['非现场处罚(系统)'] / (W_案件统计['入库数(系统)'] + 0.00001) * 100).round(2)
        W_案件统计['非现场处罚率(含抄告)'] = (
                    (W_案件统计['非现场处罚(系统)'] + W_案件统计['外省抄告(系统)']) / (W_案件统计['入库数(系统)'] + 0.00001) * 100).round(2)
        W_案件统计.loc[W_案件统计['非现场处罚率(含抄告)'] > 100, '非现场处罚率(含抄告)'] = 100
        starttime2 = starttime.strftime("%Y-%m")
        W_案件统计['statistics_date'] = starttime2
        # starttime1 = starttime.strftime("%Y%m")
        # W_案件统计['id']= W_案件统计['area_city'].astype('string')+W_案件统计['area_county'].astype('string') + starttime1
        # W_案件统计['id']=W_案件统计['id'].apply(np.int64)
        W_案件统计.rename(
            columns={'入库数(系统)': 'in_case_num',
                     '现场处罚(系统)': 'p_site_num', '非现场处罚(系统)': 'p_offsite_num', '外省抄告(系统)': 'f_inform_num',
                     '案件数': 'total_case_num', '非现场处罚率(不含抄告)': 'p_offsite_rate', '非现场处罚率(含抄告)': 'p_offsite_inform_rate'},
            inplace=True)
        W_案件统计区县 = pd.DataFrame(W_案件统计,
                                columns=['statistics_date', 'area_city', 'city',
                                         'area_county', 'county',
                                         'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
                                         'total_case_num',
                                         'p_offsite_rate', 'p_offsite_inform_rate'])

        ###地市
        W_案件统计地市 = W_案件统计区县.groupby(['statistics_date', 'area_city', 'city']).sum().reset_index()
        W_案件统计地市['p_offsite_rate'] = (W_案件统计地市['p_offsite_num'] / (W_案件统计地市['in_case_num'] + 0.00001) * 100).round(2)
        W_案件统计地市['p_offsite_inform_rate'] = ((W_案件统计地市['p_offsite_num'] + W_案件统计地市['f_inform_num']) / (
                    W_案件统计地市['in_case_num'] + 0.00001) * 100).round(2)
        W_案件统计地市.loc[W_案件统计地市['p_offsite_inform_rate'] > 100, 'p_offsite_inform_rate'] = 100

        ##省级
        W_案件统计省级 = W_案件统计地市.groupby(['statistics_date']).sum().reset_index()
        W_案件统计省级['p_offsite_rate'] = (W_案件统计省级['p_offsite_num'] / (W_案件统计省级['in_case_num'] + 0.00001) * 100).round(2)
        W_案件统计省级['p_offsite_inform_rate'] = ((W_案件统计省级['p_offsite_num'] + W_案件统计省级['f_inform_num']) / (
                    W_案件统计省级['in_case_num'] + 0.00001) * 100).round(2)
        W_案件统计省级.loc[W_案件统计省级['p_offsite_inform_rate'] > 100, 'p_offsite_inform_rate'] = 100
        W_案件统计省级['area_code'] = '330000'
        W_案件统计省级['area_name'] = '浙江'

        ##合并

        W_案件统计区县 = pd.DataFrame(W_案件统计,
                                columns=['statistics_date',
                                         'area_county', 'county',
                                         'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
                                         'total_case_num',
                                         'p_offsite_rate', 'p_offsite_inform_rate'])

        W_案件统计地市 = pd.DataFrame(W_案件统计地市,
                                columns=['statistics_date', 'area_city', 'city',
                                         'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
                                         'total_case_num',
                                         'p_offsite_rate', 'p_offsite_inform_rate'])

        W_案件统计区县.rename(
            columns={'area_county': 'area_code', 'county': 'area_name'}, inplace=True)
        W_案件统计地市.rename(
            columns={'area_city': 'area_code', 'city': 'area_name'}, inplace=True)

        W_案件统计 = pd.concat([W_案件统计区县, W_案件统计地市, W_案件统计省级])
        W_案件统计.loc[W_案件统计['p_offsite_rate'] > 100, 'p_offsite_rate'] = 100
        W_案件统计.loc[W_案件统计['p_offsite_inform_rate'] > 100, 'p_offsite_inform_rate'] = 100
        starttime1 = starttime.strftime("%Y%m")
        W_案件统计 = W_案件统计.fillna(0)
        W_案件统计['id'] = W_案件统计['area_code'].astype('string') + starttime1
        W_案件统计 = pd.DataFrame(W_案件统计,
                              columns=['id', 'statistics_date', 'area_code', 'area_name',
                                       'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
                                       'total_case_num',
                                       'p_offsite_rate', 'p_offsite_inform_rate'])

        # wide_table31.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\wide_table31.xlsx')

        print(W_案件统计)

        # W_案件统计.to_excel(r'C:\Users\liu.wenjie\Desktop\测试\W_案件统计22.xlsx')

        '''数据库删除'''
        db = pymysql.connect(host="172.19.116.150", port=11806, user='zjzhzcuser', password='F4dus0ee',
                             database='db_manage_overruns')
        # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
        #                  database='jingdong_ceshi')
        mycursor = db.cursor()
        sql = "DELETE FROM t_bas_case_statistics_data WHERE statistics_date = '{}'".format(starttime2)
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
            W_案件统计.to_sql(name='t_bas_case_statistics_data', con=engine, if_exists='append', index=False)
        except Exception as e:
            print("mysql插入失败")
        #
        # #
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
        # W_案件统计.fillna("", inplace=True)  # 替换NaN,否则数据写入时会报错,也可替换成其他
        # # 连接数据库,定义变量
        # db = pymysql.connect(host='172.19.116.150', user='zjzhzcuser', password='F4dus0ee', port=11806,
        #                      db='db_manage_overruns')
        # cursor = db.cursor()
        # table = "t_bas_case_statistics_data"  # 写入表名
        #
        # # 写入数据
        # DBUtils.insert_data(DBUtils(db, cursor, W_案件统计, table))
        del t_bas_over_data_collection_31
        del t_case_sign_result
        del t_bas_over_data_collection_makecopy

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
            str(next_year) + "-" + str(next_month) + "-" + str(next_day) + " " + "22:05:00", "%Y-%m-%d %H:%M:%S")
        print('下次12点程序的运行时间', next_time2)
        #
        # next_time = datetime.datetime.strptime(str(next_year) + "-" + str(next_month) + "-" + str(next_day) + " 22:00:00",
        #                                        "%Y-%m-%d %H:%M:%S")
        #

        timer_start_time2 = (next_time - now_time).total_seconds()

        t2 = Timer(timer_start_time2, case_month_statistic)  # 此处使用递归调用实现
        t2.start()



def case_day_statistic():
        from datetime import datetime
        day = datetime.now().date()  # 获取当前系统时间
        today = datetime.now()
        from datetime import datetime
        ks = datetime.now()
        print('运行开始时间', ks)
        import datetime
        starttime = day - datetime.timedelta(days=0)
        # import datetime
        # today = datetime.datetime.today()
        # year = today.year
        #
        # from dateutil.relativedelta import relativedelta
        # now = datetime.datetime.now()
        # this_month_start = datetime.datetime(now.year, now.month, 1).date()
        # q = ('{}' + '-01-01').format(year)
        print('starttime', starttime)
        endtime =  day + datetime.timedelta(days=1)
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



        sql = "SELECT * FROM t_code_area"
        t_code_area = get_df_from_db(sql)

        sql = "SELECT * FROM t_bas_over_data_collection_31 where valid_time >= '{} 00:00:00' and valid_time < '{} 00:00:00'".format(
            starttime, endtime)
        t_bas_over_data_collection_31 = get_df_from_db(sql)
        sql = "SELECT * FROM t_case_sign_result where close_case_time >= '{} 00:00:00' and close_case_time < '{} 00:00:00'".format(
            starttime, endtime)
        t_case_sign_result = get_df_from_db(sql)

        sql = "SELECT * FROM t_bas_over_data_collection_makecopy where insert_time >='{} 00:00:00' and insert_time <'{} 00:00:00' ".format(
            starttime, endtime)
        t_bas_over_data_collection_makecopy = get_df_from_db(sql)

        sql = "SELECT * FROM t_sys_station "
        t_sys_station = get_df_from_db(sql)

        U_检测点_区域表 = pd.merge(t_bas_over_data_collection_31, t_code_area, left_on='area_county', right_on='county_code',
                             how='left')

        U_案件处罚_区域表 = pd.merge(t_case_sign_result, t_code_area, left_on='area_county', right_on='county_code',
                              how='left')

        U_外省抄告_区域表 = pd.merge(t_bas_over_data_collection_makecopy, t_code_area, left_on='area_county',
                              right_on='county_code', how='left')

        T_入库查询 = U_检测点_区域表.loc[(U_检测点_区域表['law_judgment'] == "1")
        ]

        ###地市
        T_现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 99)
                            & (U_案件处罚_区域表.insert_type == 5)
                            & (U_案件处罚_区域表.area_province == '330000')
                            ]

        T_非现场处罚 = U_案件处罚_区域表[(U_案件处罚_区域表.record_type == 31)
                             & (U_案件处罚_区域表.insert_type == 1)
                             & (U_案件处罚_区域表.data_source == 1)
                             & (U_案件处罚_区域表.case_type == 1)
                             & (U_案件处罚_区域表.area_province == '330000')
                             ]

        T_外省抄告 = U_外省抄告_区域表

        T_入库查询 = T_入库查询.sort_values(['area_county'], ascending=False).reset_index(drop=True)

        T_入库数 = T_入库查询.groupby(['area_city', 'city', 'area_county', 'county'])['id_x'].count().reset_index(
            name='入库数(系统)')

        T_现场处罚 = T_现场处罚.drop_duplicates(['CASE_NUM'])
        T_现场处罚计数 = T_现场处罚.groupby(['area_city', 'city', 'area_county', 'county'])['CASE_NUM'].count().reset_index(
            name='现场处罚(系统)')

        T_非现场处罚 = T_非现场处罚.drop_duplicates(['CASE_NUM'])
        T_非现场处罚计数 = T_非现场处罚.groupby(['area_city', 'city', 'area_county', 'county'])['CASE_NUM'].count().reset_index(
            name='非现场处罚(系统)')

        T_外省抄告计数 = T_外省抄告.groupby(['area_city', 'city', 'area_county', 'county'])['id_x'].count().reset_index(
            name='外省抄告(系统)')

        W_案件统计 = pd.merge(T_入库数, T_现场处罚计数, on=['area_city', 'city', 'area_county', 'county'], how='outer')
        W_案件统计 = pd.merge(W_案件统计, T_非现场处罚计数, on=['area_city', 'city', 'area_county', 'county'], how='outer')
        W_案件统计 = pd.merge(W_案件统计, T_外省抄告计数, on=['area_city', 'city', 'area_county', 'county'], how='outer')
        W_案件统计[['外省抄告(系统)']] = W_案件统计[['外省抄告(系统)']].apply(np.int64)
        W_案件统计.loc[W_案件统计['外省抄告(系统)'] < 0, '外省抄告(系统)'] = 0
        W_案件统计 = W_案件统计.fillna(0, inplace=False)
        W_案件统计['案件数'] = W_案件统计['非现场处罚(系统)'] + W_案件统计['现场处罚(系统)']
        W_案件统计['非现场处罚率(不含抄告)'] = (W_案件统计['非现场处罚(系统)'] / (W_案件统计['入库数(系统)'] + 0.00001) * 100).round(2)
        W_案件统计['非现场处罚率(含抄告)'] = (
                    (W_案件统计['非现场处罚(系统)'] + W_案件统计['外省抄告(系统)']) / (W_案件统计['入库数(系统)'] + 0.00001) * 100).round(2)
        W_案件统计.loc[W_案件统计['非现场处罚率(含抄告)'] > 100, '非现场处罚率(含抄告)'] = 100
        starttime2 = starttime.strftime("%Y-%m-%d")
        W_案件统计['statistics_date'] = starttime
        # starttime1 = starttime.strftime("%Y%m")
        # W_案件统计['id']= W_案件统计['area_city'].astype('string')+W_案件统计['area_county'].astype('string') + starttime1
        # W_案件统计['id']=W_案件统计['id'].apply(np.int64)
        W_案件统计.rename(
            columns={'入库数(系统)': 'in_case_num',
                     '现场处罚(系统)': 'p_site_num', '非现场处罚(系统)': 'p_offsite_num', '外省抄告(系统)': 'f_inform_num',
                     '案件数': 'total_case_num', '非现场处罚率(不含抄告)': 'p_offsite_rate', '非现场处罚率(含抄告)': 'p_offsite_inform_rate'},
            inplace=True)
        W_案件统计区县 = pd.DataFrame(W_案件统计,
                                columns=['statistics_date', 'area_city', 'city',
                                         'area_county', 'county',
                                         'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
                                         'total_case_num',
                                         'p_offsite_rate', 'p_offsite_inform_rate'])

        ###地市
        W_案件统计地市 = W_案件统计区县.groupby(['statistics_date', 'area_city', 'city']).sum().reset_index()
        W_案件统计地市['p_offsite_rate'] = (W_案件统计地市['p_offsite_num'] / (W_案件统计地市['in_case_num'] + 0.00001) * 100).round(2)
        W_案件统计地市['p_offsite_inform_rate'] = ((W_案件统计地市['p_offsite_num'] + W_案件统计地市['f_inform_num']) / (
                    W_案件统计地市['in_case_num'] + 0.00001) * 100).round(2)
        W_案件统计地市.loc[W_案件统计地市['p_offsite_inform_rate'] > 100, 'p_offsite_inform_rate'] = 100

        ##省级
        W_案件统计省级 = W_案件统计地市.groupby(['statistics_date']).sum().reset_index()
        W_案件统计省级['p_offsite_rate'] = (W_案件统计省级['p_offsite_num'] / (W_案件统计省级['in_case_num'] + 0.00001) * 100).round(2)
        W_案件统计省级['p_offsite_inform_rate'] = ((W_案件统计省级['p_offsite_num'] + W_案件统计省级['f_inform_num']) / (
                    W_案件统计省级['in_case_num'] + 0.00001) * 100).round(2)
        W_案件统计省级.loc[W_案件统计省级['p_offsite_inform_rate'] > 100, 'p_offsite_inform_rate'] = 100
        W_案件统计省级['area_code'] = '330000'
        W_案件统计省级['area_name'] = '浙江'

        ##合并

        W_案件统计区县 = pd.DataFrame(W_案件统计,
                                columns=['statistics_date',
                                         'area_county', 'county',
                                         'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
                                         'total_case_num',
                                         'p_offsite_rate', 'p_offsite_inform_rate'])

        W_案件统计地市 = pd.DataFrame(W_案件统计地市,
                                columns=['statistics_date', 'area_city', 'city',
                                         'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
                                         'total_case_num',
                                         'p_offsite_rate', 'p_offsite_inform_rate'])

        W_案件统计区县.rename(
            columns={'area_county': 'area_code', 'county': 'area_name'}, inplace=True)
        W_案件统计地市.rename(
            columns={'area_city': 'area_code', 'city': 'area_name'}, inplace=True)

        W_案件统计 = pd.concat([W_案件统计区县, W_案件统计地市, W_案件统计省级])
        W_案件统计.loc[W_案件统计['p_offsite_rate'] > 100, 'p_offsite_rate'] = 100
        W_案件统计.loc[W_案件统计['p_offsite_inform_rate'] > 100, 'p_offsite_inform_rate'] = 100
        starttime1 = starttime.strftime("%Y%m%d")
        W_案件统计 = W_案件统计.fillna(0)
        W_案件统计['id'] = W_案件统计['area_code'].astype('string') + starttime1
        W_案件统计 = pd.DataFrame(W_案件统计,
                              columns=['id', 'statistics_date', 'area_code', 'area_name',
                                       'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
                                       'total_case_num',
                                       'p_offsite_rate', 'p_offsite_inform_rate'])

        # wide_table31.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\wide_table31.xlsx')

        print(W_案件统计)

        # W_案件统计.to_excel(r'C:\Users\liu.wenjie\Desktop\测试\W_案件统计22.xlsx')

        '''数据库删除'''
        db = pymysql.connect(host="172.19.116.150", port=11806, user='zjzhzcuser', password='F4dus0ee',
                             database='db_manage_overruns')
        # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
        #                  database='jingdong_ceshi')
        mycursor = db.cursor()
        sql = "DELETE FROM t_bas_case_statistics_data WHERE statistics_date = '{}'".format(starttime2)
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
            W_案件统计.to_sql(name='t_bas_case_statistics_data', con=engine, if_exists='append', index=False)
        except Exception as e:
            print("mysql插入失败")
        #
        # #
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
        # W_案件统计.fillna("", inplace=True)  # 替换NaN,否则数据写入时会报错,也可替换成其他
        # # 连接数据库,定义变量
        # db = pymysql.connect(host='172.19.116.150', user='zjzhzcuser', password='F4dus0ee', port=11806,
        #                      db='db_manage_overruns')
        # cursor = db.cursor()
        # table = "t_bas_case_statistics_data"  # 写入表名
        #
        # # 写入数据
        # DBUtils.insert_data(DBUtils(db, cursor, W_案件统计, table))
        del t_bas_over_data_collection_31
        del t_case_sign_result
        del t_bas_over_data_collection_makecopy


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
            str(next_year) + "-" + str(next_month) + "-" + str(next_day) + " " + "22:00:00", "%Y-%m-%d %H:%M:%S")
        print('下次12点程序的运行时间', next_time2)
        #
        # next_time = datetime.datetime.strptime(str(next_year) + "-" + str(next_month) + "-" + str(next_day) + " 22:00:00",
        #                                        "%Y-%m-%d %H:%M:%S")
        #

        timer_start_time2 = (next_time - now_time).total_seconds()

        t2 = Timer(timer_start_time2, case_day_statistic)  # 此处使用递归调用实现
        t2.start()

if __name__ == "__main__":
    case_month_statistic()
    case_day_statistic()