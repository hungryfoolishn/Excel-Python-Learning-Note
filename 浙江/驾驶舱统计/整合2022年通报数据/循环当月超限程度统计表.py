i=int(0)
I=int(1)
while i!=I:

            import pymysql
            import pandas as pd
            from datetime import datetime
            from urllib import parse
            import calendar

            day = datetime.now().date()  # 获取当前系统时间

            today = datetime.now()

            from datetime import datetime

            ks = datetime.now()
            print('运行开始时间', ks)

            import datetime
            now = datetime.datetime.now()
            this_month_start = datetime.datetime(now.year, now.month, 1).date()
            this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
            this_month =datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")


            starttime = this_month_start+datetime.timedelta(days=0)
            print('starttime', starttime)
            endtime = this_month_end + datetime.timedelta(days=1)
            print('endtime', endtime)
            import time


            def sleeptime(hour, min, sec):
                return 3600 * hour + 60 * min + sec


            print('暂停：', sleeptime(1, 0, 0), '秒')
            seconds = sleeptime(5, 0, 30)
            time.sleep(seconds)
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
            sql = "SELECT * FROM t_bas_basic_data_pass WHERE statistic_date >= '{}' AND statistic_date < '{}' and station_status=0 and is_check_station =1  and station_type = 31".format(
                starttime, endtime)
            t_bas_basic_data_pass = get_df_from_db(sql)

            sql = "SELECT * FROM t_bas_basic_data_pass WHERE statistic_date >= '{}' AND statistic_date < '{}' and station_status=0   and station_type = 71".format(
                starttime, endtime)
            t_bas_basic_data_pass71 = get_df_from_db(sql)

            # t_bas_basic_data_pass71.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\t_bas_basic_data_pass71.xlsx')

            # 区县汇总=t_bas_basic_data_pass.groupby(['地市','区县']).agg({'货车数': ['sum'],'超限数': ['sum'],'剔除10%超限数（不包含临界点）': ['sum'],'剔除20%超限数（不包含临界点）': ['sum']})


            ###非现场表
            # county_sum31= t_bas_basic_data_pass[(t_bas_basic_data_pass.station_type == 31)]
            county_sum31 = t_bas_basic_data_pass.loc[(t_bas_basic_data_pass.station_type == 31)]
            ##站点
            county_sum31= county_sum31.groupby(['station_code','station_name']).sum().reset_index()
            county_sum31= pd.DataFrame(county_sum31)
            # county_sum31.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\county_sum311230.xlsx')
            county_sum31['hundred_king_num']=county_sum31['total_weight_100']
            county_sum31['overrun100_num']=county_sum31['overrun_100']
            county_sum31['overrun_rate']=(county_sum31['overrun_num']/county_sum31['truck_num']*100).round(2)
            county_sum31['overrun020_count']=county_sum31['overrun_0_10']+county_sum31['overrun_10_20']
            county_sum31['overrun020_rate']=(county_sum31['overrun020_count']/county_sum31['truck_num']*100).round(2)
            county_sum31['overrun2050_count']=county_sum31['overrun_20_30']+county_sum31['overrun_30_40']+county_sum31['overrun_40_50']
            county_sum31['overrun2050_rate']=(county_sum31['overrun2050_count']/county_sum31['truck_num']*100).round(2)
            county_sum31['overrun50100_count']=county_sum31['overrun_50_60']+county_sum31['overrun_60_70']+county_sum31['overrun_70_80']+county_sum31['overrun_80_90']+county_sum31['overrun_90_100']
            county_sum31['overrun50100_rate']=(county_sum31['overrun50100_count']/county_sum31['truck_num']*100).round(2)
            county_sum31['overrun100_count']=county_sum31['overrun_100']
            county_sum31['overrun100_rate']=(county_sum31['overrun100_count']/county_sum31['truck_num']*100).round(2)
            county_sum31['station_type']=31
            county_sum31['update_time']=today
            county_sum31['create_time']=today
            county_sum31['statistics_date']=this_month
            this_month2 =datetime.datetime(now.year, now.month, 1).strftime("%Y%m")
            county_sum31['id']=this_month2+county_sum31['county_code'].astype('str')+county_sum31['station_type'].astype('str')
            county_sum31 = pd.DataFrame(county_sum31,
                                      columns=['id', 'statistics_date', 'city_code', 'city_name', 'county_code', 'county_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])

            ##县级


            county_sum31= county_sum31.groupby(['city_code','city_name','county_code','county_name']).sum().reset_index()
            county_sum31= pd.DataFrame(county_sum31)
            # county_sum31.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\county_sum311230.xlsx')
            county_sum31['hundred_king_num']=county_sum31['total_weight_100']
            county_sum31['overrun100_num']=county_sum31['overrun_100']
            county_sum31['overrun_rate']=(county_sum31['overrun_num']/county_sum31['truck_num']*100).round(2)
            county_sum31['overrun020_count']=county_sum31['overrun_0_10']+county_sum31['overrun_10_20']
            county_sum31['overrun020_rate']=(county_sum31['overrun020_count']/county_sum31['truck_num']*100).round(2)
            county_sum31['overrun2050_count']=county_sum31['overrun_20_30']+county_sum31['overrun_30_40']+county_sum31['overrun_40_50']
            county_sum31['overrun2050_rate']=(county_sum31['overrun2050_count']/county_sum31['truck_num']*100).round(2)
            county_sum31['overrun50100_count']=county_sum31['overrun_50_60']+county_sum31['overrun_60_70']+county_sum31['overrun_70_80']+county_sum31['overrun_80_90']+county_sum31['overrun_90_100']
            county_sum31['overrun50100_rate']=(county_sum31['overrun50100_count']/county_sum31['truck_num']*100).round(2)
            county_sum31['overrun100_count']=county_sum31['overrun_100']
            county_sum31['overrun100_rate']=(county_sum31['overrun100_count']/county_sum31['truck_num']*100).round(2)
            county_sum31['station_type']=31
            county_sum31['update_time']=today
            county_sum31['create_time']=today
            county_sum31['statistics_date']=this_month
            this_month2 =datetime.datetime(now.year, now.month, 1).strftime("%Y%m")
            county_sum31['id']=this_month2+county_sum31['county_code'].astype('str')+county_sum31['station_type'].astype('str')
            county_sum31 = pd.DataFrame(county_sum31,
                                      columns=['id', 'statistics_date', 'city_code', 'city_name', 'county_code', 'county_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])
            ##市级
            county_sum地市= county_sum31.groupby(['city_code','city_name']).sum().reset_index()
            county_sum地市['overrun_rate']=(county_sum地市['overrun_num']/county_sum地市['truck_num']*100).round(2)
            county_sum地市['overrun020_rate']=(county_sum地市['overrun020_count']/county_sum地市['truck_num']*100).round(2)
            county_sum地市['overrun2050_rate']=(county_sum地市['overrun2050_count']/county_sum地市['truck_num']*100).round(2)
            county_sum地市['overrun50100_rate']=(county_sum地市['overrun50100_count']/county_sum地市['truck_num']*100).round(2)
            county_sum地市['overrun100rate']=(county_sum地市['overrun100_count']/county_sum地市['truck_num']*100).round(2)
            county_sum地市['station_type']=31
            county_sum地市['update_time']=today
            county_sum地市['create_time']=today
            county_sum地市['statistics_date']=this_month
            county_sum地市['id']=this_month2+county_sum地市['city_code'].astype('str')+county_sum地市['station_type'].astype('str')

            ###省级
            county_sum省=county_sum地市
            county_sum省['area_code']='330000'
            county_sum省['area_name']='浙江'
            county_sum省= county_sum省.groupby(['area_code','area_name']).sum().reset_index()
            county_sum省.drop(['overrun100rate'],axis=1,inplace=True)
            county_sum省['overrun_rate']=(county_sum省['overrun_num']/county_sum省['truck_num']*100).round(2)
            county_sum省['overrun020_rate']=(county_sum省['overrun020_count']/county_sum省['truck_num']*100).round(2)
            county_sum省['overrun2050_rate']=(county_sum省['overrun2050_count']/county_sum省['truck_num']*100).round(2)
            county_sum省['overrun50100_rate']=(county_sum省['overrun50100_count']/county_sum省['truck_num']*100).round(2)
            county_sum省['overrun100_rate']=(county_sum省['overrun100_count']/county_sum省['truck_num']*100).round(2)
            county_sum省['station_type']=31
            county_sum省['update_time']=today
            county_sum省['create_time']=today
            county_sum省['statistics_date']=this_month
            county_sum省['id']=this_month2+county_sum省['area_code'].astype('str')+county_sum省['station_type'].astype('str')
            # county_sum省.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\county_sum省.xlsx')

            ##合并


            county_sum31 = pd.DataFrame(county_sum31,
                                      columns=['id', 'statistics_date','county_code', 'county_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])
            county_sum地市 = pd.DataFrame(county_sum地市,
                                      columns=['id', 'statistics_date','city_code', 'city_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])
            county_sum31.rename(
                columns={'county_code': 'area_code','county_name': 'area_name'}, inplace=True)
            county_sum地市.rename(
                columns={'city_code': 'area_code','city_name': 'area_name'}, inplace=True)

            wide_table31 = pd.concat([county_sum31,county_sum地市,county_sum省])
            # wide_table31.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\wide_table31.xlsx')

            ###货运源头表
            ##县级
            county_sum71 = t_bas_basic_data_pass71
            county_sum71= county_sum71.groupby(['city_code','city_name','county_code','county_name']).sum().reset_index()
            county_sum71= pd.DataFrame(county_sum71)
            # county_sum71.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\county_sum71区县汇总.xlsx')
            county_sum71['hundred_king_num']=county_sum71['total_weight_100']
            county_sum71['overrun100_num']=county_sum71['overrun_100']
            county_sum71['overrun_rate']=(county_sum71['overrun_num']/county_sum71['truck_num']*100).round(2)
            county_sum71['overrun020_count']=county_sum71['overrun_0_10']+county_sum71['overrun_10_20']
            county_sum71['overrun020_rate']=(county_sum71['overrun020_count']/county_sum71['truck_num']*100).round(2)
            county_sum71['overrun2050_count']=county_sum71['overrun_20_30']+county_sum71['overrun_30_40']+county_sum71['overrun_40_50']
            county_sum71['overrun2050_rate']=(county_sum71['overrun2050_count']/county_sum71['truck_num']*100).round(2)
            county_sum71['overrun50100_count']=county_sum71['overrun_50_60']+county_sum71['overrun_60_70']+county_sum71['overrun_70_80']+county_sum71['overrun_80_90']+county_sum71['overrun_90_100']
            county_sum71['overrun50100_rate']=(county_sum71['overrun50100_count']/county_sum71['truck_num']*100).round(2)
            county_sum71['overrun100_count']=county_sum71['overrun_100']
            county_sum71['overrun100_rate']=(county_sum71['overrun100_count']/county_sum71['truck_num']*100).round(2)
            county_sum71['station_type']=71
            county_sum71['update_time']=today
            county_sum71['create_time']=today
            county_sum71['statistics_date']=this_month
            this_month2 =datetime.datetime(now.year, now.month, 1).strftime("%Y%m")
            county_sum71['id']=this_month2+county_sum71['county_code'].astype('str')+county_sum71['station_type'].astype('str')
            county_sum71 = pd.DataFrame(county_sum71,
                                      columns=['id', 'statistics_date', 'city_code', 'city_name', 'county_code', 'county_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])
            ##市级
            county_sum地市71= county_sum71.groupby(['city_code','city_name']).sum().reset_index()
            county_sum地市71['overrun_rate']=(county_sum地市71['overrun_num']/county_sum地市71['truck_num']*100).round(2)
            county_sum地市71['overrun020_rate']=(county_sum地市71['overrun020_count']/county_sum地市71['truck_num']*100).round(2)
            county_sum地市71['overrun2050_rate']=(county_sum地市71['overrun2050_count']/county_sum地市71['truck_num']*100).round(2)
            county_sum地市71['overrun50100_rate']=(county_sum地市71['overrun50100_count']/county_sum地市71['truck_num']*100).round(2)
            county_sum地市71['overrun100rate']=(county_sum地市71['overrun100_count']/county_sum地市71['truck_num']*100).round(2)
            county_sum地市71['station_type']=71
            county_sum地市71['update_time']=today
            county_sum地市71['create_time']=today
            county_sum地市71['statistics_date']=this_month
            county_sum地市71['id']=this_month2+county_sum地市71['city_code'].astype('str')+county_sum地市71['station_type'].astype('str')

            ###省级
            county_sum省71=county_sum地市71
            county_sum省71['area_code']='330000'
            county_sum省71['area_name']='浙江'
            county_sum省71= county_sum省71.groupby(['area_code','area_name']).sum().reset_index()
            county_sum省71.drop(['overrun100rate'],axis=1,inplace=True)
            county_sum省71['overrun_rate']=(county_sum省71['overrun_num']/county_sum省71['truck_num']*100).round(2)
            county_sum省71['overrun020_rate']=(county_sum省71['overrun020_count']/county_sum省71['truck_num']*100).round(2)
            county_sum省71['overrun2050_rate']=(county_sum省71['overrun2050_count']/county_sum省71['truck_num']*100).round(2)
            county_sum省71['overrun50100_rate']=(county_sum省71['overrun50100_count']/county_sum省71['truck_num']*100).round(2)
            county_sum省71['overrun100_rate']=(county_sum省71['overrun100_count']/county_sum省71['truck_num']*100).round(2)
            county_sum省71['station_type']=71
            county_sum省71['update_time']=today
            county_sum省71['create_time']=today
            county_sum省71['statistics_date']=this_month
            county_sum省71['id']=this_month2+county_sum省71['area_code'].astype('str')+county_sum省71['station_type'].astype('str')
            # county_sum省71.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\county_sum省71.xlsx')

            ##合并


            county_sum71 = pd.DataFrame(county_sum71,
                                      columns=['id', 'statistics_date','county_code', 'county_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])
            county_sum地市71 = pd.DataFrame(county_sum地市71,
                                      columns=['id', 'statistics_date','city_code', 'city_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])
            county_sum71.rename(
                columns={'county_code': 'area_code','county_name': 'area_name'}, inplace=True)
            county_sum地市71.rename(
                columns={'city_code': 'area_code','city_name': 'area_name'}, inplace=True)

            wide_table71 = pd.concat([county_sum71,county_sum地市71,county_sum省71])
            wide_table = pd.concat([wide_table31 ,wide_table71])

            wide_table = wide_table.fillna(value=0)
            wide_table = wide_table[(wide_table.id != 0)]
            wide_table = wide_table[(wide_table.id != '0')]
            wide_table['overrun100_num']=wide_table['overrun100_count']
            wide_table['device_good_rate']=0.00
            wide_table['off_site_punish_rate']=0.00
            wide_table['off_site_qualified_rate']=0.00
            wide_table = pd.DataFrame(wide_table,
                                      columns=['id', 'statistics_date','area_code', 'area_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun100_num','overrun_rate', 'device_good_rate','off_site_punish_rate','off_site_qualified_rate','overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])
            # print(wide_table.info())
            #
            wide_table= wide_table[(wide_table.area_code !='0')
            ]
            wide_table.drop_duplicates('id', keep='last',inplace=True)
            # wide_table.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\12月\wide_table.xlsx')

            '''数据库删除'''
            db = pymysql.connect(host="172.19.116.150", port=11806, user='zjzhzcuser', password='F4dus0ee',
                                 database='db_manage_overruns')
            # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
            #                  database='jingdong_ceshi')
            mycursor = db.cursor()
            sql = "DELETE FROM t_bas_pass_statistics_data WHERE statistics_date = '{}'".format(this_month)
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
                wide_table.to_sql(name='t_bas_pass_statistics_data', con=engine, if_exists='append', index=False)
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
            #                 print(self.cursor.execute(sql, tuple(self.data.loc[i]) * 2))
            #                 self.db.commit()
            #             except Exception as e:
            #                 print("数据写入失败,原因为:" + e)
            #                 self.db.rollback()
            #
            #
            #         self.cursor.close()
            #         self.db.close()
            #         print('数据已全部写入完成!')
            #
            #
            # wide_table.fillna("", inplace=True)  # 替换NaN,否则数据写入时会报错,也可替换成其他
            # # 连接数据库,定义变量
            # db = pymysql.connect(host='172.19.116.150', user='zjzhzcuser', password='F4dus0ee', port=11806,
            #                      db='db_manage_overruns')
            # cursor = db.cursor()
            # table = "t_bas_pass_statistics_data_test"  # 写入表名
            #
            # # 写入数据
            # DBUtils.insert_data(DBUtils(db, cursor, wide_table, table))

            del t_bas_basic_data_pass
            del t_bas_basic_data_pass71

            from datetime import datetime

            js = datetime.now()
            sjc = js - ks
            print('运行耗时', sjc)
