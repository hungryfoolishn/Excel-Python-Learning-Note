i=int(0)
I=int(1)
while i!=I:
            import pymysql
            import pandas as pd
            from datetime import datetime
            from urllib import parse
            import time


            def sleeptime(hour, min, sec):
                return 3600 * hour + 60 * min + sec


            print('暂停：', sleeptime(0, 0, 0), '秒')
            seconds = sleeptime(0, 0, 0)
            time.sleep(seconds)
            from datetime import datetime
            day = datetime.now().date()  # 获取当前系统时间
            today = datetime.now()
            ks = datetime.now()
            print('运行开始时间', ks)

            import datetime
            from dateutil.relativedelta import relativedelta
            starttime = day - datetime.timedelta(days=0)
            today = datetime.datetime.today()
            year = today.year
            now = datetime.datetime.now()
            this_month_start = datetime.datetime(now.year, now.month, 1).date()
            this_month_end = this_month_start + relativedelta(months=1)



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

            ##原始表获取
            # sql = "select  out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_21 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'".format(
            #     starttime, endtime)
            # t_bas_pass_data_21 = get_df_from_db(sql)
            sql = "SELECT a.id as company_id,c.county_code as area_code,c.county as area_name,b.station_code,a.source_company as company_name FROM t_bas_source_company a inner  JOIN t_bas_source_company_equipment b  on a.id = b.source_company_id " \
                  "left join t_code_area c on a.area_county =c.county_code where b.is_deleted = 0 OR b.is_deleted IS NULL "
            company_name = get_df_from_db(sql)
            company_name = pd.DataFrame(company_name)
            # pass_truck_num.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\pass_truck_num.xlsx')
            print(company_name)

            sql = "SELECT station_code,sum(pass_num) as pass_num,sum(truck_num) as truck_num,sum(overrun_num) as overrun_num,sum(overrun_100) AS overrun100_num,sum(total_weight_100) AS hundred_king_num,0 AS data_type " \
                  "from t_bas_basic_data_pass where statistic_date >= '{}' and statistic_date < '{}' AND station_type = 71 GROUP BY station_code".format(
                this_month_start, this_month_end)
            t_bas_pass_data_71 = get_df_from_db(sql)
            t_bas_pass_data_71 = pd.DataFrame(t_bas_pass_data_71)
            wide_table_71 = pd.merge(company_name, t_bas_pass_data_71, on=['station_code'], how='left')
            starttime2 = this_month_start.strftime("%Y-%m")
            wide_table_71['statistics_date']=starttime2

            starttime1 = this_month_start.strftime("%Y%m")
            wide_table_71 = wide_table_71.fillna(value=0)
            wide_table_71['car_num'] = 0
            wide_table_71['id'] = wide_table_71['station_code'].astype('string') + starttime1
            wide_table_71['company_id'] = wide_table_71['company_id'].astype('string')
            wide_table_71['area_code'] = wide_table_71['area_code'].astype('string')
            wide_table_71['area_name'] = wide_table_71['area_name'].astype('string')
            wide_table_71['statistics_date'] = wide_table_71['statistics_date'].astype('string')
            wide_table_71['company_name'] = wide_table_71['company_name'].astype('string')
            wide_table_71['pass_num'] = wide_table_71['pass_num'].values.astype('int')
            wide_table_71['truck_num'] = wide_table_71['truck_num'].values.astype('int')
            wide_table_71['overrun_num'] = wide_table_71['overrun_num'].values.astype('int')
            wide_table_71['overrun100_num'] = wide_table_71['overrun100_num'].values.astype('int')
            wide_table_71['hundred_king_num'] = wide_table_71['hundred_king_num'].values.astype('int')
            wide_table_71['data_type'] = wide_table_71['data_type'].values.astype('int')
            wide_table_71['car_num'] = wide_table_71['car_num'].values.astype('int')
            wide_table_71 = pd.DataFrame(wide_table_71,
                                      columns=['id', 'area_code', 'area_name', 'statistics_date', 'company_id', 'company_name',
                                               'car_num', 'pass_num', 'truck_num', 'overrun_num', 'overrun100_num',
                                               'hundred_king_num', 'data_type'])

            print(wide_table_71.dtypes)
            wide_table_71=pd.DataFrame(wide_table_71)
            with pd.ExcelWriter(r'C:\Users\liu.wenjie\Desktop\导出数据\源头企业0223.xlsx') as writer1:
                t_bas_pass_data_71.to_excel(writer1, sheet_name='全部', index=False)
                company_name.to_excel(writer1, sheet_name='company_name', index=False)
                wide_table_71.to_excel(writer1, sheet_name='wide_table_71', index=False)

            '''数据库删除'''
            db = pymysql.connect(host="172.19.116.150", port=11806, user='zjzhzcuser', password='F4dus0ee',
                                 database='db_manage_overruns')
            # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
            #                  database='jingdong_ceshi')
            mycursor = db.cursor()
            sql = "DELETE FROM t_bas_company_pass_statistics_data WHERE statistics_date = '{}'".format(starttime2)
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
                wide_table_71.to_sql(name='t_bas_company_pass_statistics_data', con=engine, if_exists='append', index=False)
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
            # wide_table_71.fillna("", inplace=True)  # 替换NaN,否则数据写入时会报错,也可替换成其他
            # # 连接数据库,定义变量
            # db = pymysql.connect(host='172.19.116.150', user='zjzhzcuser', password='F4dus0ee', port=11806,
            #                      db='db_manage_overruns')
            # cursor = db.cursor()
            # table = "t_bas_company_pass_statistics_data"  # 写入表名
            #
            # # 写入数据
            # DBUtils.insert_data(DBUtils(db, cursor, wide_table_71, table))


            del t_bas_pass_data_71
            from datetime import datetime

            js = datetime.now()
            sjc = js - ks
            print('运行耗时', sjc)


            seconds = sleeptime(10, 3, 0)
            time.sleep(seconds)