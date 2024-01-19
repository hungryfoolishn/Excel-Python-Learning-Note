# coding:utf8

def t_bas_basic_data_pass():
            print("开始运行6点的程序")
            import pymysql
            import pandas as pd
            from datetime import datetime
            from urllib import parse

            from datetime import datetime
            day = datetime.now().date()  # 获取当前系统时间
            today = datetime.now()
            ks = datetime.now()
            print('运行开始时间', ks)

            import datetime

            # starttime = day - datetime.timedelta(days=1)
            # print('starttime', starttime)
            # endtime = day + datetime.timedelta(days=0)
            # print('endtime', endtime)
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
            sql = """ SELECT record_code,out_station,out_station_time,car_no,total_weight,limit_weight,overrun,axis,site_name,direction,overrun_rate,area_city,area_county FROM t_bas_over_data_31 a  WHERE out_station_time >= '2023-01-01 00:00:00' AND out_station_time < '2023-08-01 00:00:00' and a.area_county=330781 and a.car_no like '%牌%' and a.overrun_rate>50 and a.allow is NULL and a.is_unusual =0 """
            wide_table = get_df_from_db(sql)

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
                wide_table.to_sql(name='t_bas_lanxi_no_car', con=engine, if_exists='append', index=False)
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
            # wide_table.fillna("", inplace=True)  # 替换NaN,否则数据写入时会报错,也可替换成其他
            # # 连接数据库,定义变量
            # db = pymysql.connect(host='172.19.116.150', user='zjzhzcuser', password='F4dus0ee', port=11806,
            #                      db='db_manage_overruns')
            # cursor = db.cursor()
            # table = "t_bas_lanxi_no_car"  # 写入表名
            #
            # # 写入数据
            # DBUtils.insert_data(DBUtils(db, cursor, wide_table, table))

            from datetime import datetime

            js = datetime.now()
            sjc = js - ks
            print('运行耗时', sjc)


            from threading import Timer
            import datetime
            #每隔两秒执行一次任务
            # t = Timer(2, t_bas_basic_data_pass)  # 此处使用递归调用实现
            # t.start()

            """定时间隔1分钟"""
            now_time = datetime.datetime.now()

            next_time = now_time + datetime.timedelta(minutes=+160)
            print('next_time', next_time)

            timer_start_time = (next_time - now_time).total_seconds()
            print(timer_start_time)

            timer = Timer(timer_start_time, t_bas_basic_data_pass)
            timer.start()

            # """定时1天"""
            # now_time = datetime.datetime.now()
            #
            # next_time = now_time + datetime.timedelta(days=+1)
            # next_year = next_time.date().year
            # next_month = next_time.date().month
            # next_day = next_time.date().day
            # #
            # # get next day 3:00 time
            # next_time = datetime.datetime.strptime(str(next_year)+"-"+str(next_month)+"-"+str(next_day)+" "+"6:00:00","%Y-%m-%d %H:%M:%S")
            # print('下次6点程序的运行时间',next_time)
            # #
            # # next_time = datetime.datetime.strptime(str(next_year) + "-" + str(next_month) + "-" + str(next_day) + " 22:00:00",
            # #                                        "%Y-%m-%d %H:%M:%S")
            # #
            # timer_start_time = (next_time - now_time).total_seconds()
            # #
            # # timer = Timer(timer_start_time, t_bas_basic_data_pass)
            # # timer.start()
            # t = Timer(timer_start_time, t_bas_basic_data_pass)  # 此处使用递归调用实现
            # t.start()

if __name__ == "__main__":
    t_bas_basic_data_pass()

