i=int(0)
I=int(1)
while i!=I:

            import pymysql
            import pandas as pd
            from urllib import parse
            import calendar
            import time
            def sleeptime(hour, min, sec):
                return 3600 * hour + 60 * min + sec
            print('暂停：', sleeptime(18, 0, 0), '秒')
            seconds = sleeptime(0, 0, 0)
            time.sleep(seconds)
            from datetime import datetime
            day = datetime.now().date()  # 获取当前系统时间
            today = datetime.now()
            ks = datetime.now()
            print('运行开始时间', ks)

            import datetime
            from dateutil.relativedelta import relativedelta

            now = datetime.datetime.now()
            this_month_start = datetime.datetime(now.year, now.month, 1).date()
            this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
            this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")

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


            """ 引入原始表 """

            ##原始表获取
            # sql = "select  out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_21 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'".format(
            #     starttime, endtime)
            # t_bas_pass_data_21 = get_df_from_db(sql)
            sql = "select COUNT(1) num,SUM(if(total_weight < 1.2,1,0)) sm,SUM(if(total_weight >= 1.2 and total_weight < 7,1,0)) kc,SUM(if(total_weight >= 7,1,0)) hc,car_no,area_city,  area_county ,out_station " \
                  "from t_bas_pass_data_31 where out_station_time>= '{}' and out_station_time < '{}'  GROUP by car_no,area_city,area_county ,out_station ".format(
                starttime, endtime)
            t_bas_pass_data_31 = get_df_from_db(sql)
            print(t_bas_pass_data_31)

            sql = "SELECT station_code FROM t_sys_station where is_deleted=0 and `status` !=1 and station_type = 31"
            t_sys_station = get_df_from_db(sql)

            station_code = t_sys_station['station_code']

            # t_bas_basic_data_pass71.to_excel(r'C:\Users\liu.wenjie\Desktop\测试\t_bas_basic_data_pass71.xlsx')

            # 区县汇总=t_bas_basic_data_pass.groupby(['地市','区县']).agg({'货车数': ['sum'],'超限数': ['sum'],'剔除10%超限数（不包含临界点）': ['sum'],'剔除20%超限数（不包含临界点）': ['sum']})

            ##站点汇总
            t_bas_pass_data_31['d_sm']=0
            t_bas_pass_data_31['d_kc']=0
            t_bas_pass_data_31['d_hc']=0
            t_bas_pass_data_31.loc[t_bas_pass_data_31['sm'] > 0, 'd_sm'] = 1
            t_bas_pass_data_31.loc[t_bas_pass_data_31['kc'] > 0, 'd_kc'] = 1
            t_bas_pass_data_31.loc[t_bas_pass_data_31['hc'] > 0, 'd_hc'] = 1
            t_bas_pass_data_31['num'] = t_bas_pass_data_31['num'].astype(int)
            t_bas_pass_data_31['sm'] = t_bas_pass_data_31['sm'].astype(int)
            t_bas_pass_data_31['kc'] = t_bas_pass_data_31['kc'].astype(int)
            t_bas_pass_data_31['hc']= t_bas_pass_data_31['hc'].astype(int)
            t_bas_pass_data_31 = t_bas_pass_data_31[t_bas_pass_data_31.loc[:, 'out_station'].isin(station_code)]
            zd_num = t_bas_pass_data_31.groupby(['area_city', 'area_county', 'out_station']).sum().reset_index()
            # zd_num = t_bas_pass_data_31.groupby(['area_city', 'area_county', 'out_station']).agg({'num': ['sum'],'sm': ['sum'],'kc': ['sum'],'hc': ['sum'],'d_sm': ['sum'],'d_kc': ['sum'],'d_hc': ['sum']}).reset_index()
            zd_num['area_type']=1


            ##区县汇总.
            qx_num = zd_num.groupby(['area_city', 'area_county']).sum().reset_index()
            # qx_num = zd_num.groupby(['area_city', 'area_county']).agg({'num': ['sum'],'sm': ['sum'],'kc': ['sum'],'hc': ['sum'],'d_sm': ['sum'],'d_kc': ['sum'],'d_hc': ['sum']}).reset_index()
            qx_num['area_type']=2

            ##地市汇总
            ds_num =qx_num.groupby(['area_city']).sum().reset_index()
            # ds_num = qx_num.groupby(['area_city']).agg({'num': ['sum'],'sm': ['sum'],'kc': ['sum'],'hc': ['sum'],'d_sm': ['sum'],'d_kc': ['sum'],'d_hc': ['sum']}).reset_index()
            ds_num['area_type']=3

            ##合并

            zd_num = pd.DataFrame(zd_num,
                                columns=[ 'out_station', 'area_type','num', 'sm', 'kc', 'hc', 'd_sm', 'd_kc', 'd_hc'])
            qx_num = pd.DataFrame(qx_num ,
                                columns=[ 'area_county','area_type', 'num', 'sm', 'kc', 'hc', 'd_sm', 'd_kc', 'd_hc'])

            zd_num.rename(
                columns={'out_station': 'area_code'}, inplace=True)
            qx_num.rename(
                columns={'area_county': 'area_code'}, inplace=True)
            ds_num.rename(
                columns={'area_city': 'area_code'}, inplace=True)

            U_all = pd.concat([zd_num, qx_num, ds_num])
            starttime1 = starttime.strftime("%Y%m%d")
            U_all['statistics_date'] = starttime.strftime("%Y-%m-%d")
            U_all = U_all.fillna(0)
            U_all['id'] = U_all['area_code'].astype('string') + starttime1
            U_all['d_num']=U_all['d_sm']+U_all['d_kc']+U_all['d_hc']
            # U_all.to_excel(r'C:\Users\liu.wenjie\Desktop\测试\U_all.xlsx')

            '''数据库删除'''
            db = pymysql.connect(host="172.19.116.150", port=11806, user='zjzhzcuser', password='F4dus0ee',
                                 database='db_manage_overruns')
            # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
            #                  database='jingdong_ceshi')
            mycursor = db.cursor()
            sql = "DELETE FROM t_bas_pass_ll_statistics_data WHERE statistics_date = '{}'".format(
                starttime.strftime("%Y-%m-%d"))
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
                U_all.to_sql(name='t_bas_pass_ll_statistics_data', con=engine, if_exists='append', index=False)
            except Exception as e:
                print("mysql插入失败")
            del t_bas_pass_data_31
            from datetime import datetime
            js = datetime.now()
            sjc = js - ks
            print('运行耗时', sjc)

