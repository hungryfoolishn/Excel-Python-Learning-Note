i=int(0)
I=int(1)
while i!=I:

            import pymysql
            import pandas as pd
            from datetime import datetime
            from urllib import parse
            import calendar

            day = datetime.now().date()  # ��ȡ��ǰϵͳʱ��

            today = datetime.now()

            from datetime import datetime

            ks = datetime.now()
            print('���п�ʼʱ��', ks)

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


            print('��ͣ��', sleeptime(1, 0, 0), '��')
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
                cursor = db.cursor()  # ʹ��cursor()������ȡ����ִ��SQL�����α�
                cursor.execute(sql)  # ִ��SQL���
                """
                ʹ��fetchall������Ԫ����ʽ�������в�ѯ�������ӡ����
                fetchone()���ص�һ�У�fetchmany(n)����ǰn��
                �α�ִ��һ�κ���λ�ڵ�ǰ�����У���һ�β����ӵ�ǰ�����п�ʼ
                """
                data = cursor.fetchall()

                # ����Ϊ����ȡ������ת��Ϊdataframe��ʽ
                columnDes = cursor.description  # ��ȡ���Ӷ����������Ϣ
                columnNames = [columnDes[i][0] for i in range(len(columnDes))]  # ��ȡ����
                df = pd.DataFrame([list(i) for i in data], columns=columnNames)  # �õ���dataΪ��άԪ�飬����ȡ����ת��Ϊ�б���ת��Ϊdf

                """
                ʹ�����֮����ر��α�����ݿ����ӣ�������Դռ��,cursor.close(),db.close()
                db.commit()�������ݿ�������޸ģ�������ύ֮���ٹر�
                """

                return df





            """ ����ԭʼ�� """

            ##ԭʼ���ȡ
            # sql = "select  out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_21 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'".format(
            #     starttime, endtime)
            # t_bas_pass_data_21 = get_df_from_db(sql)
            sql = "SELECT * FROM t_bas_basic_data_pass WHERE statistic_date >= '{}' AND statistic_date < '{}' and station_status=0 and is_check_station =1  and station_type = 31".format(
                starttime, endtime)
            t_bas_basic_data_pass = get_df_from_db(sql)

            sql = "SELECT * FROM t_bas_basic_data_pass WHERE statistic_date >= '{}' AND statistic_date < '{}' and station_status=0   and station_type = 71".format(
                starttime, endtime)
            t_bas_basic_data_pass71 = get_df_from_db(sql)

            # t_bas_basic_data_pass71.to_excel(r'C:\Users\liu.wenjie\Desktop\�±�\10��\t_bas_basic_data_pass71.xlsx')

            # ���ػ���=t_bas_basic_data_pass.groupby(['����','����']).agg({'������': ['sum'],'������': ['sum'],'�޳�10%���������������ٽ�㣩': ['sum'],'�޳�20%���������������ٽ�㣩': ['sum']})


            ###���ֳ���
            # county_sum31= t_bas_basic_data_pass[(t_bas_basic_data_pass.station_type == 31)]
            county_sum31 = t_bas_basic_data_pass.loc[(t_bas_basic_data_pass.station_type == 31)]
            ##վ��
            county_sum31= county_sum31.groupby(['station_code','station_name']).sum().reset_index()
            county_sum31= pd.DataFrame(county_sum31)
            # county_sum31.to_excel(r'C:\Users\liu.wenjie\Desktop\�±�\10��\county_sum311230.xlsx')
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

            ##�ؼ�


            county_sum31= county_sum31.groupby(['city_code','city_name','county_code','county_name']).sum().reset_index()
            county_sum31= pd.DataFrame(county_sum31)
            # county_sum31.to_excel(r'C:\Users\liu.wenjie\Desktop\�±�\10��\county_sum311230.xlsx')
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
            ##�м�
            county_sum����= county_sum31.groupby(['city_code','city_name']).sum().reset_index()
            county_sum����['overrun_rate']=(county_sum����['overrun_num']/county_sum����['truck_num']*100).round(2)
            county_sum����['overrun020_rate']=(county_sum����['overrun020_count']/county_sum����['truck_num']*100).round(2)
            county_sum����['overrun2050_rate']=(county_sum����['overrun2050_count']/county_sum����['truck_num']*100).round(2)
            county_sum����['overrun50100_rate']=(county_sum����['overrun50100_count']/county_sum����['truck_num']*100).round(2)
            county_sum����['overrun100rate']=(county_sum����['overrun100_count']/county_sum����['truck_num']*100).round(2)
            county_sum����['station_type']=31
            county_sum����['update_time']=today
            county_sum����['create_time']=today
            county_sum����['statistics_date']=this_month
            county_sum����['id']=this_month2+county_sum����['city_code'].astype('str')+county_sum����['station_type'].astype('str')

            ###ʡ��
            county_sumʡ=county_sum����
            county_sumʡ['area_code']='330000'
            county_sumʡ['area_name']='�㽭'
            county_sumʡ= county_sumʡ.groupby(['area_code','area_name']).sum().reset_index()
            county_sumʡ.drop(['overrun100rate'],axis=1,inplace=True)
            county_sumʡ['overrun_rate']=(county_sumʡ['overrun_num']/county_sumʡ['truck_num']*100).round(2)
            county_sumʡ['overrun020_rate']=(county_sumʡ['overrun020_count']/county_sumʡ['truck_num']*100).round(2)
            county_sumʡ['overrun2050_rate']=(county_sumʡ['overrun2050_count']/county_sumʡ['truck_num']*100).round(2)
            county_sumʡ['overrun50100_rate']=(county_sumʡ['overrun50100_count']/county_sumʡ['truck_num']*100).round(2)
            county_sumʡ['overrun100_rate']=(county_sumʡ['overrun100_count']/county_sumʡ['truck_num']*100).round(2)
            county_sumʡ['station_type']=31
            county_sumʡ['update_time']=today
            county_sumʡ['create_time']=today
            county_sumʡ['statistics_date']=this_month
            county_sumʡ['id']=this_month2+county_sumʡ['area_code'].astype('str')+county_sumʡ['station_type'].astype('str')
            # county_sumʡ.to_excel(r'C:\Users\liu.wenjie\Desktop\�±�\10��\county_sumʡ.xlsx')

            ##�ϲ�


            county_sum31 = pd.DataFrame(county_sum31,
                                      columns=['id', 'statistics_date','county_code', 'county_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])
            county_sum���� = pd.DataFrame(county_sum����,
                                      columns=['id', 'statistics_date','city_code', 'city_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])
            county_sum31.rename(
                columns={'county_code': 'area_code','county_name': 'area_name'}, inplace=True)
            county_sum����.rename(
                columns={'city_code': 'area_code','city_name': 'area_name'}, inplace=True)

            wide_table31 = pd.concat([county_sum31,county_sum����,county_sumʡ])
            # wide_table31.to_excel(r'C:\Users\liu.wenjie\Desktop\�±�\10��\wide_table31.xlsx')

            ###����Դͷ��
            ##�ؼ�
            county_sum71 = t_bas_basic_data_pass71
            county_sum71= county_sum71.groupby(['city_code','city_name','county_code','county_name']).sum().reset_index()
            county_sum71= pd.DataFrame(county_sum71)
            # county_sum71.to_excel(r'C:\Users\liu.wenjie\Desktop\�±�\10��\county_sum71���ػ���.xlsx')
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
            ##�м�
            county_sum����71= county_sum71.groupby(['city_code','city_name']).sum().reset_index()
            county_sum����71['overrun_rate']=(county_sum����71['overrun_num']/county_sum����71['truck_num']*100).round(2)
            county_sum����71['overrun020_rate']=(county_sum����71['overrun020_count']/county_sum����71['truck_num']*100).round(2)
            county_sum����71['overrun2050_rate']=(county_sum����71['overrun2050_count']/county_sum����71['truck_num']*100).round(2)
            county_sum����71['overrun50100_rate']=(county_sum����71['overrun50100_count']/county_sum����71['truck_num']*100).round(2)
            county_sum����71['overrun100rate']=(county_sum����71['overrun100_count']/county_sum����71['truck_num']*100).round(2)
            county_sum����71['station_type']=71
            county_sum����71['update_time']=today
            county_sum����71['create_time']=today
            county_sum����71['statistics_date']=this_month
            county_sum����71['id']=this_month2+county_sum����71['city_code'].astype('str')+county_sum����71['station_type'].astype('str')

            ###ʡ��
            county_sumʡ71=county_sum����71
            county_sumʡ71['area_code']='330000'
            county_sumʡ71['area_name']='�㽭'
            county_sumʡ71= county_sumʡ71.groupby(['area_code','area_name']).sum().reset_index()
            county_sumʡ71.drop(['overrun100rate'],axis=1,inplace=True)
            county_sumʡ71['overrun_rate']=(county_sumʡ71['overrun_num']/county_sumʡ71['truck_num']*100).round(2)
            county_sumʡ71['overrun020_rate']=(county_sumʡ71['overrun020_count']/county_sumʡ71['truck_num']*100).round(2)
            county_sumʡ71['overrun2050_rate']=(county_sumʡ71['overrun2050_count']/county_sumʡ71['truck_num']*100).round(2)
            county_sumʡ71['overrun50100_rate']=(county_sumʡ71['overrun50100_count']/county_sumʡ71['truck_num']*100).round(2)
            county_sumʡ71['overrun100_rate']=(county_sumʡ71['overrun100_count']/county_sumʡ71['truck_num']*100).round(2)
            county_sumʡ71['station_type']=71
            county_sumʡ71['update_time']=today
            county_sumʡ71['create_time']=today
            county_sumʡ71['statistics_date']=this_month
            county_sumʡ71['id']=this_month2+county_sumʡ71['area_code'].astype('str')+county_sumʡ71['station_type'].astype('str')
            # county_sumʡ71.to_excel(r'C:\Users\liu.wenjie\Desktop\�±�\10��\county_sumʡ71.xlsx')

            ##�ϲ�


            county_sum71 = pd.DataFrame(county_sum71,
                                      columns=['id', 'statistics_date','county_code', 'county_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])
            county_sum����71 = pd.DataFrame(county_sum����71,
                                      columns=['id', 'statistics_date','city_code', 'city_name',
                                               'pass_num', 'truck_num',
                                               'overrun_num', 'hundred_king_num', 'overrun_rate', 'overrun020_count', 'overrun020_rate',
                                               'overrun2050_count', 'overrun2050_rate', 'overrun50100_count', 'overrun50100_rate',
                                               'overrun100_count', 'overrun100_rate', 'station_type',
                                               'update_time', 'create_time'])
            county_sum71.rename(
                columns={'county_code': 'area_code','county_name': 'area_name'}, inplace=True)
            county_sum����71.rename(
                columns={'city_code': 'area_code','city_name': 'area_name'}, inplace=True)

            wide_table71 = pd.concat([county_sum71,county_sum����71,county_sumʡ71])
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
            # wide_table.to_excel(r'C:\Users\liu.wenjie\Desktop\�±�\12��\wide_table.xlsx')

            '''���ݿ�ɾ��'''
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
                # Ҫд������ݱ�����д�Ļ�Ҫ��ǰ�����ݿ⽨�ñ�
                # t_bas_though.to_sql(name='t_bas_car_through', con=engine, if_exists='append', index=False)
                # result_table
                # Ҫд������ݱ�����д�Ļ�Ҫ��ǰ�����ݿ⽨�ñ�
                wide_table.to_sql(name='t_bas_pass_statistics_data', con=engine, if_exists='append', index=False)
            except Exception as e:
                print("mysql����ʧ��")


            # import pymysql
            #
            #
            # class DBUtils:
            #     """
            #     ���ݿ⹤����
            #     """
            #
            #     """:param
            #     db:     ���ݿ�����:  db = pymysql.connect(host='192.168.1.1', user='root', password='1234', port=3306, db='database_name')
            #     cursor: ���ݿ��α�:  cursor = db.cursor()
            #     data:   ��д������:  Dataframe
            #     table:  д�����
            #     """
            #
            #     def __init__(self, db, cursor, data, table):
            #         self.db = db
            #         self.cursor = cursor
            #         self.data = data
            #         self.table = table
            #
            #     # ������ȥ��׷�Ӹ���
            #     def insert_data(self):
            #         keys = ', '.join('`' + self.data.keys() + '`')
            #         values = ', '.join(['%s'] * len(self.data.columns))
            #         # ���ݱ��Ψһ����ȥ��׷�Ӹ���
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
            #                 print("����д��ʧ��,ԭ��Ϊ:" + e)
            #                 self.db.rollback()
            #
            #
            #         self.cursor.close()
            #         self.db.close()
            #         print('������ȫ��д�����!')
            #
            #
            # wide_table.fillna("", inplace=True)  # �滻NaN,��������д��ʱ�ᱨ��,Ҳ���滻������
            # # �������ݿ�,�������
            # db = pymysql.connect(host='172.19.116.150', user='zjzhzcuser', password='F4dus0ee', port=11806,
            #                      db='db_manage_overruns')
            # cursor = db.cursor()
            # table = "t_bas_pass_statistics_data_test"  # д�����
            #
            # # д������
            # DBUtils.insert_data(DBUtils(db, cursor, wide_table, table))

            del t_bas_basic_data_pass
            del t_bas_basic_data_pass71

            from datetime import datetime

            js = datetime.now()
            sjc = js - ks
            print('���к�ʱ', sjc)
