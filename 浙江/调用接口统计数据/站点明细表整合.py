c=2
j=1
hz=[]
while c<=232:
            import pandas as pd
            from datetime import datetime
            day = datetime.now().date()  # 获取当前系统时间

            today = datetime.now()

            from datetime import datetime

            ks = datetime.now()
            print('运行开始时间', ks)

            import datetime

            starttime = day - datetime.timedelta(days=c)
            print('starttime', starttime)
            endtime = day - datetime.timedelta(days=j)
            print('endtime', endtime)


            """ 引入原始表 """

            ##原始表获取


            # sql = "select  out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_21 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'".format(
            #     starttime, endtime)
            # t_bas_pass_data_21 = get_df_from_db(sql)
            pass_truck_num = pd.read_excel(r'G:\智诚\2023日常给出数据\其他任务\金华兰溪截止0819超限明细数据.xlsx', sheet_name='pass_31')
            pass_truck_num = pass_truck_num[(pass_truck_num.statistics_data == '{}'.format(starttime))]

            pass_truck_num['truck_num'] = pass_truck_num['truck_num'].astype('int')

            # sql = "SELECT out_station,count(1) as pass_num,sum(is_truck) as truck_num FROM t_bas_pass_data_31 WHERE out_station_time >= '{} 00:00:00' AND out_station_time < '{} 00:00:00' GROUP BY out_station".format(
            #     starttime, endtime)
            # pass_truck_num = get_df_from_db(sql)
            # pass_truck_num = pd.DataFrame(pass_truck_num)
            # pass_truck_num.to_excel(r'C:\Users\liu.wenjie\Desktop\月报\10月\pass_truck_num.xlsx')

            t_bas_pass_data_31 = pd.read_excel(r'G:\智诚\2023日常给出数据\其他任务\金华兰溪截止0819超限明细数据.xlsx', sheet_name='31')
            t_bas_pass_data_31 = t_bas_pass_data_31[(t_bas_pass_data_31.out_station_time >= '{} 00:00:00'.format(starttime))&(t_bas_pass_data_31.out_station_time <= '{} 23:59:59'.format(starttime))]
            t_bas_pass_data_31['total_weight'] = t_bas_pass_data_31['total_weight'].astype('float')
            t_bas_pass_data_31['limit_weight'] = t_bas_pass_data_31['limit_weight'].astype('float')
            # sql = "select  area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight from t_bas_over_data_31 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00' AND allow IS NULL AND is_unusual = 0 ".format(
            #     starttime, endtime)
            # t_bas_pass_data_31 = get_df_from_db(sql)


            # and area_county = 330781
            t_bas_pass_data_71 = pd.read_excel(r'G:\智诚\2023日常给出数据\其他任务\金华兰溪截止0819超限明细数据.xlsx', sheet_name='71')
            t_bas_pass_data_71 = t_bas_pass_data_71[(t_bas_pass_data_71.out_station_time >= '{} 00:00:00'.format(starttime))&(t_bas_pass_data_71.out_station_time <= '{} 23:59:59'.format(starttime))]
            t_bas_pass_data_71['total_weight'] = t_bas_pass_data_71['total_weight'].astype('float')
            t_bas_pass_data_71['limit_weight'] = t_bas_pass_data_71['limit_weight'].astype('float')
            t_bas_pass_data_71['out_station'] = t_bas_pass_data_71['out_station'].astype('string')
            # sql = "select  area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight from t_bas_pass_data_71 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'  ".format(
            #     starttime, endtime)
            # t_bas_pass_data_71 = get_df_from_db(sql)
            t_bas_pass_data_21 = pd.read_excel(r'G:\智诚\2023日常给出数据\其他任务\金华兰溪截止0819超限明细数据.xlsx', sheet_name='21')
            t_bas_pass_data_21 = t_bas_pass_data_21[(t_bas_pass_data_21.out_station_time >= '{} 00:00:00'.format(starttime))&(t_bas_pass_data_21.out_station_time <= '{} 23:59:59'.format(starttime))]
            t_bas_pass_data_21['total_weight'] = t_bas_pass_data_21['total_weight'].astype('float')
            t_bas_pass_data_21['limit_weight'] = t_bas_pass_data_21['limit_weight'].astype('float')
            t_bas_pass_data_21['out_station'] = t_bas_pass_data_21['out_station'].astype('string')
            # sql = "select out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate from t_bas_pass_data_71 where out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00' ".format(
            #     starttime, endtime)
            # t_bas_pass_data_71 = get_df_from_db(sql)
            t_sys_station = pd.read_excel(r'G:\智诚\2023日常给出数据\基础表\basic.xlsx', sheet_name='t_sys_station')
            t_sys_station = t_sys_station[(t_sys_station.station_type != 41)&(t_sys_station.is_deleted == 0)]
            t_sys_station = pd.DataFrame(t_sys_station,
                                           columns=['station_code', 'station_status', 'station_type', 'is_check_station', 'station_name',
                                                    'area_county'])
            t_code_area = pd.read_excel(r'G:\智诚\2023日常给出数据\基础表\basic.xlsx', sheet_name='t_code_area')
            t_code_area = pd.DataFrame(t_sys_station,
                                           columns=['city_code', 'county_code', 'city', 'county'])
            # sql1 = "select  station_code,station_status,station_type,is_check_station,station_name, area_county from t_sys_station where is_deleted = 0   and station_type in (31,71,21) "
            # t_sys_station = get_df_from_db2(sql1)
            # sql1 = "select city_code,county_code,city,county FROM t_code_area  "
            # t_code_area = get_df_from_db2(sql1)


            ##站点区域表连接
            U_station_area = pd.merge(t_sys_station, t_code_area, left_on='area_county', right_on='county_code', how='left')
            # U_station_area.to_excel('C:/Users/Administrator/Desktop/U_station_area.xlsx')

            U_station_area.rename(
                columns={'city': 'city_name', 'county': 'county_name'}, inplace=True)
            U_station_area = pd.DataFrame(U_station_area,
                                          columns=['city_code', 'city_name', 'county_code', 'county_name', 'station_code',
                                                   'station_name', 'station_status', 'station_type','is_check_station'])

            ##各表car_no空值填充
            # U_pass21_area_station = t_bas_pass_data_21
            # U_pass21_area_station['car_no'] = U_pass21_area_station['car_no'].fillna(value=0)
            # # U_pass21_area_station['car_no'] = U_pass21_area_station['car_no'].apply(lambda car_no: car_no[:2])
            U_pass31_area_station = t_bas_pass_data_31
            # U_pass31_area_station['car_no'] = U_pass31_area_station['car_no'].fillna(value=0)

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
                U_pass31_area_station.loc[
                    ((U_pass31_area_station['area_county'] == key) | (U_pass31_area_station['area_city'] == key)) & (
                            U_pass31_area_station['total_weight'] < 100), 'limit_weight'] = U_pass31_area_station[
                    'limit_weight'].map(lambda x: float(x) * value).round(0)

            U_pass31_area_station['limit_weight'] = U_pass31_area_station['limit_weight'].fillna(value=1)
            U_pass31_area_station.loc[U_pass31_area_station['limit_weight'] < 1, 'limit_weight'] = 9
            U_pass31_area_station['total_weight'] = U_pass31_area_station['total_weight'].astype('float')
            U_pass31_area_station['limit_weight'] = U_pass31_area_station['limit_weight'].astype('float')
            U_pass31_area_station['overrun_rate'] = (U_pass31_area_station['total_weight'] - U_pass31_area_station[
                'limit_weight']) * 100 / U_pass31_area_station['limit_weight']

            # U_pass31_area_station['car_no'] = U_pass31_area_station['car_no'].apply(lambda car_no: car_no[:2])
            U_pass71_area_station = t_bas_pass_data_71
            U_pass71_area_station['car_no'] = U_pass71_area_station['car_no'].fillna(value=0)
            U_pass21_area_station = t_bas_pass_data_21
            U_pass21_area_station['car_no'] = U_pass21_area_station['car_no'].fillna(value=0)
            # # U_pass41_area_station['car_no'] = U_pass41_area_station['car_no'].apply(lambda car_no: car_no[:2])
            # U_pass71_area_station = t_bas_pass_data_71
            # U_pass71_area_station['car_no'] = U_pass71_area_station['car_no'].fillna(value=0)
            # # U_pass71_area_station['car_no'] = U_pass71_area_station['car_no'].apply(lambda car_no: car_no[:2])

            U_pass31_area_station = pd.DataFrame(U_pass31_area_station,
                                                 columns=['out_station', 'out_station_time', 'car_no', 'total_weight',
                                                          'limit_weight',
                                                          'overrun_rate'])

            U_pass71_area_station = pd.DataFrame(U_pass71_area_station,
                                                 columns=['out_station', 'out_station_time', 'car_no', 'total_weight',
                                                          'limit_weight',
                                                          'overrun_rate'])

            # 31表
            # 建立关键字段
            wide_table_31 = pass_truck_num
            wide_table_31 = pd.merge(wide_table_31, t_sys_station, left_on='out_station', right_on='station_code',
                                     how='left')
            wide_table_31['pass_num'] = wide_table_31['pass_num'].astype('float')
            wide_table_31['truck_num'] = wide_table_31['truck_num'].astype('float')
            wide_table_31 = pd.DataFrame(wide_table_31)

            T0_5T = U_pass31_area_station[
                ((0 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 5))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T0_5T')

            U_pass31_area_station = U_pass31_area_station[(U_pass31_area_station.total_weight >= 2.5)]

            over_num = U_pass31_area_station[(U_pass31_area_station['overrun_rate'] > 0)].groupby(['out_station'])[
                'car_no'].count().reset_index(name='over_num')
            wide_table_31 = pd.merge(wide_table_31, over_num, on=['out_station'], how='left')
            wide_table_31 = wide_table_31.fillna(value=0)
            wide_table_31['over_rate'] = (wide_table_31['over_num'] / wide_table_31['pass_num'] * 100).round(2)
            wide_table_31['truck_over_rate'] = (wide_table_31['over_num'] / wide_table_31['truck_num'] * 100).round(2)
            wide_table_31['no_over'] = wide_table_31['truck_num'] - wide_table_31['over_num']
            no_car = \
                U_pass31_area_station[(U_pass31_area_station['car_no'].str.len() <= 5)].groupby(['out_station'])[
                    'car_no'].count().reset_index(name='no_car')
            wide_table_31 = pd.merge(wide_table_31, no_car, on=['out_station'], how='left')

            ##超限程度分布
            C0_10X = U_pass31_area_station[
                ((0 < U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 10))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C0_10X')
            C10_20X = U_pass31_area_station[
                ((10 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 20))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C10_20X')
            C20_30X = U_pass31_area_station[
                ((20 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 30))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C20_30X')
            C30_40X = U_pass31_area_station[
                ((30 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 40))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C30_40X')
            C40_50X = U_pass31_area_station[
                ((40 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 50))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C40_50X')
            C50_60X = U_pass31_area_station[
                ((50 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 60))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C50_60X')
            C60_70X = U_pass31_area_station[
                ((60 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 70))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C60_70X')
            C70_80X = U_pass31_area_station[
                ((70 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 80))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C70_80X')
            C80_90X = U_pass31_area_station[
                ((80 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 90))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C80_90X')
            C90_100X = U_pass31_area_station[
                ((90 <= U_pass31_area_station['overrun_rate']) & (U_pass31_area_station['overrun_rate'] < 100))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C90_100X')
            C100X = U_pass31_area_station[(100 <= U_pass31_area_station['overrun_rate'])].groupby(['out_station'])[
                'car_no'].count().reset_index(name='C100X')

            ##聚合超限程度
            wide_table_31 = pd.merge(wide_table_31, C0_10X, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, C10_20X, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, C20_30X, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, C30_40X, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, C40_50X, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, C50_60X, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, C60_70X, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, C70_80X, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, C80_90X, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, C90_100X, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, C100X, on=['out_station'], how='left')

            ##吨位分布
            T0_5T = T0_5T
            T5_10T = U_pass31_area_station[
                ((5 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 10))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T5_10T')
            T10_20T = U_pass31_area_station[
                ((10 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 20))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T10_20T')
            T20_30T = U_pass31_area_station[
                ((20 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 30))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T20_30T')
            T30_40T = U_pass31_area_station[
                ((30 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 40))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T30_40T')
            T40_50T = U_pass31_area_station[
                ((40 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 50))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T40_50T')
            T50_60T = U_pass31_area_station[
                ((50 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 60))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T50_60T')
            T60_70T = U_pass31_area_station[
                ((60 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 70))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T60_70T')
            T70_80T = U_pass31_area_station[
                ((70 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 80))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T70_80T')
            T80_90T = U_pass31_area_station[
                ((80 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 90))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T80_90T')
            T90_100T = U_pass31_area_station[
                ((90 <= U_pass31_area_station['total_weight']) & (U_pass31_area_station['total_weight'] < 100))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T90_100T')
            T100T = U_pass31_area_station[(100 <= U_pass31_area_station['total_weight'])].groupby(['out_station'])[
                'car_no'].count().reset_index(name='T100T')

            ##聚合吨位分布
            wide_table_31 = pd.merge(wide_table_31, T0_5T, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, T5_10T, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, T10_20T, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, T20_30T, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, T30_40T, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, T40_50T, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, T50_60T, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, T60_70T, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, T70_80T, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, T80_90T, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, T90_100T, on=['out_station'], how='left')
            wide_table_31 = pd.merge(wide_table_31, T100T, on=['out_station'], how='left')

            wide_table_31 = wide_table_31.fillna(value=0)

            wide_table_31.rename(
                columns={'over_num': 'overrun_num',
                         'statistics_time': 'statistics_date', 'no_over': 'overrun_0', 'no_car': 'no_car_num',
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
                                         columns=['out_station', 'pass_num', 'truck_num',
                                                  'overrun_num', 'over_rate', 'truck_over_rate', 'no_car_num', 'overrun_0',
                                                  'overrun_0_10', 'overrun_10_20', 'overrun_20_30', 'overrun_30_40',
                                                  'overrun_40_50', 'overrun_50_60', 'overrun_60_70',
                                                  'overrun_70_80', 'overrun_80_90', 'overrun_90_100', 'overrun_100',
                                                  'total_weight_0_5', 'total_weight_5_10', 'total_weight_10_20',
                                                  'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50',
                                                  'total_weight_50_60', 'total_weight_60_70', 'total_weight_70_80',
                                                  'total_weight_80_90',
                                                  'total_weight_90_100', 'total_weight_100'])
            # print(wide_table_31)
            # though_area_31.to_excel('C:/Users/Administrator/Desktop/though_area_31.xlsx')
            # 71表
            # 建立关键字段
            U_pass71_area_station['limit_weight'] = U_pass71_area_station['limit_weight'].fillna(value=1)
            U_pass71_area_station.loc[U_pass71_area_station['limit_weight'] < 1, 'limit_weight'] = 5
            U_pass71_area_station['total_weight'] = U_pass71_area_station['total_weight'].astype('float')
            U_pass71_area_station['limit_weight'] = U_pass71_area_station['limit_weight'].astype('float')
            U_pass71_area_station['overrun_rate'] = (U_pass71_area_station['total_weight'] - U_pass71_area_station[
                'limit_weight']) * 100 / (U_pass71_area_station['limit_weight'] + 0.00001)

            wide_table_71 = U_pass71_area_station.groupby(
                ['out_station'])[
                'car_no'].count().reset_index(name='pass_num')
            wide_table_71 = pd.merge(wide_table_71, t_sys_station, left_on='out_station', right_on='station_code',
                                     how='left')
            wide_table_71 = pd.DataFrame(wide_table_71)

            T0_5T = U_pass71_area_station[
                ((0 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 5))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T0_5T')

            U_pass71_area_station = U_pass71_area_station[(U_pass71_area_station.total_weight >= 2.5)]

            truck_num = U_pass71_area_station.groupby(['out_station'])['car_no'].count().reset_index(
                name='truck_num')
            wide_table_71 = pd.merge(wide_table_71, truck_num, on=['out_station'], how='left')
            over_num = U_pass71_area_station[(U_pass71_area_station['overrun_rate'] > 0)].groupby(['out_station'])[
                'car_no'].count().reset_index(name='over_num')
            wide_table_71 = pd.merge(wide_table_71, over_num, on=['out_station'], how='left')
            wide_table_71 = wide_table_71.fillna(value=0)
            wide_table_71['over_rate'] = (wide_table_71['over_num'] / wide_table_71['pass_num'] * 100).round(2)
            wide_table_71['truck_over_rate'] = (wide_table_71['over_num'] / wide_table_71['truck_num'] * 100).round(2)
            wide_table_71['no_over'] = wide_table_71['truck_num'] - wide_table_71['over_num']
            no_car = \
                U_pass71_area_station[(U_pass71_area_station['car_no'].str.len() <= 5)].groupby(['out_station'])[
                    'car_no'].count().reset_index(name='no_car')
            wide_table_71 = pd.merge(wide_table_71, no_car, on=['out_station'], how='left')

            ##超限程度分布
            C0_10X = U_pass71_area_station[
                ((0 < U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 10))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C0_10X')
            C10_20X = U_pass71_area_station[
                ((10 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 20))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C10_20X')
            C20_30X = U_pass71_area_station[
                ((20 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 30))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C20_30X')
            C30_40X = U_pass71_area_station[
                ((30 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 40))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C30_40X')
            C40_50X = U_pass71_area_station[
                ((40 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 50))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C40_50X')
            C50_60X = U_pass71_area_station[
                ((50 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 60))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C50_60X')
            C60_70X = U_pass71_area_station[
                ((60 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 70))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C60_70X')
            C70_80X = U_pass71_area_station[
                ((70 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 80))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C70_80X')
            C80_90X = U_pass71_area_station[
                ((80 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 90))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C80_90X')
            C90_100X = U_pass71_area_station[
                ((90 <= U_pass71_area_station['overrun_rate']) & (U_pass71_area_station['overrun_rate'] < 100))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C90_100X')
            C100X = U_pass71_area_station[(100 <= U_pass71_area_station['overrun_rate'])].groupby(['out_station'])[
                'car_no'].count().reset_index(name='C100X')

            ##聚合超限程度
            wide_table_71 = pd.merge(wide_table_71, C0_10X, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, C10_20X, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, C20_30X, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, C30_40X, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, C40_50X, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, C50_60X, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, C60_70X, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, C70_80X, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, C80_90X, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, C90_100X, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, C100X, on=['out_station'], how='left')

            ##吨位分布
            T0_5T = T0_5T
            T5_10T = U_pass71_area_station[
                ((5 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 10))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T5_10T')
            T10_20T = U_pass71_area_station[
                ((10 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 20))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T10_20T')
            T20_30T = U_pass71_area_station[
                ((20 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 30))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T20_30T')
            T30_40T = U_pass71_area_station[
                ((30 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 40))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T30_40T')
            T40_50T = U_pass71_area_station[
                ((40 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 50))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T40_50T')
            T50_60T = U_pass71_area_station[
                ((50 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 60))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T50_60T')
            T60_70T = U_pass71_area_station[
                ((60 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 70))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T60_70T')
            T70_80T = U_pass71_area_station[
                ((70 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 80))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T70_80T')
            T80_90T = U_pass71_area_station[
                ((80 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 90))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T80_90T')
            T90_100T = U_pass71_area_station[
                ((90 <= U_pass71_area_station['total_weight']) & (U_pass71_area_station['total_weight'] < 100))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T90_100T')
            T100T = U_pass71_area_station[(100 <= U_pass71_area_station['total_weight'])].groupby(['out_station'])[
                'car_no'].count().reset_index(name='T100T')

            ##聚合吨位分布
            wide_table_71 = pd.merge(wide_table_71, T0_5T, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, T5_10T, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, T10_20T, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, T20_30T, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, T30_40T, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, T40_50T, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, T50_60T, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, T60_70T, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, T70_80T, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, T80_90T, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, T90_100T, on=['out_station'], how='left')
            wide_table_71 = pd.merge(wide_table_71, T100T, on=['out_station'], how='left')

            wide_table_71 = wide_table_71.fillna(value=0)

            wide_table_71.rename(
                columns={'over_num': 'overrun_num',
                         'statistics_time': 'statistics_date', 'no_over': 'overrun_0', 'no_car': 'no_car_num',
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
                                         columns=['out_station', 'pass_num', 'truck_num',
                                                  'overrun_num', 'over_rate', 'truck_over_rate', 'no_car_num', 'overrun_0',
                                                  'overrun_0_10', 'overrun_10_20', 'overrun_20_30', 'overrun_30_40',
                                                  'overrun_40_50', 'overrun_50_60', 'overrun_60_70',
                                                  'overrun_70_80', 'overrun_80_90', 'overrun_90_100', 'overrun_100',
                                                  'total_weight_0_5', 'total_weight_5_10', 'total_weight_10_20',
                                                  'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50',
                                                  'total_weight_50_60', 'total_weight_60_70', 'total_weight_70_80',
                                                  'total_weight_80_90',
                                                  'total_weight_90_100', 'total_weight_100'])
            # 21表
            # 建立关键字段
            U_pass21_area_station['limit_weight'] = U_pass21_area_station['limit_weight'].fillna(value=1)
            U_pass21_area_station.loc[U_pass21_area_station['limit_weight'] < 1, 'limit_weight'] = 5
            U_pass21_area_station['total_weight'] = U_pass21_area_station['total_weight'].astype('float')
            U_pass21_area_station['limit_weight'] = U_pass21_area_station['limit_weight'].astype('float')
            U_pass21_area_station['overrun_rate'] = (U_pass21_area_station['total_weight'] - U_pass21_area_station[
                'limit_weight']) * 100 / (U_pass21_area_station['limit_weight'] + 0.00001)

            wide_table_21 = U_pass21_area_station.groupby(
                ['out_station'])[
                'car_no'].count().reset_index(name='pass_num')
            wide_table_21 = pd.merge(wide_table_21, t_sys_station, left_on='out_station', right_on='station_code',
                                     how='left')
            wide_table_21 = pd.DataFrame(wide_table_21)

            T0_5T = U_pass21_area_station[
                ((0 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 5))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T0_5T')

            U_pass21_area_station = U_pass21_area_station[(U_pass21_area_station.total_weight >= 2.5)]

            truck_num = U_pass21_area_station.groupby(['out_station'])['car_no'].count().reset_index(
                name='truck_num')
            wide_table_21 = pd.merge(wide_table_21, truck_num, on=['out_station'], how='left')
            over_num = U_pass21_area_station[(U_pass21_area_station['overrun_rate'] > 0)].groupby(['out_station'])[
                'car_no'].count().reset_index(name='over_num')
            wide_table_21 = pd.merge(wide_table_21, over_num, on=['out_station'], how='left')
            wide_table_21 = wide_table_21.fillna(value=0)
            wide_table_21['over_rate'] = (wide_table_21['over_num'] / wide_table_21['pass_num'] * 100).round(2)
            wide_table_21['truck_over_rate'] = (wide_table_21['over_num'] / wide_table_21['truck_num'] * 100).round(2)
            wide_table_21['no_over'] = wide_table_21['truck_num'] - wide_table_21['over_num']
            no_car = \
                U_pass21_area_station[(U_pass21_area_station['car_no'].str.len() <= 5)].groupby(['out_station'])[
                    'car_no'].count().reset_index(name='no_car')
            wide_table_21 = pd.merge(wide_table_21, no_car, on=['out_station'], how='left')

            ##超限程度分布
            C0_10X = U_pass21_area_station[
                ((0 < U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 10))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C0_10X')
            C10_20X = U_pass21_area_station[
                ((10 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 20))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C10_20X')
            C20_30X = U_pass21_area_station[
                ((20 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 30))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C20_30X')
            C30_40X = U_pass21_area_station[
                ((30 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 40))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C30_40X')
            C40_50X = U_pass21_area_station[
                ((40 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 50))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C40_50X')
            C50_60X = U_pass21_area_station[
                ((50 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 60))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C50_60X')
            C60_70X = U_pass21_area_station[
                ((60 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 70))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C60_70X')
            C70_80X = U_pass21_area_station[
                ((70 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 80))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C70_80X')
            C80_90X = U_pass21_area_station[
                ((80 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 90))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C80_90X')
            C90_100X = U_pass21_area_station[
                ((90 <= U_pass21_area_station['overrun_rate']) & (U_pass21_area_station['overrun_rate'] < 100))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='C90_100X')
            C100X = U_pass21_area_station[(100 <= U_pass21_area_station['overrun_rate'])].groupby(['out_station'])[
                'car_no'].count().reset_index(name='C100X')

            ##聚合超限程度
            wide_table_21 = pd.merge(wide_table_21, C0_10X, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, C10_20X, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, C20_30X, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, C30_40X, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, C40_50X, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, C50_60X, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, C60_70X, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, C70_80X, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, C80_90X, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, C90_100X, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, C100X, on=['out_station'], how='left')

            ##吨位分布
            T0_5T = T0_5T
            T5_10T = U_pass21_area_station[
                ((5 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 10))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T5_10T')
            T10_20T = U_pass21_area_station[
                ((10 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 20))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T10_20T')
            T20_30T = U_pass21_area_station[
                ((20 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 30))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T20_30T')
            T30_40T = U_pass21_area_station[
                ((30 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 40))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T30_40T')
            T40_50T = U_pass21_area_station[
                ((40 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 50))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T40_50T')
            T50_60T = U_pass21_area_station[
                ((50 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 60))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T50_60T')
            T60_70T = U_pass21_area_station[
                ((60 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 70))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T60_70T')
            T70_80T = U_pass21_area_station[
                ((70 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 80))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T70_80T')
            T80_90T = U_pass21_area_station[
                ((80 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 90))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T80_90T')
            T90_100T = U_pass21_area_station[
                ((90 <= U_pass21_area_station['total_weight']) & (U_pass21_area_station['total_weight'] < 100))].groupby(
                ['out_station'])['car_no'].count().reset_index(name='T90_100T')
            T100T = U_pass21_area_station[(100 <= U_pass21_area_station['total_weight'])].groupby(['out_station'])[
                'car_no'].count().reset_index(name='T100T')

            ##聚合吨位分布
            wide_table_21 = pd.merge(wide_table_21, T0_5T, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, T5_10T, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, T10_20T, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, T20_30T, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, T30_40T, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, T40_50T, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, T50_60T, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, T60_70T, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, T70_80T, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, T80_90T, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, T90_100T, on=['out_station'], how='left')
            wide_table_21 = pd.merge(wide_table_21, T100T, on=['out_station'], how='left')

            wide_table_21 = wide_table_21.fillna(value=0)

            wide_table_21.rename(
                columns={'over_num': 'overrun_num',
                         'statistics_time': 'statistics_date', 'no_over': 'overrun_0', 'no_car': 'no_car_num',
                         'C0_10X': 'overrun_0_10', 'C10_20X': 'overrun_10_20', 'C20_30X': 'overrun_20_30',
                         'C30_40X': 'overrun_30_40', 'C40_50X': 'overrun_40_50', 'C50_60X': 'overrun_50_60',
                         'C60_70X': 'overrun_60_70', 'C70_80X': 'overrun_70_80', 'C80_90X': 'overrun_80_90',
                         'C90_100X': 'overrun_90_100',
                         'C100X': 'overrun_100', 'T0_5T': 'total_weight_0_5', 'T5_10T': 'total_weight_5_10',
                         'T10_20T': 'total_weight_10_20', 'T20_30T': 'total_weight_20_30',
                         'T30_40T': 'total_weight_30_40', 'T40_50T': 'total_weight_40_50', 'T50_60T': 'total_weight_50_60',
                         'T60_70T': 'total_weight_60_70', 'T70_80T': 'total_weight_70_80', 'T80_90T': 'total_weight_80_90',
                         'T90_100T': 'total_weight_90_100', 'T100T': 'total_weight_100'}, inplace=True)

            wide_table_21 = pd.DataFrame(wide_table_21,
                                         columns=['out_station', 'pass_num', 'truck_num',
                                                  'overrun_num', 'over_rate', 'truck_over_rate', 'no_car_num', 'overrun_0',
                                                  'overrun_0_10', 'overrun_10_20', 'overrun_20_30', 'overrun_30_40',
                                                  'overrun_40_50', 'overrun_50_60', 'overrun_60_70',
                                                  'overrun_70_80', 'overrun_80_90', 'overrun_90_100', 'overrun_100',
                                                  'total_weight_0_5', 'total_weight_5_10', 'total_weight_10_20',
                                                  'total_weight_20_30', 'total_weight_30_40', 'total_weight_40_50',
                                                  'total_weight_50_60', 'total_weight_60_70', 'total_weight_70_80',
                                                  'total_weight_80_90',
                                                  'total_weight_90_100', 'total_weight_100'])

            ##基础表合并
            # wide_table = pd.concat([wide_table_21, wide_table_31, wide_table_41, wide_table_71])
            wide_table = pd.concat([wide_table_31, wide_table_71, wide_table_21])
            # wide_table.to_excel('C:/Users/Administrator/Desktop/wide_tableqian.xlsx')
            wide_table = pd.merge(U_station_area, wide_table, left_on='station_code', right_on='out_station', how='left')
            # wide_table.to_excel('C:/Users/Administrator/Desktop/wide_table111.xlsx')

            # stationcode1 = t_station['out_station']
            wide_table['refresh'] = 0
            wide_table['direction'] = 0
            wide_table['statistics_date'] = starttime
            starttime1 = starttime.strftime("%Y%m%d")
            wide_table = wide_table.fillna(value=0)
            wide_table['id'] = wide_table['station_code'].astype('string') + starttime1
            wide_table['id'] = wide_table['id'].astype('object')
            from datetime import datetime

            insert_time = datetime.now()
            wide_table['insert_time'] = insert_time
            online_duration = datetime.now().strftime("%H%M%S")
            wide_table['online_duration'] = online_duration
            wide_table = wide_table[(wide_table.pass_num > 0)]
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
                                               'total_weight_90_100', 'total_weight_100', 'station_status', 'station_type','is_check_station',
                                               'refresh', 'online_duration', 'insert_time'])


            del t_bas_pass_data_31
            del t_bas_pass_data_71
            from datetime import datetime

            js = datetime.now()
            sjc = js - ks
            print('运行耗时', sjc)
            hz.append(wide_table)
            print(wide_table)
            c += 1
            j += 1
data=pd.concat(hz, axis=0)
print(data)
with pd.ExcelWriter(r'C:\Users\stayhungary\Desktop\金华站点明细汇总表0819v2.0.xlsx') as writer1:
    data.to_excel(writer1, sheet_name='地市', index=True)