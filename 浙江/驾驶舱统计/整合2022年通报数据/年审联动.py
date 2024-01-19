import pymysql
import numpy as np
import pandas as pd
from urllib import parse
import calendar

def case_day_statistic():
    from datetime import datetime
    day = datetime.now().date()  # ��ȡ��ǰϵͳʱ��
    today = datetime.now()
    from datetime import datetime
    ks = datetime.now()
    print('���п�ʼʱ��', ks)
    import datetime
    now = datetime.datetime.now()
    this_month_start = datetime.datetime(now.year, now.month, 1).date()
    this_month_end = datetime.datetime(now.year, now.month, calendar.monthrange(now.year, now.month)[1]).date()
    this_month = datetime.datetime(now.year, now.month, 1).strftime("%Y-%m")
    import datetime
    starttime = this_month_start + datetime.timedelta(days=0)
    print('starttime', starttime)
    endtime = this_month_end + datetime.timedelta(days=1)
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

    sql = """ select  (case when car_addr_code is NULL then '����' else car_addr_code end )  areaCode,count(1) overrun_num
         from (
        SELECT collection.car_no,carCity.car_addr_code
         FROM
             t_bas_over_data_collection_31 collection
         left JOIN (
             SELECT
                 car_no,car_no_color,car_addr_code
             FROM
                 t_sys_query_illegal_record_20230412 
             WHERE
                 car_addr_code IS NOT NULL

             GROUP BY
                 car_no,car_no_color,car_addr_code
         ) carCity ON carCity.car_no = collection.car_no AND carCity.car_no_color = collection.car_no_color
         WHERE collection.law_judgment = 1  
         AND collection.`status` IN (4, 6, 12, 13)
         and collection.out_station_time >= '{} 00:00:00' and collection.out_station_time < '{} 00:00:00'
         union ALL
         SELECT collection.car_no,carCity.car_addr_code
         FROM
         t_bas_over_data_collection_41 collection
         left JOIN (
         SELECT
         car_no,car_no_color,car_addr_code
         FROM
         t_sys_query_illegal_record_20230412 
         WHERE
         car_addr_code IS NOT NULL

         GROUP BY
         car_no,car_no_color
         ) carCity ON carCity.car_no = collection.car_no AND carCity.car_no_color = collection.car_no_color
         WHERE collection.law_judgment = 1  AND collection.`status` IN (4, 6, 12, 13)
         and collection.out_station_time >= '{} 00:00:00' and collection.out_station_time < '{} 00:00:00'
         ) collectionOut
         GROUP BY car_addr_code """.format(starttime, endtime, starttime, endtime)
    overrun_num = get_df_from_db(sql)
    # print(overrun_num)
    sql = """select (case when car_addr_code is NULL then '����' else car_addr_code end ) areaCode,count(1) d_overrun_num
        from (
        select  car_addr_code from (
        SELECT collection.car_no,carCity.car_addr_code,collection.car_no_color
        FROM
        t_bas_over_data_collection_31 collection
        left JOIN (
        SELECT
        car_no,car_no_color,car_addr_code
        FROM
        t_sys_query_illegal_record_20230412 
        WHERE
        car_addr_code IS NOT NULL
        GROUP BY
        car_no,car_no_color,car_addr_code
        ) carCity ON carCity.car_no = collection.car_no AND carCity.car_no_color = collection.car_no_color
         WHERE collection.law_judgment = 1  AND collection.`status` IN (4, 6, 12, 13)
         and collection.out_station_time >= '{} 00:00:00' and collection.out_station_time < '{} 00:00:00'
        union ALL
        SELECT collection.car_no,carCity.car_addr_code,collection.car_no_color
        FROM
        t_bas_over_data_collection_41 collection
        left JOIN (
        SELECT
        car_no,car_no_color,car_addr_code
        FROM
        t_sys_query_illegal_record_20230412 
        WHERE
        car_addr_code IS NOT NULL
        GROUP BY
        car_no,car_no_color
        ) carCity ON carCity.car_no = collection.car_no AND carCity.car_no_color = collection.car_no_color
         WHERE collection.law_judgment = 1  AND collection.`status` IN (4, 6, 12, 13)
         and collection.out_station_time >= '{} 00:00:00' and collection.out_station_time < '{} 00:00:00'
        ) collectionOut_A GROUP BY car_addr_code,car_no,car_no_color
        )collectionOut_B GROUP BY car_addr_code
        """.format(
        starttime, endtime, starttime, endtime)
    d_overrun_num = get_df_from_db(sql)
    # print(d_overrun_num)

    sql = """ select (case when car_addr_code is NULL then '����' else car_addr_code end ) areaCode
       ,count(1) NS_num
       from  t_sys_query_illegal_record_20230412  record
       WHERE
         out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'
        group by  car_addr_code
       """.format(
        starttime, endtime)
    NS_num = get_df_from_db(sql)
    # print(NS_num)

    sql = """ select (case when car_addr_code is NULL then '����' else car_addr_code end ) areaCode,count(1) NSLD_num 
        from (
        select car_addr_code
        from  t_sys_query_illegal_record_20230412  record
        WHERE
         out_station_time >= '{} 00:00:00' 
        and out_station_time < '{} 00:00:00'
        group by  car_addr_code,car_no,car_no_color
        ) record_A group by car_addr_code
        """.format(
        starttime, endtime)
    NSLD_num = get_df_from_db(sql)
    # print(NSLD_num)

    sql = """ select car_addr_code  areaCode,count(1) Case_bh_num 
        from (
        select  (case when car_addr_code is NULL then '����' else car_addr_code end )  car_addr_code
        FROM
        (
        SELECT car_addr_code,record_code,car_no,car_no_color
        FROM
        t_sys_query_illegal_record_20230412 record
        WHERE
         out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'
       ) record_A
        LEFT JOIN
        t_bas_over_data_collection_sign sign
        ON record_A.record_code = sign.record_code
        WHERE
        sign.`status` = 9
        group by record_A.car_addr_code,record_A.record_code        
        )record_B  group by car_addr_code
        """.format(
        starttime, endtime)
    Case_bh_num = get_df_from_db(sql)
    # print(Case_bh_num)

    sql = """ select car_addr_code  areaCode,count(1) Bhcl_num from (
        select  (case when car_addr_code is NULL then '����' else car_addr_code end )  car_addr_code
        FROM
        (
        SELECT car_addr_code,record_code,car_no,car_no_color
        FROM
        t_sys_query_illegal_record_20230412 record
        WHERE
         out_station_time >= '{} 00:00:00' and out_station_time < '{} 00:00:00'
         ) record_A
        LEFT JOIN
        t_bas_over_data_collection_sign sign
        ON record_A.record_code = sign.record_code
        WHERE
        sign.`status` = 9

        group by record_A.car_addr_code,record_A.car_no,record_A.car_no_color
        )record_B  group by car_addr_code
         """.format(
        starttime, endtime)
    Bhcl_num = get_df_from_db(sql)
    # print(Bhcl_num)

    # wide_table = pd.concat([overrun_num, d_overrun_num,NS_num,NSLD_num,Case_bh_num,Bhcl_num],axis=1,join='inner')
    wide_table = pd.merge(overrun_num, d_overrun_num, on='areaCode', how='outer')
    wide_table = pd.merge(wide_table, NS_num, on='areaCode', how='outer')
    wide_table = pd.merge(wide_table, NSLD_num, on='areaCode', how='outer')
    wide_table = pd.merge(wide_table, Case_bh_num, on='areaCode', how='outer')
    wide_table = pd.merge(wide_table, Bhcl_num, on='areaCode', how='outer')
    wide_table = wide_table.fillna(value=0)
    wide_table['NS_num'] = wide_table['NS_num'].astype('int')
    wide_table['NSLD_num'] = wide_table['NSLD_num'].astype('int')
    wide_table['Case_bh_num'] = wide_table['Case_bh_num'].astype('int')
    wide_table['Bhcl_num'] = wide_table['Bhcl_num'].astype('int')
    wide_table['statistics_date'] = starttime
    print('����', wide_table)
    # '''���ݿ�ɾ��'''
    # db = pymysql.connect(host="172.19.116.150", port=11806, user='zjzhzcuser', password='F4dus0ee',
    #                      database='db_manage_overruns')
    # # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
    # #                  database='jingdong_ceshi')
    # mycursor = db.cursor()
    # sql = "DELETE FROM t_bas_case_statistics_data WHERE statistics_date = '{}'".format(starttime2)
    # mycursor.execute(sql)
    # db.commit()
    # db.close()
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
        wide_table.to_sql(name='t_bas_illegal_statistics_data', con=engine, if_exists='append', index=False)
    except Exception as e:
        print("mysql����ʧ��")

    from threading import Timer
    import datetime
    """��ʱ1��"""
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
    print('�´�12����������ʱ��', next_time2)
    #
    # next_time = datetime.datetime.strptime(str(next_year) + "-" + str(next_month) + "-" + str(next_day) + " 22:00:00",
    #                                        "%Y-%m-%d %H:%M:%S")
    #

    timer_start_time2 = (next_time - now_time).total_seconds()

    t2 = Timer(timer_start_time2, case_day_statistic)  # �˴�ʹ�õݹ����ʵ��
    t2.start()


if __name__ == "__main__":
    case_day_statistic()

# U_����_����� = pd.merge(t_bas_over_data_collection_31, t_code_area, left_on='area_county', right_on='county_code',
#                      how='left')
#
# U_��������_����� = pd.merge(t_case_sign_result, t_code_area, left_on='area_county', right_on='county_code',
#                       how='left')
#
# U_��ʡ����_����� = pd.merge(t_bas_over_data_collection_makecopy, t_code_area, left_on='area_county',
#                       right_on='county_code', how='left')
#
# T_����ѯ = U_����_�����.loc[(U_����_�����['law_judgment'] == "1")
# ]
#
# ###����
# T_�ֳ����� = U_��������_�����[(U_��������_�����.record_type == 99)
#                     & (U_��������_�����.insert_type == 5)
#                     & (U_��������_�����.area_province == '330000')
#                     ]
#
# T_���ֳ����� = U_��������_�����[(U_��������_�����.record_type == 31)
#                      & (U_��������_�����.insert_type == 1)
#                      & (U_��������_�����.data_source == 1)
#                      & (U_��������_�����.case_type == 1)
#                      & (U_��������_�����.area_province == '330000')
#                      ]
#
# T_��ʡ���� = U_��ʡ����_�����
#
# T_����ѯ = T_����ѯ.sort_values(['area_county'], ascending=False).reset_index(drop=True)
#
# T_����� = T_����ѯ.groupby(['area_city', 'city', 'area_county', 'county'])['id_x'].count().reset_index(
#     name='�����(ϵͳ)')
#
# T_�ֳ����� = T_�ֳ�����.drop_duplicates(['CASE_NUM'])
# T_�ֳ��������� = T_�ֳ�����.groupby(['area_city', 'city', 'area_county', 'county'])['CASE_NUM'].count().reset_index(
#     name='�ֳ�����(ϵͳ)')
#
# T_���ֳ����� = T_���ֳ�����.drop_duplicates(['CASE_NUM'])
# T_���ֳ��������� = T_���ֳ�����.groupby(['area_city', 'city', 'area_county', 'county'])['CASE_NUM'].count().reset_index(
#     name='���ֳ�����(ϵͳ)')
#
# T_��ʡ������� = T_��ʡ����.groupby(['area_city', 'city', 'area_county', 'county'])['id_x'].count().reset_index(
#     name='��ʡ����(ϵͳ)')
#
# W_����ͳ�� = pd.merge(T_�����, T_�ֳ���������, on=['area_city', 'city', 'area_county', 'county'], how='outer')
# W_����ͳ�� = pd.merge(W_����ͳ��, T_���ֳ���������, on=['area_city', 'city', 'area_county', 'county'], how='outer')
# W_����ͳ�� = pd.merge(W_����ͳ��, T_��ʡ�������, on=['area_city', 'city', 'area_county', 'county'], how='outer')
# W_����ͳ��[['��ʡ����(ϵͳ)']] = W_����ͳ��[['��ʡ����(ϵͳ)']].apply(np.int64)
# W_����ͳ��.loc[W_����ͳ��['��ʡ����(ϵͳ)'] < 0, '��ʡ����(ϵͳ)'] = 0
# W_����ͳ�� = W_����ͳ��.fillna(0, inplace=False)
# W_����ͳ��['������'] = W_����ͳ��['���ֳ�����(ϵͳ)'] + W_����ͳ��['�ֳ�����(ϵͳ)']
# W_����ͳ��['���ֳ�������(��������)'] = (W_����ͳ��['���ֳ�����(ϵͳ)'] / (W_����ͳ��['�����(ϵͳ)'] + 0.00001) * 100).round(2)
# W_����ͳ��['���ֳ�������(������)'] = (
#             (W_����ͳ��['���ֳ�����(ϵͳ)'] + W_����ͳ��['��ʡ����(ϵͳ)']) / (W_����ͳ��['�����(ϵͳ)'] + 0.00001) * 100).round(2)
# W_����ͳ��.loc[W_����ͳ��['���ֳ�������(������)'] > 100, '���ֳ�������(������)'] = 100
# starttime2 = starttime.strftime("%Y-%m-%d")
# W_����ͳ��['statistics_date'] = starttime
# # starttime1 = starttime.strftime("%Y%m")
# # W_����ͳ��['id']= W_����ͳ��['area_city'].astype('string')+W_����ͳ��['area_county'].astype('string') + starttime1
# # W_����ͳ��['id']=W_����ͳ��['id'].apply(np.int64)
# W_����ͳ��.rename(
#     columns={'�����(ϵͳ)': 'in_case_num',
#              '�ֳ�����(ϵͳ)': 'p_site_num', '���ֳ�����(ϵͳ)': 'p_offsite_num', '��ʡ����(ϵͳ)': 'f_inform_num',
#              '������': 'total_case_num', '���ֳ�������(��������)': 'p_offsite_rate', '���ֳ�������(������)': 'p_offsite_inform_rate'},
#     inplace=True)
# W_����ͳ������ = pd.DataFrame(W_����ͳ��,
#                         columns=['statistics_date', 'area_city', 'city',
#                                  'area_county', 'county',
#                                  'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
#                                  'total_case_num',
#                                  'p_offsite_rate', 'p_offsite_inform_rate'])
#
# ###����
# W_����ͳ�Ƶ��� = W_����ͳ������.groupby(['statistics_date', 'area_city', 'city']).sum().reset_index()
# W_����ͳ�Ƶ���['p_offsite_rate'] = (W_����ͳ�Ƶ���['p_offsite_num'] / (W_����ͳ�Ƶ���['in_case_num'] + 0.00001) * 100).round(2)
# W_����ͳ�Ƶ���['p_offsite_inform_rate'] = ((W_����ͳ�Ƶ���['p_offsite_num'] + W_����ͳ�Ƶ���['f_inform_num']) / (
#             W_����ͳ�Ƶ���['in_case_num'] + 0.00001) * 100).round(2)
# W_����ͳ�Ƶ���.loc[W_����ͳ�Ƶ���['p_offsite_inform_rate'] > 100, 'p_offsite_inform_rate'] = 100
#
# ##ʡ��
# W_����ͳ��ʡ�� = W_����ͳ�Ƶ���.groupby(['statistics_date']).sum().reset_index()
# W_����ͳ��ʡ��['p_offsite_rate'] = (W_����ͳ��ʡ��['p_offsite_num'] / (W_����ͳ��ʡ��['in_case_num'] + 0.00001) * 100).round(2)
# W_����ͳ��ʡ��['p_offsite_inform_rate'] = ((W_����ͳ��ʡ��['p_offsite_num'] + W_����ͳ��ʡ��['f_inform_num']) / (
#             W_����ͳ��ʡ��['in_case_num'] + 0.00001) * 100).round(2)
# W_����ͳ��ʡ��.loc[W_����ͳ��ʡ��['p_offsite_inform_rate'] > 100, 'p_offsite_inform_rate'] = 100
# W_����ͳ��ʡ��['area_code'] = '330000'
# W_����ͳ��ʡ��['area_name'] = '�㽭'
#
# ##�ϲ�
#
# W_����ͳ������ = pd.DataFrame(W_����ͳ��,
#                         columns=['statistics_date',
#                                  'area_county', 'county',
#                                  'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
#                                  'total_case_num',
#                                  'p_offsite_rate', 'p_offsite_inform_rate'])
#
# W_����ͳ�Ƶ��� = pd.DataFrame(W_����ͳ�Ƶ���,
#                         columns=['statistics_date', 'area_city', 'city',
#                                  'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
#                                  'total_case_num',
#                                  'p_offsite_rate', 'p_offsite_inform_rate'])
#
# W_����ͳ������.rename(
#     columns={'area_county': 'area_code', 'county': 'area_name'}, inplace=True)
# W_����ͳ�Ƶ���.rename(
#     columns={'area_city': 'area_code', 'city': 'area_name'}, inplace=True)
#
# W_����ͳ�� = pd.concat([W_����ͳ������, W_����ͳ�Ƶ���, W_����ͳ��ʡ��])
# W_����ͳ��.loc[W_����ͳ��['p_offsite_rate'] > 100, 'p_offsite_rate'] = 100
# W_����ͳ��.loc[W_����ͳ��['p_offsite_inform_rate'] > 100, 'p_offsite_inform_rate'] = 100
# starttime1 = starttime.strftime("%Y%m%d")
# W_����ͳ�� = W_����ͳ��.fillna(0)
# W_����ͳ��['id'] = W_����ͳ��['area_code'].astype('string') + starttime1
# W_����ͳ�� = pd.DataFrame(W_����ͳ��,
#                       columns=['id', 'statistics_date', 'area_code', 'area_name',
#                                'in_case_num', 'p_site_num', 'p_offsite_num', 'f_inform_num',
#                                'total_case_num',
#                                'p_offsite_rate', 'p_offsite_inform_rate'])
#
# # wide_table31.to_excel(r'C:\Users\liu.wenjie\Desktop\�±�\10��\wide_table31.xlsx')
#
# print(W_����ͳ��)
#
# # W_����ͳ��.to_excel(r'C:\Users\liu.wenjie\Desktop\����\W_����ͳ��22.xlsx')
#
# '''���ݿ�ɾ��'''
# db = pymysql.connect(host="172.19.116.150", port=11806, user='zjzhzcuser', password='F4dus0ee',
#                      database='db_manage_overruns')
# # # db = pymysql.connect(host="127.0.0.1", port=3308, user='root', password='jingdong',
# #                  database='jingdong_ceshi')
# mycursor = db.cursor()
# sql = "DELETE FROM t_bas_case_statistics_data WHERE statistics_date = '{}'".format(starttime2)
# mycursor.execute(sql)
# db.commit()
# db.close()
# try:
#     from sqlalchemy import create_engine
#
#     user = "zjzhzcuser"
#     password = "F4dus0ee"
#     host = "172.19.116.150"
#     db = "db_manage_overruns"
#
#     pwd = parse.quote_plus(password)
#
#     engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:11806/{db}?charset=utf8")
#     # result_table
#     # Ҫд������ݱ�����д�Ļ�Ҫ��ǰ�����ݿ⽨�ñ�
#     # t_bas_though.to_sql(name='t_bas_car_through', con=engine, if_exists='append', index=False)
#     # result_table
#     # Ҫд������ݱ�����д�Ļ�Ҫ��ǰ�����ݿ⽨�ñ�
#     W_����ͳ��.to_sql(name='t_bas_case_statistics_data', con=engine, if_exists='append', index=False)
# except Exception as e:
#     print("mysql����ʧ��")
# #
# # #
# # import pymysql
# #
# #
# # class DBUtils:
# #     """
# #     ���ݿ⹤����
# #     """
# #
# #     """:param
# #     db:     ���ݿ�����:  db = pymysql.connect(host='192.168.1.1', user='root', password='1234', port=3306, db='database_name')
# #     cursor: ���ݿ��α�:  cursor = db.cursor()
# #     data:   ��д������:  Dataframe
# #     table:  д�����
# #     """
# #
# #     def __init__(self, db, cursor, data, table):
# #         self.db = db
# #         self.cursor = cursor
# #         self.data = data
# #         self.table = table
# #
# #     # ������ȥ��׷�Ӹ���
# #     def insert_data(self):
# #         keys = ', '.join('`' + self.data.keys() + '`')
# #         values = ', '.join(['%s'] * len(self.data.columns))
# #         # ���ݱ��Ψһ����ȥ��׷�Ӹ���
# #         sql = 'INSERT INTO {table}({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE'.format(table=self.table,
# #                                                                                              keys=keys,
# #                                                                                              values=values)
# #         update = ','.join(["`{key}` = %s".format(key=key) for key in self.data])
# #         sql += update
# #
# #         for i in range(len(self.data)):
# #             try:
# #                 self.cursor.execute(sql, tuple(self.data.loc[i]) * 2)
# #                 self.db.commit()
# #             except Exception as e:
# #                 print("����д��ʧ��,ԭ��Ϊ:" + e)
# #                 self.db.rollback()
# #
# #         self.cursor.close()
# #         self.db.close()
# #         print('������ȫ��д�����!')
# #
# #
# # W_����ͳ��.fillna("", inplace=True)  # �滻NaN,��������д��ʱ�ᱨ��,Ҳ���滻������
# # # �������ݿ�,�������
# # db = pymysql.connect(host='172.19.116.150', user='zjzhzcuser', password='F4dus0ee', port=11806,
# #                      db='db_manage_overruns')
# # cursor = db.cursor()
# # table = "t_bas_case_statistics_data"  # д�����
# #
# # # д������
# # DBUtils.insert_data(DBUtils(db, cursor, W_����ͳ��, table))
# del t_bas_over_data_collection_31
# del t_case_sign_result
# del t_bas_over_data_collection_makecopy
