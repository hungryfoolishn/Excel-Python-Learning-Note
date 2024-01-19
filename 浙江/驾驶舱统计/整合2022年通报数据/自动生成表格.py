import pymysql
import pandas as pd
import numpy as np
import time

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


# ##�������Լ������
# df_���ݻ��� = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\�±�\202304\4���ݻ��ܱ�0504.xlsx")
# # df_���ݻ���.columns = df_���ݻ���.iloc[0]
# # df_���ݻ��� = df_���ݻ���.iloc[3:].reset_index(drop=True)
# df_���޵�λͳ�� = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\�±�\202304\���޵�λͳ�Ʊ�.xls")
# df_���޵�λͳ��.columns = df_���޵�λͳ��.iloc[0]
# df_���޵�λͳ�� = df_���޵�λͳ��.iloc[1:].reset_index(drop=True)
""" ����ԭʼ�� """
q���� = '2023-01-01'
s���� = '2023-02-01'
q�Ϲ��� = '2023-01-01'
s�Ϲ��� = '2023-02-01'
start_time = '2023-05-01'
end_time = '2023-06-01'


def data_station():
    from datetime import datetime
    from urllib import parse

    day = datetime.now().date()  # ��ȡ��ǰϵͳʱ��
    import datetime

    now = day - datetime.timedelta(days=0)
    print('starttime', now)
    from datetime import datetime

    starttime = start_time
    endtime = end_time
    starttime1 = datetime.strptime(starttime, '%Y-%m-%d')
    endtime1 = datetime.strptime(endtime, '%Y-%m-%d')

    ��Ӧ�������� = (endtime1 - starttime1).days

    sql = "SELECT * FROM t_code_area "
    t_code_area = get_df_from_db(sql)

    ##��վ��
    sql = "SELECT station_name,station_code,station_status,station_type,area_county FROM t_sys_station where  station_type=31 and is_deleted= 0 and station_status in (0,3)"
    total_station = get_df_from_db(sql)

    ##����վ��
    sql = "SELECT b.station_name as '����վ������',a.station_code,reason as '��ע',area_county FROM t_sys_station_maintain a inner join t_sys_station b on a.station_code=b.station_code and b.station_status in (0,3) and a.insert_time <'{}' where   end_time>='{}'   or end_time is null ".format(
        endtime, starttime)
    maintain_station = get_df_from_db(sql)
    df_���޵�λͳ�� = pd.merge(maintain_station, t_code_area, how='left', left_on=['area_county'], right_on=['county_code'])
    df_���޵�λͳ�� = df_���޵�λͳ��.drop_duplicates(['station_code'])
    df_���޵�λͳ�� = df_���޵�λͳ��.sort_values('county_code', ascending=True, ignore_index=True)

    df_���޵�λͳ��.rename(
        columns={'city': '����', 'county': '����'}, inplace=True)
    df_���޵�λͳ�� = pd.DataFrame(df_���޵�λͳ��,
                             columns=['����', '����', '����վ������', '��ע'])

    ##����վ��
    online_station = total_station[~total_station.loc[:, 'station_code'].isin(maintain_station['station_code'])]

    ##����վ����ϸ����
    sql = """ 
       SELECT city_name,county_code,county_name,a.station_code,a.station_name,statistic_date,truck_num,
       overrun_num,no_car_num,overrun_0_10,overrun_10_20
       FROM t_bas_basic_data_pass a
       LEFT JOIN t_sys_station b on a.station_code=b.station_code
       where a.statistic_date  >='{}'
       and a.statistic_date  <'{}'
       and a.station_type =31
       """.format(starttime, endtime)

    pass_truck_num = get_df_from_db(sql)
    pass_truck_num = pd.DataFrame(pass_truck_num)

    pass_truck_num = pass_truck_num[pass_truck_num.loc[:, 'station_code'].isin(online_station['station_code'])]
    df = pass_truck_num.groupby(
        ['city_name', 'county_code', 'county_name', 'station_code', 'station_name']).sum().reset_index()

    pass_truck_num = pass_truck_num[(0 < pass_truck_num['truck_num'])]
    pass_truck_num['statistic_date'] = pd.to_datetime(pass_truck_num['statistic_date'])
    pass_truck_num['ȡ��'] = pass_truck_num['statistic_date'].apply(lambda x: x.strftime('%d'))
    ʵ���������� = pass_truck_num.groupby(['station_code'])['ȡ��'].nunique().reset_index(name='ʵ����������')
    df = pd.merge(df, ʵ����������, on=['station_code'], how='left')
    df['��Ӧ��������'] = ��Ӧ��������
    df['������'] = (df['ʵ����������'] / df['��Ӧ��������'] * 100).round(2)
    df['�ٶ�����'] = 0
    df['����100%��'] = 0
    # city_name,county_code,county_name,a.station_code,a.station_name,statistic_date,truck_num,
    # overrun_num,no_car_num,overrun_0_10,overrun_10_20
    df['������(%)'] = (df['overrun_num'] / df['truck_num'] * 100).round(2)
    df['����10%������'] = df['overrun_num'] - df['overrun_0_10']
    df['����10%���ⳬ����(%)'] = (df['����10%������'] / df['truck_num'] * 100).round(2)
    df['����20%������'] = df['overrun_num'] - df['overrun_0_10'] - df['overrun_10_20']
    df['����20%���ⳬ����(%)'] = (df['����20%������'] / df['truck_num'] * 100).round(2)
    df['������ʱ��'] = now
    df = df.fillna(value=0)

    df.rename(
        columns={'station_name': 'վ������',
                 'city_name': '����', 'county_name': '����', 'truck_num': '������',
                 'overrun_num': '������'}, inplace=True)

    df = pd.DataFrame(df, columns=['վ������', '����', '����', '��Ӧ��������', 'ʵ����������', '������', '������', '������', '�ٶ�����', '����100%��',
                                   '����10%���ⳬ����(%)', '����20%������', '����20%���ⳬ����(%)', '������(%)', '������ʱ��', 'county_code',
                                   'station_code'])
    df_���ݻ��� = df.sort_values('county_code', ascending=True, ignore_index=True)
    return df_���ݻ���, df_���޵�λͳ��


def overrun_site_rate():
    """����1"""
    df_���ݻ���2 = df_���ݻ���[((df_���ݻ���.ʵ���������� >= 10)
                        & (df_���ݻ���.������ > 500))]
    ������ = df_���ݻ���2.groupby(['����', '����'])['������'].sum().reset_index()
    ������ = df_���ݻ���2.groupby(['����', '����'])['����20%������'].sum().reset_index()
    df_sheet1 = pd.merge(������, ������, how='left', on=['����', '����'])
    df_sheet1.loc[df_sheet1['������'] == 0, '������'] = 1
    df_sheet1['������'] = df_sheet1.apply(lambda x: x['����20%������'] / x['������'], axis=1).round(4)
    df_sheet1 = df_sheet1.fillna(0, inplace=False)
    df_sheet1['������'] = df_sheet1['������'].apply(lambda x: format(x, '.2%'))
    df_sheet1.loc[df_sheet1['������'] == 1, '������'] = 0
    df_sheet1.rename(columns={'����20%������': '������'}, inplace=True)

    """����7"""
    # df_���ݻ��� = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\�±�\202301\1�����ݻ��ܱ�v2.0.xls")
    # df_���ݻ���.columns = df_���ݻ���.iloc[2]
    # df_���ݻ��� = df_���ݻ���.iloc[3:].reset_index(drop=True)
    ������ = df_���ݻ���.groupby(['����', '����'])['վ������'].count()
    ������ = df_���޵�λͳ��.groupby(['����', '����'])['����վ������'].count()
    ʵ��վ���� = pd.merge(������, ������, on=['����', '����'], how='outer')
    ʵ��վ���� = ʵ��վ����.fillna(0, inplace=False)
    ʵ��վ����['վ����'] = ʵ��վ����.apply(lambda x: x[0] + x[1], axis=1)
    ʵ��վ����.վ���� = ʵ��վ����.վ����.astype(int)
    ʵ��վ����.����վ������ = ʵ��վ����.����վ������.astype(int)
    ʵ��վ����.rename(columns={'վ������': '������', '����վ������': '������'}, inplace=True)
    T_10ɸѡ = df_���ݻ���[(df_���ݻ���.ʵ���������� < 10)
                     & (df_���ݻ���.ʵ���������� > 0)
                     ]
    T_10ɸѡ = T_10ɸѡ.groupby(['����', '����']).count()
    T_10ɸѡ = T_10ɸѡ.loc[:, ['վ������']]
    T_10ɸѡ.columns = ['վ������']
    T_500ɸѡ = df_���ݻ���[(df_���ݻ���.ʵ���������� >= 10) & (df_���ݻ���.������ < 500)]
    T_500ɸѡ = T_500ɸѡ.groupby(['����', '����']).count()
    T_500ɸѡ = T_500ɸѡ.loc[:, ['վ������']]
    T_500ɸѡ.columns = ['վ������']
    T_500ɸѡ.rename(columns={'վ������': '500����'}, inplace=True)
    T_ɸѡ = pd.merge(T_10ɸѡ, T_500ɸѡ, how='outer', on=['����', '����'])
    T_ɸѡ = T_ɸѡ.fillna(value=0)
    T_ɸѡ['����������10����������500����'] = T_ɸѡ['վ������'] + T_ɸѡ['500����']
    T_10��500 = T_ɸѡ['����������10����������500����']
    t = df_���ݻ���[(df_���ݻ���.ʵ���������� == 0)]
    T_����Ϊ0�� = t.groupby(['����', '����']).count()

    T_����Ϊ0�� = T_����Ϊ0��.loc[:, ['ʵ����������']]
    T_����Ϊ0��.columns = ['ʵ����������']
    T_����Ϊ0��.rename(columns={'ʵ����������': '����Ϊ0'}, inplace=True)
    ����7 = pd.merge(ʵ��վ����, T_����Ϊ0��, how='left', on=['����', '����'])
    ����7 = pd.merge(����7, T_10��500, how='left', on=['����', '����'])
    ����7 = ����7.fillna(0, inplace=False)
    ����7['�쳣��'] = ����7.apply(lambda x: x['������'] + x['����Ϊ0'] + x['����������10����������500����'], axis=1)
    ����7['�쳣��'] = ����7['�쳣��'].astype('float')
    ����7['վ����'] = ����7['վ����'].astype('float')
    ����7['�����'] = (����7['վ����'] - ����7['�쳣��']) / ����7['վ����']
    ����7 = ����7.fillna(0, inplace=False)
    ����7['�����'] = ����7['�����'].apply(lambda x: format(x, '.2%'))
    ����7 = pd.DataFrame(����7, columns=["վ����", "������", "�쳣��", "�����"]).reset_index()
    ����7 = pd.merge(df_sheet1, ����7, how='outer', on=['����', '����'])
    ����7.rename(columns={'����': 'city', '����': 'county'}, inplace=True)
    return ����7
    # q = input("������洢·��(C:/Users/Administrator/Desktop/�������/�������±���)��")


def case_statistic():
    ##���ִ�����
    # q = '2022-01-01'
    # s = '2022-10-01'
    # cs = 'ȫ��9��'
    start = time.time()
    sql = "SELECT * FROM t_code_area"
    t_code_area = get_df_from_db(sql)

    sql = "SELECT * FROM t_bas_over_data_collection_31 where valid_time >= '{} 00:00:00' and valid_time < '{} 00:00:00'".format(
        q����, s����)
    t_bas_over_data_collection_31 = get_df_from_db(sql)
    sql = "SELECT * FROM t_case_sign_result where close_case_time >= '{} 00:00:00' and close_case_time < '{} 00:00:00'".format(
        q����, s����)
    t_case_sign_result = get_df_from_db(sql)

    q = q����.replace('-', '')
    s = s����.replace('-', '')
    sql = "SELECT * FROM t_bas_over_data_collection_makecopy where insert_time >={} and insert_time <{}".format(q, s)
    t_bas_over_data_collection_makecopy = get_df_from_db(sql)

    sql = "SELECT * FROM t_sys_station "
    t_sys_station = get_df_from_db(sql)

    U_����_����� = pd.merge(t_bas_over_data_collection_31, t_code_area, left_on='area_county', right_on='county_code',
                         how='left')

    U_��������_����� = pd.merge(t_case_sign_result, t_code_area, left_on='area_county', right_on='county_code', how='left')

    U_��ʡ����_����� = pd.merge(t_bas_over_data_collection_makecopy, t_code_area, left_on='area_county',
                          right_on='county_code', how='left')

    T_����ѯ = U_����_�����.loc[(U_����_�����['law_judgment'] == "1")]
    T_����ѯ��ʡ = U_����_�����.loc[((U_����_�����['law_judgment'] == "1") & (U_����_�����['car_no'].str.contains('��')))]

    T_�ֳ����� = U_��������_�����[(U_��������_�����.record_type == 99)
                        & (U_��������_�����.insert_type == 5)

                        ]

    T_���ֳ����� = U_��������_�����[(U_��������_�����.record_type == 31)
                         & (U_��������_�����.insert_type == 1)
                         & (U_��������_�����.data_source == 1)
                         & (U_��������_�����.case_type == 1)

                         ]

    T_���ֳ�������ʡ = U_��������_�����[(U_��������_�����.record_type == 31)
                           & (U_��������_�����.insert_type == 1)
                           & (U_��������_�����.data_source == 1)
                           & (U_��������_�����.case_type == 1)
                           & (U_��������_�����['car_no'].str.contains('��'))

                           ]

    T_��ʡ���� = U_��ʡ����_�����

    T_����ѯ = T_����ѯ.sort_values(['area_county'], ascending=False).reset_index(drop=True)
    T_����� = T_����ѯ.groupby([T_����ѯ['area_county'], T_����ѯ['county']]).count()
    T_�������ʡ = T_����ѯ��ʡ.groupby([T_����ѯ��ʡ['area_county'], T_����ѯ��ʡ['county']]).count()

    T_�������ʡ = T_�������ʡ.loc[:, ['id_x']]
    T_�������ʡ.rename(columns={'id_x': '�������ʡ(ϵͳ)'}, inplace=True)
    T_����� = T_�����.loc[:, ['id_x']]
    T_�����.rename(columns={'id_x': '�����(ϵͳ)'}, inplace=True)
    # print("T_�����",T_�����)
    # print("T_�������ʡ",T_�������ʡ)

    T_�ֳ����� = T_�ֳ�����.drop_duplicates(['CASE_NUM'])
    T_�ֳ��������� = T_�ֳ�����.groupby([T_�ֳ�����['area_county']])['CASE_NUM'].count()
    T_�ֳ��������� = T_�ֳ���������.to_frame()
    T_�ֳ���������.rename(columns={'CASE_NUM': '�ֳ�����(ϵͳ)'}, inplace=True)
    # T_�ֳ���������.to_excel(r"C:\Users\liu.wenjie\Desktop\�±�\9��\{}T_�ֳ���������.xlsx".format(cs))

    T_���ֳ����� = T_���ֳ�����.drop_duplicates(['CASE_NUM'])
    T_���ֳ��������� = T_���ֳ�����.groupby([T_���ֳ�����['area_county']])['CASE_NUM'].count()
    T_���ֳ��������� = T_���ֳ���������.to_frame()
    T_���ֳ���������.rename(columns={'CASE_NUM': '���ֳ�����(ϵͳ)'}, inplace=True)

    T_���ֳ�������ʡ = T_���ֳ�������ʡ.drop_duplicates(['CASE_NUM'])
    T_���ֳ�����������ʡ = T_���ֳ�������ʡ.groupby([T_���ֳ�������ʡ['area_county']])['CASE_NUM'].count()
    T_���ֳ�����������ʡ = T_���ֳ�����������ʡ.to_frame()
    T_���ֳ�����������ʡ.rename(columns={'CASE_NUM': '���ֳ�������ʡ(ϵͳ)'}, inplace=True)
    # print('T_���ֳ�����������ʡ',T_���ֳ�����������ʡ)
    # print('T_���ֳ���������',T_���ֳ���������)

    T_��ʡ���� = T_��ʡ����.groupby([T_��ʡ����['area_county']]).count()
    T_��ʡ������� = T_��ʡ����.loc[:, ['id_x']]
    T_��ʡ�������.rename(columns={'id_x': '��ʡ����(ϵͳ)'}, inplace=True)
    W_����ͳ�� = pd.merge(T_�����, T_�������ʡ, left_on='area_county', right_on='area_county', how='outer')
    W_����ͳ�� = pd.merge(W_����ͳ��, T_�ֳ���������, left_on='area_county', right_on='area_county', how='outer')
    W_����ͳ�� = pd.merge(W_����ͳ��, T_���ֳ���������, left_on='area_county', right_on='area_county', how='outer')
    W_����ͳ�� = pd.merge(W_����ͳ��, T_���ֳ�����������ʡ, left_on='area_county', right_on='area_county', how='outer')
    W_����ͳ�� = pd.merge(W_����ͳ��, T_��ʡ�������, left_on='area_county', right_on='area_county', how='outer')
    W_����ͳ��[['��ʡ����(ϵͳ)']] = W_����ͳ��[['��ʡ����(ϵͳ)']].apply(np.int64)
    W_����ͳ��.loc[W_����ͳ��['��ʡ����(ϵͳ)'] < 0, '��ʡ����(ϵͳ)'] = 0
    W_����ͳ�� = W_����ͳ��.fillna(0, inplace=False)
    W_����ͳ�� = W_����ͳ��.reset_index()
    W_����ͳ��.rename(columns={'area_county': 'county_code'}, inplace=True)
    return W_����ͳ��


def case_statistic����():
    ##���ִ�����
    # q = '2022-01-01'
    # s = '2022-10-01'
    # cs = 'ȫ��9��'
    start = time.time()
    sql = "SELECT * FROM t_code_area"
    t_code_area = get_df_from_db(sql)

    sql = """SELECT
          area_county,
          count(DISTINCT case_number) as �����ֳ��鴦��
          FROM
          t_bas_police_road_site a
          LEFT JOIN t_code_area b ON a.area_county = b.county_code
          WHERE 
          a.punish_time >= '{} 00:00:00'
          and a.punish_time < '{} 00:00:00'
          and a.create_time< '{} 00:00:00'
          and case_status=2
          GROUP BY area_county
          """.format(q����, s����, s����)
    T_�����ֳ����� = get_df_from_db(sql)
    sql = "SELECT c.area_county, record_type,insert_type,data_source,case_type,c.car_no,c.CASE_NUM " \
          "FROM t_case_sign_result c LEFT JOIN   t_bas_over_data_collection_31  b ON c.record_code = b.record_code  " \
          "where close_case_time >= '{} 00:00:00' and close_case_time < '{} 00:00:00' and b.valid_time >= '{} 00:00:00' ".format(
        q����, s����, q����)
    t_case_sign_result = get_df_from_db(sql)

    U_��������_����� = pd.merge(t_case_sign_result, t_code_area, left_on='area_county', right_on='county_code', how='left')

    T_���ֳ����� = U_��������_�����[(U_��������_�����.record_type == 31)
                         & (U_��������_�����.insert_type == 1)
                         & (U_��������_�����.data_source == 1)
                         & (U_��������_�����.case_type == 1)

                         ]

    T_���ֳ�������ʡ = U_��������_�����[(U_��������_�����.record_type == 31)
                           & (U_��������_�����.insert_type == 1)
                           & (U_��������_�����.data_source == 1)
                           & (U_��������_�����.case_type == 1)
                           & (U_��������_�����['car_no'].str.contains('��'))

                           ]
    T_���ֳ����� = T_���ֳ�����.drop_duplicates(['CASE_NUM'])
    T_���ֳ��������� = T_���ֳ�����.groupby([T_���ֳ�����['area_county']])['CASE_NUM'].count()
    T_���ֳ��������� = T_���ֳ���������.to_frame()
    T_���ֳ���������.rename(columns={'CASE_NUM': '���ֳ�����(·��)����'}, inplace=True)

    T_���ֳ�������ʡ = T_���ֳ�������ʡ.drop_duplicates(['CASE_NUM'])
    T_���ֳ�����������ʡ = T_���ֳ�������ʡ.groupby([T_���ֳ�������ʡ['area_county']])['CASE_NUM'].count()
    T_���ֳ�����������ʡ = T_���ֳ�����������ʡ.to_frame()
    T_���ֳ�����������ʡ.rename(columns={'CASE_NUM': '���ֳ�������ʡ(·��)����'}, inplace=True)
    # print('T_���ֳ�����������ʡ',T_���ֳ�����������ʡ)
    # print('T_���ֳ���������',T_���ֳ���������)
    W_����ͳ�� = pd.merge(T_���ֳ���������, T_���ֳ�����������ʡ, on='area_county', how='outer')
    W_����ͳ�� = pd.merge(W_����ͳ��, T_�����ֳ�����, on='area_county', how='outer')
    W_����ͳ�� = W_����ͳ��.fillna(0, inplace=False)
    W_����ͳ�� = W_����ͳ��.reset_index()
    W_����ͳ��.rename(columns={'area_county': 'county_code'}, inplace=True)
    return W_����ͳ��


def Compliance_rate():
    station_code = df_���ݻ���['station_code']

    sql = "SELECT * FROM t_code_area "
    t_code_area = get_df_from_db(sql)

    sql = "SELECT * FROM t_bas_over_data_31 where out_station_time >= '{} 00:00:00' and  out_station_time <'{} 00:00:00' and total_weight>80   AND is_unusual = 0  ".format(
        q�Ϲ���, s�Ϲ���)
    t_bas_over_data_31 = get_df_from_db(sql)

    sql = "SELECT * FROM t_bas_over_data_collection_31 where out_station_time >= '{} 00:00:00' and  out_station_time <'{} 00:00:00' and total_weight>80  ".format(
        q�Ϲ���, s�Ϲ���)
    t_bas_over_data_collection_31 = get_df_from_db(sql)

    U_����_����� = pd.merge(t_bas_over_data_31, t_code_area, left_on='area_county', right_on='county_code', how='left')
    U_�������_����� = pd.merge(t_bas_over_data_collection_31, t_code_area, left_on='area_county', right_on='county_code',
                          how='left')
    U_�������_����� = U_�������_�����[U_�������_�����['out_station'].isin(station_code)]
    U_����_վ��� = U_����_�����[U_����_�����.loc[:, 'out_station'].isin(station_code)]

    �³���80�������� = U_����_վ���.groupby(['city_code', 'city', 'county_code', 'county']).count()

    �³���80�������� = �³���80��������['id_x']

    U_����_վ��� = U_����_վ���[(U_����_վ���['is_collect'] == 1)
    ]

    �³���80���������㴦���������� = U_����_վ���.groupby(['city_code', 'city', 'county_code', 'county']).count()

    �³���80���������㴦���������� = �³���80���������㴦����������['id_x']

    snum = [3, 4, 5, 6, 9, 12, 13]

    U_����_վ��� = U_�������_�����[U_�������_�����.loc[:, 'status_x'].isin(snum)]

    �³���80�������ͨ������ = U_����_վ���.groupby(['city_code', 'city', 'county_code', 'county']).count()

    �³���80�������ͨ������ = �³���80�������ͨ������['id_x']

    ����80�� = pd.merge(�³���80��������, �³���80���������㴦����������, on=['city_code', 'city', 'county_code', 'county'], how='left')

    ����80�� = pd.merge(����80��, �³���80�������ͨ������, on=['city_code', 'city', 'county_code', 'county'], how='left')

    ����80��.rename(columns={'id_x_x': '�³���80%��������', 'id_x_y': '�³���80%���������㴦����������', 'id_x': '�³���80%�������ͨ������'},
                 inplace=True)
    ����80�� = ����80��.fillna(0, inplace=False)
    return ����80��


def Key_freight_sources():
    """ ����ԭʼ�� """
    sql = "SELECT city,county,city_code,county_code  FROM t_code_area where province_code = '330000'"
    t_code_area = get_df_from_db(sql)
    # sql = "SELECT * from t_bas_source_company where area_province = '330000' and is_statictis =1  and is_deleted=0 "
    # t_bas_source_company=get_df_from_db(sql)
    # sql = "SELECT * FROM t_bas_source_company_equipment WHERE is_deleted = 0 OR is_deleted IS NULL"
    # t_bas_source_company_equipment=get_df_from_db(sql)
    վ������ = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\�±�\������\�ص����Դͷ445����ϸ0309.xlsx")
    ����վ������ = վ������.groupby(['city', 'city_code', 'county', 'county_code'])['id'].count().reset_index(name='����վ������')

    ����վ������['county_code'] = ����վ������['county_code'].astype('string')

    sql = "SELECT * FROM t_sys_station WHERE station_type = 71 AND is_deleted = 0 AND station_status != 1  and station_code IS NOT null "
    t_sys_station = get_df_from_db(sql)
    sql = "SELECT area_city,area_county,out_station,out_station_time,car_no,total_weight,limit_weight,overrun,overrun_rate,axis" \
          " FROM t_bas_pass_data_71  where out_station_time >='{} 00:00:00' and out_station_time <='{} 00:00:00'  and is_truck =1 and insert_time <='{} 00:00:00' ".format(
        start_time, end_time, end_time)
    t_bas_pass_data_71 = get_df_from_db(sql)

    """ƴ�ӱ�"""
    U_Դͷ_����� = pd.merge(t_bas_pass_data_71, t_code_area, left_on='area_county', right_on='county_code', how='left')

    ��ҵ_Դͷ_վ��� = pd.merge(t_sys_station, t_code_area, left_on='area_county', right_on='county_code', how='left')

    station_code = ��ҵ_Դͷ_վ���['station_code']
    U_Դͷ_����� = U_Դͷ_�����[U_Դͷ_�����.loc[:, 'out_station'].isin(station_code)]
    # q = input("�����봢��·��(C:/Users/Administrator/Desktop/�������/�±���������ݻ���)��")
    # with pd.ExcelWriter('{}/U_Դͷ_�����.xlsx'.format(q))as writer1:
    #      U_Դͷ_�����.to_excel(writer1, sheet_name='sheet1', index=True)

    """������"""
    ����20_50 = U_Դͷ_�����[(U_Դͷ_�����['overrun_rate'] > 20) & (U_Դͷ_�����['overrun_rate'] <= 50)].groupby(
        ['city', 'county', 'county_code'])['car_no'].count().reset_index(name='20-50%��')
    ����50_100 = U_Դͷ_�����[(U_Դͷ_�����['overrun_rate'] > 50) & (U_Դͷ_�����['overrun_rate'] <= 100)].groupby(
        ['city', 'county', 'county_code'])['car_no'].count().reset_index(name='50-100%��')
    ����100 = U_Դͷ_�����[(U_Դͷ_�����['overrun_rate'] > 100) & (U_Դͷ_�����['overrun_rate'] <= 450)].groupby(
        ['city', 'county', 'county_code'])['car_no'].count().reset_index(name='100%������')

    """�豸������"""

    U_Դͷ_�����['ȡ��'] = U_Դͷ_�����['out_station_time'].apply(lambda x: x.strftime('%d'))
    ������������20�� = U_Դͷ_�����.groupby(['city', 'county_code', 'county', 'out_station'])['ȡ��'].nunique().reset_index(
        name='��������')
    ����������2��� = U_Դͷ_�����.groupby(['city', 'county_code', 'county', 'out_station'])['total_weight'].sum().reset_index(
        name='��������')
    ����������410�� = U_Դͷ_�����.groupby(['city', 'county_code', 'county', 'out_station'])['city'].count().reset_index(
        name='������')
    վ������� = pd.merge(������������20��, ����������2���, on=['city', 'county_code', 'county', 'out_station'], how='left')
    վ������� = pd.merge(վ�������, ����������410��, on=['city', 'county_code', 'county', 'out_station'], how='left')
    վ�������['��������'] = pd.to_numeric(վ�������['��������'], errors='coerce')

    վ����������� = վ�������.groupby(['city', 'county', 'county_code'])['������', '��������'].sum()
    ����վ���� = վ�������[(վ�������['��������'] > 20) | (վ�������['��������'] > 20000) | (վ�������['������'] > 410)].groupby(
        ['city', 'county', 'county_code'])['out_station'].count().reset_index(name='����վ����')

    """�ۺ�"""

    ����Դͷ������� = pd.merge(����վ������, ����20_50, on=['city', 'county', 'county_code'], how='left')
    ����Դͷ������� = pd.merge(����Դͷ�������, ����50_100, on=['city', 'county', 'county_code'], how='left')
    ����Դͷ������� = pd.merge(����Դͷ�������, ����100, on=['city', 'county', 'county_code'], how='left')
    ����Դͷ������� = pd.merge(����Դͷ�������, ����վ����, on=['city', 'county', 'county_code'], how='left')
    ����Դͷ������� = pd.merge(����Դͷ�������, վ�����������, on=['city', 'county', 'county_code'], how='left')
    ����Դͷ������� = ����Դͷ�������.fillna(0, inplace=False)
    ����Դͷ�������['Դͷ��λƽ�������������Σ�'] = ����Դͷ�������.apply(lambda x: x['������'] / (x['����վ������'] + 0.0000001), axis=1).round(0)
    ����Դͷ�������['�豸�����ʣ�%��'] = ����Դͷ�������.apply(lambda x: x['����վ����'] / (x['����վ������']), axis=1)
    ����Դͷ�������['20-50%ռ��'] = ����Դͷ�������.apply(lambda x: x['20-50%��'] / (x['������'] + 0.0000001), axis=1)
    ����Դͷ�������['50-100%ռ��'] = ����Դͷ�������.apply(lambda x: x['50-100%��'] / (x['������'] + 0.0000001), axis=1)
    ����Դͷ�������['100%����ռ��'] = ����Դͷ�������.apply(lambda x: x['100%������'] / (x['������'] + 0.0000001), axis=1)
    ����Դͷ�������['�豸�����ʣ�%��'] = ����Դͷ�������.apply(lambda x: x['����վ����'] / (x['����վ������']), axis=1)
    ����Դͷ������� = ����Դͷ�������.fillna(0, inplace=False)
    ����Դͷ�������['�豸�����ʣ�%��'] = ����Դͷ�������['�豸�����ʣ�%��'].apply(lambda x: format(x, '.2%'))
    ����Դͷ�������['20-50%ռ��'] = ����Դͷ�������['20-50%ռ��'].apply(lambda x: format(x, '.2%'))
    ����Դͷ�������['50-100%ռ��'] = ����Դͷ�������['50-100%ռ��'].apply(lambda x: format(x, '.2%'))
    ����Դͷ�������['100%����ռ��'] = ����Դͷ�������['100%����ռ��'].apply(lambda x: format(x, '.2%'))
    ����Դͷ�������.rename(columns={'city': '����', 'county': '����'}, inplace=True)
    ����Դͷ������� = pd.DataFrame(����Դͷ�������,
                            columns=['����', '����', '����վ������', '������', 'Դͷ��λƽ�������������Σ�', '����վ����', '�豸�����ʣ�%��', '20-50%��',
                                     '50-100%��', '100%������', '20-50%ռ��', '50-100%ռ��', '100%����ռ��', 'city_code',
                                     'county_code'])
    ����Դͷ������� = ����Դͷ�������.sort_values('county_code', ascending=True)

    ����Դͷ������ݵ��� = ����Դͷ�������.groupby(['����', 'city_code']).sum().reset_index()
    ����Դͷ������ݵ��� = ����Դͷ������ݵ���.fillna(0, inplace=False)
    ����Դͷ������ݵ���['Դͷ��λƽ�������������Σ�'] = ����Դͷ������ݵ���.apply(lambda x: x['������'] / (x['����վ������'] + 0.0000001), axis=1).round(0)
    ����Դͷ������ݵ���['�豸�����ʣ�%��'] = ����Դͷ������ݵ���.apply(lambda x: x['����վ����'] / (x['����վ������']), axis=1)
    ����Դͷ������ݵ���['20-50%ռ��'] = ����Դͷ������ݵ���.apply(lambda x: x['20-50%��'] / (x['������'] + 0.0000001), axis=1)
    ����Դͷ������ݵ���['50-100%ռ��'] = ����Դͷ������ݵ���.apply(lambda x: x['50-100%��'] / (x['������'] + 0.0000001), axis=1)
    ����Դͷ������ݵ���['100%����ռ��'] = ����Դͷ������ݵ���.apply(lambda x: x['100%������'] / (x['������'] + 0.0000001), axis=1)
    ����Դͷ������ݵ���['�豸�����ʣ�%��'] = ����Դͷ������ݵ���.apply(lambda x: x['����վ����'] / (x['����վ������']), axis=1)
    ����Դͷ������ݵ��� = ����Դͷ������ݵ���.fillna(0, inplace=False)
    ����Դͷ������ݵ���['�豸�����ʣ�%��'] = ����Դͷ������ݵ���['�豸�����ʣ�%��'].apply(lambda x: format(x, '.2%'))
    ����Դͷ������ݵ���['20-50%ռ��'] = ����Դͷ������ݵ���['20-50%ռ��'].apply(lambda x: format(x, '.2%'))
    ����Դͷ������ݵ���['50-100%ռ��'] = ����Դͷ������ݵ���['50-100%ռ��'].apply(lambda x: format(x, '.2%'))
    ����Դͷ������ݵ���['100%����ռ��'] = ����Դͷ������ݵ���['100%����ռ��'].apply(lambda x: format(x, '.2%'))

    ����Դͷ������ݵ��� = pd.DataFrame(����Դͷ������ݵ���,
                              columns=['����', '����վ������', '������', 'Դͷ��λƽ�������������Σ�', '����վ����', '�豸�����ʣ�%��', '20-50%��',
                                       '50-100%��', '100%������', '20-50%ռ��', '50-100%ռ��', '100%����ռ��', 'city_code'])
    ����Դͷ������ݵ��� = ����Դͷ������ݵ���.sort_values('city_code', ascending=True)
    ����Դͷ�������ʡ = ����Դͷ������ݵ���
    ����Դͷ�������ʡ['ʡ'] = '�㽭ʡ'
    ����Դͷ�������ʡ = ����Դͷ�������ʡ.groupby(['ʡ']).sum().reset_index()
    ����Դͷ�������ʡ = ����Դͷ�������ʡ.fillna(0, inplace=False)
    ����Դͷ�������ʡ['Դͷ��λƽ�������������Σ�'] = ����Դͷ�������ʡ.apply(lambda x: x['������'] / (x['����վ������'] + 0.0000001), axis=1).round(0)
    ����Դͷ�������ʡ['�豸�����ʣ�%��'] = ����Դͷ�������ʡ.apply(lambda x: x['����վ����'] / (x['����վ������']), axis=1)
    ����Դͷ�������ʡ['20-50%ռ��'] = ����Դͷ�������ʡ.apply(lambda x: x['20-50%��'] / (x['������'] + 0.0000001), axis=1)
    ����Դͷ�������ʡ['50-100%ռ��'] = ����Դͷ�������ʡ.apply(lambda x: x['50-100%��'] / (x['������'] + 0.0000001), axis=1)
    ����Դͷ�������ʡ['100%����ռ��'] = ����Դͷ�������ʡ.apply(lambda x: x['100%������'] / (x['������'] + 0.0000001), axis=1)
    ����Դͷ�������ʡ['�豸�����ʣ�%��'] = ����Դͷ�������ʡ.apply(lambda x: x['����վ����'] / (x['����վ������']), axis=1)
    ����Դͷ�������ʡ = ����Դͷ�������ʡ.fillna(0, inplace=False)
    ����Դͷ�������ʡ['�豸�����ʣ�%��'] = ����Դͷ�������ʡ['�豸�����ʣ�%��'].apply(lambda x: format(x, '.2%'))
    ����Դͷ�������ʡ['20-50%ռ��'] = ����Դͷ�������ʡ['20-50%ռ��'].apply(lambda x: format(x, '.2%'))
    ����Դͷ�������ʡ['50-100%ռ��'] = ����Դͷ�������ʡ['50-100%ռ��'].apply(lambda x: format(x, '.2%'))
    ����Դͷ�������ʡ['100%����ռ��'] = ����Դͷ�������ʡ['100%����ռ��'].apply(lambda x: format(x, '.2%'))
    ����Դͷ�������ʡ.rename(columns={'ʡ': '����'}, inplace=True)
    ����Դͷ�������ʡ = pd.DataFrame(����Դͷ�������ʡ, columns=['����', '����վ������', '������', 'Դͷ��λƽ�������������Σ�', '����վ����', '�豸�����ʣ�%��', '20-50%��',
                                                 '50-100%��', '100%������', '20-50%ռ��', '50-100%ռ��', '100%����ռ��'])
    ����Դͷ�������ʡ�� = pd.concat([����Դͷ������ݵ���, ����Դͷ�������ʡ])

    # with pd.ExcelWriter(r'C:\Users\liu.wenjie\Desktop\ȫ��Դͷ.xlsx') as writer1:
    #     ����Դͷ�������.to_excel(writer1, sheet_name='����', index=False)
    #     ����Դͷ������ݵ���.to_excel(writer1, sheet_name='����', index=False)
    #     ����Դͷ�������ʡ��.to_excel(writer1, sheet_name='ʡ', index=False)
    #     վ�������.to_excel(writer1, sheet_name='վ��������ϸ', index=False)
    ����Դͷ������� = pd.DataFrame(����Դͷ�������,
                            columns=['������Դͷ����', '����վ������', '������', 'Դͷ��λƽ�������������Σ�', '����վ����', '�豸�����ʣ�%��', '20-50%��',
                                     '50-100%��', '100%������', '20-50%ռ��', '50-100%ռ��', '100%����ռ��', 'county_code'])
    ����Դͷ�������.rename(columns={'������': 'Դͷ������'}, inplace=True)
    return ����Դͷ�������, ����Դͷ�������ʡ��, վ�������


if __name__ == "__main__":
    df_���ݻ���, df_���޵�λͳ�� = data_station()
    print(df_���ݻ���)
    ����7 = overrun_site_rate()
    W_����ͳ�� = case_statistic()
    print(W_����ͳ��)
    ����80�� = Compliance_rate()
    print(����80��)
    W_����ͳ�Ƶ��� = case_statistic����()
    ����Դͷ�����������, ����Դͷ�������ʡ��, վ������� = Key_freight_sources()
    sql = "SELECT city_code, city, county_code, county FROM t_code_area where province_code = 330000 "
    t_code_area = get_df_from_db(sql)
    U_all = pd.merge(t_code_area, ����7, on=['city', 'county'], how='left')
    U_all = pd.merge(U_all, W_����ͳ��, on=['county_code'], how='left')
    U_all = pd.merge(U_all, W_����ͳ�Ƶ���, on=['county_code'], how='left')
    U_all = pd.merge(U_all, ����80��, on=['city_code', 'city', 'county_code', 'county'], how='left')
    U_all = pd.merge(U_all, ����Դͷ�����������, on=['county_code'], how='left')
    U_all���� = U_all.groupby(['city_code', 'city']).sum()
    print(U_all����)
    with pd.ExcelWriter(r'C:\Users\liu.wenjie\Desktop\��������\test0625.xlsx') as writer1:
        U_all.to_excel(writer1, sheet_name='����', index=False)
        U_all����.to_excel(writer1, sheet_name='����')
        ����Դͷ�������ʡ��.to_excel(writer1, sheet_name='Դͷ���м�', index=False)
        վ�������.to_excel(writer1, sheet_name='Դͷվ��������ϸ', index=False)
        df_���޵�λͳ��.to_excel(writer1, sheet_name='����', index=False)
        df_���ݻ���.to_excel(writer1, sheet_name='����')
