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


# ##超限率以及完好率
# df_数据汇总 = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\月报\202304\4数据汇总表0504.xlsx")
# # df_数据汇总.columns = df_数据汇总.iloc[0]
# # df_数据汇总 = df_数据汇总.iloc[3:].reset_index(drop=True)
# df_报修点位统计 = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\月报\202304\报修点位统计表.xls")
# df_报修点位统计.columns = df_报修点位统计.iloc[0]
# df_报修点位统计 = df_报修点位统计.iloc[1:].reset_index(drop=True)
""" 引入原始表 """
q案件 = '2023-01-01'
s案件 = '2023-02-01'
q合规率 = '2023-01-01'
s合规率 = '2023-02-01'
start_time = '2023-05-01'
end_time = '2023-06-01'


def data_station():
    from datetime import datetime
    from urllib import parse

    day = datetime.now().date()  # 获取当前系统时间
    import datetime

    now = day - datetime.timedelta(days=0)
    print('starttime', now)
    from datetime import datetime

    starttime = start_time
    endtime = end_time
    starttime1 = datetime.strptime(starttime, '%Y-%m-%d')
    endtime1 = datetime.strptime(endtime, '%Y-%m-%d')

    理应在线天数 = (endtime1 - starttime1).days

    sql = "SELECT * FROM t_code_area "
    t_code_area = get_df_from_db(sql)

    ##总站点
    sql = "SELECT station_name,station_code,station_status,station_type,area_county FROM t_sys_station where  station_type=31 and is_deleted= 0 and station_status in (0,3)"
    total_station = get_df_from_db(sql)

    ##报修站点
    sql = "SELECT b.station_name as '报修站点名称',a.station_code,reason as '备注',area_county FROM t_sys_station_maintain a inner join t_sys_station b on a.station_code=b.station_code and b.station_status in (0,3) and a.insert_time <'{}' where   end_time>='{}'   or end_time is null ".format(
        endtime, starttime)
    maintain_station = get_df_from_db(sql)
    df_报修点位统计 = pd.merge(maintain_station, t_code_area, how='left', left_on=['area_county'], right_on=['county_code'])
    df_报修点位统计 = df_报修点位统计.drop_duplicates(['station_code'])
    df_报修点位统计 = df_报修点位统计.sort_values('county_code', ascending=True, ignore_index=True)

    df_报修点位统计.rename(
        columns={'city': '地市', 'county': '区县'}, inplace=True)
    df_报修点位统计 = pd.DataFrame(df_报修点位统计,
                             columns=['地市', '区县', '报修站点名称', '备注'])

    ##在用站点
    online_station = total_station[~total_station.loc[:, 'station_code'].isin(maintain_station['station_code'])]

    ##在用站点明细数据
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
    pass_truck_num['取日'] = pass_truck_num['statistic_date'].apply(lambda x: x.strftime('%d'))
    实际在线天数 = pass_truck_num.groupby(['station_code'])['取日'].nunique().reset_index(name='实际在线天数')
    df = pd.merge(df, 实际在线天数, on=['station_code'], how='left')
    df['理应在线天数'] = 理应在线天数
    df['在线率'] = (df['实际在线天数'] / df['理应在线天数'] * 100).round(2)
    df['百吨王数'] = 0
    df['超限100%数'] = 0
    # city_name,county_code,county_name,a.station_code,a.station_name,statistic_date,truck_num,
    # overrun_num,no_car_num,overrun_0_10,overrun_10_20
    df['超限率(%)'] = (df['overrun_num'] / df['truck_num'] * 100).round(2)
    df['超限10%除外数'] = df['overrun_num'] - df['overrun_0_10']
    df['超限10%除外超限率(%)'] = (df['超限10%除外数'] / df['truck_num'] * 100).round(2)
    df['超限20%除外数'] = df['overrun_num'] - df['overrun_0_10'] - df['overrun_10_20']
    df['超限20%除外超限率(%)'] = (df['超限20%除外数'] / df['truck_num'] * 100).round(2)
    df['最后接收时间'] = now
    df = df.fillna(value=0)

    df.rename(
        columns={'station_name': '站点名称',
                 'city_name': '地市', 'county_name': '区县', 'truck_num': '货车数',
                 'overrun_num': '超限数'}, inplace=True)

    df = pd.DataFrame(df, columns=['站点名称', '地市', '区县', '理应在线天数', '实际在线天数', '在线率', '货车数', '超限数', '百吨王数', '超限100%数',
                                   '超限10%除外超限率(%)', '超限20%除外数', '超限20%除外超限率(%)', '超限率(%)', '最后接收时间', 'county_code',
                                   'station_code'])
    df_数据汇总 = df.sort_values('county_code', ascending=True, ignore_index=True)
    return df_数据汇总, df_报修点位统计


def overrun_site_rate():
    """附件1"""
    df_数据汇总2 = df_数据汇总[((df_数据汇总.实际在线天数 >= 10)
                        & (df_数据汇总.货车数 > 500))]
    货车数 = df_数据汇总2.groupby(['地市', '区县'])['货车数'].sum().reset_index()
    超限数 = df_数据汇总2.groupby(['地市', '区县'])['超限20%除外数'].sum().reset_index()
    df_sheet1 = pd.merge(货车数, 超限数, how='left', on=['地市', '区县'])
    df_sheet1.loc[df_sheet1['货车数'] == 0, '货车数'] = 1
    df_sheet1['超限率'] = df_sheet1.apply(lambda x: x['超限20%除外数'] / x['货车数'], axis=1).round(4)
    df_sheet1 = df_sheet1.fillna(0, inplace=False)
    df_sheet1['超限率'] = df_sheet1['超限率'].apply(lambda x: format(x, '.2%'))
    df_sheet1.loc[df_sheet1['货车数'] == 1, '货车数'] = 0
    df_sheet1.rename(columns={'超限20%除外数': '超限数'}, inplace=True)

    """附件7"""
    # df_数据汇总 = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\月报\202301\1月数据汇总表v2.0.xls")
    # df_数据汇总.columns = df_数据汇总.iloc[2]
    # df_数据汇总 = df_数据汇总.iloc[3:].reset_index(drop=True)
    在用数 = df_数据汇总.groupby(['地市', '区县'])['站点名称'].count()
    报修数 = df_报修点位统计.groupby(['地市', '区县'])['报修站点名称'].count()
    实际站点数 = pd.merge(在用数, 报修数, on=['地市', '区县'], how='outer')
    实际站点数 = 实际站点数.fillna(0, inplace=False)
    实际站点数['站点数'] = 实际站点数.apply(lambda x: x[0] + x[1], axis=1)
    实际站点数.站点数 = 实际站点数.站点数.astype(int)
    实际站点数.报修站点名称 = 实际站点数.报修站点名称.astype(int)
    实际站点数.rename(columns={'站点名称': '在用数', '报修站点名称': '报修数'}, inplace=True)
    T_10筛选 = df_数据汇总[(df_数据汇总.实际在线天数 < 10)
                     & (df_数据汇总.实际在线天数 > 0)
                     ]
    T_10筛选 = T_10筛选.groupby(['地市', '区县']).count()
    T_10筛选 = T_10筛选.loc[:, ['站点名称']]
    T_10筛选.columns = ['站点名称']
    T_500筛选 = df_数据汇总[(df_数据汇总.实际在线天数 >= 10) & (df_数据汇总.货车数 < 500)]
    T_500筛选 = T_500筛选.groupby(['地市', '区县']).count()
    T_500筛选 = T_500筛选.loc[:, ['站点名称']]
    T_500筛选.columns = ['站点名称']
    T_500筛选.rename(columns={'站点名称': '500辆次'}, inplace=True)
    T_筛选 = pd.merge(T_10筛选, T_500筛选, how='outer', on=['地市', '区县'])
    T_筛选 = T_筛选.fillna(value=0)
    T_筛选['在线天数＜10天或货车数＜500辆次'] = T_筛选['站点名称'] + T_筛选['500辆次']
    T_10或500 = T_筛选['在线天数＜10天或货车数＜500辆次']
    t = df_数据汇总[(df_数据汇总.实际在线天数 == 0)]
    T_数据为0数 = t.groupby(['地市', '区县']).count()

    T_数据为0数 = T_数据为0数.loc[:, ['实际在线天数']]
    T_数据为0数.columns = ['实际在线天数']
    T_数据为0数.rename(columns={'实际在线天数': '数据为0'}, inplace=True)
    附件7 = pd.merge(实际站点数, T_数据为0数, how='left', on=['地市', '区县'])
    附件7 = pd.merge(附件7, T_10或500, how='left', on=['地市', '区县'])
    附件7 = 附件7.fillna(0, inplace=False)
    附件7['异常数'] = 附件7.apply(lambda x: x['报修数'] + x['数据为0'] + x['在线天数＜10天或货车数＜500辆次'], axis=1)
    附件7['异常数'] = 附件7['异常数'].astype('float')
    附件7['站点数'] = 附件7['站点数'].astype('float')
    附件7['完好率'] = (附件7['站点数'] - 附件7['异常数']) / 附件7['站点数']
    附件7 = 附件7.fillna(0, inplace=False)
    附件7['完好率'] = 附件7['完好率'].apply(lambda x: format(x, '.2%'))
    附件7 = pd.DataFrame(附件7, columns=["站点数", "报修数", "异常数", "完好率"]).reset_index()
    附件7 = pd.merge(df_sheet1, 附件7, how='outer', on=['地市', '区县'])
    附件7.rename(columns={'地市': 'city', '区县': 'county'}, inplace=True)
    return 附件7
    # q = input("请输入存储路径(C:/Users/Administrator/Desktop/输出报表/其他市月报表)：")


def case_statistic():
    ##非现处罚率
    # q = '2022-01-01'
    # s = '2022-10-01'
    # cs = '全部9月'
    start = time.time()
    sql = "SELECT * FROM t_code_area"
    t_code_area = get_df_from_db(sql)

    sql = "SELECT * FROM t_bas_over_data_collection_31 where valid_time >= '{} 00:00:00' and valid_time < '{} 00:00:00'".format(
        q案件, s案件)
    t_bas_over_data_collection_31 = get_df_from_db(sql)
    sql = "SELECT * FROM t_case_sign_result where close_case_time >= '{} 00:00:00' and close_case_time < '{} 00:00:00'".format(
        q案件, s案件)
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
          """.format(q案件, s案件, s案件)
    T_交警现场处罚 = get_df_from_db(sql)
    sql = "SELECT c.area_county, record_type,insert_type,data_source,case_type,c.car_no,c.CASE_NUM " \
          "FROM t_case_sign_result c LEFT JOIN   t_bas_over_data_collection_31  b ON c.record_code = b.record_code  " \
          "where close_case_time >= '{} 00:00:00' and close_case_time < '{} 00:00:00' and b.valid_time >= '{} 00:00:00' ".format(
        q案件, s案件, q案件)
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
    station_code = df_数据汇总['station_code']

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
    """ 引入原始表 """
    sql = "SELECT city,county,city_code,county_code  FROM t_code_area where province_code = '330000'"
    t_code_area = get_df_from_db(sql)
    # sql = "SELECT * from t_bas_source_company where area_province = '330000' and is_statictis =1  and is_deleted=0 "
    # t_bas_source_company=get_df_from_db(sql)
    # sql = "SELECT * FROM t_bas_source_company_equipment WHERE is_deleted = 0 OR is_deleted IS NULL"
    # t_bas_source_company_equipment=get_df_from_db(sql)
    站点总数 = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\月报\拉出表\重点货运源头445家明细0309.xlsx")
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
                                     '50-100%数', '100%以上数', '20-50%占比', '50-100%占比', '100%以上占比', 'county_code'])
    货运源头监控数据.rename(columns={'货车数': '源头货车数'}, inplace=True)
    return 货运源头监控数据, 货运源头监控数据省市, 站点完好数


if __name__ == "__main__":
    df_数据汇总, df_报修点位统计 = data_station()
    print(df_数据汇总)
    附件7 = overrun_site_rate()
    W_案件统计 = case_statistic()
    print(W_案件统计)
    超限80数 = Compliance_rate()
    print(超限80数)
    W_案件统计当年 = case_statistic当年()
    货运源头监控数据区县, 货运源头监控数据省市, 站点完好数 = Key_freight_sources()
    sql = "SELECT city_code, city, county_code, county FROM t_code_area where province_code = 330000 "
    t_code_area = get_df_from_db(sql)
    U_all = pd.merge(t_code_area, 附件7, on=['city', 'county'], how='left')
    U_all = pd.merge(U_all, W_案件统计, on=['county_code'], how='left')
    U_all = pd.merge(U_all, W_案件统计当年, on=['county_code'], how='left')
    U_all = pd.merge(U_all, 超限80数, on=['city_code', 'city', 'county_code', 'county'], how='left')
    U_all = pd.merge(U_all, 货运源头监控数据区县, on=['county_code'], how='left')
    U_all地市 = U_all.groupby(['city_code', 'city']).sum()
    print(U_all地市)
    with pd.ExcelWriter(r'C:\Users\liu.wenjie\Desktop\导出数据\test0625.xlsx') as writer1:
        U_all.to_excel(writer1, sheet_name='区县', index=False)
        U_all地市.to_excel(writer1, sheet_name='地市')
        货运源头监控数据省市.to_excel(writer1, sheet_name='源头地市级', index=False)
        站点完好数.to_excel(writer1, sheet_name='源头站点数据明细', index=False)
        df_报修点位统计.to_excel(writer1, sheet_name='报修', index=False)
        df_数据汇总.to_excel(writer1, sheet_name='在线')
