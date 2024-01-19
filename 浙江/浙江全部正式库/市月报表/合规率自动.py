import pymysql
import pandas as pd
import time
from decimal import *
import numpy as np
import datetime as dt
import io

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
start = time.time()



df_数据汇总 = pd.read_excel(r"G:\智诚\日常给出数据汇总\月通报\10月统计\拉出表\月数据汇总表.xls")
df_数据汇总.columns = df_数据汇总.iloc[2]
df_数据汇总 = df_数据汇总.iloc[3:].reset_index(drop=False)

sql = "SELECT * FROM t_sys_station"
t_sys_station = get_df_from_db(sql)
U_汇总_站点表 = pd.merge(df_数据汇总, t_sys_station, left_on='站点名称', right_on='station_name', how='left')
station_code = U_汇总_站点表['station_code']

# sql = "SELECT * FROM t_sys_station where station_status=0 and station_type =31"
# t_sys_station = get_df_from_db(sql)
#
# station_code = t_sys_station['station_code']

sql = "SELECT * FROM t_code_area  "
t_code_area = get_df_from_db(sql)

sql = "SELECT * FROM t_bas_over_data_31 where out_station_time >= '2022-01-01 00:00:00' and  out_station_time <'2022-11-01 00:00:00' and overrun_rate>80  AND allow IS NULL AND is_unusual = 0  "
t_bas_over_data_31 = get_df_from_db(sql)

sql = "SELECT * FROM t_bas_over_data_collection_31 where out_station_time >= '2022-01-01 00:00:00' and out_station_time <'2022-11-01 00:00:00'  and overrun_rate>80  "
t_bas_over_data_collection_31 = get_df_from_db(sql)
end = time.time()
time = end - start
print(time)


def DF2Excel(data_path, data_list, sheet_name_list):
    '''将多个dataframe 保存到同一个excel 的不同sheet 上
    参数：
    data_path：str
        需要保存的文件地址及文件名
    data_list：list
        需要保存到excel的dataframe
    sheet_name_list：list
        sheet name 每个sheet 的名称
    '''

    write = pd.ExcelWriter(data_path)
    for da, sh_name in zip(data_list, sheet_name_list):
        da.to_excel(write, sheet_name=sh_name, index=False)

    # 必须运行write.save()，不然不能输出到本地
    write.save()


I = ["杭州", "宁波", "温州", "嘉兴", "湖州", "绍兴", "金华", "衢州", "舟山", "台州", "丽水"]
data_list = []
sheet_name_list = []
# I=['浙江']
for cs in I:
    if cs == '浙江':

        U_过车_区域表 = pd.merge(t_bas_over_data_31, t_code_area, left_on='area_county', right_on='county_code', how='left')

        U_案件审核_区域表 = pd.merge(t_bas_over_data_collection_31, t_code_area, left_on='area_county', right_on='county_code',
                              how='left')

        U_案件审核_区域表 = U_案件审核_区域表[U_案件审核_区域表['out_station'].isin(station_code)]

        U_过车_站点表 = U_过车_区域表[U_过车_区域表.loc[:, 'out_station'].isin(station_code)]
        月超限80以上总数 = U_过车_站点表.groupby(['city_code', 'city']).count()
        print(月超限80以上总数)

        月超限80以上总数 = 月超限80以上总数['id_x']
        print(月超限80以上总数)

        U_过车_站点表 = U_过车_站点表[(U_过车_站点表['is_collect'] == 1)
        ]

        月超限80以上且满足处罚条件总数 = U_过车_站点表.groupby(['city_code', 'city']).count()

        月超限80以上且满足处罚条件总数 = 月超限80以上且满足处罚条件总数['id_x']

        snum = [3, 4, 5, 6, 9, 12, 13]

        U_过车_站点表 = U_案件审核_区域表[U_案件审核_区域表.loc[:, 'status_x'].isin(snum)]

        月超限80以上审核通过总数 = U_过车_站点表.groupby(['city_code', 'city']).count()

        月超限80以上审核通过总数 = 月超限80以上审核通过总数['id_x']

        超限80数 = pd.merge(月超限80以上总数, 月超限80以上且满足处罚条件总数, on='city', how='left')

        超限80数 = pd.merge(超限80数, 月超限80以上审核通过总数, on='city', how='left')

        超限80数.rename(columns={'id_x_x': '月超限80%以上总数', 'id_x_y': '月超限80%以上且满足处罚条件总数', 'id_x': '月超限80%以上审核通过总数'},
                     inplace=True)
        print(超限80数)
        超限80数.to_excel(r"G:\智诚\日常给出数据汇总\月通报\10月统计\结果表\月报\浙江省超限80%以上数据.xlsx")

    else:

            U_过车_区域表 = pd.merge(t_bas_over_data_31, t_code_area, left_on='area_county', right_on='county_code',
                                how='left')
            U_案件审核_区域表 = pd.merge(t_bas_over_data_collection_31, t_code_area, left_on='area_county',
                                  right_on='county_code',
                                  how='left')
            U_案件审核_区域表 = U_案件审核_区域表[U_案件审核_区域表['out_station'].isin(station_code)]
            U_过车_站点表 = U_过车_区域表[U_过车_区域表.loc[:, 'out_station'].isin(station_code)]

            U_案件审核_区域表 = U_案件审核_区域表[(U_案件审核_区域表.city == '{}'.format(cs))]
            U_过车_站点表 = U_过车_站点表[(U_过车_站点表.city == '{}'.format(cs))]

            月超限80以上总数 = U_过车_站点表.groupby(['county_code', 'county']).count()
            print(月超限80以上总数)

            月超限80以上总数 = 月超限80以上总数['id_x']
            print(月超限80以上总数)

            U_过车_站点表 = U_过车_站点表[(U_过车_站点表['is_collect'] == 1)
            ]

            月超限80以上且满足处罚条件总数 = U_过车_站点表.groupby(['county_code', 'county']).count()

            月超限80以上且满足处罚条件总数 = 月超限80以上且满足处罚条件总数['id_x']

            snum = [3, 4, 5, 6, 9, 12, 13]

            U_过车_站点表 = U_案件审核_区域表[U_案件审核_区域表.loc[:, 'status_x'].isin(snum)]

            月超限80以上审核通过总数 = U_过车_站点表.groupby(['county_code', 'county']).count()

            月超限80以上审核通过总数 = 月超限80以上审核通过总数['id_x']

            超限80数 = pd.merge(月超限80以上总数, 月超限80以上且满足处罚条件总数, on='county', how='left')

            超限80数 = pd.merge(超限80数, 月超限80以上审核通过总数, on='county', how='left')

            超限80数.rename(columns={'id_x_x': '月超限80%以上总数', 'id_x_y': '月超限80%以上且满足处罚条件总数', 'id_x': '月超限80%以上审核通过总数'},
                         inplace=True)
            超限80数= 超限80数.reset_index()
            print(超限80数)
            超限80数= 超限80数.fillna(0, inplace=False)
            data_list.append(超限80数)
            sheet_name_list.append(cs)



# ---------- 调用函数 ------------------

# 需要保存的文件地址及文件名
data_path = r'G:\智诚\日常给出数据汇总\月通报\10月统计\结果表\月报\10月市超限80%以上数据.xlsx'
# 调用函数
DF2Excel(data_path, data_list, sheet_name_list)