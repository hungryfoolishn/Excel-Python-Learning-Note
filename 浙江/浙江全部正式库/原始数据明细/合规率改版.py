import pymysql
import pandas as pd
import  time
from decimal import *
import numpy as np
import datetime as dt
import io

def get_data_from_sql(sql, cs_list):
    cursor = db.cursor()
    #print('1')
    #print(cs_list)
    cursor.execute(sql, cs_list)
    #print('2')
    data = cursor.fetchall()
    #print('3')
    data = pd.DataFrame(list(data))
    if data.empty == False:
        data.columns = [i[0] for i in cursor.description]
        return data
    else:
        return data


def set_columns_date_frame(df, select_rows, tail_rows):
    # 得到一个包含列名的大的数据块
    ff = df.values
    # 数据块分为两块：不规则列名块和规则数据块
    # 第一步处理列名
    if tail_rows == 0:
        f1 = pd.DataFrame(ff[0:select_rows, :]).fillna(method='ffill', axis=0)
        # 第二步处理后列名和数据块打包
        f2 = pd.DataFrame(ff[select_rows:, :], columns=f1.values[-1])
    else:
        f1 = pd.DataFrame(ff[0:select_rows, :]).fillna(method='ffill', axis=0)
        # 第二步处理后列名和数据块打包
        f2 = pd.DataFrame(ff[select_rows:-tail_rows, :], columns=f1.values[-1])
    return f2


db = pymysql.connect(
    host='192.168.2.119',
    user='zjzhzcuser',
    passwd='F4dus0ee',
    port=3306,
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
''''''
dz=r"C:\Users\Administrator\Desktop\7月汇总表-已更改.xls"
cs = input("请输入省市名：")
city_tuple=tuple(set_columns_date_frame(pd.read_excel(dz),2,0)['站点名称'].dropna())
city_list=tuple(set_columns_date_frame(pd.read_excel(dz),2,0)['地市'].dropna().drop_duplicates())
print(city_tuple)

sql = "SELECT * FROM t_code_area  "
t_code_area=get_df_from_db(sql)


sql = "SELECT * FROM t_bas_over_data_31 where out_station_time >= '2022-07-01 00:00:00' and  out_station_time <'2022-08-01 00:00:00' and overrun_rate>80  AND allow IS NULL AND is_unusual = 0 and site_name in %s "
t_bas_over_data_31=get_data_from_sql(sql,[city_tuple])

sql = "SELECT * FROM t_bas_over_data_collection_31 where out_station_time >= '2022-07-01 00:00:00' and out_station_time <'2022-08-01 00:00:00'  and overrun_rate>80 and site_name in %s "
t_bas_over_data_collection_31=get_data_from_sql(sql,[city_tuple])
end = time.time()
time = end - start
print(time)

if cs=='浙江':

    U_过车_区域表 = pd.merge(t_bas_over_data_31, t_code_area, left_on='area_county', right_on='county_code', how='left')

    U_案件审核_区域表 = pd.merge(t_bas_over_data_collection_31, t_code_area, left_on='area_county', right_on='county_code',
                          how='left')

    # U_案件审核_区域表 = U_案件审核_区域表[U_案件审核_区域表['out_station'].isin(station_code)]
    #
    # U_过车_站点表 = U_过车_区域表[U_过车_区域表.loc[:, 'out_station'].isin(station_code)]
    U_过车_站点表 = U_过车_区域表
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
    超限80数.to_excel("C:/Users/Administrator/Desktop/输出报表/合规率/777浙江省月超限80%以上数据.xlsx")

else:
    U_过车_区域表 = pd.merge(t_bas_over_data_31, t_code_area, left_on='area_county', right_on='county_code', how='left')
    U_案件审核_区域表 = pd.merge(t_bas_over_data_collection_31, t_code_area, left_on='area_county', right_on='county_code',
                          how='left')

    U_过车_站点表 = U_过车_区域表
    print(city_list)
    for i in city_list:
        U_案件审核_区域表_1 = U_案件审核_区域表[(U_案件审核_区域表.city ==i )]
        U_过车_站点表_1 = U_过车_站点表[(U_过车_站点表.city ==i )]
        print('-------')
        print(U_过车_站点表.head())
        print('------')
        月超限80以上总数 = U_过车_站点表_1.groupby(['county_code', 'county']).count()
        print(月超限80以上总数)

        月超限80以上总数 = 月超限80以上总数['id_x']
        print(月超限80以上总数)

        U_过车_站点表_2 = U_过车_站点表_1[U_过车_站点表_1['is_collect'] == 1]

        月超限80以上且满足处罚条件总数_1 = U_过车_站点表_2.groupby(['county_code', 'county']).count()

        月超限80以上且满足处罚条件总数 = 月超限80以上且满足处罚条件总数_1['id_x']

        snum = [3, 4, 5, 6, 9, 12, 13]

        U_过车_站点表_2 = U_案件审核_区域表_1[U_案件审核_区域表_1.loc[:, 'status_x'].isin(snum)]

        月超限80以上审核通过总数 = U_过车_站点表_2.groupby(['county_code', 'county']).count()

        月超限80以上审核通过总数 = 月超限80以上审核通过总数['id_x']

        超限80数 = pd.merge(月超限80以上总数, 月超限80以上且满足处罚条件总数, on='county', how='left')

        超限80数 = pd.merge(超限80数, 月超限80以上审核通过总数, on='county', how='left')

        超限80数.rename(columns={'id_x_x': '月超限80%以上总数', 'id_x_y': '月超限80%以上且满足处罚条件总数', 'id_x': '月超限80%以上审核通过总数'},
                     inplace=True)
        print(超限80数)
        # q = input("请输入储存路径(C:/Users/Administrator/Desktop/输出报表/合规率)：")
        with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/合规率/截止7月{}市超限80%以上数据.xlsx'.format(i))as writer1:
            超限80数.to_excel(writer1, index=True)
        print(i)
        print('good')