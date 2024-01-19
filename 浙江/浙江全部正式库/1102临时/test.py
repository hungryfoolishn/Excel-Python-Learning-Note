import pymysql
import pandas as pd
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


""" 引入原始表 """
q = input("请输入起始年月日(比如2022-04-01)：")
s = input("请输入截止年月日(比如2022-05-01)：")
cs = input("请输入省市名：")

start = time.time()

sql = "SELECT * FROM t_sys_station "
t_sys_station = get_df_from_db(sql)
sql = "SELECT * FROM t_code_area  "
t_code_area = get_df_from_db(sql)

sql = "SELECT * FROM t_bas_over_data_31 where is_unusual = 0 and out_station_time >= '{} 00:00:00' and  out_station_time <'{} 00:00:00' and allow is null   and area_province=330000 and total_weight >= 100 ".format(q,s)
t_bas_over_data_31 = get_df_from_db(sql)


U_过车_区域表 = pd.merge(t_bas_over_data_31, t_code_area, left_on='area_county', right_on='county_code', how='left')
U_过车_站点表 = pd.merge(U_过车_区域表, t_sys_station, left_on='out_station', right_on='station_code', how='left')
U_过车_站点表 = U_过车_站点表[(U_过车_站点表['station_status'] == 0)]

end = time.time()
time = end - start
print(time)


if cs == '浙江':
    U_过车_站点表 = U_过车_站点表[(U_过车_站点表['province'] == '浙江')
    ]
    U_过车_站点表 = pd.DataFrame(U_过车_站点表,
                            columns=["city", "county", "car_no", "out_station_time", "axis", "total_weight", "overrun",
                                     "overrun_rate", "site_name", "status_x", "is_collect", "is_unusual", "record_code",
                                     "photo1", "photo2", "photo3", "vedio"])
    U_过车_站点表 = U_过车_站点表.sort_values(by=['city', 'county', 'out_station_time']).reset_index(drop=True)
    U_过车_站点表 = U_过车_站点表.drop_duplicates(subset=['record_code'])
    百吨王车辆统计 = U_过车_站点表.groupby(['city','county'])['record_code'].count().reset_index(name='百吨王（辆次）')
    print(百吨王车辆统计)
    q = input("请输入储存路径(C:/Users/Administrator/Desktop/输出报表/省级月报)：")
    with pd.ExcelWriter('{}/浙江省百吨王明细数据.xlsx'.format(q))as writer1:
        百吨王车辆统计.to_excel(writer1, sheet_name='百吨王车辆数统计', index=True)
        U_过车_站点表.to_excel(writer1, sheet_name='Sheet1', index=True)

