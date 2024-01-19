import pymysql
import pandas as pd
import  time


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


yue = input("请输入月：")


df_数据汇总= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/月数据汇总表.xls")
df_数据汇总.columns = df_数据汇总.iloc[2]
df_数据汇总= df_数据汇总.iloc[3:].reset_index(drop=True)
df_接入数= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/接入数.xlsx")
df_报修点位统计= pd.read_excel("C:/Users/Administrator/Desktop/系统拉出表/省级月报/报修点位统计表.xls")

sql = "SELECT * FROM t_sys_station"
t_sys_station = get_df_from_db(sql)
U_汇总_站点表 = pd.merge(df_数据汇总, t_sys_station, left_on='站点名称', right_on='station_name', how='left')
station_code = U_汇总_站点表['station_code']
sql = "SELECT * FROM t_code_area  "
t_code_area = get_df_from_db(sql)

sql = "SELECT car_no,out_station_time,out_station,overrun_rate,area_county FROM t_bas_over_data_31 where is_unusual = 0 and area_province=330000 and out_station_time >= '2022-07-01 00:00:00' and  out_station_time <'2022-08-01 00:00:00' and allow is null  and is_unusual=0 and total_weight<=100 "
t_bas_over_data_31 = get_df_from_db(sql)

"""超限1000全省明细"""

U_过车_区域表 = pd.merge(t_bas_over_data_31, t_code_area, left_on='area_county', right_on='county_code', how='left')
U_过车_站点表 = pd.merge(U_过车_区域表, t_sys_station, left_on='out_station', right_on='station_code', how='left')
U_过车_站点表 = U_过车_站点表[U_过车_站点表.loc[:, 'out_station'].isin(station_code)]

U_过车_站点表0 = U_过车_站点表[U_过车_站点表['overrun_rate'] > 0]
货车数0 = U_过车_站点表0.groupby(['county'])['car_no'].count()
U_过车_站点表50 = U_过车_站点表[((U_过车_站点表['overrun_rate'] > 20) & (U_过车_站点表['overrun_rate'] <= 50))]
货车数50 = U_过车_站点表50.groupby(['county'])['car_no'].count()
U_过车_站点表100 = U_过车_站点表[((U_过车_站点表['overrun_rate'] > 50) & (U_过车_站点表['overrun_rate'] <= 100))]
货车数100 = U_过车_站点表100.groupby(['county'])['car_no'].count()
全部 = pd.merge(货车数0, 货车数50, left_on='county', right_on='county', how='left')
全部 = pd.merge(全部, 货车数100, left_on='county', right_on='county', how='left')
全部.rename(columns={'car_no_x': '超限数', 'car_no_y': '20-50%', 'car_no': '50-100%', }, inplace=True)

全部.to_excel("C:/Users/Administrator/Desktop/输出报表/2022年{}浙江省各区县超限率.xlsx".format(yue))
print(全部)




