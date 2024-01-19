import pymysql
import pandas as pd
import  time


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

'''输入变量'''


cs ='浙江'

df_数据汇总 = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\月报\12月\12月1.03\12月数据汇总表v4.0.xls")
df_数据汇总.columns = df_数据汇总.iloc[2]
df_数据汇总 = df_数据汇总.iloc[3:].reset_index(drop=False)
df_接入数 = pd.read_excel(r"C:\Users\liu.wenjie\Desktop\月报\12月\拉出表\接入数.xlsx")




"""浙江超限100%明细数据 以及地市货车数及超限100%数"""
if cs=='浙江' :
    q = input("请输入起始年月日(比如2022-04-01)：")
    s = input("请输入截止年月日(比如2022-05-01)：")
    """ 引入原始表 """
    start = time.time()
    print('请等待2分钟左右......')
    sql = "SELECT * FROM t_sys_station"
    t_sys_station = get_df_from_db(sql)
    U_汇总_站点表 = pd.merge(df_数据汇总, t_sys_station, left_on='站点名称', right_on='station_name', how='left')
    station_code = U_汇总_站点表['station_code']
    sql = "SELECT * FROM t_code_area  "
    t_code_area = get_df_from_db(sql)
    sql = "SELECT * FROM t_bas_over_data_31 where is_unusual = 0 and out_station_time >= '{} 00:00:00' and  out_station_time <'{} 00:00:00' and status!=15 and allow is null   and overrun_rate>=100 and total_weight<100".format(q, s)
    t_bas_over_data_31 = get_df_from_db(sql)
    end = time.time()
    time = end - start
    print(time)

    """超限1000全省明细"""

    U_过车_区域表 = pd.merge(t_bas_over_data_31, t_code_area, left_on='area_county', right_on='county_code', how='left')
    U_过车_站点表 = pd.merge(U_过车_区域表, t_sys_station, left_on='out_station', right_on='station_code', how='left')
    U_过车_站点表 = U_过车_站点表[U_过车_站点表.loc[:, 'out_station'].isin(station_code)]

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
        "330122": 1.2,
        "330183": 1.21,
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
        U_过车_站点表.loc[((U_过车_站点表['area_county_x'] == key) | (U_过车_站点表['area_city_x'] == key)) & (
                    U_过车_站点表['total_weight'] < 100), 'vehicle_brand'] = U_过车_站点表['limit_weight'].map(
            lambda x: float(x) * value).round(4)
    U_过车_站点表['limit_weight'] = U_过车_站点表['limit_weight'].astype('float')
    U_过车_站点表['total_weight'] = U_过车_站点表['total_weight'].astype('float')
    U_过车_站点表['vehicle_brand'] = U_过车_站点表['vehicle_brand'].astype('float')
    U_过车_站点表['超限率100%'] = U_过车_站点表.apply(lambda x: (x['total_weight'] - x['vehicle_brand']) * 100 / x['vehicle_brand'],
                                         axis=1).round(2)
    U_过车_站点表 = U_过车_站点表[(U_过车_站点表['超限率100%'] >= 100)
    ]
    U_过车_站点表 = pd.DataFrame(U_过车_站点表,
                            columns=["city", "county", "car_no", "out_station_time", "axis", "total_weight", "overrun",
                                     "overrun_rate", "site_name", "status_x", "is_collect", "is_unusual", "record_code",
                                     "photo1", "photo2", "photo3", "vedio",'area_county'])

    U_过车_站点表 = U_过车_站点表.sort_values(by=['area_county','out_station_time'], ascending=False).reset_index(drop=True)
    U_过车_站点表 = U_过车_站点表.drop_duplicates(subset=['record_code'])
    U_过车_站点表.loc[U_过车_站点表['status_x'] == 1, 'status_x'] = '待初审'
    U_过车_站点表.loc[U_过车_站点表['status_x'] == 2, 'status_x'] = '待审核'
    U_过车_站点表.loc[U_过车_站点表['status_x'] == 9, 'status_x'] = '判定不处理'
    U_过车_站点表.loc[U_过车_站点表['status_x'] == 15, 'status_x'] = '初审不通过'
    U_过车_站点表.loc[U_过车_站点表['is_collect'] == 1, 'is_collect'] = '满足'
    U_过车_站点表.loc[U_过车_站点表['is_collect'] == 0, 'is_collect'] = '不满足'

    """车牌处理"""
    U_过车_站点表['car_no'].fillna('无牌', inplace=True)
    U_过车_站点表['字节数'] = U_过车_站点表['car_no'].str.len()
    U_过车_站点表.loc[U_过车_站点表['字节数'] <= 5, 'car_no'] = '无牌'
    U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['is_collect'] == '满足')), 'photo1'] = ''
    U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['is_collect'] == '满足')), 'photo2'] = ''
    U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['is_collect'] == '满足')), 'photo3'] = ''
    U_过车_站点表.loc[~((U_过车_站点表['字节数'] <= 5) & (U_过车_站点表['is_collect'] == '满足')), 'vedio'] = ''

    """地市货车数及超限100%数"""
    货车数 = df_数据汇总.groupby(['地市'])['货车数'].sum()
    货车数 = 货车数.to_frame()
    货车数= 货车数.reset_index()
    超限100 = U_过车_站点表.groupby(['city'])['record_code'].count()
    超限100 = 超限100.to_frame()
    超限100 = 超限100.reset_index()
    附件2 = pd.merge(货车数, 超限100,left_on='地市',right_on='city', how='left', )
    附件2.rename(columns={'record_code': '超限100%数'}, inplace=True)
    附件2 = 附件2.fillna(0, inplace=False)
    附件2['超限100%数占货车数比例'] = 附件2.apply(lambda x: x['超限100%数']/x['货车数'], axis=1)
    附件2['排名（占比由高到低）'] = 附件2['超限100%数占货车数比例'] .rank(ascending=False, method='first')
    附件2['超限100%数占货车数比例'] = 附件2['超限100%数占货车数比例'].apply(lambda x: format(x, '.3%'))
    未识别到车牌数 = U_过车_站点表[U_过车_站点表['car_no'] == '无牌'].groupby(['city'])['car_no'].count().reset_index(name='未识别到车牌')
    满足证据条件数 = U_过车_站点表[((U_过车_站点表['car_no'] == '无牌') & (U_过车_站点表['is_collect'] == '满足'))].groupby(['city'])['car_no'].count().reset_index(name='满足证据条件')
    附件2 = pd.merge(df_接入数, 附件2, on='地市', how='left')
    附件2 = pd.merge(附件2, 未识别到车牌数, left_on='地市',right_on='city', how='left')
    附件2 = pd.merge(附件2, 满足证据条件数, left_on='地市',right_on='city', how='left')
    附件2['满足证据条件且故意遮挡车牌'] = 0
    附件2 = pd.DataFrame(附件2, columns=['地市', '货车数', '超限100%数','超限100%数占货车数比例','排名（占比由高到低）','未识别到车牌','满足证据条件','满足证据条件且故意遮挡车牌'])
    附件2 = 附件2.fillna(0, inplace=False)
    附件2 =附件2.drop(index=(附件2.loc[(附件2['地市'] == '义乌')].index))
    print(附件2)
    with pd.ExcelWriter('C:/Users/Administrator/Desktop/输出报表/省级月报/XX月地市超限100%数据.xlsx')as writer1:
        附件2.to_excel(writer1, sheet_name='地市货车数及超限100%数', index=True)
        U_过车_站点表.to_excel(writer1, sheet_name='超限100%数明细表', index=True)


